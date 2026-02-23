"""
Vibe-Link 영화 도메인 — 파이프라인 오케스트레이션

run_pipeline()이 전체 ETL 흐름을 순차 실행합니다.
추후 Airflow DAG에서는 각 단계를 개별 Task로 호출할 수 있습니다.
"""

import json
from datetime import datetime

from common.config import TMDB_API_KEY
from common.db import get_db_connection
from common.logging_config import get_logger
from movie.constants import COLLECTION_SOURCES
from movie.extract import TMDBClient, deduplicate_movies, filter_adult_movies, validate_raw
from movie.transform import transform_movie
from movie.load import load_movie_to_postgres

log = get_logger("movie.pipeline")


def run_pipeline(test_mode: bool = False):
    """영화 수집 파이프라인 v3.1 — 전체 오케스트레이션

    Args:
        test_mode: True이면 소스당 1페이지만 수집
    """
    started_at = datetime.now()
    log.info("=" * 60)
    log.info(f"🎬 Vibe-Link 영화 수집 파이프라인 v3.1 시작 {'(TEST MODE)' if test_mode else ''}")
    log.info("=" * 60)

    # ── 0. 사전 확인 ──
    if not TMDB_API_KEY:
        log.error("❌ TMDB_API_KEY 환경변수를 설정해주세요.")
        return

    tmdb = TMDBClient(TMDB_API_KEY)
    if not tmdb.check_health():
        log.error("❌ TMDB API 헬스체크 실패")
        return
    log.info("✅ TMDB API 정상")

    # ── 1. Extract: 영화 목록 수집 ──
    log.info("\n📥 [Step 1] 영화 목록 수집...")
    max_pages = 1 if test_mode else None
    all_movies = []
    for src in COLLECTION_SOURCES:
        movies = tmdb.fetch_movie_list(src, max_pages=max_pages)
        all_movies.extend(movies)

    log.info(f"  수집 합계: {len(all_movies)}건")

    all_movies = deduplicate_movies(all_movies)
    log.info(f"  중복 제거 후: {len(all_movies)}건")

    all_movies = filter_adult_movies(all_movies)
    all_movies = validate_raw(all_movies)
    log.info(f"  검증A 통과: {len(all_movies)}건")

    # ── 2. Enrich + Transform ──
    log.info(f"\n🔄 [Step 2] 상세정보 보강 + 다국어 수집 + 줄거리 추출 + Transform ({len(all_movies)}편)...")
    transformed = []
    overview_stats = {"wikipedia_ko": 0, "wikipedia_en": 0, "tmdb_ko": 0, "tmdb_en": 0, "failed": 0}

    for i, m in enumerate(all_movies, 1):
        if i % 10 == 0:
            log.info(f"  진행: {i}/{len(all_movies)}")

        detail = tmdb.fetch_movie_detail(m["id"])
        if not detail:
            overview_stats["failed"] += 1
            continue

        result = transform_movie(detail, tmdb)
        if not result:
            overview_stats["failed"] += 1
            continue

        src = result.get("overview_source", "failed")
        overview_stats[src] = overview_stats.get(src, 0) + 1
        transformed.append(result)

    log.info(f"  Transform 완료: {len(transformed)}편")
    log.info(f"  줄거리 소스: {json.dumps(overview_stats, ensure_ascii=False)}")

    # ── 3. Load: PostgreSQL 저장 ──
    log.info(f"\n💾 [Step 3] PostgreSQL 저장 ({len(transformed)}편)...")
    conn = get_db_connection()
    saved = 0
    failed = 0
    for movie in transformed:
        item_id = load_movie_to_postgres(conn, movie)
        if item_id:
            saved += 1
        else:
            failed += 1
    conn.close()

    # ── 4. 결과 리포트 ──
    elapsed = (datetime.now() - started_at).total_seconds()
    log.info("\n" + "=" * 60)
    log.info("📊 수집 파이프라인 v3.1 완료 리포트")
    log.info("=" * 60)
    log.info(f"  ⏱️  소요시간: {elapsed:.0f}초 ({elapsed/60:.1f}분)")
    log.info(f"  📥 수집(중복제거 후): {len(all_movies)}편")
    log.info(f"  🔄 Transform 성공: {len(transformed)}편")
    log.info(f"  💾 DB 저장 성공: {saved}편")
    log.info(f"  ❌ DB 저장 실패: {failed}편")
    log.info(f"  📖 줄거리 소스:")
    for src, cnt in overview_stats.items():
        pct = (cnt / max(len(all_movies), 1)) * 100
        log.info(f"      {src}: {cnt}편 ({pct:.1f}%)")

    # ── 5. DB 현황 확인 ──
    _log_db_status()
    log.info("=" * 60)


def _log_db_status():
    """DB 저장 현황을 로그로 출력"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM movie_details")
        total = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) FROM movie_details
            WHERE overview IS NOT NULL AND LENGTH(overview) > 50
        """)
        with_overview = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) FROM movie_details
            WHERE release_dates IS NOT NULL
        """)
        with_ratings = cur.fetchone()[0]

        cur.execute("""
            SELECT l.language_code, COUNT(*)
            FROM item_translations it
            JOIN languages l ON l.language_id = it.language_id
            JOIN items i ON i.item_id = it.item_id
            JOIN item_categories ic ON ic.category_id = i.category_id
            WHERE ic.category_key = 'video'
            GROUP BY l.language_code
            ORDER BY l.language_code
        """)
        translation_stats = dict(cur.fetchall())

        cur.execute("SELECT sync_status, COUNT(*) FROM neo4j_sync_status GROUP BY sync_status")
        sync_stats = dict(cur.fetchall())

        cur.close()
        conn.close()

        log.info(f"\n  📋 DB 현황:")
        log.info(f"      총 영화: {total}편")
        log.info(f"      줄거리 확보: {with_overview}편 ({with_overview/max(total,1)*100:.1f}%)")
        log.info(f"      등급 확보: {with_ratings}편 ({with_ratings/max(total,1)*100:.1f}%)")
        log.info(f"      번역 현황: {json.dumps(translation_stats, ensure_ascii=False)}")
        log.info(f"      동기화 상태: {sync_stats}")
    except Exception as e:
        log.warning(f"  DB 현황 조회 실패: {e}")