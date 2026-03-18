"""
Vibe-Link 영화 수집 파이프라인 — Airflow DAG (Stage 1: COLLECT)

기존 movie/ 모듈을 Airflow Task로 래핑합니다.
Schedule: Daily 02:00 KST (17:00 UTC)

Tasks:
  health_check → extract_raw → transform_and_load → trigger_graph_sync
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "vibe-link",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="movie_collect_pipeline",
    default_args=default_args,
    description="영화 데이터 수집 파이프라인 (TMDB/Wikipedia → PostgreSQL)",
    schedule="0 17 * * *",  # Daily 02:00 KST = 17:00 UTC
    start_date=datetime(2026, 3, 13),
    catchup=False,
    tags=["movie", "collect", "stage1"],
)
def movie_collect_pipeline():

    @task
    def health_check():
        """TMDB API 헬스체크"""
        from common.config import TMDB_API_KEY
        from movie.extract import TMDBClient

        tmdb = TMDBClient(TMDB_API_KEY)
        if not tmdb.check_health():
            raise RuntimeError("TMDB API health check failed")
        return True

    @task
    def extract_raw(**context):
        """8개 소스에서 영화 목록 수집 + 중복 제거 + 검증A"""
        import json

        from common.config import TMDB_API_KEY
        from movie.constants import COLLECTION_SOURCES
        from movie.extract import (
            TMDBClient,
            deduplicate_movies,
            filter_adult_movies,
            validate_raw,
        )

        tmdb = TMDBClient(TMDB_API_KEY)
        all_movies = []
        for src in COLLECTION_SOURCES:
            movies = tmdb.fetch_movie_list(src)
            all_movies.extend(movies)

        all_movies = deduplicate_movies(all_movies)
        all_movies = filter_adult_movies(all_movies)
        all_movies = validate_raw(all_movies)

        # XCom 크기 제한 회피: 임시 파일에 저장
        filepath = "/tmp/movie_raw_data.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(all_movies, f, ensure_ascii=False)

        return {"count": len(all_movies), "filepath": filepath}

    @task(execution_timeout=timedelta(minutes=60))
    def transform_and_load(extract_result: dict):
        """상세정보 보강 + Transform + PostgreSQL 저장

        배치 단위(50편)로 처리하며 중간 진행 로그를 출력합니다.
        """
        import json
        import logging

        from common.config import TMDB_API_KEY
        from common.db import get_db_connection
        from movie.extract import TMDBClient
        from movie.load import load_movie_to_postgres
        from movie.transform import transform_movie

        log = logging.getLogger(__name__)

        with open(extract_result["filepath"], encoding="utf-8") as f:
            all_movies = json.load(f)

        tmdb = TMDBClient(TMDB_API_KEY)
        conn = get_db_connection()

        saved, failed = 0, 0
        total = len(all_movies)
        BATCH_SIZE = 50

        log.info(f"transform_and_load 시작: 총 {total}편, 배치 크기 {BATCH_SIZE}")

        for i, m in enumerate(all_movies):
            detail = tmdb.fetch_movie_detail(m["id"])
            if not detail:
                failed += 1
                continue
            result = transform_movie(detail, tmdb)
            if not result:
                failed += 1
                continue
            item_id = load_movie_to_postgres(conn, result)
            if item_id:
                saved += 1
            else:
                failed += 1

            # 배치 단위 진행 로그
            if (i + 1) % BATCH_SIZE == 0 or (i + 1) == total:
                log.info(f"  진행: {i + 1}/{total} ({(i + 1) / total * 100:.0f}%) "
                         f"— 저장 {saved}건, 실패 {failed}건")

        conn.close()
        log.info(f"transform_and_load 완료: 저장 {saved}건, 실패 {failed}건")
        return {"saved": saved, "failed": failed}

    trigger_graph_sync = TriggerDagRunOperator(
        task_id="trigger_graph_sync",
        trigger_dag_id="movie_graph_sync_pipeline",
        wait_for_completion=False,
    )

    # Task dependencies
    check = health_check()
    extracted = extract_raw()
    loaded = transform_and_load(extracted)
    check >> extracted >> loaded >> trigger_graph_sync


movie_collect_pipeline()
