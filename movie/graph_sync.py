"""
Vibe-Link 영화 도메인 — Stage 2 Graph Sync (PostgreSQL → Neo4j)

PostgreSQL의 PENDING 아이템을 조회하여 AIServer를 통해 Neo4j에 동기화합니다.
run_graph_sync()이 전체 흐름을 순차 실행합니다.
"""

import json
from datetime import datetime

import requests

from common.config import AISERVER_BASE_URL
from common.db import get_db_connection
from common.logging_config import get_logger

log = get_logger("movie.graph_sync")


# ─────────────────────────────────────────────
# 헬스체크
# ─────────────────────────────────────────────

def check_aiserver_health() -> bool:
    """AIServer GET /api/v1/health 확인"""
    url = f"{AISERVER_BASE_URL}/api/v1/health"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get("success"):
            neo4j_status = data.get("data", {}).get("neo4j", "unknown")
            log.info(f"  AIServer 정상 (Neo4j: {neo4j_status})")
            return True
        log.error(f"  AIServer 응답 비정상: {data}")
        return False
    except requests.exceptions.RequestException as e:
        log.error(f"  AIServer 접속 실패: {e}")
        return False


# ─────────────────────────────────────────────
# PENDING 아이템 조회 + PROCESSING 전환
# ─────────────────────────────────────────────

def fetch_pending_items(conn, batch_size: int = 50) -> list[dict]:
    """PENDING 상태 아이템 조회 + PROCESSING으로 원자적 전환

    FOR UPDATE SKIP LOCKED로 동시 실행 시 충돌 방지.
    items + movie_details + item_translations(ko) JOIN 조회.
    """
    cur = conn.cursor()
    try:
        cur.execute("""
            WITH pending AS (
                SELECT ns.item_id
                FROM neo4j_sync_status ns
                JOIN movie_details md ON md.item_id = ns.item_id
                WHERE ns.sync_status = 'PENDING'
                ORDER BY ns.created_at
                LIMIT %(batch_size)s
                FOR UPDATE OF ns SKIP LOCKED
            ),
            updated AS (
                UPDATE neo4j_sync_status
                SET sync_status = 'PROCESSING', updated_at = CURRENT_TIMESTAMP
                WHERE item_id IN (SELECT item_id FROM pending)
                RETURNING item_id
            )
            SELECT
                i.item_id, i.item_key, i.brand, i.image_url,
                i.external_link, i.external_service,
                md.tmdb_id, md.genres, md.keywords, md.vote_average,
                md.runtime, md.overview,
                it.item_value AS name_ko
            FROM updated u
            JOIN items i ON i.item_id = u.item_id
            JOIN movie_details md ON md.item_id = i.item_id
            LEFT JOIN item_translations it ON it.item_id = i.item_id
                AND it.language_id = (SELECT language_id FROM languages WHERE language_code = 'ko')
        """, {"batch_size": batch_size})

        rows = cur.fetchall()
        conn.commit()

        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        conn.rollback()
        log.error(f"  PENDING 아이템 조회 실패: {e}")
        return []
    finally:
        cur.close()


# ─────────────────────────────────────────────
# PostgreSQL → AIServer 페이로드 변환
# ─────────────────────────────────────────────

def build_ingest_payload(items: list[dict]) -> dict:
    """PostgreSQL 데이터 → AIServer IngestItemsRequest 형식 변환

    AIServer ItemData 스키마:
        item_id, item_key, category_key, name, image_url, details(dict)
    """
    payload_items = []
    for item in items:
        # JSON 컬럼은 psycopg2가 자동으로 Python 객체로 변환
        genres = item["genres"] if item["genres"] else []
        keywords = item["keywords"] if item["keywords"] else []

        payload_items.append({
            "item_id": item["item_id"],
            "item_key": item["item_key"],
            "category_key": "movie",
            "name": item["name_ko"] or item["item_key"],
            "image_url": item["image_url"] or "",
            "details": {
                "tmdb_id": item["tmdb_id"],
                "genres": genres,
                "keywords": keywords,
                "vote_average": float(item["vote_average"]) if item["vote_average"] else None,
                "runtime": item["runtime"],
                "overview": item["overview"] or "",
                "brand": item["brand"],
                "external_link": item["external_link"],
                "external_service": item["external_service"],
            },
        })
    return {"items": payload_items}


# ─────────────────────────────────────────────
# AIServer 호출: Neo4j 동기화
# ─────────────────────────────────────────────

def sync_to_neo4j(items: list[dict]) -> dict:
    """AIServer POST /graph/ingest/items 호출

    Returns:
        {
            "success": bool,
            "synced_item_ids": list[int],
            "failed_item_ids": list[int],
            "nodes_created": int,
            "relationships_created": int,
            "error": str | None,
        }
    """
    payload = build_ingest_payload(items)
    url = f"{AISERVER_BASE_URL}/api/v1/graph/ingest/items"
    all_ids = [item["item_id"] for item in items]

    try:
        resp = requests.post(url, json=payload, timeout=120)
        resp.raise_for_status()
        result = resp.json()

        if result.get("success"):
            data = result.get("data", {})
            log.info(f"  Neo4j 동기화 성공: nodes={data.get('nodes_created', 0)}, "
                     f"relationships={data.get('relationships_created', 0)}")
            return {
                "success": True,
                "synced_item_ids": all_ids,
                "failed_item_ids": [],
                "nodes_created": data.get("nodes_created", 0),
                "relationships_created": data.get("relationships_created", 0),
                "error": None,
            }
        else:
            error_msg = result.get("message", "AIServer returned success=false")
            log.error(f"  AIServer 응답 실패: {error_msg}")
            return {
                "success": False,
                "synced_item_ids": [],
                "failed_item_ids": all_ids,
                "nodes_created": 0,
                "relationships_created": 0,
                "error": error_msg,
            }
    except requests.exceptions.RequestException as e:
        log.error(f"  AIServer 호출 실패: {e}")
        return {
            "success": False,
            "synced_item_ids": [],
            "failed_item_ids": all_ids,
            "nodes_created": 0,
            "relationships_created": 0,
            "error": str(e),
        }


# ─────────────────────────────────────────────
# neo4j_sync_status 업데이트
# ─────────────────────────────────────────────

def update_sync_status(conn, synced_ids: list[int], failed_ids: list[int],
                       error_msg: str | None = None):
    """동기화 결과에 따라 neo4j_sync_status 업데이트"""
    cur = conn.cursor()
    try:
        if synced_ids:
            cur.execute("""
                UPDATE neo4j_sync_status
                SET sync_status = 'SYNCED',
                    last_synced_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE item_id = ANY(%(ids)s)
            """, {"ids": synced_ids})
            log.info(f"  SYNCED 업데이트: {len(synced_ids)}건")

        if failed_ids:
            cur.execute("""
                UPDATE neo4j_sync_status
                SET sync_status = 'FAILED',
                    retry_count = retry_count + 1,
                    error_message = %(error_msg)s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE item_id = ANY(%(ids)s)
            """, {"ids": failed_ids, "error_msg": error_msg or "AIServer sync failed"})
            log.info(f"  FAILED 업데이트: {len(failed_ids)}건")

        conn.commit()
    except Exception as e:
        conn.rollback()
        log.error(f"  동기화 상태 업데이트 실패: {e}")
    finally:
        cur.close()


# ─────────────────────────────────────────────
# AIServer 호출: LLM 가중치 평가 (선택적)
# ─────────────────────────────────────────────

def trigger_evaluate_fitness() -> dict:
    """AIServer POST /graph/ingest/evaluate-fitness 호출

    LLM(GPT-4o-mini)이 각 아이템과 75개 옵션 간 FITS_* 가중치를 평가합니다.
    시간이 오래 걸리고 OpenAI API 비용이 발생합니다.
    """
    url = f"{AISERVER_BASE_URL}/api/v1/graph/ingest/evaluate-fitness"

    try:
        resp = requests.post(url, timeout=600)
        resp.raise_for_status()
        result = resp.json()

        if result.get("success"):
            data = result.get("data", {})
            log.info(f"  FITS 평가 완료: {data.get('message', '')}")
            return {"success": True, "data": data}
        else:
            error_msg = result.get("message", "evaluate-fitness failed")
            log.error(f"  FITS 평가 실패: {error_msg}")
            return {"success": False, "error": error_msg}
    except requests.exceptions.RequestException as e:
        log.error(f"  FITS 평가 호출 실패: {e}")
        return {"success": False, "error": str(e)}


# ─────────────────────────────────────────────
# 파이프라인 오케스트레이션
# ─────────────────────────────────────────────

def run_graph_sync(skip_fitness: bool = True) -> dict:
    """Stage 2 Graph Sync 파이프라인 — 전체 오케스트레이션

    1. AIServer 헬스체크
    2. PENDING 아이템 조회 + PROCESSING 전환
    3. AIServer /graph/ingest/items 호출
    4. neo4j_sync_status 업데이트 (SYNCED / FAILED)
    5. (선택) evaluate-fitness 호출
    6. 결과 리포트

    Args:
        skip_fitness: True이면 evaluate-fitness 생략 (기본값)
    """
    started_at = datetime.now()
    log.info("=" * 60)
    log.info("Stage 2: Graph Sync Pipeline 시작")
    log.info("=" * 60)

    # ── 1. AIServer 헬스체크 ──
    log.info("\n[Step 1] AIServer 헬스체크...")
    if not check_aiserver_health():
        log.error("AIServer 헬스체크 실패 — 파이프라인 중단")
        return {"success": False, "error": "AIServer unreachable"}

    # ── 2. PENDING 아이템 조회 ──
    log.info("\n[Step 2] PENDING 아이템 조회...")
    conn = get_db_connection()
    items = fetch_pending_items(conn, batch_size=50)

    if not items:
        log.info("  PENDING 아이템 없음 — 완료")
        conn.close()
        return {"success": True, "synced": 0, "failed": 0, "message": "No pending items"}

    log.info(f"  PENDING → PROCESSING 전환: {len(items)}건")

    # ── 3. AIServer 호출 ──
    log.info(f"\n[Step 3] Neo4j 동기화 ({len(items)}건)...")
    result = sync_to_neo4j(items)

    # ── 4. 상태 업데이트 ──
    log.info("\n[Step 4] sync_status 업데이트...")
    update_sync_status(conn, result["synced_item_ids"], result["failed_item_ids"],
                       result.get("error"))

    # ── 5. evaluate-fitness (선택적) ──
    fitness_result = None
    if not skip_fitness and result["synced_item_ids"]:
        log.info("\n[Step 5] FITS 가중치 평가 (LLM)...")
        fitness_result = trigger_evaluate_fitness()
    elif skip_fitness:
        log.info("\n[Step 5] FITS 평가 생략 (--skip-fitness)")
    else:
        log.info("\n[Step 5] FITS 평가 생략 (동기화된 아이템 없음)")

    conn.close()

    # ── 6. 결과 리포트 ──
    elapsed = (datetime.now() - started_at).total_seconds()
    synced_count = len(result["synced_item_ids"])
    failed_count = len(result["failed_item_ids"])

    log.info("\n" + "=" * 60)
    log.info("Stage 2: Graph Sync Pipeline 완료 리포트")
    log.info("=" * 60)
    log.info(f"  소요시간: {elapsed:.0f}초 ({elapsed/60:.1f}분)")
    log.info(f"  동기화 성공: {synced_count}건")
    log.info(f"  동기화 실패: {failed_count}건")
    log.info(f"  Neo4j 노드 생성: {result.get('nodes_created', 0)}개")
    log.info(f"  Neo4j 관계 생성: {result.get('relationships_created', 0)}개")
    if fitness_result:
        log.info(f"  FITS 평가: {'성공' if fitness_result.get('success') else '실패'}")
    if result.get("error"):
        log.error(f"  에러: {result['error']}")

    # ── DB 현황 확인 ──
    _log_sync_status()
    log.info("=" * 60)

    return {
        "success": result["success"],
        "synced": synced_count,
        "failed": failed_count,
        "nodes_created": result.get("nodes_created", 0),
        "relationships_created": result.get("relationships_created", 0),
        "fitness": fitness_result,
        "elapsed_seconds": elapsed,
    }


def _log_sync_status():
    """neo4j_sync_status 현황 로그 출력"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT sync_status, COUNT(*) FROM neo4j_sync_status GROUP BY sync_status")
        stats = dict(cur.fetchall())
        cur.close()
        conn.close()
        log.info(f"\n  동기화 현황: {stats}")
    except Exception as e:
        log.warning(f"  동기화 현황 조회 실패: {e}")
