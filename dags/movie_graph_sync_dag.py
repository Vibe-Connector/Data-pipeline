"""
Vibe-Link 영화 Graph Sync 파이프라인 — Airflow DAG (Stage 2: GRAPH_PROCESS)

PostgreSQL PENDING 아이템 → AIServer → Neo4j 동기화
Schedule: None (Stage 1 완료 후 트리거 또는 수동 실행)

Tasks:
  check_aiserver → fetch_and_sync → evaluate_fitness → report
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task

default_args = {
    "owner": "vibe-link",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


@dag(
    dag_id="movie_graph_sync_pipeline",
    default_args=default_args,
    description="영화 Neo4j 동기화 파이프라인 (PostgreSQL → AIServer → Neo4j)",
    schedule=None,  # Stage 1 완료 후 트리거 또는 수동
    start_date=datetime(2026, 3, 13),
    catchup=False,
    tags=["movie", "graph_sync", "stage2"],
    params={"with_fitness": False},  # 기본 스킵, 수동 트리거 시 True로 변경 가능
)
def movie_graph_sync_pipeline():

    @task
    def check_aiserver():
        """AIServer + Neo4j 접속 확인"""
        from movie.graph_sync import check_aiserver_health

        if not check_aiserver_health():
            raise RuntimeError("AIServer health check failed")
        return True

    @task
    def fetch_and_sync():
        """PENDING 아이템 조회 → AIServer /ingest/items 호출 → sync_status 업데이트"""
        from common.db import get_db_connection
        from movie.graph_sync import (
            fetch_pending_items,
            sync_to_neo4j,
            update_sync_status,
        )

        conn = get_db_connection()
        items = fetch_pending_items(conn, batch_size=50)

        if not items:
            conn.close()
            return {"synced": 0, "failed": 0, "message": "No pending items"}

        result = sync_to_neo4j(items)
        update_sync_status(
            conn,
            result["synced_item_ids"],
            result["failed_item_ids"],
            result.get("error"),
        )
        conn.close()

        return {
            "synced": len(result["synced_item_ids"]),
            "failed": len(result["failed_item_ids"]),
            "nodes_created": result.get("nodes_created", 0),
            "relationships_created": result.get("relationships_created", 0),
        }

    @task
    def evaluate_fitness(sync_result: dict, **context):
        """(조건부) 신규 동기화 아이템에 대해 LLM FITS_* 가중치 평가

        기본 스킵. DAG params에서 with_fitness=True로 설정 시 실행.
        (LLM 비용 + 수분 소요)
        """
        with_fitness = context["params"].get("with_fitness", False)

        if not with_fitness:
            return {"skipped": True, "reason": "with_fitness=False (default)"}

        if sync_result["synced"] == 0:
            return {"skipped": True, "reason": "No items synced"}

        from movie.graph_sync import trigger_evaluate_fitness

        return trigger_evaluate_fitness()

    @task
    def report(sync_result: dict, fitness_result: dict):
        """파이프라인 결과 로깅"""
        from common.logging_config import get_logger

        log = get_logger("dag.graph_sync.report")
        log.info(f"Graph Sync 완료: "
                 f"동기화 {sync_result['synced']}건, "
                 f"실패 {sync_result['failed']}건, "
                 f"노드 {sync_result.get('nodes_created', 0)}개, "
                 f"관계 {sync_result.get('relationships_created', 0)}개")
        if fitness_result.get("skipped"):
            log.info(f"FITS 평가 생략: {fitness_result.get('reason', '')}")
        else:
            log.info(f"FITS 평가: {'성공' if fitness_result.get('success') else '실패'}")

    # Task dependencies
    check = check_aiserver()
    synced = fetch_and_sync()
    fitness = evaluate_fitness(synced)
    report_task = report(synced, fitness)
    check >> synced >> fitness >> report_task


movie_graph_sync_pipeline()
