"""
Vibe-Link 영화 수집 파이프라인 — Airflow DAG (Stage 1: COLLECT)

기존 movie/ 모듈을 Airflow Task로 래핑합니다.
Schedule: Daily 02:00 KST (17:00 UTC)

Tasks:
  health_check → extract_raw → prepare_batches
    → transform_and_load_batch.expand() → aggregate_results → trigger_graph_sync

Dynamic Task Mapping: transform_and_load를 50편 단위 배치로 분할하여
각 배치가 독립 task instance로 실행됩니다.
(Airflow scheduler zombie detection 5분 제한 회피)
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "vibe-link",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

BATCH_SIZE = 50


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
    def extract_raw():
        """8개 소스에서 영화 목록 수집 + 중복 제거 + 검증"""
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

    @task
    def prepare_batches(extract_result: dict):
        """영화 목록을 BATCH_SIZE 단위로 분할하여 배치 파일 생성"""
        import json

        with open(extract_result["filepath"], encoding="utf-8") as f:
            all_movies = json.load(f)

        batches = []
        for idx in range(0, len(all_movies), BATCH_SIZE):
            chunk = all_movies[idx : idx + BATCH_SIZE]
            batch_filepath = f"/tmp/movie_batch_{idx // BATCH_SIZE}.json"
            with open(batch_filepath, "w", encoding="utf-8") as f:
                json.dump(chunk, f, ensure_ascii=False)
            batches.append({
                "batch_index": idx // BATCH_SIZE,
                "filepath": batch_filepath,
                "count": len(chunk),
                "total": len(all_movies),
            })

        return batches

    @task(execution_timeout=timedelta(minutes=10))
    def transform_and_load_batch(batch: dict):
        """배치 단위 상세정보 보강 + Transform + PostgreSQL 저장

        Dynamic Task Mapping으로 배치별 독립 실행됩니다.
        """
        import json
        import logging

        from common.config import TMDB_API_KEY
        from common.db import get_db_connection
        from movie.extract import TMDBClient
        from movie.load import load_movie_to_postgres
        from movie.transform import transform_movie

        log = logging.getLogger(__name__)

        with open(batch["filepath"], encoding="utf-8") as f:
            movies = json.load(f)

        batch_idx = batch["batch_index"]
        count = batch["count"]
        total = batch["total"]
        log.info(f"배치 {batch_idx} 시작: {count}편 (전체 {total}편 중)")

        tmdb = TMDBClient(TMDB_API_KEY)
        conn = get_db_connection()

        saved, failed = 0, 0
        for i, m in enumerate(movies):
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

            if (i + 1) % 10 == 0 or (i + 1) == count:
                log.info(f"  배치 {batch_idx} 진행: {i + 1}/{count} "
                         f"— 저장 {saved}건, 실패 {failed}건")

        conn.close()
        log.info(f"배치 {batch_idx} 완료: 저장 {saved}건, 실패 {failed}건")
        return {"batch_index": batch_idx, "saved": saved, "failed": failed}

    @task
    def aggregate_results(batch_results: list):
        """모든 배치 결과 집계"""
        import logging

        log = logging.getLogger(__name__)

        total_saved = sum(r["saved"] for r in batch_results)
        total_failed = sum(r["failed"] for r in batch_results)
        log.info(f"전체 완료: {len(batch_results)}개 배치, "
                 f"저장 {total_saved}건, 실패 {total_failed}건")
        return {"saved": total_saved, "failed": total_failed,
                "batches": len(batch_results)}

    trigger_graph_sync = TriggerDagRunOperator(
        task_id="trigger_graph_sync",
        trigger_dag_id="movie_graph_sync_pipeline",
        wait_for_completion=False,
    )

    # Task dependencies (Dynamic Task Mapping)
    check = health_check()
    extracted = extract_raw()
    batches = prepare_batches(extracted)
    batch_results = transform_and_load_batch.expand(batch=batches)
    aggregated = aggregate_results(batch_results)
    check >> extracted >> batches >> batch_results >> aggregated >> trigger_graph_sync


movie_collect_pipeline()
