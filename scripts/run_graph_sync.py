#!/usr/bin/env python3
"""
Vibe-Link 영화 Graph Sync 파이프라인 — CLI 진입점

PostgreSQL PENDING 아이템 → AIServer → Neo4j 동기화

사용법:
  # Data-pipeline/ 디렉토리에서 실행
  python -m scripts.run_graph_sync                # Neo4j 동기화 (fitness 생략)
  python -m scripts.run_graph_sync --with-fitness  # Neo4j 동기화 + LLM FITS 가중치 평가
"""

import argparse
import sys
import os

# Data-pipeline/ 루트를 Python path에 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.logging_config import setup_logging
from movie.graph_sync import run_graph_sync


def main():
    parser = argparse.ArgumentParser(
        description="Vibe-Link 영화 Graph Sync 파이프라인 (Stage 2)"
    )
    parser.add_argument(
        "--with-fitness",
        action="store_true",
        help="evaluate-fitness 포함 (LLM FITS_* 가중치 생성, OpenAI 비용 발생)",
    )
    args = parser.parse_args()

    setup_logging()
    run_graph_sync(skip_fitness=not args.with_fitness)


if __name__ == "__main__":
    main()
