#!/usr/bin/env python3
"""
Vibe-Link 영화 수집 파이프라인 — CLI 진입점

사용법:
  # Data-pipeline/ 디렉토리에서 실행
  python -m scripts.run_movie_pipeline          # 전체 수집
  python -m scripts.run_movie_pipeline --test   # 테스트 (소스당 1페이지)
"""

import argparse
import sys
import os

# Data-pipeline/ 루트를 Python path에 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.logging_config import setup_logging
from movie.pipeline import run_pipeline


def main():
    parser = argparse.ArgumentParser(
        description="Vibe-Link 영화 수집 파이프라인 v3.1"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="테스트 모드 (소스당 1페이지만 수집)",
    )
    args = parser.parse_args()

    setup_logging()
    run_pipeline(test_mode=args.test)


if __name__ == "__main__":
    main()