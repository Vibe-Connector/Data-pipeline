"""
Vibe-Link 데이터 파이프라인 — 로깅 설정

모든 도메인 파이프라인에서 공유하는 로깅 포맷과 레벨을 설정합니다.
"""

import logging


def setup_logging(level: int = logging.INFO) -> None:
    """전역 로깅 포맷을 설정합니다. 파이프라인 시작 시 1회 호출."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def get_logger(name: str) -> logging.Logger:
    """도메인별 로거를 반환합니다.

    사용 예:
        log = get_logger("movie_pipeline")
        log.info("수집 시작")
    """
    return logging.getLogger(name)