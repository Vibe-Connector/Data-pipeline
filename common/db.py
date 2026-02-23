"""
Vibe-Link 데이터 파이프라인 — PostgreSQL 연결 헬퍼

모든 도메인 파이프라인에서 공유하는 DB 연결 함수를 제공합니다.
"""

import psycopg2
from common.config import DB_CONFIG


def get_db_connection():
    """PostgreSQL 연결 객체를 반환합니다."""
    return psycopg2.connect(**DB_CONFIG)