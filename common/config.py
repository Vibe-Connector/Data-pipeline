"""
Vibe-Link 데이터 파이프라인 — 공통 환경 설정

모든 도메인 파이프라인에서 공유하는 환경변수, DB 설정, API 키를 관리합니다.
.env 파일 또는 시스템 환경변수에서 값을 로딩합니다.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────
# PostgreSQL
# ─────────────────────────────────────────────
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": int(os.environ.get("DB_PORT", 5432)),
    "dbname": os.environ.get("DB_NAME", "vibelink"),
    "user": os.environ.get("DB_USER", "postgres"),
    "password": os.environ.get("DB_PASSWORD", ""),
}

# ─────────────────────────────────────────────
# TMDB API
# ─────────────────────────────────────────────
TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "")
TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMG_BASE = "https://image.tmdb.org/t/p/w500"