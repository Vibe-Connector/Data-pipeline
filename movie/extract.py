"""
Vibe-Link 영화 도메인 — Extract (수집)

TMDB API 클라이언트, 영화 목록 수집, 중복 제거, adult 필터링, 검증 A를 담당합니다.
"""

import time
from datetime import datetime, timezone

import requests

from common.config import TMDB_BASE, TMDB_API_KEY
from common.logging_config import get_logger

log = get_logger("movie.extract")


# =============================================================================
# TMDB API 클라이언트 (v3.1: 다국어 호출 지원)
# =============================================================================


class TMDBClient:
    """TMDB API 클라이언트"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()

    def _get(self, path: str, params: dict | None = None) -> dict:
        params = params or {}
        params["api_key"] = self.api_key
        url = f"{TMDB_BASE}{path}"
        resp = self.session.get(url, params=params, timeout=10)
        resp.raise_for_status()
        time.sleep(0.05)
        return resp.json()

    def check_health(self) -> bool:
        """TMDB API 상태 확인"""
        try:
            data = self._get("/configuration")
            return "images" in data
        except Exception as e:
            log.error(f"TMDB 헬스체크 실패: {e}")
            return False

    def fetch_movie_list(self, source: dict, max_pages: int | None = None) -> list[dict]:
        """소스 설정에 따라 영화 목록 수집"""
        pages = max_pages or source["pages"]
        movies = []
        for page in range(1, pages + 1):
            params = {**source["params"], "page": page}
            try:
                data = self._get(source["url"], params)
                results = data.get("results", [])
                movies.extend(results)
                log.info(f"  [{source['name']}] page {page}/{pages} → {len(results)}편")
                if page >= data.get("total_pages", 1):
                    break
            except Exception as e:
                log.warning(f"  [{source['name']}] page {page} 실패: {e}")
        return movies

    def fetch_movie_detail(self, tmdb_id: int) -> dict | None:
        """v3.1: 메인 상세정보 (ko-KR, append_to_response 포함)"""
        try:
            data = self._get(
                f"/movie/{tmdb_id}",
                {
                    "language": "ko-KR",
                    "append_to_response": "keywords,credits,release_dates",
                },
            )
            data["_fetched_at"] = datetime.now(tz=timezone.utc).isoformat()
            return data
        except Exception as e:
            log.warning(f"  상세정보 실패 tmdb_id={tmdb_id}: {e}")
            return None

    def fetch_localized_info(self, tmdb_id: int, language: str) -> dict:
        """v3.1: 지정 언어로 제목/overview 조회

        Args:
            tmdb_id: TMDB 영화 ID
            language: TMDB 언어 코드 (예: "en-US", "zh-CN")

        Returns:
            {"title": str, "overview": str} — 실패 시 빈 문자열
        """
        try:
            data = self._get(f"/movie/{tmdb_id}", {"language": language})
            return {
                "title": data.get("title", "").strip(),
                "overview": data.get("overview", "").strip(),
            }
        except Exception:
            return {"title": "", "overview": ""}


# =============================================================================
# 필터링 & 검증
# =============================================================================


def deduplicate_movies(all_movies: list[dict]) -> list[dict]:
    """tmdb_id 기준 중복 제거"""
    seen = set()
    unique = []
    for m in all_movies:
        mid = m.get("id")
        if mid and mid not in seen:
            seen.add(mid)
            unique.append(m)
    return unique


def filter_adult_movies(movies: list[dict]) -> list[dict]:
    """v3.0: adult=true 방어적 필터링"""
    filtered = [m for m in movies if not m.get("adult", False)]
    removed = len(movies) - len(filtered)
    if removed > 0:
        log.info(f"  adult=true {removed}건 필터링")
    return filtered


def validate_raw(movies: list[dict]) -> list[dict]:
    """검증 A: 필수 필드 NULL 체크, vote_count >= 10"""
    valid = []
    for m in movies:
        if not m.get("id") or not m.get("title"):
            continue
        if (m.get("vote_count") or 0) < 10:
            continue
        valid.append(m)
    removed = len(movies) - len(valid)
    if removed > 0:
        log.info(f"  검증A 탈락: {removed}건")
    return valid