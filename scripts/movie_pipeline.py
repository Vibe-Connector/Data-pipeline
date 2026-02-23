"""
Vibe-Link 영화 도메인 1단계 수집 파이프라인
파이프라인 설계서 v3.1 + DB 스키마 v3.2 기준

v3.1 변경사항:
  - TMDB 다국어 3회 호출 (ko/en/zh) → fetch_localized_info 범용 메서드
  - Wikipedia ko/en 독립 수집 (첫 성공 시 중단하지 않음)
  - overview 한국어 최우선 저장 (Wiki ko → TMDB ko → Wiki en → TMDB en)
  - item_translations ko/en/zh 3개 언어 저장
  - title_en을 TMDB en-US title로 수정 (original_title → movie_details에만)
  - 빈 JSON 필드 NULL 처리
  - Transform 반환 구조 개편 (translations dict)
  - Load 3개 언어 루프

실행 전 필요:
  pip install requests psycopg2-binary beautifulsoup4 python-dotenv

  # languages 테이블에 zh 추가 (1회)
  # INSERT INTO languages (language_code, language_name, is_default, is_active)
  # VALUES ('zh', '中文', FALSE, TRUE);

사용법:
  python movie_pipeline.py          # 전체 수집
  python movie_pipeline.py --test   # 테스트 (소스당 1페이지)
"""

import os
import sys
import re
import json
import time
import logging
import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import quote

import requests
import psycopg2
from psycopg2.extras import Json
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# 환경 설정
# ─────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("movie_pipeline")

TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "")
TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMG_BASE = "https://image.tmdb.org/t/p/w500"

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": int(os.environ.get("DB_PORT", 5432)),
    "dbname": os.environ.get("DB_NAME", "vibelink"),
    "user": os.environ.get("DB_USER", "postgres"),
    "password": os.environ.get("DB_PASSWORD", ""),
}

# v3.0: 수집 소스 정의 (모든 소스에 include_adult=false)
COLLECTION_SOURCES = [
    {
        "name": "popular",
        "url": "/movie/popular",
        "params": {"language": "ko-KR", "region": "KR", "include_adult": False},
        "pages": 5,
    },
    {
        "name": "top_rated",
        "url": "/movie/top_rated",
        "params": {"language": "ko-KR", "include_adult": False},
        "pages": 5,
    },
    {
        "name": "korean_movies",
        "url": "/discover/movie",
        "params": {
            "language": "ko-KR",
            "with_original_language": "ko",
            "sort_by": "popularity.desc",
            "include_adult": False,
        },
        "pages": 5,
    },
    {
        "name": "genre_drama",
        "url": "/discover/movie",
        "params": {
            "language": "ko-KR",
            "with_genres": "18",
            "sort_by": "vote_average.desc",
            "vote_count.gte": 100,
            "include_adult": False,
        },
        "pages": 2,
    },
    {
        "name": "genre_romance",
        "url": "/discover/movie",
        "params": {
            "language": "ko-KR",
            "with_genres": "10749",
            "sort_by": "vote_average.desc",
            "vote_count.gte": 100,
            "include_adult": False,
        },
        "pages": 2,
    },
    {
        "name": "genre_comedy",
        "url": "/discover/movie",
        "params": {
            "language": "ko-KR",
            "with_genres": "35",
            "sort_by": "vote_average.desc",
            "vote_count.gte": 100,
            "include_adult": False,
        },
        "pages": 2,
    },
    {
        "name": "genre_animation",
        "url": "/discover/movie",
        "params": {
            "language": "ko-KR",
            "with_genres": "16",
            "sort_by": "vote_average.desc",
            "vote_count.gte": 100,
            "include_adult": False,
        },
        "pages": 2,
    },
    {
        "name": "trending",
        "url": "/trending/movie/week",
        "params": {"language": "ko-KR"},
        "pages": 1,
    },
]

# v3.0: 등급 매핑
AGE_RATING_MAP = {
    "All": "all", "전체관람가": "all",
    "12": "12+", "12세이상관람가": "12+",
    "15": "15+", "15세이상관람가": "15+",
    "19": "18+", "청소년관람불가": "18+",
    "G": "all", "PG": "all", "PG-13": "12+",
    "R": "15+", "NC-17": "18+",
    "U": "all", "12A": "12+", "18": "18+",
}
COUNTRY_PRIORITY = ["KR", "US", "GB"]


# =============================================================================
# Step 6: TMDB API 수집 (Extract)
# =============================================================================


class TMDBClient:
    """TMDB API 클라이언트 (v3.1: 다국어 호출 지원)"""

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

    # ── v3.1 신규: 범용 다국어 정보 조회 ──
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
# 검증 & 필터링
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


# =============================================================================
# Step 8: Wikipedia 줄거리 추출
# =============================================================================


@dataclass
class MoviePlotResult:
    query: str
    wiki_title: Optional[str]
    wiki_url: Optional[str]
    plot: Optional[str]
    search_strategy: Optional[str]
    error: Optional[str] = None


class WikiMoviePlotExtractor:
    """Wikipedia에서 영화 줄거리를 추출 (4단계 검색 전략)"""

    PLOT_KW_KO = {"줄거리", "내용", "시놉시스", "스토리", "플롯"}
    PLOT_KW_EN = {"Plot", "Synopsis", "Story", "Plot summary"}

    def __init__(self, lang: str = "ko", delay: float = 0.3):
        self.lang = lang
        self.delay = delay
        self.base_url = f"https://{lang}.wikipedia.org/w/api.php"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "VibeLinkBot/1.0 (vibe-link-project; contact@vibe-link.com)"
        })

    def _api(self, **params) -> dict:
        params["format"] = "json"
        time.sleep(self.delay)
        try:
            resp = self.session.get(self.base_url, params=params, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except Exception:
            return {}

    def extract(self, title: str, release_year: Optional[int] = None) -> MoviePlotResult:
        page_title, strategy = self._find_movie_page(title, release_year)
        if not page_title:
            return MoviePlotResult(title, None, None, None, None, "페이지 없음")

        plot = self._extract_plot(page_title)
        wiki_url = (
            f"https://{self.lang}.wikipedia.org/wiki/"
            f"{quote(page_title.replace(' ', '_'))}"
        )
        return MoviePlotResult(
            query=title,
            wiki_title=page_title,
            wiki_url=wiki_url,
            plot=plot,
            search_strategy=strategy,
            error=None if plot else "줄거리 섹션 없음",
        )

    # ── 4단계 검색 전략 ──
    def _find_movie_page(self, title: str, release_year: Optional[int] = None):
        # 전략 1: "{제목} (영화)" 또는 "{제목} ({연도}년 영화)"
        candidates = []
        if self.lang == "ko":
            if release_year:
                candidates.append(f"{title} ({release_year}년 영화)")
            candidates.append(f"{title} (영화)")
        else:
            if release_year:
                candidates.append(f"{title} ({release_year} film)")
            candidates.append(f"{title} (film)")

        for c in candidates:
            if self._page_exists(c) and self._is_movie_page(c):
                return c, f"전략1: '{c}'"

        # 전략 2: 제목 그대로
        if self._page_exists(title):
            if self._is_disambiguation(title):
                found = self._find_in_disambiguation(title, release_year)
                if found:
                    return found, f"전략2-동음이의: '{title}'→'{found}'"
            elif self._is_movie_page(title):
                return title, f"전략2: '{title}'"

        # 전략 3: "영화 {제목}" 검색
        search_prefix = "영화" if self.lang == "ko" else "film"
        for rt in self._search(f"{search_prefix} {title}"):
            if self._is_relevant_movie(rt, title):
                return rt, f"전략3: '{search_prefix} {title}'→'{rt}'"

        # 전략 4: 일반 검색
        for rt in self._search(title):
            if self._is_relevant_movie(rt, title):
                return rt, f"전략4: '{title}'→'{rt}'"

        return None, None

    def _page_exists(self, title: str) -> bool:
        data = self._api(action="query", titles=title, prop="info")
        pages = data.get("query", {}).get("pages", {})
        return not any(p.get("missing") is not None for p in pages.values())

    def _is_movie_page(self, title: str) -> bool:
        data = self._api(action="query", titles=title, prop="categories", cllimit=50)
        for page in data.get("query", {}).get("pages", {}).values():
            cats = [c.get("title", "") for c in page.get("categories", [])]
            kw = ["영화", "film", "movie", "Film", "Movie"]
            return any(k in cat for cat in cats for k in kw)
        return False

    def _is_disambiguation(self, title: str) -> bool:
        data = self._api(action="query", titles=title, prop="categories", cllimit=50)
        for page in data.get("query", {}).get("pages", {}).values():
            cats = " ".join(c.get("title", "") for c in page.get("categories", []))
            return "동음이의" in cats or "disambiguation" in cats.lower()
        return False

    def _find_in_disambiguation(self, title: str, release_year: Optional[int]) -> Optional[str]:
        data = self._api(action="parse", page=title, prop="links")
        links = data.get("parse", {}).get("links", [])
        for link in links:
            lt = link.get("*", "")
            if "영화" in lt or "film" in lt.lower():
                if release_year and str(release_year) in lt:
                    return lt
                return lt
        return None

    def _search(self, query: str) -> list[str]:
        data = self._api(action="query", list="search", srsearch=query, srlimit=5)
        return [r["title"] for r in data.get("query", {}).get("search", [])]

    def _is_relevant_movie(self, page_title: str, original_title: str) -> bool:
        if not self._is_movie_page(page_title):
            return False
        intro = self._get_page_intro(page_title)
        kw = ["영화", "film", "movie", "감독", "director"]
        return any(k in intro.lower() for k in kw)

    def _get_page_intro(self, title: str) -> str:
        data = self._api(
            action="query", titles=title,
            prop="extracts", exintro=True, explaintext=True, exchars=500,
        )
        for page in data.get("query", {}).get("pages", {}).values():
            return page.get("extract", "")[:500]
        return ""

    def _extract_plot(self, title: str) -> Optional[str]:
        """줄거리 섹션 추출 — sections API 방식"""
        data = self._api(action="parse", page=title, prop="sections")
        if "parse" not in data:
            return None

        plot_kw = self.PLOT_KW_KO if self.lang == "ko" else self.PLOT_KW_EN
        section_index = None
        for s in data["parse"]["sections"]:
            if s["line"].strip() in plot_kw:
                section_index = s["index"]
                break

        if section_index is None:
            return None

        data = self._api(
            action="parse", page=title, prop="text",
            section=section_index, disabletoc=True,
        )
        html = data.get("parse", {}).get("text", {}).get("*", "")
        if not html:
            return None

        soup = BeautifulSoup(html, "html.parser")
        for tag in soup.find_all("sup"):
            tag.decompose()
        for tag in soup.select(".mw-editsection"):
            tag.decompose()
        for tag in soup.find_all("table"):
            tag.decompose()

        paragraphs = []
        for p in soup.find_all("p"):
            text = p.get_text(strip=True)
            if not text:
                continue
            text = re.sub(r"\[\w+\]", "", text).strip()
            if text:
                paragraphs.append(text)

        return "\n\n".join(paragraphs) if paragraphs else None


# Wikipedia 줄거리 인스턴스
wiki_ko = WikiMoviePlotExtractor(lang="ko", delay=0.3)
wiki_en = WikiMoviePlotExtractor(lang="en", delay=0.3)


def extract_multilingual_plots(
    title_ko: str, title_en: str, release_year: Optional[int],
) -> dict:
    """v3.1: ko/en Wikipedia 줄거리를 독립적으로 수집 (첫 성공 시 중단하지 않음)

    Returns:
        {
            "wiki_ko": {"plot": str|None, "url": str|None, "strategy": str|None},
            "wiki_en": {"plot": str|None, "url": str|None, "strategy": str|None},
        }
    """
    result = {
        "wiki_ko": {"plot": None, "url": None, "strategy": None},
        "wiki_en": {"plot": None, "url": None, "strategy": None},
    }

    # 한국어 Wikipedia — 항상 시도
    if title_ko:
        r = wiki_ko.extract(title_ko, release_year)
        if r.plot and len(r.plot) >= 50:
            result["wiki_ko"] = {
                "plot": r.plot,
                "url": r.wiki_url,
                "strategy": r.search_strategy,
            }

    # 영어 Wikipedia — 항상 시도 (ko 성공 여부와 무관)
    if title_en:
        r = wiki_en.extract(title_en, release_year)
        if r.plot and len(r.plot) >= 50:
            result["wiki_en"] = {
                "plot": r.plot,
                "url": r.wiki_url,
                "strategy": r.search_strategy,
            }

    return result


def select_best_overview(
    wiki_plots: dict,
    tmdb_ko_overview: str,
    tmdb_en_overview: str,
) -> dict:
    """v3.1: overview에 한국어 줄거리를 최우선 저장

    우선순위: Wikipedia ko → TMDB ko → Wikipedia en → TMDB en

    Returns:
        {"overview": str|None, "overview_source": str|None,
         "wiki_url": str|None, "wiki_strategy": str|None}
    """
    # 1순위: Wikipedia 한국어
    wiki_ko_plot = wiki_plots["wiki_ko"]["plot"]
    if wiki_ko_plot:
        return {
            "overview": wiki_ko_plot,
            "overview_source": "wikipedia_ko",
            "wiki_url": wiki_plots["wiki_ko"]["url"],
            "wiki_strategy": wiki_plots["wiki_ko"]["strategy"],
        }

    # 2순위: TMDB 한국어
    if tmdb_ko_overview and len(tmdb_ko_overview.strip()) >= 10:
        return {
            "overview": tmdb_ko_overview.strip(),
            "overview_source": "tmdb_ko",
            "wiki_url": None,
            "wiki_strategy": None,
        }

    # 3순위: Wikipedia 영어
    wiki_en_plot = wiki_plots["wiki_en"]["plot"]
    if wiki_en_plot:
        return {
            "overview": wiki_en_plot,
            "overview_source": "wikipedia_en",
            "wiki_url": wiki_plots["wiki_en"]["url"],
            "wiki_strategy": wiki_plots["wiki_en"]["strategy"],
        }

    # 4순위: TMDB 영어
    if tmdb_en_overview and len(tmdb_en_overview.strip()) >= 10:
        return {
            "overview": tmdb_en_overview.strip(),
            "overview_source": "tmdb_en",
            "wiki_url": None,
            "wiki_strategy": None,
        }

    return {"overview": None, "overview_source": None,
            "wiki_url": None, "wiki_strategy": None}


# =============================================================================
# Step 7: Transform — 데이터 정제 (v3.1)
# =============================================================================


def extract_structured_cast_info(credits: dict) -> dict:
    """감독 1명 + 출연진 5명 구조화"""
    director = None
    for crew in credits.get("crew", []):
        if crew.get("job") == "Director":
            director = {"name": crew.get("name", ""), "tmdb_person_id": crew.get("id")}
            break
    cast_list = [
        {
            "name": a.get("name", ""),
            "character": a.get("character", ""),
            "tmdb_person_id": a.get("id"),
            "order": a.get("order", 0),
        }
        for a in credits.get("cast", [])[:5]
    ]
    return {"director": director, "cast": cast_list}


def extract_release_dates(rd_response: dict) -> dict | None:
    """v3.1: release_dates 정제 — certification이 있는 항목만 보존, 빈 경우 None"""
    results = rd_response.get("results", [])
    cleaned = []
    for country in results:
        iso = country.get("iso_3166_1", "")
        dates = [
            rd for rd in country.get("release_dates", [])
            if rd.get("certification", "").strip()
        ]
        if dates:
            cleaned.append({"iso_3166_1": iso, "release_dates": dates})
    # v3.1: 빈 경우 NULL 저장
    return {"results": cleaned} if cleaned else None


def transform_movie(raw: dict, tmdb: TMDBClient) -> dict | None:
    """v3.1: TMDB raw → PostgreSQL 스키마 v3.2 매핑 (다국어 확장)"""

    if not raw.get("title") or not raw.get("id"):
        return None
    if raw.get("adult", False):
        return None

    tmdb_id = raw["id"]

    # ── v3.1: TMDB 다국어 정보 조회 (en-US, zh-CN) ──
    en_info = tmdb.fetch_localized_info(tmdb_id, "en-US")
    zh_info = tmdb.fetch_localized_info(tmdb_id, "zh-CN")

    # v3.1: 최소 1개 언어 이상의 제목이 있어야 수집
    tmdb_ko_title = raw.get("title", "").strip()
    tmdb_en_title = en_info["title"]
    tmdb_zh_title = zh_info["title"]

    if not any([tmdb_ko_title, tmdb_en_title, tmdb_zh_title]):
        log.warning(f"  제목 없음 — 스킵: tmdb_id={tmdb_id}")
        return None

    # 개봉연도 추출
    release_year = None
    if raw.get("release_date"):
        try:
            release_year = int(raw["release_date"][:4])
        except (ValueError, IndexError):
            pass

    # ── v3.1: TMDB overview 다국어 확보 ──
    tmdb_ko_overview = raw.get("overview", "").strip()
    tmdb_en_overview = en_info["overview"]
    tmdb_zh_overview = zh_info["overview"]

    # ── v3.1: Wikipedia ko/en 독립 수집 ──
    wiki_plots = extract_multilingual_plots(
        title_ko=tmdb_ko_title,
        title_en=en_info["title"] or raw.get("original_title", ""),
        release_year=release_year,
    )

    # ── v3.1: overview 한국어 최우선 선택 ──
    best = select_best_overview(wiki_plots, tmdb_ko_overview, tmdb_en_overview)

    overview = best["overview"]
    if not overview or len(overview) < 10:
        log.warning(f"  줄거리 없음 — 스킵: {raw.get('title')}")
        return None

    # cast_info / release_dates 정제
    cast_info = extract_structured_cast_info(raw.get("credits", {}))
    release_dates = extract_release_dates(raw.get("release_dates", {}))

    # ── v3.1: genres/keywords/production_countries 빈 값 NULL 처리 ──
    genres = [g["name"] for g in raw.get("genres", [])]
    keywords = [k["name"] for k in raw.get("keywords", {}).get("keywords", [])]
    prod_countries = [c["iso_3166_1"] for c in raw.get("production_countries", [])]

    # ── v3.1: item_translations 3개 언어 데이터 구성 ──
    # ko description: Wiki ko → TMDB ko overview fallback
    ko_desc = wiki_plots["wiki_ko"]["plot"] or (tmdb_ko_overview if len(tmdb_ko_overview) >= 10 else None)
    # en description: Wiki en → TMDB en overview fallback
    en_desc = wiki_plots["wiki_en"]["plot"] or (tmdb_en_overview if len(tmdb_en_overview) >= 10 else None)
    # zh description: TMDB zh overview only (Wikipedia 미사용)
    zh_desc = tmdb_zh_overview if len(tmdb_zh_overview) >= 10 else None

    return {
        # ── items 테이블 ──
        "item_key": f"movie_tmdb_{tmdb_id}",
        "category_key": "video",
        "brand": None,
        "image_url": f"{TMDB_IMG_BASE}{raw['poster_path']}" if raw.get("poster_path") else None,
        "external_link": f"https://www.themoviedb.org/movie/{tmdb_id}",
        "external_service": "TMDB",
        "is_active": True,

        # ── movie_details 테이블 ──
        "tmdb_id": tmdb_id,
        "original_title": raw.get("original_title", "").strip(),
        "overview": overview,
        "overview_source": best["overview_source"],
        "release_date": raw.get("release_date") or None,
        "runtime": raw.get("runtime"),
        "vote_average": round(float(raw.get("vote_average", 0)), 1),
        "vote_count": int(raw.get("vote_count", 0)),
        "popularity": round(float(raw.get("popularity", 0)), 3),
        "poster_path": raw.get("poster_path"),
        "genres": genres or None,              # v3.1: 빈 리스트 → None
        "keywords": keywords or None,          # v3.1: 빈 리스트 → None
        "production_countries": prod_countries or None,  # v3.1: 빈 리스트 → None
        "original_language": raw.get("original_language"),
        "cast_info": cast_info if (cast_info.get("director") or cast_info.get("cast")) else None,
        "release_dates": release_dates,        # v3.1: extract_release_dates가 None 반환 가능
        "content_type": "MOVIE",
        "tmdb_updated_at": raw.get("_fetched_at"),

        # ── v3.1: item_translations 3개 언어 ──
        "translations": {
            "ko": {
                "title": tmdb_ko_title or None,
                "description": ko_desc[:2000] if ko_desc else None,
            },
            "en": {
                "title": tmdb_en_title or None,  # v3.1 핵심: original_title → TMDB en-US title
                "description": en_desc[:2000] if en_desc else None,
            },
            "zh": {
                "title": tmdb_zh_title or None,
                "description": zh_desc[:2000] if zh_desc else None,
            },
        },
    }


# =============================================================================
# Step 9: PostgreSQL Upsert (Load) — v3.1
# =============================================================================


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def load_movie_to_postgres(conn, movie: dict) -> int | None:
    """v3.1: items → movie_details → item_translations(3언어) → neo4j_sync_status Upsert"""
    cur = conn.cursor()
    try:
        # 1. items Upsert
        cur.execute("""
            INSERT INTO items (category_id, item_key, brand, image_url,
                               external_link, external_service, is_active)
            VALUES (
                (SELECT category_id FROM item_categories WHERE category_key = %(category_key)s),
                %(item_key)s, %(brand)s, %(image_url)s,
                %(external_link)s, %(external_service)s, %(is_active)s
            )
            ON CONFLICT (item_key) DO UPDATE SET
                image_url = EXCLUDED.image_url,
                external_link = EXCLUDED.external_link,
                updated_at = CURRENT_TIMESTAMP
            RETURNING item_id
        """, movie)
        item_id = cur.fetchone()[0]

        # 2. movie_details Upsert (v3.1: 빈 JSON → NULL)
        cur.execute("""
            INSERT INTO movie_details (
                item_id, tmdb_id, original_title, overview,
                release_date, runtime, vote_average, vote_count, popularity,
                poster_path, genres, keywords,
                production_countries, original_language, cast_info,
                release_dates, content_type, tmdb_updated_at
            ) VALUES (
                %(item_id)s, %(tmdb_id)s, %(original_title)s, %(overview)s,
                %(release_date)s, %(runtime)s, %(vote_average)s, %(vote_count)s, %(popularity)s,
                %(poster_path)s, %(genres)s, %(keywords)s,
                %(production_countries)s, %(original_language)s, %(cast_info)s,
                %(release_dates)s, %(content_type)s, %(tmdb_updated_at)s
            )
            ON CONFLICT (tmdb_id) DO UPDATE SET
                overview = EXCLUDED.overview,
                vote_average = EXCLUDED.vote_average,
                vote_count = EXCLUDED.vote_count,
                popularity = EXCLUDED.popularity,
                release_dates = EXCLUDED.release_dates,
                cast_info = EXCLUDED.cast_info,
                tmdb_updated_at = EXCLUDED.tmdb_updated_at,
                updated_at = CURRENT_TIMESTAMP
        """, {
            "item_id": item_id,
            "tmdb_id": movie["tmdb_id"],
            "original_title": movie["original_title"][:255],
            "overview": movie["overview"],
            "release_date": movie["release_date"],
            "runtime": movie["runtime"],
            "vote_average": movie["vote_average"],
            "vote_count": movie["vote_count"],
            "popularity": movie["popularity"],
            "poster_path": movie["poster_path"],
            # v3.1: 빈 값은 None이므로 Json(None) → NULL
            "genres": Json(movie["genres"]) if movie["genres"] else None,
            "keywords": Json(movie["keywords"]) if movie["keywords"] else None,
            "production_countries": Json(movie["production_countries"]) if movie["production_countries"] else None,
            "original_language": movie["original_language"],
            "cast_info": Json(movie["cast_info"]) if movie["cast_info"] else None,
            "release_dates": Json(movie["release_dates"]) if movie["release_dates"] else None,
            "content_type": movie["content_type"],
            "tmdb_updated_at": movie["tmdb_updated_at"],
        })

        # 3. v3.1: item_translations — 3개 언어 루프
        for lang_code, data in movie["translations"].items():
            if not data["title"]:
                continue  # 해당 언어 제목 없으면 스킵

            cur.execute("""
                INSERT INTO item_translations (item_id, language_id, item_value, description)
                VALUES (
                    %(item_id)s,
                    (SELECT language_id FROM languages WHERE language_code = %(lang)s),
                    %(title)s, %(description)s
                )
                ON CONFLICT (item_id, language_id) DO UPDATE SET
                    item_value = EXCLUDED.item_value,
                    description = EXCLUDED.description
            """, {
                "item_id": item_id,
                "lang": lang_code,
                "title": data["title"][:255],
                "description": data["description"],
            })

        # 4. neo4j_sync_status → PENDING
        cur.execute("""
            INSERT INTO neo4j_sync_status (item_id, sync_status)
            VALUES (%(item_id)s, 'PENDING')
            ON CONFLICT (item_id) DO UPDATE SET
                sync_status = 'PENDING',
                updated_at = CURRENT_TIMESTAMP
        """, {"item_id": item_id})

        conn.commit()
        return item_id

    except Exception as e:
        conn.rollback()
        log.error(f"  DB 저장 실패 [{movie.get('item_key')}]: {e}")
        return None
    finally:
        cur.close()


# =============================================================================
# 파이프라인 실행 — v3.1
# =============================================================================


def run_pipeline(test_mode: bool = False):
    started_at = datetime.now()
    log.info("=" * 60)
    log.info(f"🎬 Vibe-Link 영화 수집 파이프라인 v3.1 시작 {'(TEST MODE)' if test_mode else ''}")
    log.info("=" * 60)

    # ── 0. 사전 확인 ──
    if not TMDB_API_KEY:
        log.error("❌ TMDB_API_KEY 환경변수를 설정해주세요.")
        sys.exit(1)

    tmdb = TMDBClient(TMDB_API_KEY)
    if not tmdb.check_health():
        log.error("❌ TMDB API 헬스체크 실패")
        sys.exit(1)
    log.info("✅ TMDB API 정상")

    # ── 1. Extract: 영화 목록 수집 ──
    log.info("\n📥 [Step 1] 영화 목록 수집...")
    max_pages = 1 if test_mode else None
    all_movies = []
    for src in COLLECTION_SOURCES:
        movies = tmdb.fetch_movie_list(src, max_pages=max_pages)
        all_movies.extend(movies)

    log.info(f"  수집 합계: {len(all_movies)}건")

    all_movies = deduplicate_movies(all_movies)
    log.info(f"  중복 제거 후: {len(all_movies)}건")

    all_movies = filter_adult_movies(all_movies)
    all_movies = validate_raw(all_movies)
    log.info(f"  검증A 통과: {len(all_movies)}건")

    # ── 2. Enrich: 상세정보 + 다국어 + Wikipedia + Transform ──
    log.info(f"\n🔄 [Step 2] 상세정보 보강 + 다국어 수집 + 줄거리 추출 + Transform ({len(all_movies)}편)...")
    transformed = []
    overview_stats = {"wikipedia_ko": 0, "wikipedia_en": 0, "tmdb_ko": 0, "tmdb_en": 0, "failed": 0}

    for i, m in enumerate(all_movies, 1):
        if i % 10 == 0:
            log.info(f"  진행: {i}/{len(all_movies)}")

        detail = tmdb.fetch_movie_detail(m["id"])
        if not detail:
            overview_stats["failed"] += 1
            continue

        result = transform_movie(detail, tmdb)
        if not result:
            overview_stats["failed"] += 1
            continue

        src = result.get("overview_source", "failed")
        overview_stats[src] = overview_stats.get(src, 0) + 1
        transformed.append(result)

    log.info(f"  Transform 완료: {len(transformed)}편")
    log.info(f"  줄거리 소스: {json.dumps(overview_stats, ensure_ascii=False)}")

    # ── 3. Load: PostgreSQL 저장 ──
    log.info(f"\n💾 [Step 3] PostgreSQL 저장 ({len(transformed)}편)...")
    conn = get_db_connection()
    saved = 0
    failed = 0
    for movie in transformed:
        item_id = load_movie_to_postgres(conn, movie)
        if item_id:
            saved += 1
        else:
            failed += 1
    conn.close()

    # ── 4. 결과 리포트 ──
    elapsed = (datetime.now() - started_at).total_seconds()
    log.info("\n" + "=" * 60)
    log.info("📊 수집 파이프라인 v3.1 완료 리포트")
    log.info("=" * 60)
    log.info(f"  ⏱️  소요시간: {elapsed:.0f}초 ({elapsed/60:.1f}분)")
    log.info(f"  📥 수집(중복제거 후): {len(all_movies)}편")
    log.info(f"  🔄 Transform 성공: {len(transformed)}편")
    log.info(f"  💾 DB 저장 성공: {saved}편")
    log.info(f"  ❌ DB 저장 실패: {failed}편")
    log.info(f"  📖 줄거리 소스:")
    for src, cnt in overview_stats.items():
        pct = (cnt / max(len(all_movies), 1)) * 100
        log.info(f"      {src}: {cnt}편 ({pct:.1f}%)")

    # ── 5. DB 저장 확인 쿼리 ──
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM movie_details")
    total = cur.fetchone()[0]
    cur.execute("""
        SELECT COUNT(*) FROM movie_details
        WHERE overview IS NOT NULL AND LENGTH(overview) > 50
    """)
    with_overview = cur.fetchone()[0]
    cur.execute("""
        SELECT COUNT(*) FROM movie_details
        WHERE release_dates IS NOT NULL
    """)
    with_ratings = cur.fetchone()[0]
    # v3.1: 다국어 번역 현황 확인
    cur.execute("""
        SELECT l.language_code, COUNT(*)
        FROM item_translations it
        JOIN languages l ON l.language_id = it.language_id
        JOIN items i ON i.item_id = it.item_id
        JOIN item_categories ic ON ic.category_id = i.category_id
        WHERE ic.category_key = 'video'
        GROUP BY l.language_code
        ORDER BY l.language_code
    """)
    translation_stats = dict(cur.fetchall())
    cur.execute("SELECT sync_status, COUNT(*) FROM neo4j_sync_status GROUP BY sync_status")
    sync_stats = dict(cur.fetchall())
    cur.close()
    conn.close()

    log.info(f"\n  📋 DB 현황:")
    log.info(f"      총 영화: {total}편")
    log.info(f"      줄거리 확보: {with_overview}편 ({with_overview/max(total,1)*100:.1f}%)")
    log.info(f"      등급 확보: {with_ratings}편 ({with_ratings/max(total,1)*100:.1f}%)")
    log.info(f"      번역 현황: {json.dumps(translation_stats, ensure_ascii=False)}")
    log.info(f"      동기화 상태: {sync_stats}")
    log.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Vibe-Link 영화 수집 파이프라인 v3.1")
    parser.add_argument("--test", action="store_true", help="테스트 모드 (소스당 1페이지만)")
    args = parser.parse_args()
    run_pipeline(test_mode=args.test)