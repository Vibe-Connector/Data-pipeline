"""
Vibe-Link 영화 도메인 1단계 수집 파이프라인
파이프라인 설계서 v3.0 + DB 스키마 v3.2 기준

실행 전 필요:
  pip install requests psycopg2-binary beautifulsoup4 python-dotenv

사용법:
  # .env에 TMDB_API_KEY 등 설정 후
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
from datetime import datetime
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

# v3.0: 등급 매핑 (KR → US → GB 우선순위)
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
        time.sleep(0.05)  # ~40 req/s 준수
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
        """상세정보 + keywords + credits + reviews + release_dates (1회 호출)"""
        try:
            data = self._get(
                f"/movie/{tmdb_id}",
                {
                    "language": "ko-KR",
                    "append_to_response": "keywords,credits,reviews,release_dates",
                },
            )
            data["_fetched_at"] = datetime.now(tz=__import__('datetime').timezone.utc).isoformat()
            return data
        except Exception as e:
            log.warning(f"  상세정보 실패 tmdb_id={tmdb_id}: {e}")
            return None

    def fetch_english_overview(self, tmdb_id: int) -> str:
        """영어 줄거리 fallback"""
        try:
            data = self._get(f"/movie/{tmdb_id}", {"language": "en-US"})
            return data.get("overview", "").strip()
        except Exception:
            return ""


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
# Step 8: Wikipedia 줄거리 추출 (v3.0 신규)
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
    """Wikipedia에서 영화 줄거리를 추출 (4단계 검색 전략)

    전략 1: "{제목} (영화)" 또는 "{제목} ({연도}년 영화)" 직접 접근
    전략 2: 제목 그대로 접근 후 영화인지 확인 (동음이의어 처리 포함)
    전략 3: "영화 {제목}" 키워드 검색
    전략 4: 일반 검색 후 카테고리 기반 필터링
    """

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

    # ── 메인 추출 ──
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
        if release_year:
            candidates.append(f"{title} ({release_year}년 영화)")
        candidates.append(f"{title} (영화)")
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
        for rt in self._search(f"영화 {title}"):
            if self._is_relevant_movie(rt, title):
                return rt, f"전략3: '영화 {title}'→'{rt}'"

        # 전략 4: 일반 검색
        for rt in self._search(title):
            if self._is_relevant_movie(rt, title):
                return rt, f"전략4: '{title}'→'{rt}'"

        return None, None

    # ── 헬퍼 메서드들 ──
    def _page_exists(self, title: str) -> bool:
        data = self._api(action="query", titles=title, prop="info")
        pages = data.get("query", {}).get("pages", {})
        return not any(pid == "-1" for pid in pages)

    def _is_movie_page(self, title: str) -> bool:
        data = self._api(action="query", titles=title, prop="categories", cllimit=50)
        kw = {"영화", "film", "movie", "애니메이션 영화"}
        for page in data.get("query", {}).get("pages", {}).values():
            for cat in page.get("categories", []):
                if any(k in cat.get("title", "").lower() for k in kw):
                    return True
        intro = self._get_intro(title)
        return bool(intro and "영화" in intro)

    def _is_disambiguation(self, title: str) -> bool:
        data = self._api(action="query", titles=title, prop="categories", cllimit=50)
        for page in data.get("query", {}).get("pages", {}).values():
            for cat in page.get("categories", []):
                ct = cat.get("title", "")
                if "동음이의" in ct or "disambiguation" in ct.lower():
                    return True
        return False

    def _is_relevant_movie(self, result_title: str, query: str) -> bool:
        """검색 결과가 우리가 찾는 영화인지 (속편 필터 포함)"""
        if query not in result_title:
            return False
        suffix = result_title.replace(query, "").strip()
        if re.match(r"^\d+$", suffix):
            return False
        return self._is_movie_page(result_title)

    def _find_in_disambiguation(self, title: str, release_year: Optional[int]) -> Optional[str]:
        data = self._api(action="parse", page=title, prop="links")
        if "error" in data:
            return None
        links = data.get("parse", {}).get("links", [])
        candidates = [
            l.get("*", "") for l in links
            if title in l.get("*", "") and "영화" in l.get("*", "")
        ]
        if not candidates:
            return None
        if release_year:
            for c in candidates:
                if str(release_year) in c:
                    return c
        for c in candidates:
            if c == f"{title} (영화)":
                return c
        return candidates[0]

    def _search(self, query: str, limit: int = 5) -> list[str]:
        data = self._api(
            action="query", list="search",
            srsearch=query, srlimit=limit, srnamespace=0,
        )
        return [r["title"] for r in data.get("query", {}).get("search", [])]

    def _get_intro(self, title: str) -> str:
        data = self._api(
            action="query", titles=title,
            prop="extracts", exintro=True, explaintext=True, exchars=500,
        )
        for page in data.get("query", {}).get("pages", {}).values():
            return page.get("extract", "")[:500]
        return ""

    def _extract_plot(self, title: str) -> Optional[str]:
        """줄거리 섹션 추출 — sections API 방식 (가장 안정적)"""
        # 1단계: 섹션 목록에서 줄거리 index 찾기
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

        # 2단계: 해당 섹션 HTML만 가져오기
        data = self._api(
            action="parse", page=title, prop="text",
            section=section_index, disabletoc=True,
        )
        html = data.get("parse", {}).get("text", {}).get("*", "")
        if not html:
            return None

        # 3단계: HTML → 텍스트
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


# Wikipedia 줄거리 인스턴스 (ko + en)
wiki_ko = WikiMoviePlotExtractor(lang="ko", delay=0.3)
wiki_en = WikiMoviePlotExtractor(lang="en", delay=0.3)


def extract_full_plot(
    title_ko: str, title_en: str, release_year: Optional[int],
    tmdb_overview_ko: str, tmdb_overview_en: str,
) -> dict:
    """줄거리 추출 — 4단계 fallback

    1순위: 한국어 위키피디아 줄거리 (전문)
    2순위: 영어 위키피디아 줄거리 (전문)
    3순위: TMDB 한국어 overview (요약)
    4순위: TMDB 영어 overview (요약)
    """
    # 1순위: 한국어 위키
    r = wiki_ko.extract(title_ko, release_year)
    if r.plot and len(r.plot) >= 50:
        return {
            "overview": r.plot, "overview_source": "wikipedia_ko",
            "wiki_url": r.wiki_url, "wiki_strategy": r.search_strategy,
        }

    # 2순위: 영어 위키
    if title_en:
        r = wiki_en.extract(title_en, release_year)
        if r.plot and len(r.plot) >= 50:
            return {
                "overview": r.plot, "overview_source": "wikipedia_en",
                "wiki_url": r.wiki_url, "wiki_strategy": r.search_strategy,
            }

    # 3순위: TMDB 한국어
    if tmdb_overview_ko and len(tmdb_overview_ko.strip()) >= 10:
        return {
            "overview": tmdb_overview_ko.strip(), "overview_source": "tmdb_ko",
            "wiki_url": None, "wiki_strategy": None,
        }

    # 4순위: TMDB 영어
    if tmdb_overview_en and len(tmdb_overview_en.strip()) >= 10:
        return {
            "overview": tmdb_overview_en.strip(), "overview_source": "tmdb_en",
            "wiki_url": None, "wiki_strategy": None,
        }

    return {"overview": None, "overview_source": None, "wiki_url": None, "wiki_strategy": None}


# =============================================================================
# Step 7: Transform — 데이터 정제
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


def extract_release_dates(rd_response: dict) -> dict:
    """v3.0: release_dates 정제 — certification이 있는 항목만 보존"""
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
    return {"results": cleaned}


def transform_movie(raw: dict, tmdb: TMDBClient) -> dict | None:
    """TMDB raw → PostgreSQL 스키마 v3.2 매핑"""

    if not raw.get("title") or not raw.get("id"):
        return None
    if raw.get("adult", False):
        return None

    # 개봉연도 추출
    release_year = None
    if raw.get("release_date"):
        try:
            release_year = int(raw["release_date"][:4])
        except (ValueError, IndexError):
            pass

    # 줄거리 fallback
    tmdb_overview_ko = raw.get("overview", "").strip()
    tmdb_overview_en = ""
    if len(tmdb_overview_ko) < 10:
        tmdb_overview_en = tmdb.fetch_english_overview(raw["id"])

    plot = extract_full_plot(
        title_ko=raw.get("title", ""),
        title_en=raw.get("original_title", ""),
        release_year=release_year,
        tmdb_overview_ko=tmdb_overview_ko,
        tmdb_overview_en=tmdb_overview_en,
    )

    overview = plot["overview"]
    if not overview or len(overview) < 10:
        log.warning(f"  줄거리 없음 — 스킵: {raw.get('title')}")
        return None

    cast_info = extract_structured_cast_info(raw.get("credits", {}))
    release_dates = extract_release_dates(raw.get("release_dates", {}))

    return {
        # items 테이블
        "item_key": f"movie_tmdb_{raw['id']}",
        "category_key": "video",
        "brand": None,
        "image_url": f"{TMDB_IMG_BASE}{raw['poster_path']}" if raw.get("poster_path") else None,
        "external_link": f"https://www.themoviedb.org/movie/{raw['id']}",
        "external_service": "TMDB",
        "is_active": True,
        # movie_details 테이블
        "tmdb_id": raw["id"],
        "original_title": raw.get("original_title", "").strip(),
        "overview": overview,
        "overview_source": plot["overview_source"],
        "release_date": raw.get("release_date") or None,
        "runtime": raw.get("runtime"),
        "vote_average": round(float(raw.get("vote_average", 0)), 1),
        "vote_count": int(raw.get("vote_count", 0)),
        "popularity": round(float(raw.get("popularity", 0)), 3),
        "poster_path": raw.get("poster_path"),
        "genres": [g["name"] for g in raw.get("genres", [])],
        "keywords": [k["name"] for k in raw.get("keywords", {}).get("keywords", [])],
        "production_countries": [c["iso_3166_1"] for c in raw.get("production_countries", [])],
        "original_language": raw.get("original_language"),
        "cast_info": cast_info,
        "release_dates": release_dates,
        "content_type": "MOVIE",
        "tmdb_updated_at": raw.get("_fetched_at"),
        # item_translations 테이블
        "title_ko": raw.get("title", "").strip(),
        "title_en": raw.get("original_title", "").strip(),
        "description_ko": overview[:2000] if overview else None,
    }


# =============================================================================
# Step 9: PostgreSQL Upsert (Load)
# =============================================================================


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def load_movie_to_postgres(conn, movie: dict) -> int | None:
    """items → movie_details → item_translations → neo4j_sync_status Upsert

    Returns:
        item_id on success, None on failure
    """
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

        # 2. movie_details Upsert
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
            "genres": Json(movie["genres"]),
            "keywords": Json(movie["keywords"]),
            "production_countries": Json(movie["production_countries"]),
            "original_language": movie["original_language"],
            "cast_info": Json(movie["cast_info"]),
            "release_dates": Json(movie["release_dates"]),
            "content_type": movie["content_type"],
            "tmdb_updated_at": movie["tmdb_updated_at"],
        })

        # 3. item_translations (한국어)
        if movie.get("title_ko"):
            cur.execute("""
                INSERT INTO item_translations (item_id, language_id, item_value, description)
                VALUES (
                    %(item_id)s,
                    (SELECT language_id FROM languages WHERE language_code = 'ko'),
                    %(item_value)s, %(description)s
                )
                ON CONFLICT (item_id, language_id) DO UPDATE SET
                    item_value = EXCLUDED.item_value,
                    description = EXCLUDED.description
            """, {
                "item_id": item_id,
                "item_value": movie["title_ko"][:255],
                "description": movie.get("description_ko"),
            })

        # item_translations (영어)
        if movie.get("title_en"):
            cur.execute("""
                INSERT INTO item_translations (item_id, language_id, item_value, description)
                VALUES (
                    %(item_id)s,
                    (SELECT language_id FROM languages WHERE language_code = 'en'),
                    %(item_value)s, NULL
                )
                ON CONFLICT (item_id, language_id) DO UPDATE SET
                    item_value = EXCLUDED.item_value
            """, {
                "item_id": item_id,
                "item_value": movie["title_en"][:255],
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
# 파이프라인 실행
# =============================================================================


def run_pipeline(test_mode: bool = False):
    started_at = datetime.now()
    log.info("=" * 60)
    log.info(f"🎬 Vibe-Link 영화 수집 파이프라인 v3.0 시작 {'(TEST MODE)' if test_mode else ''}")
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

    # 중복 제거 + adult 필터 + 검증 A
    all_movies = deduplicate_movies(all_movies)
    log.info(f"  중복 제거 후: {len(all_movies)}건")

    all_movies = filter_adult_movies(all_movies)
    all_movies = validate_raw(all_movies)
    log.info(f"  검증A 통과: {len(all_movies)}건")

    # ── 2. Enrich: 상세정보 + Wikipedia 줄거리 + Transform ──
    log.info(f"\n🔄 [Step 2] 상세정보 보강 + 줄거리 추출 + Transform ({len(all_movies)}편)...")
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
    log.info("📊 수집 파이프라인 완료 리포트")
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

    # ── 5. 저장 확인 쿼리 ──
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
    cur.execute("SELECT sync_status, COUNT(*) FROM neo4j_sync_status GROUP BY sync_status")
    sync_stats = dict(cur.fetchall())
    cur.close()
    conn.close()

    log.info(f"\n  📋 DB 현황:")
    log.info(f"      총 영화: {total}편")
    log.info(f"      줄거리 확보: {with_overview}편 ({with_overview/max(total,1)*100:.1f}%)")
    log.info(f"      등급 확보: {with_ratings}편 ({with_ratings/max(total,1)*100:.1f}%)")
    log.info(f"      동기화 상태: {sync_stats}")
    log.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Vibe-Link 영화 수집 파이프라인 v3.0")
    parser.add_argument("--test", action="store_true", help="테스트 모드 (소스당 1페이지만)")
    args = parser.parse_args()
    run_pipeline(test_mode=args.test)