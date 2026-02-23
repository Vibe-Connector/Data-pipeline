"""
Vibe-Link 영화 도메인 — Wikipedia 줄거리 추출

4단계 검색 전략으로 Wikipedia에서 영화 줄거리를 추출합니다.
v3.1: ko/en 독립 수집 (첫 성공 시 중단하지 않음)
"""

import re
import time
from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

from common.logging_config import get_logger

log = get_logger("movie.wikipedia")


# =============================================================================
# 데이터 클래스
# =============================================================================


@dataclass
class MoviePlotResult:
    query: str
    wiki_title: Optional[str]
    wiki_url: Optional[str]
    plot: Optional[str]
    search_strategy: Optional[str]
    error: Optional[str] = None


# =============================================================================
# Wikipedia 줄거리 추출기
# =============================================================================


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

    # ── 보조 메서드 ──

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

    # ── 줄거리 섹션 추출 ──

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


# =============================================================================
# 모듈 레벨 인스턴스 & 통합 함수
# =============================================================================

# Wikipedia 줄거리 추출기 (ko + en)
wiki_ko = WikiMoviePlotExtractor(lang="ko", delay=0.3)
wiki_en = WikiMoviePlotExtractor(lang="en", delay=0.3)


def extract_multilingual_plots(
    title_ko: str, title_en: str, release_year: Optional[int],
) -> dict:
    """v3.1: ko/en Wikipedia 줄거리를 독립적으로 수집

    ko 성공 여부와 무관하게 en도 항상 시도합니다.

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

    # 영어 Wikipedia — ko 성공 여부와 무관하게 항상 시도
    if title_en:
        r = wiki_en.extract(title_en, release_year)
        if r.plot and len(r.plot) >= 50:
            result["wiki_en"] = {
                "plot": r.plot,
                "url": r.wiki_url,
                "strategy": r.search_strategy,
            }

    return result