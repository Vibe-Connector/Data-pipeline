"""
Vibe-Link 영화 도메인 — Transform (정제)

TMDB raw 데이터를 PostgreSQL 스키마 v3.2에 맞게 정제합니다.
v3.1: 다국어 확장, overview 한국어 최우선, 빈 JSON NULL 처리
"""

from common.config import TMDB_IMG_BASE
from common.logging_config import get_logger
from movie.extract import TMDBClient
from movie.wikipedia import extract_multilingual_plots

log = get_logger("movie.transform")


# =============================================================================
# 보조 정제 함수
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
    return {"results": cleaned} if cleaned else None


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
# 메인 Transform 함수
# =============================================================================


def transform_movie(raw: dict, tmdb: TMDBClient) -> dict | None:
    """v3.1: TMDB raw → PostgreSQL 스키마 v3.2 매핑 (다국어 확장)"""

    if not raw.get("title") or not raw.get("id"):
        return None
    if raw.get("adult", False):
        return None

    tmdb_id = raw["id"]

    # ── TMDB 다국어 정보 조회 (en-US, zh-CN) ──
    en_info = tmdb.fetch_localized_info(tmdb_id, "en-US")
    zh_info = tmdb.fetch_localized_info(tmdb_id, "zh-CN")

    # 제목 확보 체크 (최소 1개 언어)
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

    # ── TMDB overview 다국어 확보 ──
    tmdb_ko_overview = raw.get("overview", "").strip()
    tmdb_en_overview = en_info["overview"]
    tmdb_zh_overview = zh_info["overview"]

    # ── Wikipedia ko/en 독립 수집 ──
    wiki_plots = extract_multilingual_plots(
        title_ko=tmdb_ko_title,
        title_en=en_info["title"] or raw.get("original_title", ""),
        release_year=release_year,
    )

    # ── overview 한국어 최우선 선택 ──
    best = select_best_overview(wiki_plots, tmdb_ko_overview, tmdb_en_overview)

    overview = best["overview"]
    if not overview or len(overview) < 10:
        log.warning(f"  줄거리 없음 — 스킵: {raw.get('title')}")
        return None

    # 정제
    cast_info = extract_structured_cast_info(raw.get("credits", {}))
    release_dates = extract_release_dates(raw.get("release_dates", {}))

    # 빈 JSON → None
    genres = [g["name"] for g in raw.get("genres", [])]
    keywords = [k["name"] for k in raw.get("keywords", {}).get("keywords", [])]
    prod_countries = [c["iso_3166_1"] for c in raw.get("production_countries", [])]

    # item_translations 3개 언어 description 구성
    ko_desc = wiki_plots["wiki_ko"]["plot"] or (tmdb_ko_overview if len(tmdb_ko_overview) >= 10 else None)
    en_desc = wiki_plots["wiki_en"]["plot"] or (tmdb_en_overview if len(tmdb_en_overview) >= 10 else None)
    zh_desc = tmdb_zh_overview if len(tmdb_zh_overview) >= 10 else None

    return {
        # ── items ──
        "item_key": f"movie_tmdb_{tmdb_id}",
        "category_key": "video",
        "brand": None,
        "image_url": f"{TMDB_IMG_BASE}{raw['poster_path']}" if raw.get("poster_path") else None,
        "external_link": f"https://www.themoviedb.org/movie/{tmdb_id}",
        "external_service": "TMDB",
        "is_active": True,

        # ── movie_details ──
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
        "genres": genres or None,
        "keywords": keywords or None,
        "production_countries": prod_countries or None,
        "original_language": raw.get("original_language"),
        "cast_info": cast_info if (cast_info.get("director") or cast_info.get("cast")) else None,
        "release_dates": release_dates,
        "content_type": "MOVIE",
        "tmdb_updated_at": raw.get("_fetched_at"),

        # ── item_translations (3언어) ──
        "translations": {
            "ko": {
                "title": tmdb_ko_title or None,
                "description": ko_desc[:2000] if ko_desc else None,
            },
            "en": {
                "title": tmdb_en_title or None,
                "description": en_desc[:2000] if en_desc else None,
            },
            "zh": {
                "title": tmdb_zh_title or None,
                "description": zh_desc[:2000] if zh_desc else None,
            },
        },
    }