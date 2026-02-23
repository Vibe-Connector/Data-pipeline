"""
Vibe-Link 영화 도메인 — 상수 정의

TMDB 수집 소스, 등급 매핑, 국가 우선순위 등 영화 전용 상수를 관리합니다.
"""

# ─────────────────────────────────────────────
# TMDB 수집 소스 (v3.0: 모든 소스에 include_adult=false)
# ─────────────────────────────────────────────
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

# ─────────────────────────────────────────────
# 등급 매핑 (v3.0)
# ─────────────────────────────────────────────
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