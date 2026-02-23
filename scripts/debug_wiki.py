"""
Wikipedia 줄거리 추출기 디버깅 스크립트
movie_pipeline.py와 같은 위치(scripts/)에 넣고 실행:
  python scripts/debug_wiki.py
"""

import requests
import re
import time
import json
from bs4 import BeautifulSoup
from urllib.parse import quote

BASE_KO = "https://ko.wikipedia.org/w/api.php"
BASE_EN = "https://en.wikipedia.org/w/api.php"
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "VibeLinkBot/1.0 (vibe-link-project; contact@vibe-link.com)"
})


def wiki_api(base_url: str, **params) -> dict:
    params["format"] = "json"
    try:
        resp = SESSION.get(base_url, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"  ❌ API 호출 실패: {e}")
        return {}


def test_movie(title: str, year: int, lang: str = "ko"):
    base = BASE_KO if lang == "ko" else BASE_EN
    print(f"\n{'='*60}")
    print(f"🎬 테스트: \"{title}\" ({year}) [{lang}]")
    print(f"{'='*60}")

    # ── 1. 페이지 존재 확인 ──
    candidates = [f"{title} ({year}년 영화)", f"{title} (영화)", title]
    if lang == "en":
        candidates = [f"{title} ({year} film)", f"{title} (film)", title]

    for c in candidates:
        print(f"\n  📌 페이지 확인: \"{c}\"")
        data = wiki_api(base, action="query", titles=c, prop="info")
        pages = data.get("query", {}).get("pages", {})
        print(f"     API 응답 키: {list(pages.keys())}")

        if "-1" in pages:
            print(f"     → 페이지 없음")
            continue

        print(f"     → 페이지 존재!")

        # 카테고리 확인
        cat_data = wiki_api(base, action="query", titles=c, prop="categories", cllimit=50)
        cats = []
        for page in cat_data.get("query", {}).get("pages", {}).values():
            cats = [cat.get("title", "") for cat in page.get("categories", [])]
        print(f"     카테고리: {cats[:5]}")

        movie_kw = {"영화", "film", "movie", "애니메이션 영화"}
        is_movie = any(any(kw in ct.lower() for kw in movie_kw) for ct in cats)
        print(f"     영화 페이지?: {is_movie}")

        if not is_movie:
            # intro 확인
            intro_data = wiki_api(
                base, action="query", titles=c,
                prop="extracts", exintro=True, explaintext=True, exchars=200,
            )
            for page in intro_data.get("query", {}).get("pages", {}).values():
                intro = page.get("extract", "")[:200]
                print(f"     첫 문장: {intro}")
                if "영화" in intro or "film" in intro.lower():
                    is_movie = True
                    print(f"     → 첫 문장으로 영화 확인!")

        if not is_movie:
            print(f"     → 영화 아님, 다음 후보로")
            continue

        # ── 2. 줄거리 추출 시도 ──
        print(f"\n  📖 줄거리 추출 시도...")

        # 방법 A: sections API로 줄거리 섹션 찾기
        sec_data = wiki_api(base, action="parse", page=c, prop="sections")
        if "parse" in sec_data:
            sections = sec_data["parse"]["sections"]
            plot_kw = {"줄거리", "내용", "시놉시스", "스토리", "플롯"} if lang == "ko" else {"Plot", "Synopsis", "Story", "Plot summary"}
            print(f"     섹션 목록: {[s['line'] for s in sections]}")

            section_idx = None
            for s in sections:
                if s["line"].strip() in plot_kw:
                    section_idx = s["index"]
                    break

            if section_idx:
                print(f"     → 줄거리 섹션 발견 (index={section_idx})")
                text_data = wiki_api(
                    base, action="parse", page=c, prop="text",
                    section=section_idx, disabletoc=True,
                )
                html = text_data.get("parse", {}).get("text", {}).get("*", "")
                if html:
                    soup = BeautifulSoup(html, "html.parser")
                    for tag in soup.find_all("sup"):
                        tag.decompose()
                    paras = [p.get_text(strip=True) for p in soup.find_all("p") if p.get_text(strip=True)]
                    plot = "\n".join(paras)
                    print(f"     → 줄거리 {len(plot)}자 추출 성공!")
                    print(f"     미리보기: {plot[:150]}...")
                else:
                    print(f"     → HTML 비어있음")
            else:
                print(f"     → 줄거리 섹션 없음")
        else:
            print(f"     → sections API 실패: {sec_data.get('error', 'unknown')}")

        # 방법 B: 전체 HTML에서 heading으로 찾기 (pipeline 방식)
        print(f"\n  🔍 방법 B (pipeline 방식): 전체 HTML heading 파싱...")
        full_data = wiki_api(base, action="parse", page=c, prop="text")
        html = full_data.get("parse", {}).get("text", {}).get("*", "")
        if html:
            soup = BeautifulSoup(html, "html.parser")
            plot_kw_set = {"줄거리", "내용", "시놉시스", "스토리", "플롯"} if lang == "ko" else {"Plot", "Synopsis", "Story", "Plot summary"}

            # mw-headline 방식 (현재 코드)
            headlines = []
            for h in soup.find_all(["h2", "h3"]):
                span = h.find("span", class_="mw-headline")
                if span:
                    headlines.append(span.get_text(strip=True))
            print(f"     mw-headline 방식: {headlines[:10]}")

            # id 방식 (최신 Wikipedia)
            id_headlines = []
            for h in soup.find_all(["h2", "h3"]):
                h_id = h.get("id", "")
                h_text = h.get_text(strip=True)
                if h_id or h_text:
                    id_headlines.append({"tag": h.name, "id": h_id, "text": h_text[:50]})
            print(f"     id/text 방식: {json.dumps(id_headlines[:10], ensure_ascii=False)}")
        else:
            print(f"     → 전체 HTML 비어있음")

        return  # 첫 번째 성공한 후보에서 종료

    print(f"\n  ⚠️ 모든 후보에서 실패")


if __name__ == "__main__":
    print("=" * 60)
    print("Wikipedia 줄거리 추출기 디버깅")
    print("=" * 60)

    # 한국어 위키 테스트
    test_movie("기생충", 2019, "ko")
    test_movie("인터스텔라", 2014, "ko")

    # 영어 위키 테스트
    test_movie("Parasite", 2019, "en")
    test_movie("Interstellar", 2014, "en")