#!/usr/bin/env python3
"""
☕ Vibe-Link 커피 수집 파이프라인 v1.0
Notion DB → Transform → Validate → PostgreSQL

데이터 소스: Notion Coffee Details (v4.2) + Item Translations
대상 스키마: DB v3.2 + coffee_details v4.3 (category 삭제)

사용법:
    python scripts/coffee_pipeline.py              # 전체 실행
    python scripts/coffee_pipeline.py --dry-run    # DB 저장 없이 검증만
"""

import json
import logging
import os
import re
import sys
from datetime import datetime

import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv

# =============================================================================
# 설정
# =============================================================================
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME", "vibelink"),
    "user": os.getenv("DB_USER", "vibelink"),
    "password": os.getenv("DB_PASSWORD", ""),
}

# Notion에서 추출한 데이터 파일 경로
COFFEE_DETAILS_PATH = os.getenv(
    "COFFEE_DETAILS_PATH", "data/coffee_details_notion.json"
)
ITEM_TRANSLATIONS_PATH = os.getenv(
    "ITEM_TRANSLATIONS_PATH", "data/item_translations_notion.json"
)


# =============================================================================
# Step 1: Extract — Notion JSON 로드
# =============================================================================


def load_json_file(path: str) -> list[dict]:
    """JSON 파일 로드"""
    if not os.path.exists(path):
        log.error(f"파일 없음: {path}")
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, list) else data.get("capsules", data.get("data", []))


def clean_notion_json_string(raw: str) -> any:
    """노션 TEXT 필드의 이중 이스케이프 JSON → 정상 JSON 파싱

    노션은 JSON을 TEXT로 저장할 때 백슬래시 이스케이프를 추가함:
      \\["브라질","에티오피아"\\]  →  ["브라질","에티오피아"]
      \\{"primary":\\["코코아"\\]\\}  →  {"primary":["코코아"]}
    """
    if not raw or not isinstance(raw, str):
        return None
    # 이스케이프 패턴 제거
    cleaned = raw.replace("\\\\", "")
    cleaned = re.sub(r"\\([{}\[\]])", r"\1", cleaned)
    try:
        return json.loads(cleaned)
    except (json.JSONDecodeError, TypeError):
        log.warning(f"JSON 파싱 실패: {raw[:80]}...")
        return None


def empty_to_none(val: any) -> any:
    """빈 문자열 → None 변환"""
    if isinstance(val, str) and val.strip() == "":
        return None
    return val


def notion_bool(val: any) -> bool:
    """노션 CHECKBOX 값 → Python bool"""
    if val == "__YES__" or val is True:
        return True
    return False


# =============================================================================
# Step 2: Transform — 노션 데이터 → v4.3 스키마 매핑
# =============================================================================


def transform_coffee(raw: dict, en_translations: dict) -> dict:
    """노션 Coffee Details 1건 → PostgreSQL 적재용 dict 변환

    Args:
        raw: 노션 Coffee Details 페이지 properties
        en_translations: {capsule_key: {"item_value": ..., "description": ...}}
    """
    key = raw.get("capsule_key", "").strip()

    # JSON 필드 파싱 (이스케이프 제거)
    cup_sizes = clean_notion_json_string(raw.get("cup_sizes"))
    origins = clean_notion_json_string(raw.get("origins"))
    aroma_profile = clean_notion_json_string(raw.get("aroma_profile"))

    # 수치 필드: int 변환, 범위 밖이면 None
    def safe_int(val, lo=None, hi=None):
        if val is None:
            return None
        try:
            v = int(float(val))
            if lo is not None and v < lo:
                return None
            if hi is not None and v > hi:
                return None
            return v
        except (ValueError, TypeError):
            return None

    capsule_name = (raw.get("capsule_name") or "").strip() or None
    flavor_notes = (raw.get("flavor_notes") or "").strip() or None

    # en 번역 조회 (Notion Item Translations에서)
    en_tr = en_translations.get(key, {})

    return {
        # ── items 테이블 ──
        "item_key": f"coffee_{key}",
        "category_key": "coffee",
        "brand": "Nespresso",
        "image_url": None,
        "external_link": f"https://www.nespresso.com/kr/ko/order/capsules/{key}",
        "external_service": "NESPRESSO",
        "is_active": True,
        # ── coffee_details 테이블 (v4.3: category 삭제) ──
        "capsule_key": key,
        "capsule_name": capsule_name,
        "line": (raw.get("line") or "").strip().upper() or None,
        "sub_category": empty_to_none(raw.get("sub_category")),
        "intensity": safe_int(raw.get("intensity"), 1, 14),
        "intensity_max": safe_int(raw.get("intensity_max"), 1, 14),
        "cup_sizes": cup_sizes or [],
        "bean_type": empty_to_none(raw.get("bean_type")),
        "origins": origins,
        "roast_level": empty_to_none(raw.get("roast_level")),
        "aroma_profile": aroma_profile,
        "flavor_notes": flavor_notes,
        "body": safe_int(raw.get("body"), 1, 5),
        "bitterness": safe_int(raw.get("bitterness"), 1, 5),
        "acidity": safe_int(raw.get("acidity"), 1, 5),
        "roasting": safe_int(raw.get("roasting"), 1, 5),
        "is_decaf": notion_bool(raw.get("is_decaf")),
        "is_limited_edition": notion_bool(raw.get("is_limited_edition")),
        "price_per_capsule_krw": safe_int(raw.get("price_per_capsule_krw"), 0),
        # ── item_translations ──
        "translations": {
            "ko": {
                "item_value": capsule_name,        # coffee_details.capsule_name (한국어)
                "description": flavor_notes,        # coffee_details.flavor_notes (한국어)
            },
            "en": {
                "item_value": en_tr.get("item_value"),
                "description": en_tr.get("description"),
            },
        },
    }


# =============================================================================
# Step 3: Validate — 검증 A (원시) + 검증 B (변환 후)
# =============================================================================


def validate_a_raw(raw: dict) -> str | None:
    """검증 A: 원시 데이터 무결성. 실패 시 사유 문자열 반환, 성공 시 None."""
    key = raw.get("capsule_key", "")
    if not key or not isinstance(key, str) or not key.strip():
        return "capsule_key 누락"
    name = raw.get("capsule_name", "")
    if not name or not isinstance(name, str) or not name.strip():
        return f"capsule_name 누락 [{key}]"
    line = (raw.get("line") or "").strip().upper()
    if line not in ("ORIGINAL", "VERTUO"):
        return f"line 값 불일치({line}) [{key}]"
    # intensity: NULL 허용, 값 있으면 1~14
    intensity = raw.get("intensity")
    if intensity is not None:
        try:
            iv = int(float(intensity))
            if not (1 <= iv <= 14):
                return f"intensity 범위 초과({iv}) [{key}]"
        except (ValueError, TypeError):
            return f"intensity 파싱 실패 [{key}]"
    # cup_sizes 존재 확인
    cs = raw.get("cup_sizes")
    if not cs or (isinstance(cs, str) and cs.strip() in ("", "[]")):
        return f"cup_sizes 비어있음 [{key}]"
    return None


def validate_b_transformed(item: dict) -> tuple[dict, list[str]]:
    """검증 B: 변환 후 스키마 적합성. (교정된 item, 경고 목록) 반환."""
    warnings = []
    key = item["capsule_key"]

    # item_key prefix
    if not item["item_key"].startswith("coffee_"):
        item["item_key"] = f"coffee_{key}"
        warnings.append(f"item_key 형식 교정 [{key}]")

    # VARCHAR 길이 제한
    limits = {
        "capsule_key": 100, "capsule_name": 255, "line": 50,
        "sub_category": 100, "bean_type": 100, "roast_level": 100,
    }
    for field, mx in limits.items():
        v = item.get(field)
        if v and isinstance(v, str) and len(v) > mx:
            item[field] = v[:mx]
            warnings.append(f"{field} 길이 초과 truncate [{key}]")

    # cup_sizes JSON 유효성
    cs = item.get("cup_sizes")
    if not isinstance(cs, list) or len(cs) == 0:
        warnings.append(f"cup_sizes 비어있음 — 빈 배열 [{key}]")
        item["cup_sizes"] = []
    else:
        for entry in cs:
            if not isinstance(entry, dict) or "type" not in entry or "ml" not in entry:
                warnings.append(f"cup_sizes 항목 구조 불량 [{key}]")
                break

    # origins JSON
    ori = item.get("origins")
    if ori is not None and not isinstance(ori, list):
        if isinstance(ori, str):
            item["origins"] = [ori]
            warnings.append(f"origins 문자열 → 배열 래핑 [{key}]")
        else:
            item["origins"] = None

    # aroma_profile JSON
    ap = item.get("aroma_profile")
    if ap is not None and not isinstance(ap, dict):
        item["aroma_profile"] = None
        warnings.append(f"aroma_profile 비정상 → NULL [{key}]")

    # price 양수
    price = item.get("price_per_capsule_krw")
    if price is not None and price < 0:
        item["price_per_capsule_krw"] = None
        warnings.append(f"price 음수 → NULL [{key}]")

    return item, warnings


# =============================================================================
# Step 4: Load — PostgreSQL Upsert
# =============================================================================


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def load_coffee_to_postgres(conn, item: dict) -> int | None:
    """items → coffee_details → item_translations(ko/en) → neo4j_sync_status Upsert

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
                brand = EXCLUDED.brand,
                external_link = EXCLUDED.external_link,
                updated_at = CURRENT_TIMESTAMP
            RETURNING item_id
        """, item)
        item_id = cur.fetchone()[0]

        # 2. coffee_details Upsert (v4.3: category 삭제)
        cur.execute("""
            INSERT INTO coffee_details (
                item_id, capsule_key, capsule_name, line, sub_category,
                intensity, intensity_max, cup_sizes, bean_type, origins,
                roast_level, aroma_profile, flavor_notes,
                body, bitterness, acidity, roasting,
                is_decaf, is_limited_edition, price_per_capsule_krw
            ) VALUES (
                %(item_id)s, %(capsule_key)s, %(capsule_name)s, %(line)s, %(sub_category)s,
                %(intensity)s, %(intensity_max)s, %(cup_sizes)s, %(bean_type)s, %(origins)s,
                %(roast_level)s, %(aroma_profile)s, %(flavor_notes)s,
                %(body)s, %(bitterness)s, %(acidity)s, %(roasting)s,
                %(is_decaf)s, %(is_limited_edition)s, %(price_per_capsule_krw)s
            )
            ON CONFLICT (capsule_key) DO UPDATE SET
                capsule_name = EXCLUDED.capsule_name,
                line = EXCLUDED.line,
                sub_category = EXCLUDED.sub_category,
                intensity = EXCLUDED.intensity,
                intensity_max = EXCLUDED.intensity_max,
                cup_sizes = EXCLUDED.cup_sizes,
                bean_type = EXCLUDED.bean_type,
                origins = EXCLUDED.origins,
                roast_level = EXCLUDED.roast_level,
                aroma_profile = EXCLUDED.aroma_profile,
                flavor_notes = EXCLUDED.flavor_notes,
                body = EXCLUDED.body,
                bitterness = EXCLUDED.bitterness,
                acidity = EXCLUDED.acidity,
                roasting = EXCLUDED.roasting,
                is_decaf = EXCLUDED.is_decaf,
                is_limited_edition = EXCLUDED.is_limited_edition,
                price_per_capsule_krw = EXCLUDED.price_per_capsule_krw,
                updated_at = CURRENT_TIMESTAMP
        """, {
            "item_id": item_id,
            "capsule_key": item["capsule_key"],
            "capsule_name": item["capsule_name"][:255],
            "line": item["line"],
            "sub_category": item["sub_category"],
            "intensity": item["intensity"],
            "intensity_max": item["intensity_max"],
            "cup_sizes": Json(item["cup_sizes"]),
            "bean_type": item["bean_type"],
            "origins": Json(item["origins"]) if item["origins"] else None,
            "roast_level": item["roast_level"],
            "aroma_profile": Json(item["aroma_profile"]) if item["aroma_profile"] else None,
            "flavor_notes": item["flavor_notes"],
            "body": item["body"],
            "bitterness": item["bitterness"],
            "acidity": item["acidity"],
            "roasting": item["roasting"],
            "is_decaf": item["is_decaf"],
            "is_limited_edition": item["is_limited_edition"],
            "price_per_capsule_krw": item["price_per_capsule_krw"],
        })

        # 3. item_translations (ko/en)
        translations = item.get("translations", {})
        for lang_code, tr in translations.items():
            iv = tr.get("item_value")
            if not iv:
                continue
            cur.execute("""
                INSERT INTO item_translations (item_id, language_id, item_value, description)
                VALUES (
                    %(item_id)s,
                    (SELECT language_id FROM languages WHERE language_code = %(lang)s),
                    %(item_value)s, %(description)s
                )
                ON CONFLICT (item_id, language_id) DO UPDATE SET
                    item_value = EXCLUDED.item_value,
                    description = EXCLUDED.description
            """, {
                "item_id": item_id,
                "lang": lang_code,
                "item_value": iv[:255],
                "description": tr.get("description"),
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
        log.error(f"  DB 저장 실패 [{item.get('capsule_key')}]: {e}")
        return None
    finally:
        cur.close()


# =============================================================================
# Step 5: 파이프라인 실행
# =============================================================================


def build_en_translation_map(translations_data: list[dict]) -> dict:
    """Item Translations 데이터에서 {capsule_key: {item_value, description}} 매핑 생성

    노션의 item_id 릴레이션을 capsule_key로 변환하기 어려우므로,
    item_value(영어 캡슐명)를 기준으로 Coffee Details와 매칭한다.
    → 사전에 capsule_key 매핑을 포함한 JSON을 준비해야 함.
    """
    en_map = {}
    for tr in translations_data:
        lang = tr.get("language_id", "")
        if lang != "en":
            continue
        # capsule_key는 JSON 준비 시 포함되어야 함
        key = tr.get("capsule_key", "").strip()
        if not key:
            # fallback: item_value를 key로 변환 시도
            continue
        en_map[key] = {
            "item_value": (tr.get("item_value") or "").strip() or None,
            "description": (tr.get("description") or "").strip() or None,
        }
    return en_map


def run_pipeline(dry_run: bool = False):
    started_at = datetime.now()
    log.info("=" * 60)
    log.info(f"☕ Vibe-Link 커피 수집 파이프라인 v1.0 {'(DRY RUN)' if dry_run else ''}")
    log.info("=" * 60)

    # ── 1. Extract ──
    log.info("[1/5] 데이터 로드...")
    raw_capsules = load_json_file(COFFEE_DETAILS_PATH)
    raw_translations = load_json_file(ITEM_TRANSLATIONS_PATH)
    log.info(f"  Coffee Details: {len(raw_capsules)}건")
    log.info(f"  Item Translations: {len(raw_translations)}건")

    en_map = build_en_translation_map(raw_translations)
    log.info(f"  en 번역 매핑: {len(en_map)}건")

    # ── 2. 검증 A: 원시 데이터 ──
    log.info("[2/5] 검증 A — 원시 데이터 무결성...")
    valid_raws = []
    seen_keys = set()
    skip_a = {"total": 0, "reasons": {}}

    for raw in raw_capsules:
        reason = validate_a_raw(raw)
        if reason:
            skip_a["total"] += 1
            skip_a["reasons"][reason] = skip_a["reasons"].get(reason, 0) + 1
            log.warning(f"  검증A 스킵: {reason}")
            continue
        # 중복 제거 (capsule_key 기준 첫 번째만 유지)
        key = raw["capsule_key"].strip()
        if key in seen_keys:
            log.warning(f"  중복 스킵: {key}")
            skip_a["total"] += 1
            continue
        seen_keys.add(key)
        valid_raws.append(raw)

    log.info(f"  검증A 통과: {len(valid_raws)}건 (스킵: {skip_a['total']}건)")

    # ── 3. Transform + 검증 B ──
    log.info("[3/5] Transform + 검증 B...")
    transformed = []
    warn_b_total = 0

    for raw in valid_raws:
        item = transform_coffee(raw, en_map)
        item, warnings = validate_b_transformed(item)
        for w in warnings:
            log.warning(f"  검증B 경고: {w}")
            warn_b_total += 1
        transformed.append(item)

    log.info(f"  Transform 완료: {len(transformed)}건 (경고: {warn_b_total}건)")

    # ── 4. 통계 출력 ──
    log.info("[4/5] 데이터 통계...")
    lines = {"ORIGINAL": 0, "VERTUO": 0}
    decaf_cnt = 0
    limited_cnt = 0
    no_intensity = 0
    no_en = 0

    for item in transformed:
        lines[item["line"]] = lines.get(item["line"], 0) + 1
        if item["is_decaf"]:
            decaf_cnt += 1
        if item["is_limited_edition"]:
            limited_cnt += 1
        if item["intensity"] is None:
            no_intensity += 1
        en_tr = item["translations"].get("en", {})
        if not en_tr.get("item_value"):
            no_en += 1

    log.info(f"  ORIGINAL: {lines.get('ORIGINAL', 0)}건")
    log.info(f"  VERTUO:   {lines.get('VERTUO', 0)}건")
    log.info(f"  디카페인: {decaf_cnt}건")
    log.info(f"  한정판:   {limited_cnt}건")
    log.info(f"  intensity 없음: {no_intensity}건")
    log.info(f"  en 번역 없음:   {no_en}건")

    # ── 5. Load ──
    if dry_run:
        log.info("[5/5] DRY RUN — DB 저장 스킵")
        log.info("  검증 통과 데이터 샘플 (첫 3건):")
        for item in transformed[:3]:
            log.info(f"    {item['capsule_key']} | {item['line']} | "
                     f"intensity={item['intensity']} | {item['capsule_name']}")
    else:
        log.info("[5/5] PostgreSQL 적재...")
        conn = get_db_connection()
        saved = 0
        failed = 0
        for item in transformed:
            item_id = load_coffee_to_postgres(conn, item)
            if item_id:
                saved += 1
                log.info(f"  ✅ [{item['capsule_key']}] item_id={item_id}")
            else:
                failed += 1
        conn.close()
        log.info(f"  저장 완료: {saved}건 / 실패: {failed}건")

    # ── 결과 요약 ──
    elapsed = (datetime.now() - started_at).total_seconds()
    log.info("=" * 60)
    log.info(f"☕ 파이프라인 완료 ({elapsed:.1f}초)")
    log.info(f"  원시 데이터:    {len(raw_capsules)}건")
    log.info(f"  검증A 통과:     {len(valid_raws)}건")
    log.info(f"  Transform:      {len(transformed)}건")
    if not dry_run:
        log.info(f"  DB 저장:        {saved}건")
        log.info(f"  DB 실패:        {failed}건")
    log.info("=" * 60)


# =============================================================================
# 엔트리포인트
# =============================================================================

if __name__ == "__main__":
    is_dry = "--dry-run" in sys.argv
    run_pipeline(dry_run=is_dry)