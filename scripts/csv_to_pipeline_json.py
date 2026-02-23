#!/usr/bin/env python3
"""
☕ Notion CSV Export → coffee_pipeline.py 입력 JSON 변환

사용법:
  1. 노션에서 Coffee Details (v4.2) → Export CSV
  2. 노션에서 Item Translations → Export CSV
  3. 실행:
     python scripts/csv_to_pipeline_json.py \
       --coffee  data/coffee_details_export.csv \
       --trans   data/item_translations_export.csv \
       --outdir  data/

출력:
  data/coffee_details_notion.json
  data/item_translations_notion.json
"""

import argparse
import csv
import json
import sys
from pathlib import Path


def read_csv(path: str) -> list[dict]:
    """CSV 파일 → dict 리스트 (헤더 자동 인식)"""
    with open(path, "r", encoding="utf-8-sig") as f:  # BOM 처리
        reader = csv.DictReader(f)
        # 헤더 공백 제거
        reader.fieldnames = [h.strip() for h in reader.fieldnames]
        return list(reader)


def safe_num(val: str) -> int | float | None:
    """문자열 → 숫자 변환, 실패 시 None"""
    if not val or val.strip() == "":
        return None
    try:
        f = float(val)
        return int(f) if f == int(f) else f
    except (ValueError, TypeError):
        return None


def notion_bool(val: str) -> bool:
    """노션 CSV checkbox 값 → bool"""
    if not val:
        return False
    return val.strip().lower() in ("yes", "true", "1", "__yes__")


def convert_coffee_csv(rows: list[dict]) -> list[dict]:
    """Coffee Details CSV → 파이프라인 JSON 형식"""
    capsules = []
    for row in rows:
        # 노션 CSV는 프로퍼티 이름이 컬럼 헤더
        # capsule_key는 title 프로퍼티 (Name 또는 capsule_key)
        key = (row.get("capsule_key") or row.get("Name") or "").strip()
        if not key:
            continue

        capsule = {
            "capsule_key": key,
            "capsule_name": row.get("capsule_name", "").strip() or None,
            "line": row.get("line", "").strip() or None,
            "category": row.get("category", "").strip() or None,
            "sub_category": row.get("sub_category", "").strip() or None,
            "intensity": safe_num(row.get("intensity", "")),
            "intensity_max": safe_num(row.get("intensity_max", "")),
            "cup_sizes": row.get("cup_sizes", "").strip() or None,
            "bean_type": row.get("bean_type", "").strip() or None,
            "origins": row.get("origins", "").strip() or None,
            "roast_level": row.get("roast_level", "").strip() or None,
            "aroma_profile": row.get("aroma_profile", "").strip() or None,
            "flavor_notes": row.get("flavor_notes", "").strip() or None,
            "body": safe_num(row.get("body", "")),
            "bitterness": safe_num(row.get("bitterness", "")),
            "acidity": safe_num(row.get("acidity", "")),
            "roasting": safe_num(row.get("roasting", "")),
            "is_decaf": notion_bool(row.get("is_decaf", "")),
            "is_limited_edition": notion_bool(row.get("is_limited_edition", "")),
            "price_per_capsule_krw": safe_num(row.get("price_per_capsule_krw", "")),
        }
        capsules.append(capsule)

    return capsules


def convert_translations_csv(rows: list[dict]) -> list[dict]:
    """Item Translations CSV → 파이프라인 JSON 형식

    노션 CSV에서 relation 컬럼은 연결된 페이지의 제목(= capsule_key)으로 내보내짐!
    그래서 별도 매핑이 필요 없음.
    """
    translations = []
    for row in rows:
        # item_value는 title 프로퍼티 (Name 또는 item_value)
        item_value = (row.get("item_value") or row.get("Name") or "").strip()
        # item_id relation → 노션 CSV에서 "title (URL)" 형태로 출력됨
        # 예: "caffe_florian (https://www.notion.so/caffe_florian-306571ca...)"
        raw_relation = (row.get("item_id") or "").strip()
        capsule_key = raw_relation.split(" (http")[0].strip() if " (http" in raw_relation else raw_relation
        language_id = (row.get("language_id") or "").strip()
        description = (row.get("description") or "").strip() or None

        if not capsule_key or not language_id:
            continue

        translations.append({
            "capsule_key": capsule_key,
            "language_id": language_id,
            "item_value": item_value or None,
            "description": description,
        })

    return translations


def save_json(data: list, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  ✅ 저장: {path} ({len(data)}건)")


def main():
    parser = argparse.ArgumentParser(description="Notion CSV → Pipeline JSON")
    parser.add_argument("--coffee", required=True, help="Coffee Details CSV 경로")
    parser.add_argument("--trans", required=True, help="Item Translations CSV 경로")
    parser.add_argument("--outdir", default="data", help="출력 디렉토리")
    args = parser.parse_args()

    out = Path(args.outdir)

    print("☕ Notion CSV → Pipeline JSON 변환")
    print("=" * 50)

    # Coffee Details
    print(f"\n📋 Coffee Details: {args.coffee}")
    coffee_rows = read_csv(args.coffee)
    print(f"  CSV 행 수: {len(coffee_rows)}")
    capsules = convert_coffee_csv(coffee_rows)
    save_json(capsules, out / "coffee_details_notion.json")

    # Item Translations
    print(f"\n🌐 Item Translations: {args.trans}")
    trans_rows = read_csv(args.trans)
    print(f"  CSV 행 수: {len(trans_rows)}")
    translations = convert_translations_csv(trans_rows)
    save_json(translations, out / "item_translations_notion.json")

    # 요약
    print("\n" + "=" * 50)
    print("✅ 변환 완료! 다음 단계:")
    print(f"  python scripts/coffee_pipeline.py --dry-run")


if __name__ == "__main__":
    main()