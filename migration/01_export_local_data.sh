#!/bin/bash
# =============================================================
# 로컬 Docker DB에서 데이터를 CSV로 내보내기
# 실행: bash 01_export_local_data.sh
# =============================================================

set -e

CONTAINER="vibe-link-pg"
DB_USER="vibelink"
DB_NAME="vibelink"
EXPORT_DIR="$(cd "$(dirname "$0")" && pwd)/exported_data"

mkdir -p "$EXPORT_DIR"

echo "=== 로컬 DB 데이터 내보내기 시작 ==="

# 1. items 테이블 (category_id 매핑 적용: local coffee=3→4, lighting=4→3)
echo "[1/5] items 내보내기 (category_id 매핑 적용)..."
docker exec "$CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -c "\COPY (
    SELECT
        item_id,
        CASE category_id
            WHEN 3 THEN 4   -- coffee: local 3 → RDS 4
            WHEN 4 THEN 3   -- lighting: local 4 → RDS 3
            ELSE category_id
        END AS category_id,
        item_key, brand, image_url, external_link, external_service,
        is_active, created_at, updated_at
    FROM items
    ORDER BY item_id
) TO STDOUT WITH (FORMAT csv, HEADER true)" > "$EXPORT_DIR/items.csv"
echo "  → $(tail -n +2 "$EXPORT_DIR/items.csv" | wc -l | tr -d ' ') 행 내보냄"

# 2. item_translations 테이블 (language_id 매핑 적용: local zh=3→4, ja=4→3)
echo "[2/5] item_translations 내보내기 (language_id 매핑 적용)..."
docker exec "$CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -c "\COPY (
    SELECT
        translation_id,
        item_id,
        CASE language_id
            WHEN 3 THEN 4   -- zh: local 3 → RDS 4
            WHEN 4 THEN 3   -- ja: local 4 → RDS 3
            ELSE language_id
        END AS language_id,
        item_value, description
    FROM item_translations
    ORDER BY translation_id
) TO STDOUT WITH (FORMAT csv, HEADER true)" > "$EXPORT_DIR/item_translations.csv"
echo "  → $(tail -n +2 "$EXPORT_DIR/item_translations.csv" | wc -l | tr -d ' ') 행 내보냄"

# 3. movie_details 테이블
echo "[3/5] movie_details 내보내기..."
docker exec "$CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -c "\COPY (
    SELECT item_id, tmdb_id, original_title, overview, release_date, runtime,
           vote_average, vote_count, popularity, poster_path, genres, keywords,
           production_countries, original_language, cast_info, release_dates,
           content_type, tmdb_updated_at, created_at, updated_at
    FROM movie_details
    ORDER BY item_id
) TO STDOUT WITH (FORMAT csv, HEADER true)" > "$EXPORT_DIR/movie_details.csv"
echo "  → $(tail -n +2 "$EXPORT_DIR/movie_details.csv" | wc -l | tr -d ' ') 행 내보냄"

# 4. coffee_details 테이블
echo "[4/5] coffee_details 내보내기..."
docker exec "$CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -c "\COPY (
    SELECT item_id, capsule_key, capsule_name, line, sub_category,
           intensity, intensity_max, cup_sizes, bean_type, origins,
           roast_level, aroma_profile, flavor_notes, body, bitterness,
           acidity, roasting, is_decaf, is_limited_edition,
           price_per_capsule_krw, created_at, updated_at
    FROM coffee_details
    ORDER BY item_id
) TO STDOUT WITH (FORMAT csv, HEADER true)" > "$EXPORT_DIR/coffee_details.csv"
echo "  → $(tail -n +2 "$EXPORT_DIR/coffee_details.csv" | wc -l | tr -d ' ') 행 내보냄"

# 5. neo4j_sync_status 테이블
echo "[5/5] neo4j_sync_status 내보내기..."
docker exec "$CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -c "\COPY (
    SELECT sync_id, item_id, sync_status, neo4j_node_id, last_synced_at,
           retry_count, error_message, created_at, updated_at
    FROM neo4j_sync_status
    ORDER BY sync_id
) TO STDOUT WITH (FORMAT csv, HEADER true)" > "$EXPORT_DIR/neo4j_sync_status.csv"
echo "  → $(tail -n +2 "$EXPORT_DIR/neo4j_sync_status.csv" | wc -l | tr -d ' ') 행 내보냄"

echo ""
echo "=== 내보내기 완료! ==="
echo "파일 위치: $EXPORT_DIR/"
ls -la "$EXPORT_DIR/"
