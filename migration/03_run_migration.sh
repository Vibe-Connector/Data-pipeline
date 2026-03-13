#!/bin/bash
# =============================================================
# 전체 마이그레이션 실행 스크립트
#
# 실행 순서:
#   1. 로컬 Docker DB에서 CSV 내보내기
#   2. RDS에 임시 테이블 생성 + CSV 로드 + 데이터 마이그레이션
#
# 사용법: bash 03_run_migration.sh [--dry-run]
#   --dry-run: 실제 RDS에 쓰지 않고 로컬 내보내기만 수행
# =============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
EXPORT_DIR="$SCRIPT_DIR/exported_data"

# RDS 연결 정보
RDS_HOST="vibeconnector.c1y2iaugs33c.ap-northeast-2.rds.amazonaws.com"
RDS_PORT="5432"
RDS_DB="postgres"
RDS_USER="postgres"
RDS_PASS='Bang)806'

# Docker 컨테이너 정보
CONTAINER="vibe-link-pg"

DRY_RUN=false
if [ "$1" = "--dry-run" ]; then
    DRY_RUN=true
    echo "*** DRY RUN 모드: 로컬 내보내기만 수행합니다 ***"
fi

# ============================================================
# STEP 1: 로컬 데이터 내보내기
# ============================================================
echo ""
echo "========================================"
echo " STEP 1: 로컬 DB 데이터 내보내기"
echo "========================================"
bash "$SCRIPT_DIR/01_export_local_data.sh"

if [ "$DRY_RUN" = true ]; then
    echo ""
    echo "*** DRY RUN 완료. CSV 파일 확인: $EXPORT_DIR/ ***"
    exit 0
fi

# ============================================================
# STEP 2: RDS 마이그레이션 전 현황 확인
# ============================================================
echo ""
echo "========================================"
echo " STEP 2: RDS 마이그레이션 전 현황"
echo "========================================"

docker exec -e PGPASSWORD="$RDS_PASS" "$CONTAINER" psql \
    -h "$RDS_HOST" -p "$RDS_PORT" -U "$RDS_USER" -d "$RDS_DB" -c "
SELECT 'items' AS table_name, COUNT(*) AS row_count FROM items
UNION ALL SELECT 'item_translations', COUNT(*) FROM item_translations
UNION ALL SELECT 'movie_details', COUNT(*) FROM movie_details
UNION ALL SELECT 'coffee_details', COUNT(*) FROM coffee_details
UNION ALL SELECT 'neo4j_sync_status', COUNT(*) FROM neo4j_sync_status
ORDER BY table_name;"

# ============================================================
# STEP 3: CSV 파일을 Docker 컨테이너에 복사
# ============================================================
echo ""
echo "========================================"
echo " STEP 3: CSV 파일을 Docker 컨테이너에 복사"
echo "========================================"

docker cp "$EXPORT_DIR/items.csv" "$CONTAINER":/tmp/items.csv
docker cp "$EXPORT_DIR/item_translations.csv" "$CONTAINER":/tmp/item_translations.csv
docker cp "$EXPORT_DIR/movie_details.csv" "$CONTAINER":/tmp/movie_details.csv
docker cp "$EXPORT_DIR/coffee_details.csv" "$CONTAINER":/tmp/coffee_details.csv
docker cp "$EXPORT_DIR/neo4j_sync_status.csv" "$CONTAINER":/tmp/neo4j_sync_status.csv
echo "CSV 파일 복사 완료"

# ============================================================
# STEP 4: RDS에서 마이그레이션 실행
# ============================================================
echo ""
echo "========================================"
echo " STEP 4: RDS 마이그레이션 실행"
echo "========================================"

# psql을 통해 임시 테이블 생성 → CSV 로드 → 마이그레이션 실행
docker exec -e PGPASSWORD="$RDS_PASS" "$CONTAINER" psql \
    -h "$RDS_HOST" -p "$RDS_PORT" -U "$RDS_USER" -d "$RDS_DB" <<'EOSQL'

BEGIN;

-- 임시 테이블 생성
CREATE TEMP TABLE _tmp_items (
    local_item_id BIGINT, category_id BIGINT, item_key VARCHAR(100),
    brand VARCHAR(100), image_url VARCHAR(500), external_link VARCHAR(500),
    external_service VARCHAR(50), is_active BOOLEAN,
    created_at TIMESTAMP, updated_at TIMESTAMP
);

CREATE TEMP TABLE _tmp_item_translations (
    local_translation_id BIGINT, local_item_id BIGINT,
    language_id BIGINT, item_value VARCHAR(255), description TEXT
);

CREATE TEMP TABLE _tmp_movie_details (
    local_item_id BIGINT, tmdb_id INTEGER, original_title VARCHAR(500),
    overview TEXT, release_date DATE, runtime INTEGER,
    vote_average NUMERIC(3,1), vote_count INTEGER, popularity NUMERIC(10,3),
    poster_path VARCHAR(255), genres JSONB, keywords JSONB,
    production_countries JSONB, original_language VARCHAR(10),
    cast_info JSONB, release_dates JSONB, content_type VARCHAR(20),
    tmdb_updated_at TIMESTAMP, created_at TIMESTAMP, updated_at TIMESTAMP
);

CREATE TEMP TABLE _tmp_coffee_details (
    local_item_id BIGINT, capsule_key VARCHAR(100), capsule_name VARCHAR(255),
    line VARCHAR(50), sub_category VARCHAR(100), intensity INTEGER,
    intensity_max INTEGER, cup_sizes JSONB, bean_type VARCHAR(100),
    origins JSONB, roast_level VARCHAR(100), aroma_profile JSONB,
    flavor_notes TEXT, body INTEGER, bitterness INTEGER,
    acidity INTEGER, roasting INTEGER, is_decaf BOOLEAN,
    is_limited_edition BOOLEAN, price_per_capsule_krw INTEGER,
    created_at TIMESTAMP, updated_at TIMESTAMP
);

CREATE TEMP TABLE _tmp_neo4j_sync (
    local_sync_id BIGINT, local_item_id BIGINT, sync_status VARCHAR(20),
    neo4j_node_id VARCHAR(100), last_synced_at TIMESTAMP,
    retry_count INTEGER, error_message TEXT,
    created_at TIMESTAMP, updated_at TIMESTAMP
);

COMMIT;
EOSQL

echo "임시 테이블 생성 완료"

# CSV 로드 (각각 \COPY 사용)
echo "CSV 로드 중..."

docker exec -e PGPASSWORD="$RDS_PASS" "$CONTAINER" psql \
    -h "$RDS_HOST" -p "$RDS_PORT" -U "$RDS_USER" -d "$RDS_DB" \
    -c "\COPY _tmp_items FROM '/tmp/items.csv' WITH (FORMAT csv, HEADER true)"

docker exec -e PGPASSWORD="$RDS_PASS" "$CONTAINER" psql \
    -h "$RDS_HOST" -p "$RDS_PORT" -U "$RDS_USER" -d "$RDS_DB" \
    -c "\COPY _tmp_item_translations FROM '/tmp/item_translations.csv' WITH (FORMAT csv, HEADER true)"

docker exec -e PGPASSWORD="$RDS_PASS" "$CONTAINER" psql \
    -h "$RDS_HOST" -p "$RDS_PORT" -U "$RDS_USER" -d "$RDS_DB" \
    -c "\COPY _tmp_movie_details FROM '/tmp/movie_details.csv' WITH (FORMAT csv, HEADER true)"

docker exec -e PGPASSWORD="$RDS_PASS" "$CONTAINER" psql \
    -h "$RDS_HOST" -p "$RDS_PORT" -U "$RDS_USER" -d "$RDS_DB" \
    -c "\COPY _tmp_coffee_details FROM '/tmp/coffee_details.csv' WITH (FORMAT csv, HEADER true)"

docker exec -e PGPASSWORD="$RDS_PASS" "$CONTAINER" psql \
    -h "$RDS_HOST" -p "$RDS_PORT" -U "$RDS_USER" -d "$RDS_DB" \
    -c "\COPY _tmp_neo4j_sync FROM '/tmp/neo4j_sync_status.csv' WITH (FORMAT csv, HEADER true)"

echo "CSV 로드 완료"

# 마이그레이션 쿼리 실행
echo ""
echo "마이그레이션 쿼리 실행 중..."

docker exec -e PGPASSWORD="$RDS_PASS" "$CONTAINER" psql \
    -h "$RDS_HOST" -p "$RDS_PORT" -U "$RDS_USER" -d "$RDS_DB" <<'EOSQL'

BEGIN;

-- items: 신규 아이템만 추가 (item_key 기준)
INSERT INTO items (category_id, item_key, brand, image_url, external_link,
                   external_service, is_active, created_at, updated_at)
SELECT t.category_id, t.item_key, t.brand, t.image_url, t.external_link,
       t.external_service, t.is_active, t.created_at, t.updated_at
FROM _tmp_items t
WHERE NOT EXISTS (SELECT 1 FROM items r WHERE r.item_key = t.item_key)
ORDER BY t.local_item_id;

-- ID 매핑 테이블
CREATE TEMP TABLE _tmp_id_map AS
SELECT t.local_item_id, r.item_id AS rds_item_id
FROM _tmp_items t
JOIN items r ON r.item_key = t.item_key;

-- movie_details
INSERT INTO movie_details (item_id, tmdb_id, original_title, overview, release_date,
                           runtime, vote_average, vote_count, popularity, poster_path,
                           genres, keywords, production_countries, original_language,
                           cast_info, release_dates, content_type, tmdb_updated_at,
                           created_at, updated_at)
SELECT m.rds_item_id, t.tmdb_id, t.original_title, t.overview, t.release_date,
       t.runtime, t.vote_average, t.vote_count, t.popularity, t.poster_path,
       t.genres, t.keywords, t.production_countries, t.original_language,
       t.cast_info, t.release_dates, t.content_type, t.tmdb_updated_at,
       t.created_at, t.updated_at
FROM _tmp_movie_details t
JOIN _tmp_id_map m ON m.local_item_id = t.local_item_id
WHERE NOT EXISTS (SELECT 1 FROM movie_details r WHERE r.tmdb_id = t.tmdb_id)
AND NOT EXISTS (SELECT 1 FROM movie_details r WHERE r.item_id = m.rds_item_id);

-- coffee_details
INSERT INTO coffee_details (item_id, capsule_key, capsule_name, line, sub_category,
                            intensity, intensity_max, cup_sizes, bean_type, origins,
                            roast_level, aroma_profile, flavor_notes, body, bitterness,
                            acidity, roasting, is_decaf, is_limited_edition,
                            price_per_capsule_krw, created_at, updated_at)
SELECT m.rds_item_id, t.capsule_key, t.capsule_name, t.line, t.sub_category,
       t.intensity, t.intensity_max, t.cup_sizes, t.bean_type, t.origins,
       t.roast_level, t.aroma_profile, t.flavor_notes, t.body, t.bitterness,
       t.acidity, t.roasting, t.is_decaf, t.is_limited_edition,
       t.price_per_capsule_krw, t.created_at, t.updated_at
FROM _tmp_coffee_details t
JOIN _tmp_id_map m ON m.local_item_id = t.local_item_id
WHERE NOT EXISTS (SELECT 1 FROM coffee_details r WHERE r.capsule_key = t.capsule_key)
AND NOT EXISTS (SELECT 1 FROM coffee_details r WHERE r.item_id = m.rds_item_id);

-- item_translations
INSERT INTO item_translations (item_id, language_id, item_value, description)
SELECT m.rds_item_id, t.language_id, t.item_value, t.description
FROM _tmp_item_translations t
JOIN _tmp_id_map m ON m.local_item_id = t.local_item_id
WHERE NOT EXISTS (
    SELECT 1 FROM item_translations r
    WHERE r.item_id = m.rds_item_id AND r.language_id = t.language_id
);

-- neo4j_sync_status
INSERT INTO neo4j_sync_status (item_id, sync_status, neo4j_node_id, last_synced_at,
                               retry_count, error_message, created_at, updated_at)
SELECT m.rds_item_id, t.sync_status, t.neo4j_node_id, t.last_synced_at,
       t.retry_count, t.error_message, t.created_at, t.updated_at
FROM _tmp_neo4j_sync t
JOIN _tmp_id_map m ON m.local_item_id = t.local_item_id
WHERE NOT EXISTS (SELECT 1 FROM neo4j_sync_status r WHERE r.item_id = m.rds_item_id);

-- 결과 확인
SELECT '=== 마이그레이션 후 RDS 데이터 현황 ===' AS info;
SELECT 'items' AS table_name, COUNT(*) AS row_count FROM items
UNION ALL SELECT 'item_translations', COUNT(*) FROM item_translations
UNION ALL SELECT 'movie_details', COUNT(*) FROM movie_details
UNION ALL SELECT 'coffee_details', COUNT(*) FROM coffee_details
UNION ALL SELECT 'neo4j_sync_status', COUNT(*) FROM neo4j_sync_status
ORDER BY table_name;

-- 정리
DROP TABLE IF EXISTS _tmp_id_map;
DROP TABLE IF EXISTS _tmp_items;
DROP TABLE IF EXISTS _tmp_item_translations;
DROP TABLE IF EXISTS _tmp_movie_details;
DROP TABLE IF EXISTS _tmp_coffee_details;
DROP TABLE IF EXISTS _tmp_neo4j_sync;

COMMIT;

SELECT '=== 마이그레이션 완료! ===' AS result;
EOSQL

# ============================================================
# STEP 5: Docker 컨테이너 임시 파일 정리
# ============================================================
echo ""
echo "========================================"
echo " STEP 5: 정리"
echo "========================================"
docker exec "$CONTAINER" rm -f /tmp/items.csv /tmp/item_translations.csv \
    /tmp/movie_details.csv /tmp/coffee_details.csv /tmp/neo4j_sync_status.csv
echo "컨테이너 내 임시 파일 정리 완료"

echo ""
echo "========================================"
echo " 마이그레이션 전체 완료!"
echo "========================================"
