#!/bin/bash
# =============================================================
# movie_details + coffee_details 마이그레이션 스크립트
#
# 동작:
#   1. RDS의 기존 movie_details, coffee_details 전체 삭제
#   2. 로컬의 items (video/coffee) 중 RDS에 없는 것만 추가
#   3. item_key 기준으로 로컬 item_id → RDS item_id 매핑
#   4. 로컬의 movie_details, coffee_details를 RDS에 삽입
#
# 사용법: bash migrate_movie_coffee.sh
# =============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
EXPORT_DIR="$SCRIPT_DIR/exported_data"

CONTAINER="vibe-link-pg"
RDS_HOST="vibeconnector.c1y2iaugs33c.ap-northeast-2.rds.amazonaws.com"
RDS_PORT="5432"
RDS_DB="postgres"
RDS_USER="postgres"
RDS_PASS='Bang)806'

# RDS psql 실행 헬퍼
rds_psql() {
    docker exec -e PGPASSWORD="$RDS_PASS" "$CONTAINER" psql \
        -h "$RDS_HOST" -p "$RDS_PORT" -U "$RDS_USER" -d "$RDS_DB" "$@"
}

echo "========================================"
echo " STEP 0: 마이그레이션 전 RDS 현황"
echo "========================================"
rds_psql -c "
SELECT 'items (video)' AS table_name, COUNT(*) FROM items WHERE category_id = 1
UNION ALL SELECT 'items (coffee)', COUNT(*) FROM items WHERE category_id = 4
UNION ALL SELECT 'movie_details', COUNT(*) FROM movie_details
UNION ALL SELECT 'coffee_details', COUNT(*) FROM coffee_details;"

echo ""
echo "========================================"
echo " STEP 1: RDS movie_details, coffee_details 삭제"
echo "========================================"
rds_psql -c "
DELETE FROM movie_details;
DELETE FROM coffee_details;"
echo "삭제 완료"

echo ""
echo "========================================"
echo " STEP 2: CSV를 Docker 컨테이너로 복사"
echo "========================================"
docker cp "$EXPORT_DIR/items_video_coffee.csv" "$CONTAINER":/tmp/items_video_coffee.csv
docker cp "$EXPORT_DIR/movie_details.csv" "$CONTAINER":/tmp/movie_details.csv
docker cp "$EXPORT_DIR/coffee_details.csv" "$CONTAINER":/tmp/coffee_details.csv
echo "CSV 복사 완료"

echo ""
echo "========================================"
echo " STEP 3: 임시 테이블 생성 + CSV 로드"
echo "========================================"
rds_psql <<'EOSQL'
-- 임시 테이블 생성
CREATE TEMP TABLE _tmp_items (
    local_item_id    BIGINT,
    category_id      BIGINT,
    item_key         VARCHAR(100),
    brand            VARCHAR(100),
    image_url        VARCHAR(500),
    external_link    VARCHAR(500),
    external_service VARCHAR(50),
    is_active        BOOLEAN,
    created_at       TIMESTAMP,
    updated_at       TIMESTAMP
);

CREATE TEMP TABLE _tmp_movie_details (
    local_item_id        BIGINT,
    tmdb_id              INTEGER,
    original_title       VARCHAR(500),
    overview             TEXT,
    release_date         DATE,
    runtime              INTEGER,
    vote_average         NUMERIC(3,1),
    vote_count           INTEGER,
    popularity           NUMERIC(10,3),
    poster_path          VARCHAR(255),
    genres               JSONB,
    keywords             JSONB,
    production_countries JSONB,
    original_language    VARCHAR(10),
    cast_info            JSONB,
    release_dates        JSONB,
    content_type         VARCHAR(20),
    tmdb_updated_at      TIMESTAMP,
    created_at           TIMESTAMP,
    updated_at           TIMESTAMP
);

CREATE TEMP TABLE _tmp_coffee_details (
    local_item_id         BIGINT,
    capsule_key           VARCHAR(100),
    capsule_name          VARCHAR(255),
    line                  VARCHAR(50),
    sub_category          VARCHAR(100),
    intensity             INTEGER,
    intensity_max         INTEGER,
    cup_sizes             JSONB,
    bean_type             VARCHAR(100),
    origins               JSONB,
    roast_level           VARCHAR(100),
    aroma_profile         JSONB,
    flavor_notes          TEXT,
    body                  INTEGER,
    bitterness            INTEGER,
    acidity               INTEGER,
    roasting              INTEGER,
    is_decaf              BOOLEAN,
    is_limited_edition    BOOLEAN,
    price_per_capsule_krw INTEGER,
    created_at            TIMESTAMP,
    updated_at            TIMESTAMP
);
EOSQL
echo "임시 테이블 생성 완료"

# CSV 로드
rds_psql -c "\COPY _tmp_items FROM '/tmp/items_video_coffee.csv' WITH (FORMAT csv, HEADER true)"
rds_psql -c "\COPY _tmp_movie_details FROM '/tmp/movie_details.csv' WITH (FORMAT csv, HEADER true)"
rds_psql -c "\COPY _tmp_coffee_details FROM '/tmp/coffee_details.csv' WITH (FORMAT csv, HEADER true)"
echo "CSV 로드 완료"

echo ""
echo "========================================"
echo " STEP 4: 마이그레이션 실행"
echo "========================================"
rds_psql <<'EOSQL'
BEGIN;

-- 4-1. RDS에 없는 새 items 추가 (item_id는 자동 생성)
INSERT INTO items (category_id, item_key, brand, image_url, external_link,
                   external_service, is_active, created_at, updated_at)
SELECT t.category_id, t.item_key, t.brand, t.image_url, t.external_link,
       t.external_service, t.is_active, t.created_at, t.updated_at
FROM _tmp_items t
WHERE NOT EXISTS (SELECT 1 FROM items r WHERE r.item_key = t.item_key)
ORDER BY t.local_item_id;

-- 4-2. 로컬 item_id → RDS item_id 매핑 테이블
CREATE TEMP TABLE _tmp_id_map AS
SELECT t.local_item_id, r.item_id AS rds_item_id
FROM _tmp_items t
JOIN items r ON r.item_key = t.item_key;

-- 4-3. movie_details 삽입
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
JOIN _tmp_id_map m ON m.local_item_id = t.local_item_id;

-- 4-4. coffee_details 삽입
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
JOIN _tmp_id_map m ON m.local_item_id = t.local_item_id;

COMMIT;

-- 결과 확인
SELECT '=== 마이그레이션 후 RDS 현황 ===' AS info;
SELECT 'items (total)' AS table_name, COUNT(*) FROM items
UNION ALL SELECT 'items (video)', COUNT(*) FROM items WHERE category_id = 1
UNION ALL SELECT 'items (coffee)', COUNT(*) FROM items WHERE category_id = 4
UNION ALL SELECT 'movie_details', COUNT(*) FROM movie_details
UNION ALL SELECT 'coffee_details', COUNT(*) FROM coffee_details
ORDER BY table_name;

-- 정리
DROP TABLE IF EXISTS _tmp_id_map;
DROP TABLE IF EXISTS _tmp_items;
DROP TABLE IF EXISTS _tmp_movie_details;
DROP TABLE IF EXISTS _tmp_coffee_details;
EOSQL

echo ""
echo "========================================"
echo " STEP 5: 정리"
echo "========================================"
docker exec "$CONTAINER" rm -f /tmp/items_video_coffee.csv /tmp/movie_details.csv /tmp/coffee_details.csv
echo "컨테이너 임시 파일 정리 완료"

echo ""
echo "========================================"
echo " 마이그레이션 완료!"
echo "========================================"
