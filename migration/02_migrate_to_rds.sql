-- =============================================================
-- 로컬 Data-pipeline DB → 팀 RDS 마이그레이션 스크립트
--
-- 실행 방법:
--   1. 먼저 01_export_local_data.sh 실행하여 CSV 내보내기
--   2. 03_run_migration.sh 실행 (이 SQL을 자동 실행함)
--
-- 주의사항:
--   - category_id 매핑: 로컬 coffee=3→RDS 4, lighting=4→RDS 3
--   - language_id 매핑: 로컬 zh=3→RDS 4, ja=4→RDS 3
--   - RDS items는 generated always as identity 사용
--   - item_key 기준으로 중복 체크 (ON CONFLICT DO NOTHING)
-- =============================================================

BEGIN;

-- ============================================================
-- STEP 1: 임시 테이블 생성 (CSV 데이터 로드용)
-- ============================================================

DROP TABLE IF EXISTS _tmp_items CASCADE;
CREATE TEMP TABLE _tmp_items (
    local_item_id   BIGINT,
    category_id     BIGINT,      -- 이미 매핑된 값
    item_key        VARCHAR(100),
    brand           VARCHAR(100),
    image_url       VARCHAR(500),
    external_link   VARCHAR(500),
    external_service VARCHAR(50),
    is_active       BOOLEAN,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);

DROP TABLE IF EXISTS _tmp_item_translations CASCADE;
CREATE TEMP TABLE _tmp_item_translations (
    local_translation_id BIGINT,
    local_item_id        BIGINT,
    language_id          BIGINT,  -- 이미 매핑된 값
    item_value           VARCHAR(255),
    description          TEXT
);

DROP TABLE IF EXISTS _tmp_movie_details CASCADE;
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

DROP TABLE IF EXISTS _tmp_coffee_details CASCADE;
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

DROP TABLE IF EXISTS _tmp_neo4j_sync CASCADE;
CREATE TEMP TABLE _tmp_neo4j_sync (
    local_sync_id   BIGINT,
    local_item_id   BIGINT,
    sync_status     VARCHAR(20),
    neo4j_node_id   VARCHAR(100),
    last_synced_at  TIMESTAMP,
    retry_count     INTEGER,
    error_message   TEXT,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);

-- ============================================================
-- STEP 2: CSV 데이터 로드 (03_run_migration.sh 에서 \COPY 실행)
-- 이 부분은 psql \COPY 로 실행되므로 여기서는 주석만
-- ============================================================
-- \COPY _tmp_items FROM 'exported_data/items.csv' WITH (FORMAT csv, HEADER true);
-- \COPY _tmp_item_translations FROM 'exported_data/item_translations.csv' WITH (FORMAT csv, HEADER true);
-- \COPY _tmp_movie_details FROM 'exported_data/movie_details.csv' WITH (FORMAT csv, HEADER true);
-- \COPY _tmp_coffee_details FROM 'exported_data/coffee_details.csv' WITH (FORMAT csv, HEADER true);
-- \COPY _tmp_neo4j_sync FROM 'exported_data/neo4j_sync_status.csv' WITH (FORMAT csv, HEADER true);

-- ============================================================
-- STEP 3: items 테이블에 신규 아이템 추가
-- item_key 기준 중복이면 SKIP
-- ============================================================

-- RDS의 identity column을 사용하여 자동 ID 부여
INSERT INTO items (category_id, item_key, brand, image_url, external_link,
                   external_service, is_active, created_at, updated_at)
SELECT t.category_id, t.item_key, t.brand, t.image_url, t.external_link,
       t.external_service, t.is_active, t.created_at, t.updated_at
FROM _tmp_items t
WHERE NOT EXISTS (
    SELECT 1 FROM items r WHERE r.item_key = t.item_key
)
ORDER BY t.local_item_id;

-- 결과 확인
DO $$
DECLARE
    v_count BIGINT;
BEGIN
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE '[items] % 행 추가됨', v_count;
END $$;

-- ============================================================
-- STEP 4: item_id 매핑 테이블 생성 (로컬 ID → RDS ID)
-- ============================================================

DROP TABLE IF EXISTS _tmp_id_map;
CREATE TEMP TABLE _tmp_id_map AS
SELECT t.local_item_id, r.item_id AS rds_item_id
FROM _tmp_items t
JOIN items r ON r.item_key = t.item_key;

-- 매핑 확인
-- SELECT COUNT(*) AS mapped_count FROM _tmp_id_map;

-- ============================================================
-- STEP 5: movie_details 추가 (tmdb_id 기준 중복 SKIP)
-- ============================================================

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
WHERE NOT EXISTS (
    SELECT 1 FROM movie_details r WHERE r.tmdb_id = t.tmdb_id
)
AND NOT EXISTS (
    SELECT 1 FROM movie_details r WHERE r.item_id = m.rds_item_id
);

DO $$
DECLARE
    v_count BIGINT;
BEGIN
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE '[movie_details] % 행 추가됨', v_count;
END $$;

-- ============================================================
-- STEP 6: coffee_details 추가 (capsule_key 기준 중복 SKIP)
-- ============================================================

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
WHERE NOT EXISTS (
    SELECT 1 FROM coffee_details r WHERE r.capsule_key = t.capsule_key
)
AND NOT EXISTS (
    SELECT 1 FROM coffee_details r WHERE r.item_id = m.rds_item_id
);

DO $$
DECLARE
    v_count BIGINT;
BEGIN
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE '[coffee_details] % 행 추가됨', v_count;
END $$;

-- ============================================================
-- STEP 7: item_translations 추가
-- (item_id + language_id) UNIQUE 제약 기준 중복 SKIP
-- ============================================================

INSERT INTO item_translations (item_id, language_id, item_value, description)
SELECT m.rds_item_id, t.language_id, t.item_value, t.description
FROM _tmp_item_translations t
JOIN _tmp_id_map m ON m.local_item_id = t.local_item_id
WHERE NOT EXISTS (
    SELECT 1 FROM item_translations r
    WHERE r.item_id = m.rds_item_id AND r.language_id = t.language_id
);

DO $$
DECLARE
    v_count BIGINT;
BEGIN
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE '[item_translations] % 행 추가됨', v_count;
END $$;

-- ============================================================
-- STEP 8: neo4j_sync_status 추가
-- (item_id) UNIQUE 제약 기준 중복 SKIP
-- ============================================================

INSERT INTO neo4j_sync_status (item_id, sync_status, neo4j_node_id, last_synced_at,
                               retry_count, error_message, created_at, updated_at)
SELECT m.rds_item_id, t.sync_status, t.neo4j_node_id, t.last_synced_at,
       t.retry_count, t.error_message, t.created_at, t.updated_at
FROM _tmp_neo4j_sync t
JOIN _tmp_id_map m ON m.local_item_id = t.local_item_id
WHERE NOT EXISTS (
    SELECT 1 FROM neo4j_sync_status r WHERE r.item_id = m.rds_item_id
);

DO $$
DECLARE
    v_count BIGINT;
BEGIN
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE '[neo4j_sync_status] % 행 추가됨', v_count;
END $$;

-- ============================================================
-- STEP 9: 결과 확인
-- ============================================================

SELECT '=== 마이그레이션 후 RDS 데이터 현황 ===' AS info;
SELECT 'items' AS table_name, COUNT(*) AS row_count FROM items
UNION ALL
SELECT 'item_translations', COUNT(*) FROM item_translations
UNION ALL
SELECT 'movie_details', COUNT(*) FROM movie_details
UNION ALL
SELECT 'coffee_details', COUNT(*) FROM coffee_details
UNION ALL
SELECT 'neo4j_sync_status', COUNT(*) FROM neo4j_sync_status
ORDER BY table_name;

-- ============================================================
-- STEP 10: 임시 테이블 정리
-- ============================================================
DROP TABLE IF EXISTS _tmp_id_map;
DROP TABLE IF EXISTS _tmp_items;
DROP TABLE IF EXISTS _tmp_item_translations;
DROP TABLE IF EXISTS _tmp_movie_details;
DROP TABLE IF EXISTS _tmp_coffee_details;
DROP TABLE IF EXISTS _tmp_neo4j_sync;

COMMIT;

SELECT '=== 마이그레이션 완료! ===' AS result;
