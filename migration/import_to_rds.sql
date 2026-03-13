-- =============================================================
-- movie_details + coffee_details RDS 마이그레이션
--
-- [사전 준비]
--   CSV 3개를 RDS 접속 가능한 환경에 준비:
--     - items_video_coffee.csv (388행)
--     - movie_details.csv      (304행)
--     - coffee_details.csv     (84행)
--
-- [실행 방법]
--   psql 로 RDS 접속 후:
--
--   1) 이 SQL 파일 중 STEP 1~2 실행 (임시 테이블 생성)
--   2) \COPY 명령 3개 실행 (CSV 로드)
--   3) STEP 3 실행 (마이그레이션)
--   4) STEP 4 실행 (결과 확인 + 정리)
--
-- [참고]
--   - category_id는 이미 RDS 기준으로 매핑됨 (video=1, coffee=4)
--   - items.item_id는 RDS에서 자동 생성 (generated always as identity)
--   - 기존 movie_details, coffee_details는 삭제 후 재삽입
-- =============================================================

-- =====================
-- STEP 1: 기존 데이터 삭제
-- =====================
DELETE FROM movie_details;
DELETE FROM coffee_details;

-- =====================
-- STEP 2: 임시 테이블 생성
-- =====================
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

-- =============================================
-- CSV 로드 (psql에서 아래 3줄을 실행)
-- 경로는 CSV 파일 위치에 맞게 수정!
-- =============================================
-- \COPY _tmp_items FROM 'items_video_coffee.csv' WITH (FORMAT csv, HEADER true);
-- \COPY _tmp_movie_details FROM 'movie_details.csv' WITH (FORMAT csv, HEADER true);
-- \COPY _tmp_coffee_details FROM 'coffee_details.csv' WITH (FORMAT csv, HEADER true);

-- =====================
-- STEP 3: 마이그레이션
-- =====================
BEGIN;

-- 3-1. RDS에 없는 items 추가 (item_id 자동 생성)
INSERT INTO items (category_id, item_key, brand, image_url, external_link,
                   external_service, is_active, created_at, updated_at)
SELECT t.category_id, t.item_key, t.brand, t.image_url, t.external_link,
       t.external_service, t.is_active, t.created_at, t.updated_at
FROM _tmp_items t
WHERE NOT EXISTS (SELECT 1 FROM items r WHERE r.item_key = t.item_key)
ORDER BY t.local_item_id;

-- 3-2. local_item_id → RDS item_id 매핑
CREATE TEMP TABLE _tmp_id_map AS
SELECT t.local_item_id, r.item_id AS rds_item_id
FROM _tmp_items t
JOIN items r ON r.item_key = t.item_key;

-- 3-3. movie_details 삽입
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

-- 3-4. coffee_details 삽입
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

-- =====================
-- STEP 4: 결과 확인 + 정리
-- =====================
SELECT 'items (total)' AS "테이블", COUNT(*) AS "행수" FROM items
UNION ALL SELECT 'items (video)', COUNT(*) FROM items WHERE category_id = 1
UNION ALL SELECT 'items (coffee)', COUNT(*) FROM items WHERE category_id = 4
UNION ALL SELECT 'movie_details', COUNT(*) FROM movie_details
UNION ALL SELECT 'coffee_details', COUNT(*) FROM coffee_details
ORDER BY "테이블";

DROP TABLE IF EXISTS _tmp_id_map;
DROP TABLE IF EXISTS _tmp_items;
DROP TABLE IF EXISTS _tmp_movie_details;
DROP TABLE IF EXISTS _tmp_coffee_details;
