# 로컬 DB vs 팀 RDS 비교 리포트

## 1. 연결 정보

| 구분 | Host | Port | DB Name | User |
|------|------|------|---------|------|
| 로컬 (Docker) | localhost | 5432 | vibelink | vibelink |
| 팀 RDS | vibeconnector.c1y2iaugs33c.ap-northeast-2.rds.amazonaws.com | 5432 | postgres | postgres |

## 2. 테이블 목록 비교

양쪽 모두 동일한 44개 테이블 보유 (테이블 목록 일치)

## 3. 스키마 차이점

### 3-1. RDS에만 존재하는 컬럼 (로컬에 없음)

| 테이블 | 컬럼 | 타입 | Nullable | Default |
|--------|------|------|----------|---------|
| archive_folders | folder_type | varchar(10) | NOT NULL | 'VIBE' |
| archive_items | folder_id | bigint | YES | - |
| favorites | archive_item_id | bigint | YES | - |
| items | embedding | vector(1536) | YES | - |
| users | country | varchar(10) | YES | - |
| users | timezone | varchar(50) | YES | - |

### 3-2. ID 생성 방식 차이

| 구분 | 로컬 | RDS |
|------|------|-----|
| items.item_id | nextval('items_item_id_seq') | generated always as identity |
| item_translations.translation_id | nextval() | generated always as identity (추정) |
| 기타 ID 컬럼 | nextval() | NULL (App에서 관리) |

### 3-3. Nullable 차이

| 테이블.컬럼 | 로컬 | RDS |
|-------------|------|-----|
| favorites.archive_id | NOT NULL | NULL 허용 |

## 4. 참조 테이블 ID 매핑 차이 (중요!)

### 4-1. item_categories (category_id 불일치)

| category_key | 로컬 category_id | RDS category_id |
|-------------|-------------------|-----------------|
| video | 1 | 1 |
| music | 2 | 2 |
| **coffee** | **3** | **4** |
| **lighting** | **4** | **3** |
| book | 5 | 5 |
| food~tech | 6~15 | 6~15 |

> coffee와 lighting의 ID가 뒤바뀌어 있음!

### 4-2. languages (language_id 불일치)

| language_code | 로컬 language_id | RDS language_id |
|--------------|------------------|-----------------|
| ko | 1 | 1 |
| en | 2 | 2 |
| **zh** | **3** | **4** |
| **ja** | **4** | **3** |
| es | 5 | 5 |
| fr~id | 6~15 | 6~15 |

> zh와 ja의 ID가 뒤바뀌어 있음!

## 5. 데이터 비교

### 5-1. 데이터가 로컬에 더 많은 테이블 (마이그레이션 대상)

| 테이블 | 로컬 행수 | RDS 행수 | 차이 |
|--------|----------|---------|------|
| items | 418 | 60 | +358 |
| item_translations | 1,108 | 15 | +1,093 |
| movie_details | 304 | 15 | +289 |
| neo4j_sync_status | 388 | 15 | +373 |
| coffee_details | 84 | 15 | +69 |

### 5-2. item_key 기준 중복 분석

- 공통 item_key: 49개 (같은 아이템이 양쪽에 다른 ID로 존재)
- 로컬에만 있는 item_key: **369개** (신규 마이그레이션 대상)
- RDS에만 있는 item_key: 11개

### 5-3. 카테고리별 아이템 분포

| category_key | 로컬 | RDS |
|-------------|------|-----|
| video (영화) | 304 | 15 |
| coffee (커피) | 84 | 15 |
| music (음악) | 15 | 15 |
| lighting (조명) | 15 | 15 |

## 6. 마이그레이션 시 주의사항

1. **ID 매핑 필수**: category_id, language_id가 다르므로 변환 필요
2. **Identity Column**: RDS의 items는 `generated always as identity`이므로 `OVERRIDING SYSTEM VALUE` 필요
3. **중복 처리**: item_key 기준 ON CONFLICT DO NOTHING 사용
4. **FK 의존성 순서**: item_categories → items → (movie_details, coffee_details, item_translations, neo4j_sync_status)
5. **embedding 컬럼**: RDS에만 있으므로 items INSERT 시 해당 컬럼 제외
