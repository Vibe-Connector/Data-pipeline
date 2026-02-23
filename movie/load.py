"""
Vibe-Link 영화 도메인 — Load (PostgreSQL 저장)

items → movie_details → item_translations(×3) → neo4j_sync_status Upsert를 담당합니다.
v3.1: 3개 언어 루프, 빈 JSON NULL 처리
"""

from psycopg2.extras import Json

from common.logging_config import get_logger

log = get_logger("movie.load")


def load_movie_to_postgres(conn, movie: dict) -> int | None:
    """v3.1: items → movie_details → item_translations(3언어) → neo4j_sync_status Upsert

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
                image_url = EXCLUDED.image_url,
                external_link = EXCLUDED.external_link,
                updated_at = CURRENT_TIMESTAMP
            RETURNING item_id
        """, movie)
        item_id = cur.fetchone()[0]

        # 2. movie_details Upsert (v3.1: 빈 JSON → NULL)
        cur.execute("""
            INSERT INTO movie_details (
                item_id, tmdb_id, original_title, overview,
                release_date, runtime, vote_average, vote_count, popularity,
                poster_path, genres, keywords,
                production_countries, original_language, cast_info,
                release_dates, content_type, tmdb_updated_at
            ) VALUES (
                %(item_id)s, %(tmdb_id)s, %(original_title)s, %(overview)s,
                %(release_date)s, %(runtime)s, %(vote_average)s, %(vote_count)s, %(popularity)s,
                %(poster_path)s, %(genres)s, %(keywords)s,
                %(production_countries)s, %(original_language)s, %(cast_info)s,
                %(release_dates)s, %(content_type)s, %(tmdb_updated_at)s
            )
            ON CONFLICT (tmdb_id) DO UPDATE SET
                overview = EXCLUDED.overview,
                vote_average = EXCLUDED.vote_average,
                vote_count = EXCLUDED.vote_count,
                popularity = EXCLUDED.popularity,
                release_dates = EXCLUDED.release_dates,
                cast_info = EXCLUDED.cast_info,
                tmdb_updated_at = EXCLUDED.tmdb_updated_at,
                updated_at = CURRENT_TIMESTAMP
        """, {
            "item_id": item_id,
            "tmdb_id": movie["tmdb_id"],
            "original_title": movie["original_title"][:255],
            "overview": movie["overview"],
            "release_date": movie["release_date"],
            "runtime": movie["runtime"],
            "vote_average": movie["vote_average"],
            "vote_count": movie["vote_count"],
            "popularity": movie["popularity"],
            "poster_path": movie["poster_path"],
            "genres": Json(movie["genres"]) if movie["genres"] else None,
            "keywords": Json(movie["keywords"]) if movie["keywords"] else None,
            "production_countries": Json(movie["production_countries"]) if movie["production_countries"] else None,
            "original_language": movie["original_language"],
            "cast_info": Json(movie["cast_info"]) if movie["cast_info"] else None,
            "release_dates": Json(movie["release_dates"]) if movie["release_dates"] else None,
            "content_type": movie["content_type"],
            "tmdb_updated_at": movie["tmdb_updated_at"],
        })

        # 3. item_translations — 3개 언어 루프
        for lang_code, data in movie["translations"].items():
            if not data["title"]:
                continue
            cur.execute("""
                INSERT INTO item_translations (item_id, language_id, item_value, description)
                VALUES (
                    %(item_id)s,
                    (SELECT language_id FROM languages WHERE language_code = %(lang)s),
                    %(title)s, %(description)s
                )
                ON CONFLICT (item_id, language_id) DO UPDATE SET
                    item_value = EXCLUDED.item_value,
                    description = EXCLUDED.description
            """, {
                "item_id": item_id,
                "lang": lang_code,
                "title": data["title"][:255],
                "description": data["description"],
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
        log.error(f"  DB 저장 실패 [{movie.get('item_key')}]: {e}")
        return None
    finally:
        cur.close()