"""
Ingestion script for the Data Engineering Control Plane.
Pulls data from 4 retail APIs and stores raw JSON in PostgreSQL.
Logs each run in ingestion_runs for monitoring.
"""

import os
import uuid
import json
import requests
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
AMAZON_API_HOST = os.getenv("AMAZON_API_HOST")
PRODUCT_SEARCH_API_HOST = os.getenv("PRODUCT_SEARCH_API_HOST")
WALMART_API_HOST = os.getenv("WALMART_API_HOST")

# Define all 4 API sources
API_SOURCES = [
    {
        "name": "openfoodfacts_snacks",
        "url": "https://world.openfoodfacts.org/api/v2/search?categories_tags_en=snacks&page_size=50",
        "headers": {},  # No key needed
    },
    {
        "name": "amazon_products",
        "url": "https://real-time-amazon-data.p.rapidapi.com/products-by-category?category_id=2478868012&country=US",
        "headers": {
            "X-RapidAPI-Key": RAPIDAPI_KEY,
            "X-RapidAPI-Host": AMAZON_API_HOST,
        },
    },
    {
        "name": "product_search_details",
        "url": "https://real-time-product-search.p.rapidapi.com/product-details-v2?product_id=catalogid%3A294133593583239326682%2Cproductid%3A%2Cgpcid%3A6219277726645206819&country=us&language=en",
        "headers": {
            "X-RapidAPI-Key": RAPIDAPI_KEY,
            "X-RapidAPI-Host": PRODUCT_SEARCH_API_HOST,
        },
    },
    {
        "name": "walmart_reviews",
        "url": "https://axesso-walmart-data-service.p.rapidapi.com/wlm/walmart-lookup-reviews?productId=5364974201&page=1&domainCode=com&sortBy=relevancy",
        "headers": {
            "X-RapidAPI-Key": RAPIDAPI_KEY,
            "X-RapidAPI-Host": WALMART_API_HOST,
        },
    },
]


def get_db_connection():
    """Create and return a database connection."""
    return psycopg2.connect(DATABASE_URL)


def ingest_source(conn, source):
    """
    Fetch data from one API source, store in raw_api_events,
    and log the run in ingestion_runs.
    """
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    source_name = source["name"]

    print(f"[{started_at.strftime('%H:%M:%S')}] Fetching {source_name}...", end=" ")

    try:
        # Call the API
        response = requests.get(source["url"], headers=source["headers"], timeout=30)
        response.raise_for_status()
        payload = response.json()

        # Store raw JSON in raw_api_events
        event_id = str(uuid.uuid4())
        fetched_at = datetime.now(timezone.utc)

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO raw_api_events (event_id, source, fetched_at, payload)
                VALUES (%s, %s, %s, %s)
                """,
                (event_id, source_name, fetched_at, json.dumps(payload)),
            )

        # Log successful run
        finished_at = datetime.now(timezone.utc)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ingestion_runs (run_id, source, started_at, finished_at, status, rows_written, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (run_id, source_name, started_at, finished_at, "SUCCESS", 1, None),
            )

        conn.commit()
        print(f"SUCCESS ({(finished_at - started_at).total_seconds():.2f}s)")

    except Exception as e:
        conn.rollback()
        finished_at = datetime.now(timezone.utc)
        error_msg = str(e)[:500]

        # Log failed run
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ingestion_runs (run_id, source, started_at, finished_at, status, rows_written, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (run_id, source_name, started_at, finished_at, "FAIL", 0, error_msg),
            )
        conn.commit()
        print(f"FAIL - {error_msg[:80]}")


def main():
    print("=" * 60)
    print("Data Engineering Control Plane - Ingestion Run")
    print(f"Started at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 60)

    conn = get_db_connection()
    try:
        for source in API_SOURCES:
            ingest_source(conn, source)
    finally:
        conn.close()

    print("=" * 60)
    print("Ingestion complete.")
    print("=" * 60)


if __name__ == "__main__":
    main()
