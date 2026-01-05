#!/usr/bin/env python3
"""
Seed Ayat Data to Supabase

Reads ayat from the local SQLite database and uploads to Supabase.
This is Phase 2 of the content generation pipeline.

Usage:
    python seed_ayat_to_supabase.py
"""

import os
import sys
import sqlite3
import json
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE = os.getenv("SUPABASE_SERVICE_ROLE")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE:
    print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_ROLE must be set in .env")
    sys.exit(1)

# Path to SQLite database
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
SQLITE_DB_PATH = PROJECT_ROOT / "quran_learn" / "assets" / "data" / "quran_content.db"

print(f"SQLite DB path: {SQLITE_DB_PATH}")
print(f"Supabase URL: {SUPABASE_URL}")


def get_ayat_from_sqlite():
    """Read all ayat from SQLite database."""
    conn = sqlite3.connect(SQLITE_DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get ayat with translations
    query = """
    SELECT
        a.id,
        a.surah_id,
        a.ayah_number,
        a.text_uthmani,
        a.text_simple,
        a.juz_number,
        a.page_number,
        t_en.translated_text as translation_en,
        t_id.translated_text as translation_id
    FROM ayat a
    LEFT JOIN translations t_en ON a.id = t_en.ayah_id AND t_en.language = 'en'
    LEFT JOIN translations t_id ON a.id = t_id.ayah_id AND t_id.language = 'id'
    ORDER BY a.surah_id, a.ayah_number
    """

    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]


def upload_to_supabase(ayat_list):
    """Upload ayat to Supabase using REST API."""
    import urllib.request
    import urllib.error

    url = f"{SUPABASE_URL}/rest/v1/ayat"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"  # Upsert behavior
    }

    # Prepare data for Supabase (match table schema)
    records = []
    for ayah in ayat_list:
        records.append({
            "id": ayah["id"],
            "surah_id": ayah["surah_id"],
            "ayah_number": ayah["ayah_number"],
            "text_uthmani": ayah["text_uthmani"],
            "text_simple": ayah["text_simple"],
            "juz_number": ayah["juz_number"],
            "page_number": ayah["page_number"],
            # context_summary_en and context_summary_id will be added later by AI generation
        })

    # Upload in batches of 100
    batch_size = 100
    total = len(records)

    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (total + batch_size - 1) // batch_size

        print(f"Uploading batch {batch_num}/{total_batches} ({len(batch)} records)...")

        data = json.dumps(batch).encode('utf-8')
        req = urllib.request.Request(url, data=data, headers=headers, method='POST')

        try:
            with urllib.request.urlopen(req) as response:
                if response.status in (200, 201):
                    print(f"  ✓ Batch {batch_num} uploaded successfully")
                else:
                    print(f"  ✗ Batch {batch_num} failed: {response.status}")
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8')
            print(f"  ✗ Batch {batch_num} failed: {e.code} - {error_body}")

            # If conflict, try one by one
            if e.code == 409:
                print("  Retrying records individually...")
                for record in batch:
                    try:
                        single_data = json.dumps([record]).encode('utf-8')
                        single_req = urllib.request.Request(url, data=single_data, headers=headers, method='POST')
                        with urllib.request.urlopen(single_req) as resp:
                            pass
                    except urllib.error.HTTPError:
                        # Record might already exist, that's OK
                        pass
        except Exception as e:
            print(f"  ✗ Batch {batch_num} error: {e}")


def verify_upload():
    """Verify data was uploaded correctly."""
    import urllib.request

    url = f"{SUPABASE_URL}/rest/v1/ayat?select=count"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE}",
        "Prefer": "count=exact"
    }

    req = urllib.request.Request(url, headers=headers)

    try:
        with urllib.request.urlopen(req) as response:
            count_header = response.headers.get('content-range', '')
            if '/' in count_header:
                count = count_header.split('/')[-1]
                print(f"\n✓ Verification: {count} ayat in Supabase")
                return int(count)
    except Exception as e:
        print(f"Verification failed: {e}")

    return 0


def main():
    print("=" * 60)
    print("PHASE 2: Seed Ayat Data to Supabase")
    print("=" * 60)

    # Check SQLite exists
    if not SQLITE_DB_PATH.exists():
        print(f"ERROR: SQLite database not found at {SQLITE_DB_PATH}")
        sys.exit(1)

    # Read from SQLite
    print("\n1. Reading ayat from SQLite...")
    ayat_list = get_ayat_from_sqlite()
    print(f"   Found {len(ayat_list)} ayat")

    # Show sample
    if ayat_list:
        sample = ayat_list[0]
        print(f"\n   Sample (1:1):")
        print(f"   - Arabic: {sample['text_uthmani'][:50]}...")
        print(f"   - EN: {sample['translation_en'][:50]}...")

    # Upload to Supabase
    print("\n2. Uploading to Supabase...")
    upload_to_supabase(ayat_list)

    # Verify
    print("\n3. Verifying upload...")
    count = verify_upload()

    if count == len(ayat_list):
        print("\n" + "=" * 60)
        print("SUCCESS! All ayat seeded to Supabase.")
        print("Next: Run generate_context_summaries.py for AI generation")
        print("=" * 60)
    else:
        print(f"\nWARNING: Expected {len(ayat_list)} but found {count}")


if __name__ == "__main__":
    main()
