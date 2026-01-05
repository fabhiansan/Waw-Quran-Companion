#!/usr/bin/env python3
"""
Generate Context Summaries using OpenRouter/DeepSeek

Generates "What this ayah talks about" summaries for all ayat
using OpenRouter's free DeepSeek model.

Usage:
    source venv/bin/activate
    python generate_context_summaries.py

Features:
    - Resume capability (tracks progress in JSON file)
    - Rate limiting with exponential backoff
    - Generates both English and Indonesian summaries
    - Updates Supabase ayat table
"""

import os
import sys
import json
import time
import sqlite3
import urllib.request
import urllib.error
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE = os.getenv("SUPABASE_SERVICE_ROLE")

if not all([OPENROUTER_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE]):
    print("ERROR: Required environment variables not set")
    print("  - OPENROUTER_API_KEY")
    print("  - SUPABASE_URL")
    print("  - SUPABASE_SERVICE_ROLE")
    sys.exit(1)

# Paths
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
SQLITE_DB_PATH = PROJECT_ROOT / "quran_learn" / "assets" / "data" / "quran_content.db"
PROGRESS_FILE = Path(__file__).parent / "generation_progress.json"

# OpenRouter config
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
# Free DeepSeek models available:
# - deepseek/deepseek-r1-0528:free (latest R1, reasoning-focused)
# - deepseek/deepseek-r1:free (R1 base)
# - nex-agi/deepseek-v3.1-nex-n1:free (optimized for text generation)
# - tngtech/deepseek-r1t2-chimera:free (20% faster than R1)
MODEL = "deepseek/deepseek-r1-0528:free"  # Latest free DeepSeek R1

# Rate limiting
MIN_DELAY = 2  # Minimum seconds between requests
MAX_RETRIES = 3
BACKOFF_MULTIPLIER = 2


def load_progress():
    """Load progress from JSON file."""
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {
        "last_completed_id": 0,
        "total_generated": 0,
        "errors": [],
        "started_at": datetime.now().isoformat()
    }


def save_progress(progress):
    """Save progress to JSON file."""
    progress["updated_at"] = datetime.now().isoformat()
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)


def get_ayat_without_summaries():
    """Get ayat IDs that don't have context_summary_en yet from Supabase."""
    url = f"{SUPABASE_URL}/rest/v1/ayat?select=id&context_summary_en=is.null&order=id"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE}",
    }

    req = urllib.request.Request(url, headers=headers)

    try:
        with urllib.request.urlopen(req) as response:
            result = json.loads(response.read().decode('utf-8'))
            return [r['id'] for r in result]
    except Exception as e:
        print(f"Warning: Could not check Supabase for existing summaries: {e}")
        return None  # Fall back to local progress tracking


def get_ayat_with_translations():
    """Get all ayat with translations from SQLite."""
    conn = sqlite3.connect(SQLITE_DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    query = """
    SELECT
        a.id,
        a.surah_id,
        a.ayah_number,
        a.text_uthmani,
        s.name_english as surah_name,
        t_en.translated_text as translation_en,
        t_id.translated_text as translation_id
    FROM ayat a
    JOIN surahs s ON a.surah_id = s.id
    LEFT JOIN translations t_en ON a.id = t_en.ayah_id AND t_en.language = 'en'
    LEFT JOIN translations t_id ON a.id = t_id.ayah_id AND t_id.language = 'id'
    ORDER BY a.id
    """

    cursor.execute(query)
    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]


def call_openrouter(prompt, language):
    """Call OpenRouter API with DeepSeek model."""
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://quran-learn.app",
        "X-Title": "Quran Learn App"
    }

    # Prompt designed based on brainstorming requirements:
    # - Core differentiator: understanding not recitation
    # - Explain Arabic beauty (balaghah) to non-Arabic speakers
    # - Accessible for beginners, not intimidating
    # - Relevant to daily life
    system_prompt = f"""You are a Quran scholar explaining verses to general Muslim readers who want to understand, not just recite.

Your task is to write a "What this ayah talks about" summary that:
1. Explains the MAIN MESSAGE of the ayah in 2-3 clear sentences
2. Conveys the beauty of the Arabic expression if relevant (balaghah)
3. Connects the message to daily life or spiritual practice when possible
4. Is accessible to beginners - avoid scholarly jargon

Keep it warm and inviting, like explaining to a curious friend.
Output ONLY the summary in {language}, no additional text, labels, or formatting."""

    data = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.7,
        "max_tokens": 1000  # R1 models use reasoning tokens + response tokens
    }

    req = urllib.request.Request(
        OPENROUTER_URL,
        data=json.dumps(data).encode('utf-8'),
        headers=headers,
        method='POST'
    )

    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            result = json.loads(response.read().decode('utf-8'))
            return result['choices'][0]['message']['content'].strip()
    except urllib.error.HTTPError as e:
        error_body = e.read().decode('utf-8')
        raise Exception(f"OpenRouter API error {e.code}: {error_body}")


def generate_summary(ayah, language):
    """Generate summary for a single ayah."""
    translation = ayah['translation_en'] if language == 'English' else ayah['translation_id']

    prompt = f"""Surah: {ayah['surah_name']} (Surah {ayah['surah_id']})
Ayah: {ayah['ayah_number']}
Arabic: {ayah['text_uthmani']}
Translation: {translation}

What does this ayah talk about?"""

    return call_openrouter(prompt, language)


def update_supabase(ayah_id, summary_en, summary_id):
    """Update Supabase with generated summaries."""
    url = f"{SUPABASE_URL}/rest/v1/ayat?id=eq.{ayah_id}"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }

    data = {
        "context_summary_en": summary_en,
        "context_summary_id": summary_id
    }

    req = urllib.request.Request(
        url,
        data=json.dumps(data).encode('utf-8'),
        headers=headers,
        method='PATCH'
    )

    with urllib.request.urlopen(req) as response:
        return response.status == 204


def process_ayah(ayah, progress):
    """Process a single ayah with retries."""
    ayah_id = ayah['id']
    surah_ayah = f"{ayah['surah_id']}:{ayah['ayah_number']}"

    for attempt in range(MAX_RETRIES):
        try:
            # Generate English summary
            print(f"  Generating EN summary...", end=" ", flush=True)
            summary_en = generate_summary(ayah, "English")
            print("✓")

            time.sleep(MIN_DELAY)  # Rate limiting

            # Generate Indonesian summary
            print(f"  Generating ID summary...", end=" ", flush=True)
            summary_id = generate_summary(ayah, "Indonesian")
            print("✓")

            # Update Supabase
            print(f"  Updating Supabase...", end=" ", flush=True)
            update_supabase(ayah_id, summary_en, summary_id)
            print("✓")

            # Success
            progress["last_completed_id"] = ayah_id
            progress["total_generated"] += 1
            save_progress(progress)

            return True

        except Exception as e:
            wait_time = MIN_DELAY * (BACKOFF_MULTIPLIER ** attempt)
            print(f"\n  ✗ Attempt {attempt + 1} failed: {e}")

            if attempt < MAX_RETRIES - 1:
                print(f"  Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                progress["errors"].append({
                    "ayah_id": ayah_id,
                    "surah_ayah": surah_ayah,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                })
                save_progress(progress)
                return False

    return False


def main():
    print("=" * 60)
    print("PHASE 4-5: Generate Context Summaries with DeepSeek")
    print("=" * 60)
    print(f"Model: {MODEL}")
    print(f"Progress file: {PROGRESS_FILE}")

    # Load progress
    progress = load_progress()

    # Check Supabase for ayat without summaries (primary method)
    print("\n1. Checking Supabase for ayat without summaries...")
    missing_ids = get_ayat_without_summaries()

    # Get all ayat data
    print("2. Loading ayat data from SQLite...")
    ayat_list = get_ayat_with_translations()
    total = len(ayat_list)
    print(f"   Found {total} ayat total")

    # Filter to remaining ayat
    if missing_ids is not None:
        # Use Supabase check (more reliable)
        remaining = [a for a in ayat_list if a['id'] in missing_ids]
        already_done = total - len(remaining)
        print(f"   Already have summaries: {already_done} ayat")
        print(f"   Need summaries: {len(remaining)} ayat")
    else:
        # Fall back to local progress file
        last_id = progress["last_completed_id"]
        remaining = [a for a in ayat_list if a['id'] > last_id]
        print(f"   Remaining (from local progress): {len(remaining)} ayat")

    if not remaining:
        print("\n✓ All ayat already have summaries!")
        return

    # Process each ayah
    print("\n3. Generating summaries...")
    print("-" * 60)

    for i, ayah in enumerate(remaining, 1):
        ayah_id = ayah['id']
        surah_ayah = f"{ayah['surah_id']}:{ayah['ayah_number']}"
        current = progress["total_generated"] + 1

        print(f"\n[{current}/{total}] Ayah {surah_ayah} (ID: {ayah_id})")

        success = process_ayah(ayah, progress)

        if success:
            print(f"  ✓ Complete")
        else:
            print(f"  ✗ Failed (will skip)")

        # Rate limiting between ayat
        time.sleep(MIN_DELAY)

    # Summary
    print("\n" + "=" * 60)
    print("GENERATION COMPLETE")
    print("=" * 60)
    print(f"Total generated: {progress['total_generated']}")
    print(f"Errors: {len(progress['errors'])}")

    if progress['errors']:
        print("\nFailed ayat:")
        for err in progress['errors']:
            print(f"  - {err['surah_ayah']}: {err['error'][:50]}...")


if __name__ == "__main__":
    main()
