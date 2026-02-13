# utils.py
import os
import hashlib
import time
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from bs4 import BeautifulSoup
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
import instaloader
from openai import AsyncOpenAI, OpenAIError
import json
import urllib.parse
import re
import html
from html import unescape
from googleapiclient.discovery import build
import random
from ntscraper import Nitter
import json
# ================ CONFIG ================
DB_URL = os.getenv("DATABASE_URL")                       # main cache DB (social posts)
TG_DB_URL = os.getenv("USERS_DATABASE_URL") or os.getenv("TG_DB_URL")   # separate TG DB
CACHE_HOURS = 24
POST_LIMIT = 10
GROQ_API_KEY=os.getenv("GROQ_KEY")
RAPIDAPI_KEY = os.getenv("RAPID_API")
RAPIDAPI_HOST = 'facebook-pages-scraper2.p.rapidapi.com'
RAPIDAPI_BASE = f"https://{RAPIDAPI_HOST}"
RAPIDAPIHOST = "twitter-x-api.p.rapidapi.com"
APIFY_FALLBACK_TIMEOUT = 8
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
APIFY_API_TOKEN = os.getenv("APIFY")  # Add your Apify token to env
APIFY_ACTOR_ID = "apidojo~tweet-scraper"
APIFY_BASE = "https://api.apify.com/v2"
TWEETS_URL = "https://twitter-x-api.p.rapidapi.com/api/user/tweets"
# ================ DB CONNECTIONS ============
def get_db():
    if not DB_URL:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(DB_URL, cursor_factory=RealDictCursor)

def get_tg_db():
    if not TG_DB_URL:
        raise RuntimeError("USERS_DATABASE_URL / TG_DB_URL not set")
    return psycopg2.connect(TG_DB_URL, cursor_factory=RealDictCursor)

# ================ INIT TABLES ================
def init_tg_db():
    """
    Create/patch tg-related tables and required columns idempotently.
    Safe to call every startup.
    """
    conn = None
    try:
        conn = get_tg_db()
        cur = conn.cursor()

        # Core table (create if missing)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS tg_users (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT UNIQUE NOT NULL,
            first_name TEXT,
            is_active INTEGER DEFAULT 1,
            is_banned INTEGER DEFAULT 0,
            request_count INTEGER DEFAULT 0,
            last_request_at TIMESTAMP,
            joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Ensure optional columns exist (safe on existing DBs)
        cur.execute("ALTER TABLE tg_users ADD COLUMN IF NOT EXISTS invite_count INTEGER DEFAULT 0;")
        cur.execute("ALTER TABLE tg_users ADD COLUMN IF NOT EXISTS is_admin INTEGER DEFAULT 0;")

        # saved_accounts table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS saved_accounts (
            id SERIAL PRIMARY KEY,
            owner_telegram_id BIGINT NOT NULL,
            platform TEXT NOT NULL CHECK (platform IN ('x', 'ig', 'fb')),
            account_name TEXT NOT NULL,
            label TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(owner_telegram_id, platform, account_name)
        );
        """)

        # Rate limits table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS tg_rate_limits (
            telegram_id BIGINT PRIMARY KEY,
            minute_count INTEGER DEFAULT 0,
            hour_count INTEGER DEFAULT 0,
            day_count INTEGER DEFAULT 0,
            minute_reset TIMESTAMP,
            hour_reset TIMESTAMP,
            day_reset TIMESTAMP
        );
        """)

        # NEW: platform_types table for FK robustness
        cur.execute("""
        CREATE TABLE IF NOT EXISTS platform_types (
            platform TEXT PRIMARY KEY
        );
        """)

        # Insert allowed platforms (idempotent via ON CONFLICT)
        cur.execute("""
        INSERT INTO platform_types (platform)
        VALUES ('x'), ('ig'), ('fb'), ('yt')
        ON CONFLICT DO NOTHING;
        """)

        # seen_posts table for deduping new posts (AI gatekeeper)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS seen_posts (
            id SERIAL PRIMARY KEY,
            owner_telegram_id BIGINT NOT NULL,
            platform TEXT NOT NULL REFERENCES platform_types(platform),
            account_name TEXT NOT NULL,
            post_id TEXT NOT NULL,                  -- X: tweet ID, IG: shortcode
            post_url TEXT NOT NULL,
            seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(owner_telegram_id, platform, account_name, post_id)
        );
        """)

        # Index for fast lookups
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_seen_user_account 
        ON seen_posts(owner_telegram_id, platform, account_name);
        """)
        
        # Badges table (needed by get_explicit_badge)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS tg_badges (
            telegram_id BIGINT PRIMARY KEY,
            badge TEXT,
            assigned_at TIMESTAMP DEFAULT NOW()
        );
        """)

        # social_posts in main DB only if DB_URL is set (keeps previous behavior)
        if DB_URL:
            db_conn = get_db()
            db_cur = db_conn.cursor()
            db_cur.execute("""
            CREATE TABLE IF NOT EXISTS social_posts (
                id TEXT PRIMARY KEY,
                platform TEXT NOT NULL,
                account_name TEXT NOT NULL,
                post_url TEXT NOT NULL,
                fetched_at TIMESTAMP NOT NULL
            );
            """)
            db_conn.commit()
            db_cur.close()
            db_conn.close()

        # Add FK constraint if not present (idempotent)
        cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = 'fk_saved_owner'
            ) THEN
                ALTER TABLE saved_accounts
                ADD CONSTRAINT fk_saved_owner
                FOREIGN KEY (owner_telegram_id)
                REFERENCES tg_users(telegram_id)
                ON DELETE CASCADE;
            END IF;
        END
        $$;
        """)

        conn.commit()
        cur.close()
        conn.close()
        logging.info("[utils.init_tg_db] tg DB tables created/verified successfully.")
    except Exception:
        logging.exception("[utils.init_tg_db] Failed to initialize tg DB tables")
        try:
            if conn:
                conn.close()
        except Exception:
            pass