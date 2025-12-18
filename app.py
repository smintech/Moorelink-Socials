import os
import hashlib
from flask import Flask, request, jsonify
import psycopg2
import psycopg2.extras
from datetime import datetime

# ===================== CONFIG =====================
DB_URL = os.getenv("DATABASE_URL")

# ===================== APP ========================
app = Flask(__name__)

# ===================== DB HELPERS =================
def get_db():
    return psycopg2.connect(DB_URL)

def setup_db():
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS social_posts (
            id TEXT PRIMARY KEY,
            platform TEXT NOT NULL,
            account TEXT NOT NULL,
            post_url TEXT NOT NULL UNIQUE,
            content TEXT,
            media_url TEXT,
            fetched_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
    """)
    db.commit()
    cur.close()
    db.close()

def generate_id(platform, account, post_url):
    """Generate deterministic hash ID for a post"""
    hash_input = f"{platform}|{account}|{post_url}"
    return hashlib.sha256(hash_input.encode()).hexdigest()

def insert_or_replace_post(platform, account, post_url, content=None, media_url=None):
    id = generate_id(platform, account, post_url)
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        INSERT INTO social_posts (id, platform, account, post_url, content, media_url)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id)
        DO UPDATE SET
            platform = EXCLUDED.platform,
            account = EXCLUDED.account,
            post_url = EXCLUDED.post_url,
            content = EXCLUDED.content,
            media_url = EXCLUDED.media_url,
            fetched_at = NOW();
    """, (id, platform, account, post_url, content, media_url))
    db.commit()
    cur.close()
    db.close()

def fetch_posts(account, limit=10):
    db = get_db()
    cur = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("SELECT * FROM social_posts WHERE account=%s ORDER BY fetched_at DESC LIMIT %s", (account, limit))
    posts = cur.fetchall()
    cur.close()
    db.close()
    return posts

# ===================== ROUTES ====================
@app.route("/add_post", methods=["POST"])
def add_post():
    data = request.json
    insert_or_replace_post(
        platform=data["platform"],
        account=data["account"],
        post_url=data["post_url"],
        content=data.get("content"),
        media_url=data.get("media_url")
    )
    return jsonify({"status": "success"}), 200

@app.route("/get_posts/<account>", methods=["GET"])
def get_posts(account):
    limit = int(request.args.get("limit", 10))
    posts = fetch_posts(account, limit)
    return jsonify([dict(p) for p in posts])

# ===================== MAIN ======================
if __name__ == "__main__":
    setup_db()
    app.run(debug=True)