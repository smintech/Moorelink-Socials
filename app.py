import psycopg2
from psycopg2.extras import RealDictCursor
from config import DATABASE_URL

def get_db():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    return conn