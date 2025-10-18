import os
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from contextlib import contextmanager

load_dotenv()
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', 5434),
    'database': os.getenv('DB_NAME', 'ecommerce_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD')
}

connection_pool = None

def init_db_pool():
    global connection_pool
    try:
        connection_pool = psycopg2.pool.SimpleConnectionPool(
            1, 20,
            **DB_CONFIG
        )
        print("Database connection pool created successfully")
    except Exception as e:
        print(f"Error creating connection pool: {e}")
        raise

@contextmanager
def get_db_connection():
    conn = connection_pool.getconn()
    try:
        yield conn
    finally:
        connection_pool.putconn(conn)

@contextmanager
def get_db_cursor(commit=False):
    with get_db_connection() as conn:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        try:
            yield cursor
            if commit:
                conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()

def close_db_pool():
    if connection_pool:
        connection_pool.closeall()
        print("Database connection pool closed")