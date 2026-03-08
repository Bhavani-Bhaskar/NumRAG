import sqlite3
import os

DB_PATH = "data/qfacts.db"


def get_connection():
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn   = get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS extractedtable (
            id               INTEGER PRIMARY KEY,
            entity           TEXT    NOT NULL,
            attribute        TEXT    NOT NULL,
            value            FLOAT   NOT NULL,
            unit             TEXT,
            page_title       TEXT,
            section_heading  TEXT,
            caption          TEXT,
            row_context      TEXT,
            surrounding_text TEXT,
            source_url       TEXT,
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()