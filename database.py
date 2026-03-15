import sqlite3

DB_FILE = "agency.db"

def get_connection():
    conn = sqlite3.connect(DB_FILE, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def setup_database():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS leads (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            title         TEXT,
            description   TEXT,
            location      TEXT,
            job_condition TEXT,
            client_type   TEXT DEFAULT 'Main Client',
            profile_url   TEXT UNIQUE,
            apply_url     TEXT,
            job_time      TEXT,
            status        TEXT DEFAULT 'pending',
            created_at    TEXT DEFAULT (datetime('now'))
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS conversations (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_url   TEXT,
            company_name  TEXT,
            role          TEXT,
            message       TEXT,
            signal        TEXT DEFAULT NULL,
            if_alert      TEXT DEFAULT NULL,
            created_at    TEXT DEFAULT (datetime('now'))
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS memory (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            client_type      TEXT,
            signal           TEXT,
            what_failed      TEXT,
            better_response  TEXT,
            emotion_tone     TEXT,
            created_at       TEXT DEFAULT (datetime('now'))
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS context (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            situation        TEXT,
            bilal_response   TEXT,
            client_reaction  TEXT,
            lesson           TEXT,
            what_worked      TEXT,
            created_at       TEXT DEFAULT (datetime('now'))
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS case_studies (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            client_name   TEXT,
            service       TEXT,
            tier          TEXT,
            problem       TEXT,
            what_we_built TEXT,
            results       TEXT,
            review        TEXT,
            timeline      TEXT,
            pricing       TEXT,
            created_at    TEXT DEFAULT (datetime('now'))
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS seen_urls (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            url        TEXT UNIQUE,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)

    conn.commit()
    conn.close()
    print("Database ready — agency.db")

if __name__ == "__main__":
    setup_database()
