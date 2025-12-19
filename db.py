import os
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from datetime import datetime

# Global DB Pool
db_pool = None

def init_db_pool():
    """Initialize the database connection pool and create tables."""
    global db_pool
    DATABASE_URL = os.environ.get('DATABASE_URL')
    
    if not DATABASE_URL:
        print("⚠️ DATABASE_URL not set. Database features will be disabled.")
        return

    # Fix Railway Postgres URL (start with postgresql://)
    if DATABASE_URL.startswith('postgres://'):
        DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
    
    try:
        # Create a pool of connections (min 1, max 20)
        db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, dsn=DATABASE_URL)
        print("✅ Database connection pool created!")
        
        # Create tables if they don't exist
        conn = db_pool.getconn()
        try:
            cur = conn.cursor()
            
            # 1. Games Table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS games (
                    id SERIAL PRIMARY KEY,
                    room_name VARCHAR(255),
                    white_player VARCHAR(255),
                    black_player VARCHAR(255),
                    winner VARCHAR(50),
                    win_reason VARCHAR(100),
                    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    end_time TIMESTAMP
                );
            """)
            
            # 2. Visitors Table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS visitors (
                    id SERIAL PRIMARY KEY,
                    visit_count INTEGER DEFAULT 0,
                    visit_date DATE DEFAULT CURRENT_DATE,
                    last_updated TIMESTAMP DEFAULT NOW()
                );
            """)
            
            # Initialize visitor row 1 if not exists
            cur.execute("""
                INSERT INTO visitors (id, visit_count, visit_date, last_updated)
                VALUES (1, 0, CURRENT_DATE, NOW())
                ON CONFLICT (id) DO NOTHING;
            """)
            
            conn.commit()
            cur.close()
            print("✅ Database tables checked/created.")
        except Exception as e:
            print(f"❌ Table creation error: {e}")
            conn.rollback()
        finally:
            db_pool.putconn(conn)
            
    except Exception as e:
        print(f"❌ Failed to create DB pool: {e}")

def get_db_conn():
    """Get a connection from the pool."""
    global db_pool
    if not db_pool:
        init_db_pool()
    if db_pool:
        return db_pool.getconn()
    return None

def release_db_conn(conn):
    """Return a connection to the pool."""
    global db_pool
    if db_pool and conn:
        db_pool.putconn(conn)

# ===== HELPER FUNCTIONS FOR APP.PY =====

def increment_visitor_count():
    """Increments the visitor count and updates timestamps."""
    conn = get_db_conn()
    if not conn: return 0
    
    count = 0
    try:
        cur = conn.cursor()
        # Update existing row
        cur.execute("""
            UPDATE visitors 
            SET visit_count = visit_count + 1, 
                last_updated = NOW(), 
                visit_date = CURRENT_DATE 
            WHERE id = 1 
            RETURNING visit_count
        """)
        res = cur.fetchone()
        
        if res:
            count = res[0]
        else:
            # Fallback if row 1 was deleted
            cur.execute("""
                INSERT INTO visitors (id, visit_count, visit_date, last_updated) 
                VALUES (1, 1, CURRENT_DATE, NOW())
                RETURNING visit_count
            """)
            count = 1
            
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"Visitor count error: {e}")
        if conn: conn.rollback()
    finally:
        release_db_conn(conn)
    return count

def get_leaderboard_data(limit=5):
    """Fetches top players based on wins."""
    conn = get_db_conn()
    data = []
    if conn:
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT player_name, COUNT(*) as total,
                SUM(CASE WHEN winner = player_color THEN 1 ELSE 0 END) as wins,
                ROUND((SUM(CASE WHEN winner = player_color THEN 1 ELSE 0 END)::DECIMAL / NULLIF(COUNT(*),0))*100, 1) as win_rate
                FROM (
                    SELECT white_player as player_name, 'white' as player_color, winner FROM games WHERE white_player IS NOT NULL AND white_player != 'Bot'
                    UNION ALL
                    SELECT black_player as player_name, 'black' as player_color, winner FROM games WHERE black_player IS NOT NULL AND black_player != 'Bot'
                ) as sub GROUP BY player_name HAVING COUNT(*) > 0 ORDER BY wins DESC LIMIT %s
            """, (limit,))
            data = cur.fetchall()
            cur.close()
        except Exception as e:
            print(f"Leaderboard error: {e}")
        finally:
            release_db_conn(conn)
    return [dict(row) for row in data]

def save_game_record(room, g, end_time):
    """Saves a finished game to the database."""
    conn = get_db_conn()
    if conn:
        try:
            start_time = g.get("start_timestamp", end_time)
            win_reason = g.get("reason", "unknown")
            
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO games 
                (room_name, white_player, black_player, winner, win_reason, start_time, end_time) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                room, 
                g["white_player"], 
                g["black_player"], 
                g["winner"], 
                win_reason, 
                start_time, 
                end_time
            ))
            conn.commit()
            cur.close()
            return True
        except Exception as e:
            print(f"Save game error: {e}")
            if conn: conn.rollback()
        finally:
            release_db_conn(conn)
    return False
