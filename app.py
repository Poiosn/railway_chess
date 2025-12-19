from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit, join_room, leave_room
import chess
import time
import random
import secrets
import threading
import os
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from datetime import datetime

# ===== FLASK APP =====
app = Flask(__name__)
app.config["SECRET_KEY"] = secrets.token_hex(16)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# ===== DATABASE CONFIGURATION =====
db_pool = None

def init_db_pool():
    global db_pool
    DATABASE_URL = os.environ.get('DATABASE_URL')
    
    if not DATABASE_URL:
        print("⚠️ DATABASE_URL not set. Database features will be disabled.")
        return

    if DATABASE_URL.startswith('postgres://'):
        DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)
    
    try:
        db_pool = psycopg2.pool.SimpleConnectionPool(1, 4, dsn=DATABASE_URL)
        print("✅ Database connection pool created!")
        
        conn = db_pool.getconn()
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS visitors (
                    id SERIAL PRIMARY KEY,
                    visit_count INTEGER DEFAULT 0,
                    visit_date DATE DEFAULT CURRENT_DATE,
                    last_updated TIMESTAMP DEFAULT NOW()
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS games (
                    id SERIAL PRIMARY KEY,
                    room_name VARCHAR(255),
                    white_player VARCHAR(255),
                    black_player VARCHAR(255),
                    winner VARCHAR(50),
                    win_reason VARCHAR(100),
                    start_time TIMESTAMP,
                    end_time TIMESTAMP
                );
            """)
            cur.execute("""
                INSERT INTO visitors (id, visit_count, visit_date, last_updated) 
                VALUES (1, 0, CURRENT_DATE, NOW())
                ON CONFLICT (id) DO NOTHING;
            """)
            conn.commit()
            cur.close()
            print("✅ Database tables ready.")
        except Exception as e:
            print(f"❌ Table creation error: {e}")
            conn.rollback()
        finally:
            db_pool.putconn(conn)
            
    except Exception as e:
        print(f"❌ Failed to create DB pool: {e}")

def get_db_conn():
    global db_pool
    if not db_pool:
        init_db_pool()
    
    if db_pool:
        try:
            conn = db_pool.getconn()
            if conn.closed:
                db_pool.putconn(conn, close=True)
                return db_pool.getconn()
            return conn
        except Exception as e:
            print(f"⚠️ DB Pool Exhausted or Error: {e}")
            return None
    return None

def release_db_conn(conn):
    global db_pool
    if db_pool and conn:
        try:
            db_pool.putconn(conn)
        except Exception:
            pass 

# Initialize DB on module load (Important for Gunicorn)
init_db_pool()

# Stores game state
games = {}
sid_to_room = {}
DISCONNECT_TIMEOUT = 15.0 

# --- ROUTES ---
@app.route("/")
def index():
    increment_visitor_count()
    return render_template("landing.html")

@app.route("/game")
def game_page():
    return render_template("game.html")

# --- API ENDPOINTS ---
def increment_visitor_count():
    conn = get_db_conn()
    if not conn: return
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE visitors 
            SET visit_count = visit_count + 1, 
                last_updated = NOW(), 
                visit_date = CURRENT_DATE 
            WHERE id = 1 
            RETURNING visit_count
        """)
        if not cur.fetchone():
            cur.execute("""
                INSERT INTO visitors (id, visit_count, visit_date, last_updated) 
                VALUES (1, 1, CURRENT_DATE, NOW())
            """)
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"Visitor count error: {e}")
        conn.rollback()
    finally:
        release_db_conn(conn)

@app.route('/api/visitor-count')
def visitor_count_api():
    conn = get_db_conn()
    count = 0
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT visit_count FROM visitors WHERE id = 1")
            res = cur.fetchone()
            count = res[0] if res else 0
            cur.close()
        except Exception as e:
            print(f"Visitor API error: {e}")
            conn.rollback()
        finally: release_db_conn(conn)
    return jsonify({'visitor_count': count})

@app.route('/api/leaderboard')
def leaderboard_api():
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
                ) as sub GROUP BY player_name HAVING COUNT(*) > 0 ORDER BY wins DESC LIMIT 5
            """)
            data = cur.fetchall()
            cur.close()
        except Exception as e:
            print(f"Leaderboard error: {e}")
            conn.rollback()
        finally: release_db_conn(conn)
    return jsonify([dict(row) for row in data])

# --- CHESS LOGIC ---
def format_seconds(sec):
    total = int(max(0, round(sec)))
    return f"{total//60}:{total%60:02d}"

def board_to_matrix(board):
    grid = [["." for _ in range(8)] for _ in range(8)]
    for sq, piece in board.piece_map().items():
        grid[7 - chess.square_rank(sq)][chess.square_file(sq)] = piece.symbol()
    return grid

def export_state(room):
    g = games[room]
    return {
        "board": board_to_matrix(g["board"]),
        "turn": "white" if g["board"].turn else "black",
        "check": g["board"].is_check(),
        "winner": g["winner"],
        "reason": g.get("reason"),
        "isActive": g.get("isActive", False),
        "whiteTimeFormatted": format_seconds(g["whiteTime"]),
        "blackTimeFormatted": format_seconds(g["blackTime"])
    }

def update_time(g):
    if not g.get("isActive") or g["winner"]: return
    now = time.time()
    elapsed = now - g["lastUpdate"]
    g["lastUpdate"] = now
    
    # FIX: Use <= 0 to catch float precision issues or missed ticks
    if g["board"].turn:
        g["whiteTime"] = max(0, g["whiteTime"] - elapsed)
        if g["whiteTime"] <= 0: 
            g["winner"] = "black"
            g["reason"] = "timeout"
    else:
        g["blackTime"] = max(0, g["blackTime"] - elapsed)
        if g["blackTime"] <= 0: 
            g["winner"] = "white"
            g["reason"] = "timeout"

def save_game(room, g):
    if g.get("saved"): return
    conn = get_db_conn()
    if conn:
        try:
            end_time = datetime.utcnow()
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
            g["saved"] = True
            print(f"✅ Game {room} saved to DB.")
        except Exception as e:
            print(f"❌ Save game error: {e}")
            conn.rollback()
        finally: release_db_conn(conn)

# --- TIMEOUT WATCHER (BACKGROUND THREAD) ---
def timeout_watcher():
    """Checks for time-outs every second"""
    print("⏰ Timeout watcher started")
    while True:
        time.sleep(1)
        # Use list() to avoid runtime error if dict changes size during iteration
        for r, g in list(games.items()):
            # Only check active games that haven't finished
            if g.get("isActive") and not g["winner"]:
                with g["lock"]:
                    update_time(g)
                    # If update_time() just set a winner, notify clients
                    if g["winner"]:
                        print(f"⌛ Time out in room {r}. Winner: {g['winner']}")
                        save_game(r, g)
                        socketio.emit("game_update", {"state": export_state(r)}, room=r)

# START BACKGROUND THREAD
# FIX: Start this thread globally so Gunicorn workers pick it up
# Note: In a multi-worker Gunicorn setup without sticky sessions or redis, 
# game state in memory is isolated per worker. Ensure 1 worker or sticky sessions.
timeout_thread = threading.Thread(target=timeout_watcher, daemon=True)
timeout_thread.start()

# --- TIMEOUT HANDLER ---
def handle_disconnect_timeout(room, color):
    if room not in games: return
    g = games[room]
    
    with g["lock"]:
        if g["winner"]: return
        
        if color == "white" and g.get("white_disconnect_timer"):
            g["winner"] = "black"
            g["reason"] = "abandonment"
            g["white_disconnect_timer"] = None
        elif color == "black" and g.get("black_disconnect_timer"):
            g["winner"] = "white"
            g["reason"] = "abandonment"
            g["black_disconnect_timer"] = None
        else:
            return 

        save_game(room, g)
        socketio.emit("game_update", {"state": export_state(room)}, room=room)

def cancel_timer(g, color):
    if color == "white" and g.get("white_disconnect_timer"):
        g["white_disconnect_timer"].cancel()
        g["white_disconnect_timer"] = None
    elif color == "black" and g.get("black_disconnect_timer"):
        g["black_disconnect_timer"].cancel()
        g["black_disconnect_timer"] = None

# --- SOCKET HANDLERS ---
@socketio.on("create_room")
def create(data):
    room = data["room"]
    is_bot = data.get("bot", False)
    player_name = data.get("playerName", "White")
    
    if room in games:
        g = games[room]
        if g.get("winner") is None: 
            emit("error", {"message": f"Room '{room}' is already taken!"})
            return

    games[room] = {
        "board": chess.Board(),
        "whiteTime": float(data.get("timeControl", 300)),
        "blackTime": float(data.get("timeControl", 300)),
        "lastUpdate": time.time(),
        "start_timestamp": datetime.utcnow(),
        "isActive": True if is_bot else False,
        "winner": None,
        "bot": is_bot,
        "lock": threading.Lock(),
        "white_player": player_name,
        "black_player": "Bot" if is_bot else None,
        "white_sid": request.sid,
        "black_sid": None,
        "white_disconnect_timer": None,
        "black_disconnect_timer": None,
        "clients": {request.sid}
    }
    
    sid_to_room[request.sid] = room
    join_room(room)
    
    emit("room_created", {
        "color": "white", 
        "state": export_state(room), 
        "room": room, 
        "playerNames": {"white": games[room]["white_player"], "black": games[room]["black_player"]}
    })

@socketio.on("join_room")
def join(data):
    room = data["room"]
    player_name = data.get("playerName", "Black")

    if room not in games: 
        emit("error", {"message": "Room not found"})
        return
        
    g = games[room]
    
    reconnected = False
    
    if player_name == g["white_player"]:
        g["white_sid"] = request.sid
        cancel_timer(g, "white")
        reconnected = True
        socketio.emit("player_reconnected", {"color": "white"}, room=room)
        
    elif g["black_player"] and player_name == g["black_player"]:
        g["black_sid"] = request.sid
        cancel_timer(g, "black")
        reconnected = True
        socketio.emit("player_reconnected", {"color": "black"}, room=room)

    elif not g["black_player"]:
        g["black_player"] = player_name
        g["black_sid"] = request.sid
        g["isActive"] = True
        g["lastUpdate"] = time.time()
    else:
        if not reconnected:
            emit("error", {"message": "Room is full"})
            return

    if "clients" not in g: g["clients"] = set()
    g["clients"].add(request.sid)
    sid_to_room[request.sid] = room
    join_room(room)
    
    my_color = "white" if request.sid == g.get("white_sid") else "black"
    if request.sid != g.get("white_sid") and request.sid != g.get("black_sid"):
        my_color = "spectator"

    emit("room_joined", {
        "color": my_color, 
        "state": export_state(room), 
        "room": room, 
        "playerNames": {"white": g["white_player"], "black": g["black_player"]}
    })
    
    socketio.emit("game_start", {"state": export_state(room)}, room=room)

@socketio.on("disconnect")
def on_disconnect():
    sid = request.sid
    room = sid_to_room.pop(sid, None)
    
    if room and room in games:
        g = games[room]
        
        disconnected_color = None
        if sid == g.get("white_sid"):
            disconnected_color = "white"
        elif sid == g.get("black_sid"):
            disconnected_color = "black"

        if "clients" in g:
            g["clients"].discard(sid)

        if disconnected_color and g.get("isActive") and not g["winner"]:
            print(f"⚠️ {disconnected_color} disconnected from {room}. Starting {DISCONNECT_TIMEOUT}s timer.")
            
            socketio.emit("player_disconnected", {
                "color": disconnected_color, 
                "timeout": DISCONNECT_TIMEOUT
            }, room=room)

            t = threading.Timer(DISCONNECT_TIMEOUT, handle_disconnect_timeout, [room, disconnected_color])
            t.start()
            
            if disconnected_color == "white":
                g["white_disconnect_timer"] = t
            else:
                g["black_disconnect_timer"] = t

        if len(g["clients"]) == 0 and not g.get("white_disconnect_timer") and not g.get("black_disconnect_timer"):
            print(f"🧹 Room '{room}' is empty and idle. Deleting game.")
            del games[room]

@socketio.on("move")
def move(data):
    room = data["room"]
    if room not in games: return
    g = games[room]
    
    if not g.get("isActive"): 
        emit("error", {"message": "Waiting for opponent..."})
        return
    
    with g["lock"]:
        update_time(g)
        if g["winner"]: return
        
        board = g["board"]
        f = chess.square(data["from"]["col"], 7-data["from"]["row"])
        t = chess.square(data["to"]["col"], 7-data["to"]["row"])
        mv = chess.Move(f, t, chess.QUEEN if data.get("promotion") else None)
        
        if mv not in board.legal_moves and not data.get("promotion"):
            mv = chess.Move(f, t, chess.QUEEN)

        if mv in board.legal_moves:
            san = board.san(mv)
            board.push(mv)
            
            if board.is_game_over():
                g["winner"] = "white" if not board.turn else "black"
                if board.is_stalemate(): g["winner"] = "draw"
                else: g["reason"] = "checkmate"
                save_game(room, g)
                
            socketio.emit("game_update", {
                "state": export_state(room),
                "lastMove": data,
                "moveNotation": san
            }, room=room)

            if g["bot"] and not g["winner"]:
                socketio.start_background_task(bot_play, room)

def bot_play(room):
    time.sleep(1)
    if room not in games: return
    g = games[room]
    with g["lock"]:
        board = g["board"]
        if board.is_game_over(): return
        mv = random.choice(list(board.legal_moves))
        san = board.san(mv)
        board.push(mv)
        if board.is_game_over():
            g["winner"] = "black"
            g["reason"] = "checkmate"
            save_game(room, g)
        
        socketio.emit("game_update", {
            "state": export_state(room),
            "lastMove": {"from": {"row": 7-chess.square_rank(mv.from_square), "col": chess.square_file(mv.from_square)}, 
                         "to": {"row": 7-chess.square_rank(mv.to_square), "col": chess.square_file(mv.to_square)}},
            "moveNotation": san
        }, room=room)

@socketio.on("get_possible_moves")
def get_moves(data):
    room = data["room"]
    if room not in games: return
    board = games[room]["board"]
    f = chess.square(data["from"]["col"], 7-data["from"]["row"])
    moves = [{"row": 7-chess.square_rank(m.to_square), "col": chess.square_file(m.to_square)} for m in board.legal_moves if m.from_square == f]
    emit("possible_moves", {"moves": moves})

@socketio.on("send_message")
def msg(data): socketio.emit("chat_message", data, room=data["room"])

@socketio.on("offer_draw")
def offer_draw(data):
    socketio.emit("draw_offered", {"fromColor": data["color"]}, room=data["room"], skip_sid=request.sid)

@socketio.on("respond_draw")
def respond_draw(data):
    room = data["room"]
    if room not in games: return
    g = games[room]
    if data["accept"]:
        g["winner"] = "draw"
        g["reason"] = "agreement"
        save_game(room, g)
        socketio.emit("game_update", {"state": export_state(room)}, room=room)
    else:
        socketio.emit("draw_declined", {}, room=room)

@socketio.on("resign")
def resign(data):
    room = data["room"]
    if room not in games: return
    g = games[room]
    if g["winner"]: return
    g["winner"] = "black" if data["color"] == "white" else "white"
    g["reason"] = "resign"
    save_game(room, g)
    socketio.emit("game_update", {"state": export_state(room)}, room=room)

@socketio.on("leave_room")
def on_leave(data):
    room = data.get("room")
    if room: leave_room(room)

if __name__ == "__main__":
    socketio.run(app, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
