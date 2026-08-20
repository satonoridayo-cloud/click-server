import os
import threading
import time
import uuid
from datetime import datetime, timedelta

from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv

# 環境変数の読み込み
load_dotenv()

app = Flask(__name__)
# フロントエンドが別オリジン(HTML単体 or 別ドメイン)から叩くのでCORSを許可
CORS(app)

# ============================================================
# DB接続
# ============================================================

def get_db_connection():
    database_url = os.getenv("DATABASE_URL")
    conn = psycopg.connect(database_url)
    return conn


def init_db():
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(100) UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS single_results (
                id SERIAL PRIMARY KEY,
                username VARCHAR(100) NOT NULL,
                points INTEGER NOT NULL,
                difficulty VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS matches (
                id SERIAL PRIMARY KEY,
                player1_name VARCHAR(100) NOT NULL,
                player1_points INTEGER NOT NULL DEFAULT 0,
                player2_name VARCHAR(100) NOT NULL,
                player2_points INTEGER NOT NULL DEFAULT 0,
                winner VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        cur.close()
        print("Database tables checked/created successfully.")
    except Exception as e:
        print(f"Error initializing database: {e}")
    finally:
        if conn:
            conn.close()


# ============================================================
# マッチング / 対戦セッション管理 (インメモリ)
#
# 注意: これはプロセス内メモリで状態を持つ実装です。
# Render等で Gunicorn のワーカーを複数(--workers 2 以上)にすると
# ワーカーごとにメモリが別になり、マッチングが機能しなくなります。
# 必ず 1 ワーカーで運用するか、将来的にRedis等の共有ストアに
# 置き換えてください。
# ============================================================

lock = threading.Lock()

# 対戦待ちのプレイヤー: [{"username": str, "joined_at": float}]
waiting_queue = []

# マッチング成立後、まだ本人に通知(次のjoinポーリング)していないペア情報
# username -> {"opponent": str, "session_id": str}
pending_notifications = {}

# 進行中/結果待ちの対戦セッション
# session_id -> {
#   "player1": str, "player2": str,
#   "player1_points": int or None,
#   "player2_points": int or None,
#   "created_at": float
# }
sessions = {}

SESSION_TIMEOUT_SECONDS = 60 * 10  # 10分放置されたセッションは掃除


def cleanup_stale_sessions():
    now = time.time()
    stale_ids = [
        sid for sid, s in sessions.items()
        if now - s["created_at"] > SESSION_TIMEOUT_SECONDS
    ]
    for sid in stale_ids:
        del sessions[sid]


app.config["JSON_AS_ASCII"] = False  # 日本語をエスケープせずJSON化


# ============================================================
# ヘルスチェック
# ============================================================

@app.route('/')
def health_check():
    return jsonify({"status": "ok"}), 200


# ============================================================
# 既存: ユーザー関連
# ============================================================

@app.route('/users', methods=['GET'])
def get_users():
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(row_factory=dict_row)
        cur.execute("SELECT * FROM users ORDER BY id ASC;")
        users = cur.fetchall()
        cur.close()
        return jsonify(users), 200
    except Exception as e:
        print(e)
        return jsonify({"error": "Internal Server Error"}), 500
    finally:
        if conn:
            conn.close()


@app.route('/users', methods=['POST'])
def create_user():
    data = request.get_json(silent=True) or {}
    name = data.get('name')
    email = data.get('email')

    if not name or not email:
        return jsonify({"error": "Name and email are required"}), 400

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(row_factory=dict_row)
        query = "INSERT INTO users (name, email) VALUES (%s, %s) RETURNING *"
        cur.execute(query, (name, email))
        new_user = cur.fetchone()
        conn.commit()
        cur.close()
        return jsonify(new_user), 201
    except Exception as e:
        if conn:
            conn.rollback()
        print(e)
        return jsonify({"error": "Database error or email already exists"}), 500
    finally:
        if conn:
            conn.close()


# ============================================================
# ランキング
# ============================================================

@app.route('/api/ranking', methods=['GET'])
def get_ranking():
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(row_factory=dict_row)
        cur.execute("""
            SELECT created_at, player1_name, player1_points,
                   player2_name, player2_points, winner
            FROM matches
            ORDER BY created_at DESC
            LIMIT 50;
        """)
        rows = cur.fetchall()
        cur.close()
        # created_atはdatetimeなのでJSONにするためisoformat化
        for r in rows:
            if r.get('created_at') is not None:
                r['created_at'] = r['created_at'].isoformat()
        return jsonify(rows), 200
    except Exception as e:
        print(e)
        return jsonify({"error": "Internal Server Error"}), 500
    finally:
        if conn:
            conn.close()


# ============================================================
# 一人プレイ結果
# ============================================================

@app.route('/api/single_result', methods=['POST'])
def post_single_result():
    data = request.get_json(silent=True) or {}
    username = data.get('username')
    points = data.get('points')
    difficulty = data.get('difficulty')  # フロント側が送るなら受け取る(任意)

    if not username or points is None:
        return jsonify({"error": "username and points are required"}), 400

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(row_factory=dict_row)
        cur.execute(
            "INSERT INTO single_results (username, points, difficulty) "
            "VALUES (%s, %s, %s) RETURNING *",
            (username, points, difficulty)
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
        return jsonify(row), 201
    except Exception as e:
        if conn:
            conn.rollback()
        print(e)
        return jsonify({"error": "Internal Server Error"}), 500
    finally:
        if conn:
            conn.close()


# ============================================================
# マッチング
# ============================================================

@app.route('/api/match/join', methods=['POST'])
def match_join():
    data = request.get_json(silent=True) or {}
    username = data.get('username')
    if not username:
        return jsonify({"error": "username is required"}), 400

    with lock:
        cleanup_stale_sessions()

        # 1. 自分宛にすでに成立した通知があれば渡す
        if username in pending_notifications:
            info = pending_notifications.pop(username)
            return jsonify({
                "status": "matched",
                "opponent": info["opponent"],
                "session_id": info["session_id"],
            }), 200

        # 2. 待機列に他の誰かがいればマッチさせる
        candidate = next(
            (p for p in waiting_queue if p["username"] != username), None
        )
        if candidate:
            waiting_queue.remove(candidate)
            opponent = candidate["username"]
            session_id = str(uuid.uuid4())
            sessions[session_id] = {
                "player1": username,
                "player2": opponent,
                "player1_points": None,
                "player2_points": None,
                "created_at": time.time(),
            }
            # 相手側は次のポーリングで受け取れるようにしておく
            pending_notifications[opponent] = {
                "opponent": username,
                "session_id": session_id,
            }
            return jsonify({
                "status": "matched",
                "opponent": opponent,
                "session_id": session_id,
            }), 200

        # 3. 誰もいなければ自分を待機列に追加(重複登録は避ける)
        if not any(p["username"] == username for p in waiting_queue):
            waiting_queue.append({"username": username, "joined_at": time.time()})

        return jsonify({"status": "waiting"}), 200


@app.route('/api/match/cancel', methods=['POST'])
def match_cancel():
    data = request.get_json(silent=True) or {}
    username = data.get('username')
    if not username:
        return jsonify({"error": "username is required"}), 400

    with lock:
        waiting_queue[:] = [p for p in waiting_queue if p["username"] != username]
        pending_notifications.pop(username, None)

    return jsonify({"status": "cancelled"}), 200


@app.route('/api/match/result', methods=['POST'])
def match_result():
    """
    フロントは session_id を送ってくる前提で実装しています。
    (現状のフロントは player1_points / player2_points=0固定 で送っていて
     相手のスコアを正しく反映できていないので、フロント側も
     session_id を保持して送るよう修正が必要です。詳細は返信メッセージ参照)
    """
    data = request.get_json(silent=True) or {}
    session_id = data.get('session_id')
    username = data.get('username')
    points = data.get('points')

    if not session_id or not username or points is None:
        return jsonify({"error": "session_id, username and points are required"}), 400

    with lock:
        session = sessions.get(session_id)
        if not session:
            return jsonify({"error": "Session not found or already finished"}), 404

        if username == session["player1"]:
            session["player1_points"] = points
        elif username == session["player2"]:
            session["player2_points"] = points
        else:
            return jsonify({"error": "username does not belong to this session"}), 400

        both_done = (
            session["player1_points"] is not None
            and session["player2_points"] is not None
        )

        if not both_done:
            # まだ相手の結果待ち
            return jsonify({"status": "waiting_for_opponent"}), 200

        # 両者そろったので確定してDBに保存
        p1, p2 = session["player1"], session["player2"]
        p1_pts, p2_pts = session["player1_points"], session["player2_points"]
        if p1_pts > p2_pts:
            winner = p1
        elif p2_pts > p1_pts:
            winner = p2
        else:
            winner = "引き分け"

        del sessions[session_id]

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(row_factory=dict_row)
        cur.execute(
            "INSERT INTO matches "
            "(player1_name, player1_points, player2_name, player2_points, winner) "
            "VALUES (%s, %s, %s, %s, %s) RETURNING *",
            (p1, p1_pts, p2, p2_pts, winner)
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
        return jsonify({
            "status": "finished",
            "winner": winner,
            "player1_name": p1, "player1_points": p1_pts,
            "player2_name": p2, "player2_points": p2_pts,
        }), 201
    except Exception as e:
        if conn:
            conn.rollback()
        print(e)
        return jsonify({"error": "Internal Server Error"}), 500
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    init_db()
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_ENV") == "development"
    app.run(host='0.0.0.0', port=port, debug=debug)
