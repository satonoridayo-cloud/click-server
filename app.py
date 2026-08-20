import os
import threading
import time
import uuid
import ssl
from urllib.parse import urlparse
from datetime import datetime, timedelta

from flask import Flask, jsonify, request
from flask_cors import CORS
import pg8000.dbapi as pg8000
from dotenv import load_dotenv

# 環境変数の読み込み
load_dotenv()

app = Flask(__name__)
# フロントエンドが別オリジン(HTML単体 or 別ドメイン)から叩くのでCORSを許可
CORS(app)

# ============================================================
# DB接続 (pg8000: 純Python実装のPostgreSQLドライバ。C拡張が無いため
# Pythonバージョンが新しくてもビルド済みバイナリの有無に左右されない)
# ============================================================

def get_db_connection():
    database_url = os.getenv("DATABASE_URL")
    parsed = urlparse(database_url)

    # RenderなどのマネージドPostgreSQLは基本SSL接続が必須なので有効化しておく
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    conn = pg8000.connect(
        user=parsed.username,
        password=parsed.password,
        host=parsed.hostname,
        port=parsed.port or 5432,
        database=parsed.path.lstrip('/'),
        ssl_context=ssl_context,
    )
    return conn


def dictfetchall(cur):
    """pg8000にはRealDictCursor相当が無いので手動で辞書のリストに変換"""
    columns = [col[0] for col in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def dictfetchone(cur):
    columns = [col[0] for col in cur.description]
    row = cur.fetchone()
    return dict(zip(columns, row)) if row else None


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
#   "created_at": float,
#   "cancelled": bool (どちらかが「やめる」を押した場合 True),
#   "cancelled_by": str (辞退したユーザー名)
# }
sessions = {}

SESSION_TIMEOUT_SECONDS = 60 * 10  # 10分放置されたセッションは掃除
WAITING_QUEUE_TIMEOUT_SECONDS = 60 * 2  # 2分応答のない待機列エントリは掃除


def cleanup_stale_sessions():
    now = time.time()
    stale_ids = [
        sid for sid, s in sessions.items()
        if now - s["created_at"] > SESSION_TIMEOUT_SECONDS
    ]
    for sid in stale_ids:
        del sessions[sid]


def cleanup_stale_waiting_queue():
    """
    ブラウザが正常終了できず(sendBeaconが届かない等)、待機列に
    取り残されたままのプレイヤーを掃除する。
    正常にポーリングを続けているプレイヤーは match_join のたびに
    再追加され joined_at が更新されるので、ここで消しても実害はない。
    """
    now = time.time()
    waiting_queue[:] = [
        p for p in waiting_queue
        if now - p["joined_at"] <= WAITING_QUEUE_TIMEOUT_SECONDS
    ]


def is_valid_points(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


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
        cur = conn.cursor()
        cur.execute("SELECT * FROM users ORDER BY id ASC;")
        users = dictfetchall(cur)
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
        cur = conn.cursor()
        query = "INSERT INTO users (name, email) VALUES (%s, %s) RETURNING *"
        cur.execute(query, (name, email))
        new_user = dictfetchone(cur)
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
        cur = conn.cursor()
        cur.execute("""
            SELECT created_at, player1_name, player1_points,
                   player2_name, player2_points, winner
            FROM matches
            ORDER BY created_at DESC
            LIMIT 50;
        """)
        rows = dictfetchall(cur)
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

    if not is_valid_points(points):
        return jsonify({"error": "points must be a number"}), 400

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO single_results (username, points, difficulty) "
            "VALUES (%s, %s, %s) RETURNING *",
            (username, points, difficulty)
        )
        row = dictfetchone(cur)
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
        cleanup_stale_waiting_queue()

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
                "cancelled": False,
                "cancelled_by": None,
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

        # 3. 誰もいなければ自分を待機列に追加する
        #    (既にいる場合は joined_at を更新して「生きている」ことを示す)
        existing = next(
            (p for p in waiting_queue if p["username"] == username), None
        )
        if existing:
            existing["joined_at"] = time.time()
        else:
            waiting_queue.append({"username": username, "joined_at": time.time()})

        return jsonify({"status": "waiting"}), 200


@app.route('/api/match/cancel', methods=['POST'])
def match_cancel():
    """
    待機列からの離脱、または「対戦確認画面」で"やめる"を押した場合に呼ばれる。

    session_id が渡された場合(=マッチング確定後の辞退)は、該当セッションに
    cancelled フラグを立てる。これにより、もう片方のプレイヤーが結果を
    送信した際に waiting_for_opponent のまま固まらず、「相手が辞退した」
    という結果をすぐ受け取れるようにする。
    """
    data = request.get_json(silent=True) or {}
    username = data.get('username')
    session_id = data.get('session_id')
    if not username:
        return jsonify({"error": "username is required"}), 400

    with lock:
        waiting_queue[:] = [p for p in waiting_queue if p["username"] != username]
        pending_notifications.pop(username, None)

        if session_id:
            session = sessions.get(session_id)
            if session and not session.get("finished") \
                    and username in (session["player1"], session["player2"]):
                session["cancelled"] = True
                session["cancelled_by"] = username

    return jsonify({"status": "cancelled"}), 200


@app.route('/api/match/result', methods=['POST'])
def match_result():
    """
    session_id を軸に、両者の結果がそろうまで待ち合わせるエンドポイント。

    重要: 結果が確定しても、両者がその結果を受け取る(delivered)まで
    セッションを消さない。片方だけ先に消してしまうと、もう片方が
    再ポーリングした時に404になり、正しい勝敗を受け取れないまま
    フロント側が不整合な表示をしてしまうバグがあったため。
    """
    data = request.get_json(silent=True) or {}
    session_id = data.get('session_id')
    username = data.get('username')
    points = data.get('points')

    if not session_id or not username or points is None:
        return jsonify({"error": "session_id, username and points are required"}), 400

    if not is_valid_points(points):
        return jsonify({"error": "points must be a number"}), 400

    db_insert_needed = False
    response_payload = None

    with lock:
        session = sessions.get(session_id)
        if not session:
            return jsonify({"error": "Session not found or already finished"}), 404

        if username not in (session["player1"], session["player2"]):
            return jsonify({"error": "username does not belong to this session"}), 400

        # 相手(または自分)がすでに辞退している場合は、その場で通知して終了
        if session.get("cancelled"):
            opponent = (
                session["player2"] if username == session["player1"]
                else session["player1"]
            )
            del sessions[session_id]
            return jsonify({
                "status": "opponent_declined",
                "opponent": opponent,
            }), 200

        # まだ確定していなければ自分のスコアを記録(確定後は上書きしない)
        if not session.get("finished"):
            if username == session["player1"]:
                session["player1_points"] = points
            else:
                session["player2_points"] = points

            both_done = (
                session["player1_points"] is not None
                and session["player2_points"] is not None
            )

            if not both_done:
                return jsonify({"status": "waiting_for_opponent"}), 200

            # 両者そろったのでこの場で一度だけ勝敗を確定させる
            p1, p2 = session["player1"], session["player2"]
            p1_pts, p2_pts = session["player1_points"], session["player2_points"]
            if p1_pts > p2_pts:
                winner = p1
            elif p2_pts > p1_pts:
                winner = p2
            else:
                winner = "引き分け"

            session["finished"] = True
            session["winner"] = winner
            session["delivered"] = set()
            db_insert_needed = True

        # ここに来る時点でセッションは確定済み(今回確定した/既に確定していた両方を含む)
        p1, p2 = session["player1"], session["player2"]
        p1_pts, p2_pts = session["player1_points"], session["player2_points"]
        winner = session["winner"]
        response_payload = {
            "status": "finished",
            "winner": winner,
            "player1_name": p1, "player1_points": p1_pts,
            "player2_name": p2, "player2_points": p2_pts,
        }

        session["delivered"].add(username)
        if len(session["delivered"]) >= 2:
            # 両者が結果を受け取ったのでもう不要
            del sessions[session_id]

    if not db_insert_needed:
        # 既に確定済みのセッションへの追いつきリクエスト。DB書き込みはしない。
        return jsonify(response_payload), 200

    # DB保存は「結果をプレイヤーに返せるかどうか」とは切り離す。
    # ここが失敗しても、勝敗自体は既にメモリ上で確定しているので
    # プレイヤーには正しく結果を返す(500にしない)。
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO matches "
            "(player1_name, player1_points, player2_name, player2_points, winner) "
            "VALUES (%s, %s, %s, %s, %s) RETURNING *",
            (p1, p1_pts, p2, p2_pts, winner)
        )
        dictfetchone(cur)
        conn.commit()
        cur.close()
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"match_result: failed to save match to DB (result still returned to player): {e}")
    finally:
        if conn:
            conn.close()

    return jsonify(response_payload), 200


if __name__ == '__main__':
    init_db()
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG") == "1" or os.environ.get("FLASK_ENV") == "development"
    app.run(host='0.0.0.0', port=port, debug=debug)
