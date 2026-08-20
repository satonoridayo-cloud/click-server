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
        # ============================================================
        # match_sessions: 対戦の「進行中の状態」そのものをDBで管理する。
        # 以前はPythonプロセスのメモリ上の辞書(sessions)で管理していたが、
        # ・複数プレイヤーからの同時アクセス
        # ・通信の遅延やリトライによる多重リクエスト
        # ・将来的なワーカー複数化やサーバー再起動
        # といったケースで不整合が起きうるため、DBを正(source of truth)
        # として、行ロック(SELECT ... FOR UPDATE)を使って勝敗を確定させる
        # 方式に変更した。
        # status: 'pending'(結果待ち) / 'finished'(勝敗確定) / 'cancelled'(辞退)
        # ============================================================
        cur.execute("""
            CREATE TABLE IF NOT EXISTS match_sessions (
                session_id UUID PRIMARY KEY,
                player1_name VARCHAR(100) NOT NULL,
                player2_name VARCHAR(100) NOT NULL,
                player1_points INTEGER,
                player2_points INTEGER,
                winner VARCHAR(100),
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                player1_delivered BOOLEAN NOT NULL DEFAULT FALSE,
                player2_delivered BOOLEAN NOT NULL DEFAULT FALSE,
                cancelled_by VARCHAR(100),
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


# Gunicorn等でモジュールとしてimportされて動く本番環境では
# `if __name__ == '__main__':` の中は実行されないため、ここで
# 確実にテーブル作成を走らせておく。
# (CREATE TABLE IF NOT EXISTS なので、複数ワーカーが同時に読み込んでも
#  何度呼んでも安全)
init_db()


# ============================================================
# マッチング待機列 (インメモリ)
#
# ここに残っているのは「まだ相手が決まっていない人同士を組み合わせる」
# だけの一時的な処理。組み合わせが決まった後の点数記録・勝敗判定は
# すべて match_sessions テーブル(DB側)で行う。
#
# 注意: 待機列自体は依然インメモリなので、Gunicornのワーカーを複数に
# する場合はここだけは共有ストア(Redis等)への置き換えが必要。
# ============================================================

lock = threading.Lock()

# 対戦待ちのプレイヤー: [{"username": str, "joined_at": float}]
waiting_queue = []

# マッチング成立後、まだ本人に通知(次のjoinポーリング)していないペア情報
# username -> {"opponent": str, "session_id": str}
pending_notifications = {}

WAITING_QUEUE_TIMEOUT_SECONDS = 60 * 2  # 2分応答のない待機列エントリは掃除


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

            # 対戦セッションの本体はDBに作る(勝敗判定の正はDB)
            conn = None
            try:
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO match_sessions "
                    "(session_id, player1_name, player2_name, status) "
                    "VALUES (%s, %s, %s, 'pending')",
                    (session_id, username, opponent)
                )
                conn.commit()
                cur.close()
            except Exception as e:
                if conn:
                    conn.rollback()
                print(f"match_join: failed to create match_sessions row: {e}")
                # DB作成に失敗した場合はマッチング自体を無かったことにし、
                # 相手を待機列に戻す
                waiting_queue.append(candidate)
                return jsonify({"error": "matching failed, please retry"}), 500
            finally:
                if conn:
                    conn.close()

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

    session_id が渡された場合(=マッチング確定後の辞退)は、DB上の
    match_sessions.status を 'cancelled' にする。これにより、もう片方の
    プレイヤーが結果を送信した際に waiting_for_opponent のまま固まらず、
    「相手が辞退した」という結果をすぐ受け取れるようにする。
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
        conn = None
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(
                "UPDATE match_sessions SET status = 'cancelled', cancelled_by = %s "
                "WHERE session_id = %s AND status = 'pending'",
                (username, session_id)
            )
            conn.commit()
            cur.close()
        except Exception as e:
            if conn:
                conn.rollback()
            print(f"match_cancel: failed to update match_sessions: {e}")
        finally:
            if conn:
                conn.close()

    return jsonify({"status": "cancelled"}), 200


@app.route('/api/match/result', methods=['POST'])
def match_result():
    """
    勝敗の判定と記録はすべてDB(match_sessionsテーブル)を正として行う。

    SELECT ... FOR UPDATE で対象行をロックしてから読み書きするので、
    両プレイヤーからのリクエストがほぼ同時に来ても、通信の遅延・
    リトライ・順序の入れ替わりに関係なく、DBに書き込まれた値だけを
    見て一意に勝敗が決まる(メモリ上の状態とDBの状態がズレる心配がない)。
    """
    data = request.get_json(silent=True) or {}
    session_id = data.get('session_id')
    username = data.get('username')
    points = data.get('points')

    if not session_id or not username or points is None:
        return jsonify({"error": "session_id, username and points are required"}), 400

    if not is_valid_points(points):
        return jsonify({"error": "points must be a number"}), 400

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # 対象セッション行をロックして取得(他のリクエストはここで待たされる)
        cur.execute(
            "SELECT session_id, player1_name, player2_name, "
            "player1_points, player2_points, winner, status, "
            "player1_delivered, player2_delivered "
            "FROM match_sessions WHERE session_id = %s FOR UPDATE",
            (session_id,)
        )
        row = dictfetchone(cur)

        if not row:
            conn.rollback()
            return jsonify({"error": "Session not found"}), 404

        if username not in (row["player1_name"], row["player2_name"]):
            conn.rollback()
            return jsonify({"error": "username does not belong to this session"}), 400

        is_player1 = username == row["player1_name"]

        # 相手(または自分)がすでに辞退している場合
        if row["status"] == 'cancelled':
            opponent = row["player2_name"] if is_player1 else row["player1_name"]
            conn.commit()
            cur.close()
            return jsonify({
                "status": "opponent_declined",
                "opponent": opponent,
            }), 200

        # まだ勝敗が確定していなければ、自分のスコアをDBに記録する
        if row["status"] == 'pending':
            if is_player1:
                cur.execute(
                    "UPDATE match_sessions SET player1_points = %s WHERE session_id = %s",
                    (points, session_id)
                )
                row["player1_points"] = points
            else:
                cur.execute(
                    "UPDATE match_sessions SET player2_points = %s WHERE session_id = %s",
                    (points, session_id)
                )
                row["player2_points"] = points

            both_done = (
                row["player1_points"] is not None
                and row["player2_points"] is not None
            )

            if not both_done:
                conn.commit()
                cur.close()
                return jsonify({"status": "waiting_for_opponent"}), 200

            # 両者そろったので、DBに記録された値だけを見て勝敗を確定させる
            p1_pts, p2_pts = row["player1_points"], row["player2_points"]
            if p1_pts > p2_pts:
                winner = row["player1_name"]
            elif p2_pts > p1_pts:
                winner = row["player2_name"]
            else:
                winner = "引き分け"

            cur.execute(
                "UPDATE match_sessions SET status = 'finished', winner = %s "
                "WHERE session_id = %s",
                (winner, session_id)
            )
            row["status"] = "finished"
            row["winner"] = winner

            # ランキング表示用のmatchesテーブルにも保存
            cur.execute(
                "INSERT INTO matches "
                "(player1_name, player1_points, player2_name, player2_points, winner) "
                "VALUES (%s, %s, %s, %s, %s)",
                (row["player1_name"], p1_pts, row["player2_name"], p2_pts, winner)
            )

        # ここに来る時点で status = 'finished' (今回確定/既に確定済みの両方を含む)
        delivered_col = "player1_delivered" if is_player1 else "player2_delivered"
        cur.execute(
            f"UPDATE match_sessions SET {delivered_col} = TRUE WHERE session_id = %s",
            (session_id,)
        )

        conn.commit()
        cur.close()

        return jsonify({
            "status": "finished",
            "winner": row["winner"],
            "player1_name": row["player1_name"], "player1_points": row["player1_points"],
            "player2_name": row["player2_name"], "player2_points": row["player2_points"],
        }), 200

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"match_result error: {e}")
        return jsonify({"error": "Internal Server Error"}), 500
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    init_db()
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG") == "1" or os.environ.get("FLASK_ENV") == "development"
    app.run(host='0.0.0.0', port=port, debug=debug)
