import os
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
    """
    テーブルを1つずつ個別にコミットする。
    以前は全テーブルのCREATEを1つのトランザクションにまとめていたため、
    (例えば新しく追加したテーブルの作成失敗など)どれか1つでも例外が
    起きると commit() に到達せず、接続クローズ時に暗黙のロールバックが
    発生し、その前に成功していた他のテーブル作成まで丸ごと消えてしまう
    という問題があった。1テーブルずつコミットすることでこれを防ぐ。
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return

    table_statements = [
        ("users", """
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(100) UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """),
        ("single_results", """
            CREATE TABLE IF NOT EXISTS single_results (
                id SERIAL PRIMARY KEY,
                username VARCHAR(100) NOT NULL,
                points INTEGER NOT NULL,
                difficulty VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """),
        ("matches", """
            CREATE TABLE IF NOT EXISTS matches (
                id SERIAL PRIMARY KEY,
                player1_name VARCHAR(100) NOT NULL,
                player1_points INTEGER NOT NULL DEFAULT 0,
                player2_name VARCHAR(100) NOT NULL,
                player2_points INTEGER NOT NULL DEFAULT 0,
                winner VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """),
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
        ("match_sessions", """
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
        """),
        # ============================================================
        # waiting_players: マッチング待機列そのものをDBの行で管理する。
        # 以前はPythonプロセスのメモリ上のリスト(waiting_queue)で
        # 管理していたが、Gunicornのワーカーを複数にすると
        # ワーカーごとにメモリが別れて機能しなくなる問題があった。
        # 1人が参加ボタンを押すごとに1行追加され(使い捨てのid=idカラム)、
        # id昇順で「一番古い、まだ誰ともマッチしていない他人」を
        # 隣同士としてペアにする。ペアが決まったら両者の行に
        # session_id(match_sessionsの使い捨てID)を書き込み、
        # 本人が受け取り次第その行は削除する。
        # ============================================================
        ("waiting_players", """
            CREATE TABLE IF NOT EXISTS waiting_players (
                id SERIAL PRIMARY KEY,
                username VARCHAR(100) NOT NULL,
                session_id UUID,
                opponent_name VARCHAR(100),
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """),
    ]

    for table_name, ddl in table_statements:
        try:
            cur.execute(ddl)
            conn.commit()  # 1テーブルごとに確定させる
            print(f"Table '{table_name}' checked/created successfully.")
        except Exception as e:
            conn.rollback()
            print(f"Error creating table '{table_name}': {e}")

    cur.close()
    conn.close()


# Gunicorn等でモジュールとしてimportされて動く本番環境では
# `if __name__ == '__main__':` の中は実行されないため、ここで
# 確実にテーブル作成を走らせておく。
# (CREATE TABLE IF NOT EXISTS なので、複数ワーカーが同時に読み込んでも
#  何度呼んでも安全)
init_db()


# ============================================================
# マッチング待機列
#
# 待機列そのものをDBの waiting_players テーブルの行で管理する
# (詳細は init_db 内のコメントを参照)。これによりPythonプロセスの
# メモリに何も持たないので、Gunicornのワーカーを複数にしても
# 問題なく動作する。
# ============================================================

WAITING_QUEUE_TIMEOUT_SECONDS = 60 * 2  # 2分応答のない待機行は掃除


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
    """
    マッチング待機列そのものをDB(waiting_players)で管理する。

    流れ:
      1. 一定時間応答のない待機行(ゴースト)を掃除
      2. 自分の待機行がすでにあり、session_idが埋まっていれば
         =マッチ成立済みなので、それを返して行を削除(使い捨て)
      3. 自分の待機行がなければ新規に1行追加
      4. まだ誰ともマッチしていない一番古い他人の行を探し、
         見つかれば新しい使い捨てID(session_id)を発行して
         両者の行に書き込み、match_sessions側にも対戦を作成する
      5. 見つからなければ「待機中」のまま返す
    """
    data = request.get_json(silent=True) or {}
    username = data.get('username')
    if not username:
        return jsonify({"error": "username is required"}), 400

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # 1. ゴースト掃除(ブラウザが正常終了できず残ったままの待機行)
        cutoff = datetime.utcnow() - timedelta(seconds=WAITING_QUEUE_TIMEOUT_SECONDS)
        cur.execute(
            "DELETE FROM waiting_players WHERE session_id IS NULL AND joined_at < %s",
            (cutoff,)
        )

        # 2. 自分の待機行があるか確認(あれば行ロック)
        cur.execute(
            "SELECT id, session_id, opponent_name FROM waiting_players "
            "WHERE username = %s ORDER BY id DESC LIMIT 1 FOR UPDATE",
            (username,)
        )
        my_row = dictfetchone(cur)

        if my_row and my_row["session_id"] is not None:
            # すでにマッチ成立済み → 使い捨てなのでこの行は削除して結果を返す
            session_id = my_row["session_id"]
            opponent = my_row["opponent_name"]
            cur.execute("DELETE FROM waiting_players WHERE id = %s", (my_row["id"],))
            conn.commit()
            cur.close()
            return jsonify({
                "status": "matched",
                "opponent": opponent,
                "session_id": str(session_id),
            }), 200

        if not my_row:
            # 初回のjoin → 待機行を新規作成(使い捨てのid = このSERIAL id)
            cur.execute(
                "INSERT INTO waiting_players (username, session_id, opponent_name) "
                "VALUES (%s, NULL, NULL) RETURNING id",
                (username,)
            )
            my_row = dictfetchone(cur)

        # 3. まだ誰ともマッチしていない一番古い他人の行を探す(=隣)
        #    FOR UPDATE SKIP LOCKED: 同時に来た別のリクエストが
        #    ロック中の行はスキップして競合を避ける
        cur.execute(
            "SELECT id, username FROM waiting_players "
            "WHERE session_id IS NULL AND username != %s "
            "ORDER BY id ASC LIMIT 1 FOR UPDATE SKIP LOCKED",
            (username,)
        )
        opponent_row = dictfetchone(cur)

        if opponent_row:
            new_session_id = str(uuid.uuid4())

            # 勝敗判定の正となるmatch_sessions側にも対戦を作成
            cur.execute(
                "INSERT INTO match_sessions "
                "(session_id, player1_name, player2_name, status) "
                "VALUES (%s, %s, %s, 'pending')",
                (new_session_id, username, opponent_row["username"])
            )

            # 両者の待機行に使い捨てIDと相手の名前を書き込む(=隣と接続)
            cur.execute(
                "UPDATE waiting_players SET session_id = %s, opponent_name = %s "
                "WHERE id = %s",
                (new_session_id, opponent_row["username"], my_row["id"])
            )
            cur.execute(
                "UPDATE waiting_players SET session_id = %s, opponent_name = %s "
                "WHERE id = %s",
                (new_session_id, username, opponent_row["id"])
            )

            # 自分の分はここで確定して返せるので、自分の待機行はもう不要
            cur.execute("DELETE FROM waiting_players WHERE id = %s", (my_row["id"],))

            conn.commit()
            cur.close()
            return jsonify({
                "status": "matched",
                "opponent": opponent_row["username"],
                "session_id": new_session_id,
            }), 200

        # 4. 相手が見つからなければ引き続き待機(残った1人は次の人を待つ)
        conn.commit()
        cur.close()
        return jsonify({"status": "waiting"}), 200

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"match_join error: {e}")
        return jsonify({"error": "matching failed, please retry"}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/match/cancel', methods=['POST'])
def match_cancel():
    """
    待機列からの離脱、または「対戦確認画面」で"やめる"を押した場合に呼ばれる。

    - まだマッチしていない待機行(waiting_players)があれば削除する
    - session_id が渡された場合(=マッチング確定後の辞退)は、
      match_sessions.status を 'cancelled' にする。これにより、
      もう片方のプレイヤーが結果を送信した際に waiting_for_opponent の
      まま固まらず、「相手が辞退した」という結果をすぐ受け取れる。
    """
    data = request.get_json(silent=True) or {}
    username = data.get('username')
    session_id = data.get('session_id')
    if not username:
        return jsonify({"error": "username is required"}), 400

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute(
            "DELETE FROM waiting_players WHERE username = %s AND session_id IS NULL",
            (username,)
        )

        if session_id:
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
        print(f"match_cancel error: {e}")
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
 
