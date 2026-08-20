import os
from flask import Flask, jsonify, request
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# 環境変数の読み込み
load_dotenv()

app = Flask(__name__)

# データベース接続を取得する関数
def get_db_connection():
    database_url = os.getenv("DATABASE_URL")
    # psycopg2でPostgreSQLに接続
    conn = psycopg2.connect(database_url)
    return conn

# 起動時にテーブルを作成する関数
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
        conn.commit()
        cur.close()
        print("Database table checked/created successfully.")
    except Exception as e:
        print(f"Error initializing database: {e}")
    finally:
        if conn:
            conn.close()

# アプリ起動時にテーブル初期化を実行
# (リローダーによる二重実行を避けるため __main__ ブロック側に移動)

# 1. ユーザー一覧取得 (GET /users)
@app.route('/users', methods=['GET'])
def get_users():
    conn = None
    try:
        conn = get_db_connection()
        # RealDictCursorを使うことで、結果を辞書型（JSONにしやすい形）で取得できる
        cur = conn.cursor(cursor_factory=RealDictCursor)
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

# 2. ユーザー登録 (POST /users)
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
        cur = conn.cursor(cursor_factory=RealDictCursor)

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

if __name__ == '__main__':
    # 起動時にテーブル初期化を実行（リローダーの子プロセスのみで実行される）
    init_db()

    # ローカル実行時のポート設定（RenderではGunicornなどのWSGIサーバーを使うのが一般的）
    port = int(os.environ.get("PORT", 5000))
    # 本番では FLASK_ENV=development が設定されていない限り debug=False になる
    debug = os.environ.get("FLASK_ENV") == "development"
    app.run(host='0.0.0.0', port=port, debug=debug)
