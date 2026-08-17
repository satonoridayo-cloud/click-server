from datetime import datetime
import os
from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)
# フロントエンド（別サイト）からのアクセスを許可
CORS(app, resources={r"/api/*": {"origins": "*"}})

# ご提示いただいたPostgreSQLの接続URL
DATABASE_URL = "postgresql://click_7fat_user:NBGd2zod8zoHEiWraPSftUuzP9jDi6K5@dpg-da1e8o3l550s73fg2le0-a/click_7fat"


def get_db_connection():
  return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def init_db():
  conn = get_db_connection()
  cur = conn.cursor()
  cur.execute("""
        CREATE TABLE IF NOT EXISTS online (
            id SERIAL PRIMARY KEY,
            username VARCHAR(100) NOT NULL,
            status VARCHAR(50) DEFAULT 'waiting',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
  cur.execute("""
        CREATE TABLE IF NOT EXISTS endpoint (
            id SERIAL PRIMARY KEY,
            player1_name VARCHAR(100),
            player1_points INT,
            player2_name VARCHAR(100),
            player2_points INT,
            winner VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
  conn.commit()
  cur.close()
  conn.close()


init_db()


@app.route("/")
def index():
  return "Click Game Backend API is running!"


# 一人プレイ結果保存
@app.route("/api/single_result", methods=["POST"])
def single_result():
  data = request.json
  username = data.get("username")
  points = data.get("points")

  conn = get_db_connection()
  cur = conn.cursor()
  cur.execute(
      """
        INSERT INTO endpoint (player1_name, player1_points, player2_name, player2_points, winner)
        VALUES (%s, %s, '一人プレイ', 0, %s)
    """,
      (username, points, username),
  )
  conn.commit()
  cur.close()
  conn.close()
  return jsonify({"status": "success"})


# 二人プレイ：マッチング
@app.route("/api/match/join", methods=["POST"])
def match_join():
  data = request.json
  username = data.get("username")

  conn = get_db_connection()
  cur = conn.cursor()

  cur.execute("SELECT * FROM online WHERE username = %s", (username,))
  me = cur.fetchone()

  if not me:
    cur.execute(
        "INSERT INTO online (username, status) VALUES (%s, 'waiting')",
        (username,),
    )
    conn.commit()

  cur.execute(
      "SELECT * FROM online WHERE status = 'waiting' AND username != %s ORDER"
      " BY id ASC LIMIT 1",
      (username,),
  )
  opponent = cur.fetchone()

  if opponent:
    cur.execute(
        "UPDATE online SET status = 'matched' WHERE username IN (%s, %s)",
        (username, opponent["username"]),
    )
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({"status": "matched", "opponent": opponent["username"]})

  cur.close()
  conn.close()
  return jsonify({"status": "waiting"})


@app.route("/api/match/cancel", methods=["POST"])
def match_cancel():
  data = request.json
  username = data.get("username")

  conn = get_db_connection()
  cur = conn.cursor()
  cur.execute("DELETE FROM online WHERE username = %s", (username,))
  conn.commit()
  cur.close()
  conn.close()
  return jsonify({"status": "cancelled"})


@app.route("/api/match/result", methods=["POST"])
def match_result():
  data = request.json
  p1_name = data.get("player1_name")
  p1_pts = data.get("player1_points")
  p2_name = data.get("player2_name")
  p2_pts = data.get("player2_points")

  if p1_pts > p2_pts:
    winner = p1_name
    p1_final = p1_pts + 5
    p2_final = p2_pts + 3
  elif p2_pts > p1_pts:
    winner = p2_name
    p1_final = p1_pts + 3
    p2_final = p2_pts + 5
  else:
    winner = "引き分け"
    p1_final = p1_pts
    p2_final = p2_pts

  conn = get_db_connection()
  cur = conn.cursor()
  cur.execute(
      """
        INSERT INTO endpoint (player1_name, player1_points, player2_name, player2_points, winner)
        VALUES (%s, %s, %s, %s, %s)
    """,
      (p1_name, p1_final, p2_name, p2_final, winner),
  )
  cur.execute(
      "DELETE FROM online WHERE username IN (%s, %s)", (p1_name, p2_name)
  )
  conn.commit()
  cur.close()
  conn.close()

  return jsonify({"status": "recorded", "winner": winner})


@app.route("/api/ranking", methods=["GET"])
def get_ranking():
  conn = get_db_connection()
  cur = conn.cursor()
  cur.execute("SELECT * FROM endpoint ORDER BY id DESC LIMIT 10;")
  rows = cur.fetchall()
  cur.close()
  conn.close()
  return jsonify(rows)


if __name__ == "__main__":
  app.run(debug=True, port=5000)
