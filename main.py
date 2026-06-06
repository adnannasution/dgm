import os
import json
import threading
import psycopg2
import psycopg2.extras
from flask import Flask, render_template, request, jsonify
from neo4j import GraphDatabase
from dotenv import load_dotenv
from sync_engine import SyncEngine

load_dotenv()

app = Flask(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
NEO4J_URI    = os.getenv("NEO4J_URI",  "bolt://viaduct.proxy.rlwy.net:22569")
NEO4J_USER   = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS   = os.getenv("NEO4J_PASS", "Neo4j@2024")

sync_state = {"running": False, "logs": [], "done": False, "error": None}

def get_pg():
    return psycopg2.connect(DATABASE_URL)

def get_neo():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

def init_config_table():
    conn = get_pg()
    cur  = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS graph_relation_config (
            id            SERIAL PRIMARY KEY,
            config_name   TEXT NOT NULL,
            source_table  TEXT NOT NULL,
            source_column TEXT NOT NULL,
            target_label  TEXT NOT NULL DEFAULT 'Equipment',
            target_column TEXT NOT NULL DEFAULT 'equipment',
            relation_type TEXT NOT NULL,
            node_label    TEXT NOT NULL,
            node_columns  JSONB NOT NULL DEFAULT '[]',
            is_active     BOOLEAN DEFAULT TRUE,
            created_at    TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/configs", methods=["GET"])
def get_configs():
    conn = get_pg()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM graph_relation_config ORDER BY created_at DESC")
    rows = cur.fetchall()
    cur.close(); conn.close()
    return jsonify([dict(r) for r in rows])

@app.route("/api/configs", methods=["POST"])
def create_config():
    data = request.json
    conn = get_pg()
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO graph_relation_config
            (config_name, source_table, source_column, target_label,
             target_column, relation_type, node_label, node_columns, is_active)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING id
    """, (
        data["config_name"], data["source_table"], data["source_column"],
        data.get("target_label", "Equipment"), data.get("target_column", "equipment"),
        data["relation_type"], data["node_label"],
        json.dumps(data.get("node_columns", [])),
        data.get("is_active", True)
    ))
    new_id = cur.fetchone()[0]
    conn.commit(); cur.close(); conn.close()
    return jsonify({"id": new_id, "message": "Config berhasil disimpan"})

@app.route("/api/configs/<int:config_id>", methods=["PUT"])
def update_config(config_id):
    data = request.json
    conn = get_pg()
    cur  = conn.cursor()
    cur.execute("""
        UPDATE graph_relation_config SET
            config_name   = %s, source_table  = %s, source_column = %s,
            target_label  = %s, target_column = %s, relation_type = %s,
            node_label    = %s, node_columns  = %s, is_active     = %s
        WHERE id = %s
    """, (
        data["config_name"], data["source_table"], data["source_column"],
        data.get("target_label", "Equipment"), data.get("target_column", "equipment"),
        data["relation_type"], data["node_label"],
        json.dumps(data.get("node_columns", [])),
        data.get("is_active", True), config_id
    ))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"message": "Config berhasil diupdate"})

@app.route("/api/configs/<int:config_id>", methods=["DELETE"])
def delete_config(config_id):
    conn = get_pg()
    cur  = conn.cursor()
    cur.execute("DELETE FROM graph_relation_config WHERE id = %s", (config_id,))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"message": "Config berhasil dihapus"})

@app.route("/api/configs/<int:config_id>/toggle", methods=["POST"])
def toggle_config(config_id):
    conn = get_pg()
    cur  = conn.cursor()
    cur.execute("""
        UPDATE graph_relation_config SET is_active = NOT is_active
        WHERE id = %s RETURNING is_active
    """, (config_id,))
    new_val = cur.fetchone()[0]
    conn.commit(); cur.close(); conn.close()
    return jsonify({"is_active": new_val})

@app.route("/api/tables", methods=["GET"])
def get_tables():
    conn = get_pg()
    cur  = conn.cursor()
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
          AND table_name != 'graph_relation_config'
        ORDER BY table_name
    """)
    tables = [r[0] for r in cur.fetchall()]
    cur.close(); conn.close()
    return jsonify(tables)

@app.route("/api/tables/<table_name>/columns", methods=["GET"])
def get_columns(table_name):
    conn = get_pg()
    cur  = conn.cursor()
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
    """, (table_name,))
    cols = [{"name": r[0], "type": r[1]} for r in cur.fetchall()]
    cur.close(); conn.close()
    return jsonify(cols)

@app.route("/api/neo4j/status", methods=["GET"])
def neo4j_status():
    try:
        neo = get_neo()
        with neo.session() as session:
            labels_result = session.run("CALL db.labels() YIELD label RETURN label")
            labels = [r["label"] for r in labels_result]
            stats = []
            for label in labels:
                count = session.run(f"MATCH (n:{label}) RETURN count(n) AS c").single()["c"]
                stats.append({"label": label, "count": count})
            rels_result = session.run("""
                MATCH ()-[r]->() RETURN type(r) AS rel_type, count(r) AS cnt ORDER BY cnt DESC
            """)
            rels = [{"type": r["rel_type"], "count": r["cnt"]} for r in rels_result]
        neo.close()
        return jsonify({"nodes": stats, "relations": rels})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/sync/start", methods=["POST"])
def start_sync():
    global sync_state
    data       = request.json or {}
    config_ids = data.get("config_ids", [])
    if sync_state["running"]:
        return jsonify({"error": "Sync sedang berjalan"}), 400
    sync_state = {"running": True, "logs": [], "done": False, "error": None}
    def run_sync():
        global sync_state
        try:
            conn   = get_pg()
            neo    = get_neo()
            engine = SyncEngine(conn, neo, sync_state)
            engine.run(config_ids)
            neo.close(); conn.close()
            sync_state["done"]    = True
            sync_state["running"] = False
        except Exception as e:
            sync_state["error"]   = str(e)
            sync_state["running"] = False
            sync_state["done"]    = True
    threading.Thread(target=run_sync, daemon=True).start()
    return jsonify({"message": "Sync dimulai"})

@app.route("/api/sync/status", methods=["GET"])
def sync_status():
    return jsonify(sync_state)

@app.route("/api/sync/reset", methods=["POST"])
def sync_reset():
    global sync_state
    if not sync_state["running"]:
        sync_state = {"running": False, "logs": [], "done": False, "error": None}
    return jsonify({"message": "Reset OK"})

if __name__ == "__main__":
    init_config_table()
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
