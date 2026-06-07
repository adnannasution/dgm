import os
import json
import threading
import psycopg
from psycopg.rows import dict_row
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
    return psycopg.connect(DATABASE_URL)

def get_neo():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

def init_config_table():
    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute("""
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

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/configs", methods=["GET"])
def get_configs():
    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
        rows = conn.execute("SELECT * FROM graph_relation_config ORDER BY created_at DESC").fetchall()
    return jsonify(rows)

@app.route("/api/configs", methods=["POST"])
def create_config():
    data = request.json
    with psycopg.connect(DATABASE_URL) as conn:
        row = conn.execute("""
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
        )).fetchone()
        conn.commit()
    return jsonify({"id": row[0], "message": "Config berhasil disimpan"})

@app.route("/api/configs/<int:config_id>", methods=["PUT"])
def update_config(config_id):
    data = request.json
    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute("""
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
        conn.commit()
    return jsonify({"message": "Config berhasil diupdate"})

@app.route("/api/configs/<int:config_id>", methods=["DELETE"])
def delete_config(config_id):
    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute("DELETE FROM graph_relation_config WHERE id = %s", (config_id,))
        conn.commit()
    return jsonify({"message": "Config berhasil dihapus"})

@app.route("/api/configs/<int:config_id>/toggle", methods=["POST"])
def toggle_config(config_id):
    with psycopg.connect(DATABASE_URL) as conn:
        row = conn.execute("""
            UPDATE graph_relation_config SET is_active = NOT is_active
            WHERE id = %s RETURNING is_active
        """, (config_id,)).fetchone()
        conn.commit()
    return jsonify({"is_active": row[0]})

@app.route("/api/tables", methods=["GET"])
def get_tables():
    with psycopg.connect(DATABASE_URL) as conn:
        rows = conn.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
              AND table_name != 'graph_relation_config'
            ORDER BY table_name
        """).fetchall()
    return jsonify([r[0] for r in rows])

@app.route("/api/tables/<table_name>/columns", methods=["GET"])
def get_columns(table_name):
    with psycopg.connect(DATABASE_URL) as conn:
        rows = conn.execute("""
            SELECT column_name, data_type FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
        """, (table_name,)).fetchall()
    return jsonify([{"name": r[0], "type": r[1]} for r in rows])

@app.route("/api/neo4j/status", methods=["GET"])
def neo4j_status():
    try:
        neo = get_neo()
        with neo.session() as session:
            labels = [r["label"] for r in session.run("CALL db.labels() YIELD label RETURN label")]
            stats  = [{"label": l, "count": session.run(f"MATCH (n:{l}) RETURN count(n) AS c").single()["c"]} for l in labels]
            rels   = [{"type": r["rel_type"], "count": r["cnt"]} for r in session.run(
                "MATCH ()-[r]->() RETURN type(r) AS rel_type, count(r) AS cnt ORDER BY cnt DESC")]
        neo.close()
        return jsonify({"nodes": stats, "relations": rels})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/viz/graph", methods=["GET"])
def viz_graph():
    limit        = int(request.args.get("limit", 75))
    labels_param = request.args.get("labels", "")
    filter_labels = [l.strip() for l in labels_param.split(",") if l.strip()]

    VIZ_COLORS = [
        "#2563eb","#7c3aed","#059669","#d97706",
        "#dc2626","#0891b2","#be185d","#65a30d"
    ]

    try:
        neo = get_neo()
        with neo.session() as session:
            all_labels   = [r["label"] for r in session.run("CALL db.labels() YIELD label RETURN label")]
            label_colors = {l: VIZ_COLORS[i % len(VIZ_COLORS)] for i, l in enumerate(all_labels)}
            active_labels = filter_labels if filter_labels else all_labels

            nodes_map = {}
            used_ids  = set()
            links      = []
            seen_links = set()

            def make_display_id(props, nid):
                raw = str(list(props.values())[0]) if props else str(nid)
                uid = raw
                counter = 1
                while uid in used_ids:
                    uid = f"{raw}_{counter}"
                    counter += 1
                used_ids.add(uid)
                return uid

            def add_node(n, nid, labels_list):
                if nid in nodes_map:
                    return nodes_map[nid]["id"]
                props = dict(n)
                label = labels_list[0] if labels_list else "Unknown"
                did   = make_display_id(props, nid)
                nodes_map[nid] = {
                    "id": did, "nid": nid, "label": label,
                    "color": label_colors.get(label, "#94a3b8"), "props": props
                }
                return did

            # Fetch dari sisi monitoring -> Equipment (arah benar)
            non_eq_labels = [l for l in active_labels if l != "Equipment"]
            per_mon = max(10, limit // max(len(non_eq_labels), 1))

            for label in non_eq_labels:
                if label not in label_colors:
                    continue
                # Equipment -> Monitoring
                r1 = session.run(f"""
                    MATCH (eq:Equipment)-[r]->(mon:`{label}`)
                    RETURN eq, id(eq) AS eid, labels(eq) AS el,
                           mon, id(mon) AS mid, labels(mon) AS ml,
                           type(r) AS rt
                    LIMIT $lim
                """, lim=per_mon)
                for r in r1:
                    eid = add_node(r["eq"],  r["eid"], r["el"])
                    mid = add_node(r["mon"], r["mid"], r["ml"])
                    key = (eid, mid)
                    if key not in seen_links:
                        seen_links.add(key)
                        links.append({"source": eid, "target": mid, "type": r["rt"]})

                # Monitoring -> Equipment
                r2 = session.run(f"""
                    MATCH (mon:`{label}`)-[r]->(eq:Equipment)
                    RETURN mon, id(mon) AS mid, labels(mon) AS ml,
                           eq, id(eq) AS eid, labels(eq) AS el,
                           type(r) AS rt
                    LIMIT $lim
                """, lim=per_mon)
                for r in r2:
                    mid = add_node(r["mon"], r["mid"], r["ml"])
                    eid = add_node(r["eq"],  r["eid"], r["el"])
                    key = (mid, eid)
                    if key not in seen_links:
                        seen_links.add(key)
                        links.append({"source": mid, "target": eid, "type": r["rt"]})

            # Tambah node isolated per label yang masih kurang
            per_label = max(5, limit // max(len(active_labels), 1))
            for label in active_labels:
                if label not in label_colors:
                    continue
                existing = sum(1 for n in nodes_map.values() if n["label"] == label)
                if existing < per_label:
                    res2 = session.run(
                        f"MATCH (n:`{label}`) RETURN n, id(n) AS nid LIMIT $lim",
                        lim=per_label - existing
                    )
                    for row in res2:
                        add_node(row["n"], row["nid"], [label])

            nodes = list(nodes_map.values())

        neo.close()
        return jsonify({"nodes": nodes, "links": links, "label_colors": label_colors})
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
            sync_state["done"] = True; sync_state["running"] = False
        except Exception as e:
            sync_state["error"] = str(e); sync_state["running"] = False; sync_state["done"] = True

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