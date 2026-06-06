import json
import psycopg2.extras

class SyncEngine:
    def __init__(self, pg_conn, neo_driver, state):
        self.pg  = pg_conn
        self.neo = neo_driver
        self.state = state

    def log(self, msg):
        print(msg)
        self.state["logs"].append(msg)

    def run(self, config_ids=None):
        cur = self.pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        if config_ids:
            cur.execute(
                "SELECT * FROM graph_relation_config WHERE id = ANY(%s) AND is_active = TRUE",
                (config_ids,)
            )
        else:
            cur.execute("SELECT * FROM graph_relation_config WHERE is_active = TRUE")

        configs = cur.fetchall()
        cur.close()

        if not configs:
            self.log("⚠️ Tidak ada config aktif yang ditemukan.")
            return

        # Buat index Neo4j untuk Equipment dulu
        with self.neo.session() as session:
            session.run("CREATE INDEX eq_idx IF NOT EXISTS FOR (e:Equipment) ON (e.equipment)")

        for cfg in configs:
            self.log(f"\n🔄 Memproses: {cfg['config_name']} ({cfg['source_table']} → {cfg['node_label']})")
            try:
                self._sync_one(cfg)
                self.log(f"✅ {cfg['config_name']} selesai")
            except Exception as e:
                self.log(f"❌ Error pada {cfg['config_name']}: {str(e)}")

    def _sync_one(self, cfg, batch_size=500):
        source_table  = cfg["source_table"]
        source_column = cfg["source_column"]   # kolom penghubung ke Equipment
        target_label  = cfg["target_label"]    # biasanya "Equipment"
        target_column = cfg["target_column"]   # biasanya "equipment"
        relation_type = cfg["relation_type"]   # misal "PUNYA_ICU"
        node_label    = cfg["node_label"]      # misal "ICUMonitoring"
        node_columns  = cfg["node_columns"]    # list kolom yang jadi property node

        # Ambil semua kolom yang akan di-select
        all_cols = list(node_columns) if node_columns else []
        if source_column not in all_cols:
            all_cols = [source_column] + all_cols

        # Buat query SELECT dinamis
        safe_cols = [f'CAST("{c}" AS TEXT) AS "{c}"' for c in all_cols]
        query = f"""
            SELECT {', '.join(safe_cols)}
            FROM "{source_table}"
            WHERE "{source_column}" IS NOT NULL AND "{source_column}" != ''
        """

        cur = self.pg.cursor(cursor_factory=psycopg2.extras.RealDictCursor, name=f"cur_{source_table}")
        cur.execute(query)

        total = 0
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break

            rows_data = [dict(r) for r in rows]

            # Buat Cypher dinamis
            set_clauses = "\n".join([
                f'n.{col} = row.{col}'
                for col in all_cols if col != source_column
            ])

            # Unique key untuk node: source_column value
            cypher = f"""
                UNWIND $rows AS row
                MERGE (target:{target_label} {{{target_column}: row.{source_column}}})
                MERGE (n:{node_label} {{{source_column}: row.{source_column}}})
                {'SET ' + set_clauses if set_clauses else ''}
                MERGE (target)-[:{relation_type}]->(n)
            """

            with self.neo.session() as session:
                session.run(cypher, rows=rows_data)

            total += len(rows)
            self.log(f"  → {total} record diproses...")

        cur.close()
        self.log(f"  Total: {total} node & relasi")
