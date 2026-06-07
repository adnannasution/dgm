import json
from psycopg.rows import dict_row

class SyncEngine:
    def __init__(self, pg_conn, neo_driver, state):
        self.pg    = pg_conn
        self.neo   = neo_driver
        self.state = state

    def log(self, msg):
        print(msg)
        self.state["logs"].append(msg)

    def run(self, config_ids=None):
        if config_ids:
            rows = self.pg.execute(
                "SELECT * FROM graph_relation_config WHERE id = ANY(%s) AND is_active = TRUE",
                (config_ids,)
            ).fetchall()
        else:
            rows = self.pg.execute(
                "SELECT * FROM graph_relation_config WHERE is_active = TRUE"
            ).fetchall()

        # Convert ke dict manual (kolom dari information_schema)
        col_names = ["id","config_name","source_table","source_column","target_label",
                     "target_column","relation_type","node_label","node_columns","is_active","created_at"]
        configs = [dict(zip(col_names, r)) for r in rows]

        if not configs:
            self.log("⚠️ Tidak ada config aktif yang ditemukan.")
            return

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
        source_column = cfg["source_column"]
        target_label  = cfg["target_label"]
        target_column = cfg["target_column"]
        relation_type = cfg["relation_type"]
        node_label    = cfg["node_label"]
        node_columns  = cfg["node_columns"] or []

        all_cols = list(node_columns)
        if source_column not in all_cols:
            all_cols = [source_column] + all_cols

        safe_cols = [f'CAST("{c}" AS TEXT) AS "{c}"' for c in all_cols]
        query = f"""
            SELECT {', '.join(safe_cols)}
            FROM "{source_table}"
            WHERE "{source_column}" IS NOT NULL AND CAST("{source_column}" AS TEXT) != ''
        """

        prop_cols = [col for col in all_cols if col != source_column]
        set_clauses = ", ".join([f'n.`{col}` = row.`{col}`' for col in prop_cols])

        cypher = (
            f"UNWIND $rows AS row "
            f"MERGE (target:`{target_label}` {{{target_column}: row.`{source_column}`}}) "
            f"MERGE (n:`{node_label}` {{`{source_column}`: row.`{source_column}`}}) "
            + (f"SET {set_clauses} " if set_clauses else "")
            + f"MERGE (target)-[:`{relation_type}`]->(n)"
        )

        total  = 0
        offset = 0
        while True:
            batch = self.pg.execute(query + f" LIMIT {batch_size} OFFSET {offset}").fetchall()
            if not batch:
                break

            col_names = all_cols
            rows_data = [dict(zip(col_names, r)) for r in batch]

            with self.neo.session() as session:
                session.run(cypher, rows=rows_data)

            total  += len(batch)
            offset += batch_size
            self.log(f"  → {total} record diproses...")

        self.log(f"  Total: {total} node & relasi")