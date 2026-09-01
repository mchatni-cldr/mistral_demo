#!/usr/bin/env python3
"""Validate the ontology against the live warehouse and render it to markdown.

An ontology that lives outside the repo drifts from the schema silently, and a
semantic layer that misnames a column is worse than none -- the model trusts it
and writes SQL that fails. So this does not just template a document:

  * every table and column named in the YAML must exist in the warehouse
  * every metric's SQL must actually execute
  * enum values are read from the data, not hand-maintained

Any failure aborts before writing. Output is ontology/ONTOLOGY.md, ready to
upload to a Mistral Library or paste into an agent's instructions.

    python scripts/build_ontology.py
    python scripts/build_ontology.py --check    # validate only, write nothing
"""

import argparse
import os
import sys
from datetime import date

import yaml
from dotenv import load_dotenv
from impala.dbapi import connect

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SOURCE = os.path.join(ROOT, "ontology", "readmission.yaml")
OUTPUT = os.path.join(ROOT, "ontology", "ONTOLOGY.md")


def describe(cur, table):
    cur.execute(f"DESCRIBE {table}")
    return {r[0]: r[1] for r in cur.fetchall()}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--check", action="store_true", help="validate only")
    ap.add_argument("--env", default=os.path.join(ROOT, ".env"))
    args = ap.parse_args()

    spec = yaml.safe_load(open(SOURCE))
    load_dotenv(args.env)
    db = os.getenv("IMPALA_DATABASE")

    conn = connect(
        host=os.getenv("IMPALA_HOST"), port=int(os.getenv("IMPALA_PORT", "443")),
        user=os.getenv("IMPALA_USER"), password=os.getenv("IMPALA_PASSWORD"),
        database=db, auth_mechanism=os.getenv("IMPALA_AUTH_MECHANISM", "LDAP"),
        use_http_transport=True, http_path=os.getenv("IMPALA_HTTP_PATH", "cliservice"),
        use_ssl=True,
    )
    cur = conn.cursor()
    errors, schemas = [], {}

    try:
        cur.execute("SHOW TABLES")
        live_tables = {r[0] for r in cur.fetchall()}

        # 1. tables and columns exist
        for ent in spec["entities"]:
            table = ent["table"]
            if table not in live_tables:
                errors.append(f"entity {ent['name']}: table {table!r} does not exist")
                continue
            schemas[table] = describe(cur, table)
            for col in list(ent.get("notable_columns") or {}) + ([ent["key"]] if ent.get("key") else []):
                if col not in schemas[table]:
                    errors.append(f"{table}.{col} named in ontology but not in the table")

        # 2. join columns exist
        for j in spec["joins"]:
            for side in (j["from"], j["to"]):
                t, c = side.split(".")
                if t in schemas and c not in schemas[t]:
                    errors.append(f"join references missing column {side}")

        # 3. every metric actually runs
        metric_values = {}
        for m in spec["metrics"]:
            sql = f"SELECT {m['sql']} AS v FROM {m['entity']}"
            try:
                cur.execute(sql)
                metric_values[m["name"]] = cur.fetchall()[0][0]
            except Exception as e:
                errors.append(f"metric {m['name']!r} failed to execute: {str(e)[:120]}")

        # 4. enum values read from the data
        enums = {}
        for d in spec["dimensions"]:
            src = d.get("enum_from")
            if not src:
                continue
            t, c = src.split(".")
            cur.execute(f"SELECT DISTINCT {c} FROM {t} WHERE {c} IS NOT NULL ORDER BY {c}")
            enums[src] = [r[0] for r in cur.fetchall()]

        if errors:
            print("VALIDATION FAILED:")
            for e in errors:
                print(f"  - {e}")
            return 1

        print(f"Validated against {db}: {len(schemas)} tables, "
              f"{len(spec['metrics'])} metrics all executed.")
        for name, value in metric_values.items():
            print(f"    {name:<24} {value}")

        if args.check:
            print("\n--check: nothing written.")
            return 0

        open(OUTPUT, "w").write(render(spec, schemas, enums, metric_values, db))
        print(f"\nWrote {OUTPUT}")
        return 0
    finally:
        cur.close()
        conn.close()


def render(spec, schemas, enums, values, db) -> str:
    out = []
    w = out.append
    w(f"# {spec['domain']} — data guide\n")
    w(spec["description"].strip() + "\n")
    w(f"Database `{db}`, reached through the Cloudera Iceberg MCP connector "
      f"(`get_schema`, `execute_query`). Impala SQL, read-only.\n")
    w(f"_Generated {date.today().isoformat()} and validated against the live "
      f"schema: every column and metric below was verified to exist and execute._\n")

    w("## Entities\n")
    for ent in spec["entities"]:
        w(f"### {ent['name']} — `{ent['table']}`\n")
        w(f"{ent['description']} **Grain:** {ent['grain']}\n")
        cols = schemas[ent["table"]]
        notable = ent.get("notable_columns") or {}
        w("| Column | Type | Meaning |")
        w("|---|---|---|")
        for col, typ in cols.items():
            w(f"| `{col}` | {typ} | {notable.get(col, '')} |")
        w("")

    w("## How the tables join\n")
    w("| From | To | Cardinality | Note |")
    w("|---|---|---|---|")
    for j in spec["joins"]:
        w(f"| `{j['from']}` | `{j['to']}` | {j['cardinality']} | {j['note']} |")
    w("")

    w("## Metrics\n")
    w("Use these definitions verbatim. The current value over the whole "
      "dataset is shown so you can sanity-check a result.\n")
    w("| Metric | SQL | Over | Current |")
    w("|---|---|---|---|")
    for m in spec["metrics"]:
        unit = f" {m.get('unit', '')}".rstrip()
        w(f"| **{m['name']}** — {m['description']} | `{m['sql']}` | `{m['entity']}` "
          f"| {values.get(m['name'])}{unit} |")
    w("")

    w("## Dimensions\n")
    for d in spec["dimensions"]:
        vals = enums.get(d.get("enum_from") or "")
        if vals:
            shown = ", ".join(f"`{v}`" for v in vals)
            w(f"- **`{d['column']}`** (`{d['entity']}`): {shown}")
        else:
            w(f"- **`{d['column']}`** (`{d['entity']}`)")
    w("")

    w("## Rules and pitfalls\n")
    for g in spec["gotchas"]:
        w(f"- {g}")
    w("")
    return "\n".join(out)


if __name__ == "__main__":
    sys.exit(main())
