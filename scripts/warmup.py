#!/usr/bin/env python3
"""Warm the Impala virtual warehouse before a demo.

The first query that joins a table Impala hasn't loaded metadata for pays a
cold-start cost -- measured at 41s against this deployment, versus 0.4s once
warm. On stage that reads as a hang, and an MCP client may time out and give
up before the answer arrives.

Run this a minute before demoing. Takes the MCP endpoint URL:

    python scripts/warmup.py https://<subdomain>.<workbench-domain>/mcp
"""

import argparse
import json
import os
import sys
import time
import urllib.request

# Touch every table, then force the joins the demo actually performs.
QUERIES = [
    "SHOW TABLES",
    "SELECT COUNT(*) FROM patients",
    "SELECT COUNT(*) FROM admissions",
    "SELECT COUNT(*) FROM vitals_labs",
    "SELECT COUNT(*) FROM clinical_notes",
    "SELECT COUNT(*) FROM admissions a JOIN patients p ON a.patient_id = p.patient_id",
    "SELECT COUNT(*) FROM clinical_notes n JOIN admissions a ON n.admission_id = a.admission_id",
    "SELECT COUNT(*) FROM vitals_labs v JOIN admissions a ON v.admission_id = a.admission_id",
]


def call(url: str, token: str | None, query: str) -> tuple[float, str]:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": "execute_query", "arguments": {"query": query}},
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "MCP-Protocol-Version": "2025-06-18",
            **({"Authorization": f"Bearer {token}"} if token else {}),
        },
    )
    start = time.monotonic()
    with urllib.request.urlopen(req, timeout=300) as resp:
        body = json.loads(resp.read().decode())
    elapsed = time.monotonic() - start
    text = body.get("result", {}).get("content", [{}])[0].get("text", "")
    return elapsed, text


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("url", help="MCP endpoint URL, including /mcp")
    ap.add_argument("--token", default=os.getenv("MCP_BEARER_TOKEN"))
    args = ap.parse_args()

    slowest = 0.0
    for q in QUERIES:
        try:
            elapsed, text = call(args.url, args.token, q)
        except Exception as e:
            print(f"  !!  {q[:60]:<62} {e}")
            return 1
        slowest = max(slowest, elapsed)
        status = "ERR" if text.startswith("Error") else "ok"
        print(f"  {elapsed:5.1f}s  {status:<4}{q[:64]}")

    print(f"\nWarm. Slowest was {slowest:.1f}s; re-run until everything is under a second.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
