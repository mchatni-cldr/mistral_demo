# Cloudera Iceberg MCP → Mistral Vibe demo

A self-contained MCP server, deployed as a **Cloudera AI Application**, so that
**Mistral Vibe Work** (SaaS) can reach it as a Custom MCP Connector and answer
natural-language questions over Iceberg tables in Cloudera Data Warehouse.

Derived from [`cloudera/iceberg-mcp-server`](https://github.com/cloudera/iceberg-mcp-server)
(Apache-2.0) but with no dependency on it — see `NOTICE` for what changed and why.

```
Mistral Vibe Work (SaaS)
   │  HTTPS POST /mcp   (Streamable HTTP, stateless, JSON responses)
   ▼
Cloudera AI Application  ← "Enable Unauthenticated Access" ON
   │  uvicorn on 127.0.0.1:$CDSW_APP_PORT   (app.py)
   ▼
impyla → HTTPS/HS2, LDAP auth (CDP workload user + password)
   ▼
CDW Impala Virtual Warehouse → Iceberg tables
```

Vibe Work runs in Mistral's cloud, so it can only call an MCP server at a
**public HTTPS URL with a valid TLS certificate speaking Streamable HTTP**. A
laptop-local stdio server is invisible to it — hence the Cloudera AI Application.

## Layout

| Path | Purpose |
|---|---|
| `app.py` | The whole server: Impala access, the two MCP tools, and serving over Streamable HTTP on `$CDSW_APP_PORT`. Deliberately a single file with no local imports — see below. |
| `requirements.txt` | Pinned deps (see the pydantic note inside — it matters). |
| `.env.example` | Every env var, with the traps called out. Copy to `.env` for local runs. |
| `scripts/smoke_test.py` | Drives the real MCP protocol against an endpoint. Run this before touching the Mistral UI. |
| `NOTICE` / `LICENSE` | Apache-2.0 attribution for the upstream code this derives from. |

Four runtime dependencies (`fastmcp`, `mcp`, `impyla`, `python-dotenv`) plus
`uvicorn`. No vendored source tree, no submodule, and no Python 3.13 floor —
it runs on any 3.11+ ML Runtime.

**Why one file:** Cloudera AI PBJ runtimes execute `app.py` through an IPython
kernel, which does not put the script's directory on `sys.path` and does not
reliably define `__file__`. Any `from <local_package> import ...` is therefore
fragile there. Keeping everything in one file and importing only installed
third-party packages removes that failure mode entirely.

## Prerequisites

| # | Check |
|---|---|
| P1 | **Admin → Security → "Allow applications to be configured with unauthenticated access" is ON.** Without it the checkbox in step 3 doesn't exist, Mistral gets redirected to Cloudera SSO, and `initialize` fails against an HTML login page. Needs a workbench admin — confirm this first, it's the most likely thing to sink the demo. |
| P2 | Workbench has a public load balancer with no source-IP allowlist. Test by curling the workbench domain off-VPN. |
| P3 | CDW Impala coordinator hostname + your CDP **workload** username/password. |
| P4 | A database with Iceberg tables, and an ML Runtime with **Python 3.11+**. |

## 1. Verify locally first

Debugging on your laptop is far faster than debugging a deployed app.

```bash
python3.11 -m venv .venv && .venv/bin/pip install -r requirements.txt
cp .env.example .env      # fill in IMPALA_HOST / USER / PASSWORD / DATABASE

MCP_HOST=127.0.0.1 MCP_PORT=8000 .venv/bin/python app.py &
.venv/bin/python scripts/smoke_test.py http://127.0.0.1:8000/mcp \
    --query "SELECT * FROM <your_table> LIMIT 5"
```

Do not continue until `get_schema` returns your real table list. That proves
Impala connectivity independently of both Cloudera AI and Mistral.

## 2. Get the code into Cloudera AI

```bash
git remote add origin <your-repo-url>
git push -u origin main
```

Then in Cloudera AI: **New Project → Git**, paste the same URL. Redeploys become
`git pull` + restart the application.

Then, in a project session once: `pip install -r requirements.txt`.

The original upstream clone is gitignored and no longer used by anything — you
can delete `iceberg-mcp-server/` whenever you like.

## 3. Create the Application

*Applications → New Application*

| Field | Value |
|---|---|
| Script | `app.py` |
| Runtime | Python **3.11+** |
| Subdomain | e.g. `iceberg-mcp` — this becomes the public hostname |
| **Enable Unauthenticated Access** | ✅ **required** (see P1) |
| Resources | 1 vCPU / 2 GB is plenty |

Environment variables: the `IMPALA_*` values from your `.env`. Leave `MCP_HOST`
and `MCP_PORT` **unset** — `app.py` reads `$CDSW_APP_PORT` and binds `127.0.0.1`,
which is what the workbench ingress expects.

## 4. Verify the public endpoint

From your laptop, **off VPN**:

```bash
curl -i https://iceberg-mcp.<workbench-domain>/healthz     # expect: 200 ok
.venv/bin/python scripts/smoke_test.py https://iceberg-mcp.<workbench-domain>/mcp
```

If `/healthz` returns HTML or a 302 to a Cloudera SSO URL, unauthenticated
access is not actually in effect. Stop and fix P1 — Mistral will hit the exact
same wall.

## 5. Register the connector in Mistral Vibe Work

Connectors → **+ Add Connector** → **Custom MCP Connector**

- **URL:** `https://iceberg-mcp.<workbench-domain>/mcp`
- **Name:** `clouderaiceberg` — Mistral requires "a unique identifier (no
  spaces or special characters)". A hyphen or underscore can leave the Create
  button greyed out.
- **Description:** name the database and what's in it. This is what steers the
  model toward using the tool at all — a vague description is the usual reason
  a connected server never gets called.

Click **Connect**; Mistral auto-detects auth. With `MCP_BEARER_TOKEN` unset it
should connect with none. Confirm `get_schema` and `execute_query` appear.

## 6. Demo script

The `mistral_demo` database holds a small synthetic **30-day readmission risk**
dataset (5 patients, 5 admissions, 7 vitals/labs, 5 clinical notes):

| Table | Columns |
|---|---|
| `patients` | `patient_id`, `full_name`, `date_of_birth`, `sex`, `primary_condition` |
| `admissions` | `admission_id`, `patient_id`, `admit_date`, `discharge_date`, `admitting_diagnosis`, `prior_admissions_90d`, `days_since_discharge`, `follow_up_scheduled`, `readmission_risk_flag` |
| `vitals_labs` | `patient_id`, `admission_id`, `metric`, `value`, `unit`, `recorded_at` |
| `clinical_notes` | `note_id`, `patient_id`, `admission_id`, `note_type`, `author_role`, `note_date`, `note_text` |

Note every column except the two `int`s and two `boolean`s is typed `string`,
including all the dates — so any date arithmetic needs an explicit `CAST`.
Watch for that when the model writes its own SQL.

Chain prompts so the model composes tools rather than doing one lookup:

1. **"What tables are available in this database?"** → `get_schema`
2. **"What's in the admissions table?"** → `execute_query` with `DESCRIBE`
3. **"Which conditions have the most patients flagged as readmission risks?"**
   → the model writes its own join + aggregate. This is the turn that sells it.
4. **"For the flagged patients, was follow-up scheduled, and what do the
   discharge notes say?"** → forces a three-table join and pulls unstructured
   `note_text` alongside structured columns.

Turns 3 and 4 are the demo — the model writing SQL it was never given. This
query is verified working end-to-end and is a good rehearsal target:

```sql
SELECT p.primary_condition,
       COUNT(*) AS admissions,
       SUM(CAST(a.readmission_risk_flag AS INT)) AS flagged_at_risk,
       AVG(a.prior_admissions_90d) AS avg_prior_90d
FROM admissions a
JOIN patients p ON a.patient_id = p.patient_id
GROUP BY p.primary_condition
ORDER BY flagged_at_risk DESC
```

**Warm it up first.** A connection is opened and closed per tool call
(no pooling), so the first query of a cold demo can take a few seconds.

## Troubleshooting

| Symptom | Cause |
|---|---|
| `/healthz` returns a login page or 302 | P1 — unauthenticated access not enabled |
| Connection refused | Wrong bind host/port; check `$CDSW_APP_PORT` is being read |
| `/healthz` fine but `initialize` hangs | SSE buffering at the proxy — confirm `stateless_http=True, json_response=True` took effect |
| `get_schema` → `Error: ...` | `IMPALA_*` credentials, or a sleeping Virtual Warehouse |
| Import crash: `cannot specify both default and default_factory` | pydantic drifted past 2.11.7; re-pin from `requirements.txt` |
| `ModuleNotFoundError` for a local module | Shouldn't happen — `app.py` imports only installed third-party packages. If you reintroduce a local module, note that PBJ runtimes don't put the script's directory on `sys.path`. |
| `asyncio.run() cannot be called from a running event loop` | Same cause — the kernel already has a loop. `app.py` detects this and schedules the server on the existing loop. |
| App shows "running" but nothing answers on the URL | The server never started. Check the app log for the `Starting Iceberg MCP Server on ...` line. |
| `get_schema` → `Error:` but it worked locally | `load_dotenv()` returns `False` in Cloudera AI — `.env` is gitignored and never deployed. The `IMPALA_*` values must be set as **application environment variables**. The `[startup]` log lines print what the app actually sees; `<UNSET>` there is your answer. |
| Tools listed but never called | Connector description too vague — name the data |
| Mistral's Create button greyed out | Either the connector name has a hyphen/underscore/space, or the server negotiates an old MCP protocol version. Mistral requires `2025-06-18`; `fastmcp` 2.9.2 only ever replies `2025-03-26`. `requirements.txt` pins fastmcp 4.x for this reason. |
| `ImportError: cannot import name 'FastMCP'` after upgrading | An in-place pip upgrade from fastmcp 2.x to 4.x leaves broken artifacts. Use `pip install --force-reinstall -r requirements.txt`, or recreate the environment. |

## Security

The demo runs the endpoint **open**: anyone who learns the URL can run arbitrary
`SELECT`s against the warehouse as your workload user.

`mistral_demo` is synthetic, but its tables are named `patients` and
`clinical_notes`. On a public, unauthenticated URL that is worth closing even
for a demo — and it must be closed before this pattern is pointed at any real
clinical data.

To lock it down, set `MCP_BEARER_TOKEN` to any random string on the application
and paste the same value into the Mistral connector's bearer token. The check is
already implemented in `app.py` and `/healthz` stays public so the app tile keeps
reporting healthy. Better still, use a read-only workload account.

## Changes from upstream

`iceberg_mcp/impala.py` is a rewrite of upstream's `tools/impala_tools.py` at
commit `38a7f39`. The tool contract is identical; the fixes below each address
something that shows up during a live demo. Full list in `NOTICE`.

1. **`execute_query` returns column-keyed dicts** instead of bare tuples.
   Upstream returned `[[1,"acme"]]` with no column names, leaving the model to
   guess labels.
2. **Empty-query guard.** `query.strip().lower().split()[0]` raised an uncaught
   `IndexError` on a blank string — it sits outside the `try`. An LLM sending an
   empty argument would 500 the tool.
3. **`_safe_close()`.** impyla builds its connection object lazily, so when the
   socket was never opened (bad host, bad password, sleeping VW) `conn.close()`
   in the `finally` block raises `AttributeError` *over* the real exception —
   every connectivity failure surfaced as
   `'NoneType' object has no attribute 'close'` instead of the actual cause.
4. **Boolean env parsing.** `IMPALA_USE_SSL` / `IMPALA_USE_HTTP_TRANSPORT` were
   passed to impyla as raw strings, so `"false"` was truthy and SSL could not be
   disabled via env.
5. **Cursors closed in a `finally`**, so a failing query doesn't leak one.
