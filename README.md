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
| `scripts/warmup.py` | Pre-loads Impala metadata before a demo. Skipping it costs ~41s on the first join. |
| `scripts/reinstall_deps.py` | Repairs a broken fastmcp 2.x → 4.x in-place upgrade. |
| `scripts/register_connector.py` | Registers the connector with Mistral via the API, bypassing the UI. Use when the Create button is greyed out — the API returns a real error. |
| `scripts/seed_demo_data.py` | Generates and loads the synthetic dataset. `--dry-run` to inspect, `--yes` to write (it DROPs the tables first). |
| `ontology/readmission.yaml` | Semantic layer source of truth: entities, join paths, metric definitions, pitfalls. |
| `scripts/build_ontology.py` | Validates the ontology against the live warehouse and renders `ontology/ONTOLOGY.md`. |
| `NOTICE` / `LICENSE` | Apache-2.0 attribution for the upstream code this derives from. |

Four runtime dependencies (`fastmcp`, `mcp`, `impyla`, `python-dotenv`) plus
`uvicorn`. No vendored source tree, no submodule, and no Python 3.13 floor —
it runs on any 3.11+ ML Runtime.

**Client compatibility:** Mistral's connector validation follows its own
documented examples, which are looser than the MCP spec — its `initialize`
sample omits the required `clientInfo`, and its reachability check is a `HEAD`
request the MCP endpoint would answer `405`. `ClientCompatMiddleware` injects a
placeholder `clientInfo` and answers `HEAD` with `200`. Requests that already
carry `clientInfo` pass through untouched.

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

Then, in a project session once:

```bash
python scripts/reinstall_deps.py
```

Use that rather than a bare `pip install` when **upgrading** an existing
project. Going from fastmcp 2.x to 4.x in place leaves a broken install that
`--force-reinstall` does not repair — see the troubleshooting table.

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

**If the Create button stays greyed out — use the API instead. It works.**
Confirmed on this deployment: the UI refused to enable Create even though
Mistral's own
[connector debugger](https://console.mistral.ai/build/connectors/debugger)
validated the server (auth `NONE`, clean handshake, protocol `2025-11-25`), the
name had no special characters, and the account was its own administrator. The
identical registration through the API succeeded immediately, so this is a UI
problem, not a server or plan restriction.

```bash
pip install mistralai
export MISTRAL_API_KEY=...        # https://console.mistral.ai/
python scripts/register_connector.py https://<subdomain>.<domain>/mcp
```

## 6. Demo script

`mistral_demo` holds a synthetic **30-day readmission risk** dataset — 500
patients, 1,147 admissions, 6,583 vitals/labs, 2,294 clinical notes. Regenerate
or resize it with `scripts/seed_demo_data.py` (`--dry-run` prints the shape and
the planted signal without writing).

| Table | Columns |
|---|---|
| `patients` | `patient_id`, `full_name`, `date_of_birth` (DATE), `age`, `sex`, `primary_condition` |
| `admissions` | `admission_id`, `patient_id`, `admit_date`/`discharge_date` (DATE), `length_of_stay_days`, `admitting_diagnosis`, `department`, `prior_admissions_90d`, `days_since_discharge`, `follow_up_scheduled`, `discharge_disposition`, `readmission_risk_flag`, `readmitted_within_30d` |
| `vitals_labs` | `patient_id`, `admission_id`, `metric`, `value` (DOUBLE), `unit`, `recorded_at` (DATE), `abnormal` |
| `clinical_notes` | `note_id`, `patient_id`, `admission_id`, `note_type`, `author_role`, `note_date` (DATE), `note_text` |

Dates are real `DATE` columns and `value` is a `DOUBLE`, so date arithmetic and
averages work without casting.

### The signal that's actually in the data

Deliberately planted, so the model finds a real pattern rather than counting rows:

| Cut | 30-day readmission rate |
|---|---|
| Follow-up scheduled | **22.4%** (n=851) |
| No follow-up scheduled | **39.2%** (n=296) |
| 0 / 1 / 2 prior admissions in 90d | **24.4% / 35.3% / 50.0%** |
| Congestive heart failure → appendectomy | **44.2% → 6.8%** |

`readmission_risk_flag` is a *predicted* flag recorded at discharge, and is
imperfect on purpose — 37% precision, 62% recall — so "how well does the risk
flag actually predict readmission?" has a real, non-trivial answer.

### Prompts

1. **"What tables are available in this database?"** → `get_schema`
2. **"What's in the admissions table?"** → `DESCRIBE`
3. **"Which admitting diagnoses have the highest 30-day readmission rates?"**
   → the model writes its own aggregate. CHF 44% vs appendectomy 7%.
4. **"Does scheduling a follow-up appointment actually reduce readmissions?"**
   → 22.4% vs 39.2%. The moment the demo lands.
5. **"How accurate is the discharge risk flag?"** → forces a confusion-matrix
   style query against `readmitted_within_30d`.
6. **"Pull the discharge notes for readmitted CHF patients who had no
   follow-up scheduled."** → three-table join, unstructured text beside
   structured columns.

Turns 4 and 5 are the demo: questions with real answers that nobody handed the
model the SQL for.

**Warm it up first — this matters more than it sounds.** The first query that
joins a table Impala hasn't loaded metadata for was measured at **41s and then
121s** on two separate cold starts; the identical query runs in **0.4s** once
warm. The warehouse auto-suspends when idle, so run this a minute before
demoing, every time:

```bash
python scripts/warmup.py https://<subdomain>.<workbench-domain>/mcp
```

## 7. Give the model a semantic layer

`get_schema` tells the model the tables exist; it doesn't say how they join,
what a metric means, or that `readmission_risk_flag` is a prediction rather
than an outcome. `ontology/ONTOLOGY.md` supplies that.

Mistral custom connectors don't yet support MCP resources or prompt templates,
so the ontology is delivered on the Mistral side instead — upload
`ontology/ONTOLOGY.md` to a **Library**, or paste it into a custom **Agent's**
instructions alongside the connector.

Because it lives outside the repo it can drift from the schema silently, and a
semantic layer that misnames a column is worse than none — the model trusts it
and writes SQL that fails. So it is generated, never hand-written:

```bash
python scripts/build_ontology.py            # validate + render
python scripts/build_ontology.py --check    # validate only
```

Every table and column must exist, and **every metric's SQL must actually
execute**, or the build aborts. Enum values are read from the data. Re-run it
after any schema or data change, and re-upload the result.

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
| Mistral's Create button greyed out | Three known causes, all handled here: (1) the connector name has a hyphen/underscore/space — Mistral requires "no spaces or special characters"; (2) the server negotiates an old protocol version — Mistral needs `2025-06-18`, and fastmcp 2.9.2 only ever replies `2025-03-26`, which is why `requirements.txt` pins fastmcp 4.x; (3) Mistral's probe omits `clientInfo` from `initialize`, which a spec-compliant server rejects with `-32602` — `ClientCompatMiddleware` in `app.py` injects a placeholder. Also note custom connectors are **administrator-only**. |
| `ImportError: cannot import name 'FastMCP' from 'fastmcp' (unknown location)` | pip installed 4.0.0's files, then uninstalled 2.9.2 and deleted the filenames both versions share (`__init__.py`, `settings.py`, `exceptions.py`), leaving a directory Python treats as an empty namespace package. `--force-reinstall` does **not** fix it — the stale directory is never removed. Run `python scripts/reinstall_deps.py`. |

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
