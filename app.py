"""Cloudera AI Application: Iceberg/Impala MCP server over Streamable HTTP.

Serves two read-only tools so a SaaS MCP client (Mistral Vibe Work) can reach
them at the workbench's public application URL.

Deliberately a SINGLE FILE with no local package import. Cloudera AI PBJ
runtimes execute this through an IPython kernel, which does not put the
script's directory on sys.path and does not reliably define __file__, so any
`from <local_package> import ...` is fragile here. Only third-party packages
installed into site-packages are imported below.

Derived from cloudera/iceberg-mcp-server (Apache-2.0); see NOTICE.
"""

import asyncio
import base64
import hmac
import json
import os
import sys

import uvicorn
from dotenv import load_dotenv
from fastmcp import FastMCP
from impala.dbapi import connect
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse, PlainTextResponse, Response

load_dotenv()  # no-op in Cloudera AI: .env is gitignored, use app env vars

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Cloudera AI documents binding applications to 127.0.0.1 on $CDSW_APP_PORT;
# the ingress reaches the process inside the pod's network namespace.
HOST = os.getenv("MCP_HOST", "127.0.0.1")
PORT = int(os.getenv("CDSW_APP_PORT") or os.getenv("MCP_PORT") or "8100")
PATH = os.getenv("MCP_PATH", "/mcp")

# Unset (the default) = open endpoint, which is what "Enable Unauthenticated
# Access" gives you. Set it and paste the same value into the Mistral
# connector's bearer token to lock the endpoint down.
BEARER_TOKEN = os.getenv("MCP_BEARER_TOKEN") or None

PUBLIC_PATHS = ("/healthz", "/", "/icon.png")

# Cloudera "C" mark, 180x180 PNG, embedded rather than read from disk:
# PBJ runtimes do not reliably define __file__ or set cwd, so a path lookup
# here would reintroduce the exact failure that broke the first deployments.
ICON_PNG = base64.b64decode(
    """
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
iWSswO5Q5LXYbQpdDoNkuRaRhc8l5BQ7gERIDiD7LXYHP3sQy1JN4b+wxmIBSTyNBO81vAwjlmKMTe7EWu2olg9uSyiLY7vB
JYGody/isw8jYb9+xJY0IFGSJGJNUlR25nW4mCsElyJC0BySBLnLqjf+PPi9gKi1h9iKWCdEtguPBaHXoozgKcR2NCIkziCK
PRBcehHl3ouQPWwRW1tLslW7UjZXbN3bwn2v7yARijFEfYcRZV5g1S8vIoR/LAi8FrvZcqyLsoiIRVSugHjLMLSXRhI0HUhq
vSX4vZPVBWUjkp1sQL4U9ayq+kMPAWOViOGYhxxSRruELO4Wkeq2GcQfzyFeeAaJyd8rFKKC3SJxwGOp0OthTflqSG5ddt1F
yN2JqPje4HojqwvPZh4ucqIQSzCBKG8OIfAUosYTrNaGe6wS15Rdj13FWyXx/wE0zTgS/MPGVAAAACV0RVh0ZGF0ZTpjcmVh
dGUAMjAyNS0wMS0zMVQwMjowMToxNiswMDowME3hMK4AAAAldEVYdGRhdGU6bW9kaWZ5ADIwMjUtMDEtMzFUMDI6MDE6MTYr
MDA6MDA8vIgSAAAAIHRFWHRzb2Z0d2FyZQBodHRwczovL2ltYWdlbWFnaWNrLm9yZ7zPHZ0AAAAYdEVYdFRodW1iOjpEb2N1
bWVudDo6UGFnZXMAMaf/uy8AAAAYdEVYdFRodW1iOjpJbWFnZTo6SGVpZ2h0ADE5MkBdcVUAAAAXdEVYdFRodW1iOjpJbWFn
ZTo6V2lkdGgAMTky06whCAAAABl0RVh0VGh1bWI6Ok1pbWV0eXBlAGltYWdlL3BuZz+yVk4AAAAXdEVYdFRodW1iOjpNVGlt
ZQAxNzM4Mjg4ODc2uCmCywAAAA90RVh0VGh1bWI6OlNpemUAMEJClKI+7AAAAFZ0RVh0VGh1bWI6OlVSSQBmaWxlOi8vL21u
dGxvZy9mYXZpY29ucy8yMDI1LTAxLTMxLzVlZjg3NGE3Y2RlOWVhYjAyYTk4MmVmMDZiY2ZhYjU5Lmljby5wbmfQi1lDAAAA
AElFTkSuQmCC
"""
)


# Guard rail, not a security boundary: the warehouse account should be
# read-only in its own right. Does not stop stacked statements or WITH ... INSERT.
READONLY_PREFIXES = ("select", "show", "describe", "with")
REFUSAL = "Only read-only queries are allowed."

# ---------------------------------------------------------------------------
# Impala access
# ---------------------------------------------------------------------------


def _env_flag(name: str, default: str) -> bool:
    """Parse a boolean-ish env var.

    Upstream passed these to impyla as raw strings, so any non-empty value --
    including "false" -- was truthy and SSL could not be turned off.
    """
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "on")


def get_connection():
    """Open a connection to the Impala coordinator using IMPALA_* env vars."""
    return connect(
        host=os.getenv("IMPALA_HOST", "localhost"),
        port=int(os.getenv("IMPALA_PORT", "443")),
        user=os.getenv("IMPALA_USER", ""),
        password=os.getenv("IMPALA_PASSWORD", ""),
        database=os.getenv("IMPALA_DATABASE", "default"),
        auth_mechanism=os.getenv("IMPALA_AUTH_MECHANISM", "LDAP"),
        use_http_transport=_env_flag("IMPALA_USE_HTTP_TRANSPORT", "true"),
        http_path=os.getenv("IMPALA_HTTP_PATH", "cliservice"),
        use_ssl=_env_flag("IMPALA_USE_SSL", "true"),
    )


def _safe_close(conn) -> None:
    """Close a connection without masking the error that caused the failure.

    impyla builds its connection object lazily, so when the socket was never
    opened (bad host, bad password, sleeping virtual warehouse) conn.close()
    raises AttributeError from a `finally` block and replaces the real
    exception with "'NoneType' object has no attribute 'close'".
    """
    if conn is None:
        return
    try:
        conn.close()
    except Exception:
        pass


def _execute_query(query: str) -> str:
    tokens = query.strip().lower().split()
    if not tokens or tokens[0] not in READONLY_PREFIXES:
        return REFUSAL

    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute(query)
            if cur.description is None:
                return "Query executed successfully."
            # Key rows by column name. Bare tuples give the model no way to
            # label values, which makes every downstream answer a guess.
            columns = [d[0] for d in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
            return json.dumps(rows, default=str)
        finally:
            cur.close()
    except Exception as e:
        return f"Error: {e}"
    finally:
        _safe_close(conn)


def _get_schema() -> str:
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute("SHOW TABLES")
            return json.dumps([row[0] for row in cur.fetchall()])
        finally:
            cur.close()
    except Exception as e:
        return f"Error: {e}"
    finally:
        _safe_close(conn)


# ---------------------------------------------------------------------------
# MCP server
# ---------------------------------------------------------------------------

mcp = FastMCP(
    name="Cloudera Iceberg MCP Server",
    instructions=(
        "Read-only access to Iceberg tables in a Cloudera Data Warehouse via "
        "Impala SQL. Call get_schema first to discover tables, then DESCRIBE a "
        "table before querying it."
    ),
)


# These docstrings become the tool descriptions the model sees when choosing a
# tool, so the usage guidance lives here rather than in code comments.
@mcp.tool()
def get_schema() -> str:
    """List the tables available in the Cloudera Iceberg database.

    Call this first when you don't yet know what data exists. Takes no
    arguments. Returns a JSON array of table names.
    """
    return _get_schema()


@mcp.tool()
def execute_query(query: str) -> str:
    """Run a read-only SQL query against the Cloudera Iceberg tables via Impala.

    Use Impala SQL syntax. Only SELECT, SHOW, DESCRIBE and WITH statements are
    permitted; anything else is refused. Run `DESCRIBE <table>` before querying
    a table you have not seen, since column types are not what you might assume:
    dates are commonly stored as strings and need an explicit CAST before any
    date arithmetic or comparison.

    Returns a JSON array of row objects keyed by column name, or a string
    beginning with "Error:" if the query failed.
    """
    return _execute_query(query)


@mcp.custom_route("/healthz", methods=["GET"])
async def healthz(request):
    """Liveness probe that needs no MCP handshake and no Impala connection."""
    return PlainTextResponse("ok")


@mcp.custom_route("/icon.png", methods=["GET"])
async def icon(request):
    """Serve the connector icon.

    Mistral's icon_url needs a stable public image. Hotlinking cloudera.com
    means depending on an AEM clientlibs build path that can change on any
    site deploy -- a broken icon on stage. This URL is ours.
    """
    return Response(
        ICON_PNG,
        media_type="image/png",
        headers={"Cache-Control": "public, max-age=86400"},
    )


@mcp.custom_route("/", methods=["GET"])
async def index(request):
    return JSONResponse(
        {
            "service": "Cloudera Iceberg MCP Server",
            "mcp_endpoint": PATH,
            "transport": "streamable-http",
            "auth": "bearer" if BEARER_TOKEN else "none",
            "icon": "/icon.png",
        }
    )


class BearerAuthMiddleware:
    """Pure-ASGI bearer check. No-op unless MCP_BEARER_TOKEN is set.

    Raw ASGI rather than BaseHTTPMiddleware so it never buffers or otherwise
    interferes with the MCP response body.
    """

    def __init__(self, app, token):
        self.app = app
        self.token = token

    async def __call__(self, scope, receive, send):
        if self.token is None or scope["type"] != "http" or scope["path"] in PUBLIC_PATHS:
            await self.app(scope, receive, send)
            return

        header = ""
        for key, value in scope.get("headers", []):
            if key == b"authorization":
                header = value.decode("latin-1")
                break

        presented = header[7:] if header[:7].lower() == "bearer " else ""
        if not hmac.compare_digest(presented, self.token):
            await JSONResponse({"error": "unauthorized"}, status_code=401)(scope, receive, send)
            return

        await self.app(scope, receive, send)


class ClientCompatMiddleware:
    """Tolerate probe requests that don't quite follow the MCP spec.

    Mistral's connector validation follows its own documented examples, which
    are looser than the spec:

    * Its `initialize` example omits `clientInfo`. The spec marks that
      required, so the server answers -32602 "Invalid request parameters" and
      the platform concludes the handshake failed -- leaving the Create button
      disabled with no useful error. We inject a placeholder instead.
    * Its reachability example is `curl -I`, a HEAD request, which the MCP
      endpoint answers 405. We answer 200 so the probe sees a live server.

    Neither changes behaviour for a spec-compliant client: a request that
    already carries clientInfo is passed through untouched.
    """

    PLACEHOLDER = {"name": "unknown-client", "version": "0.0.0"}

    def __init__(self, app, paths):
        self.app = app
        self.paths = paths

    def _patch(self, body: bytes) -> bytes:
        try:
            payload = json.loads(body)
        except (ValueError, UnicodeDecodeError):
            return body
        if not isinstance(payload, dict) or payload.get("method") != "initialize":
            return body
        params = payload.get("params")
        if not isinstance(params, dict) or params.get("clientInfo"):
            return body
        params["clientInfo"] = self.PLACEHOLDER
        return json.dumps(payload).encode()

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope.get("path") not in self.paths:
            await self.app(scope, receive, send)
            return

        if scope["method"] == "HEAD":
            await PlainTextResponse("", status_code=200)(scope, receive, send)
            return

        if scope["method"] != "POST":
            await self.app(scope, receive, send)
            return

        body = b""
        while True:
            message = await receive()
            if message["type"] != "http.request":
                await self.app(scope, receive, send)
                return
            body += message.get("body", b"")
            if not message.get("more_body", False):
                break

        body = self._patch(body)
        headers = [(k, v) for k, v in scope["headers"] if k != b"content-length"]
        headers.append((b"content-length", str(len(body)).encode()))
        scope = dict(scope, headers=headers)

        delivered = False

        async def replay():
            nonlocal delivered
            if delivered:
                return {"type": "http.disconnect"}
            delivered = True
            return {"type": "http.request", "body": body, "more_body": False}

        await self.app(scope, replay, send)


class NormalizeMcpPath:
    """Serve the MCP endpoint at both /mcp and /mcp/ without a redirect.

    Starlette redirects one form to the other with a 307, and which form is
    canonical flipped between fastmcp 2.x and 4.x. A 307 on POST relies on the
    client re-sending the body, which not every client does correctly -- and a
    connector that fails this way looks like a server fault. Rewriting the path
    before routing means either URL answers 200 directly.
    """

    def __init__(self, app, path):
        self.app = app
        self.path = path
        self.variants = {path, path + "/"} if not path.endswith("/") else {path, path[:-1]}

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope.get("path") in self.variants:
            scope = dict(scope, path=self.path, raw_path=self.path.encode())
        await self.app(scope, receive, send)


# stateless_http + json_response are deliberate: every call becomes a plain
# POST -> JSON with no mcp-session-id stickiness and no long-lived SSE stream,
# which is what survives the Cloudera ingress proxy in front of the app.
_app = mcp.http_app(
    path=PATH,
    transport="http",
    stateless_http=True,
    json_response=True,
    middleware=[
        # Permissive CORS: without it, OPTIONS preflight returns 405 and any
        # browser-side validation of this endpoint fails before it can start.
        Middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_methods=["*"],
            allow_headers=["*"],
            expose_headers=["mcp-session-id", "mcp-protocol-version"],
        ),
        Middleware(BearerAuthMiddleware, token=BEARER_TOKEN),
    ],
)

# Wrapped outside the Starlette app so they run before routing. Lifespan and
# every other scope type pass straight through. NormalizeMcpPath is outermost
# so the compat layer always sees the canonical path.
app = NormalizeMcpPath(ClientCompatMiddleware(_app, {PATH}), PATH)


def serve() -> None:
    """Start the server, whether or not an event loop is already running.

    uvicorn.run() calls asyncio.run(), which raises RuntimeError inside a
    Cloudera AI PBJ runtime because the IPython kernel already has a loop
    running. In that case, schedule the server on the existing loop instead;
    the kernel process stays alive and keeps serving.
    """
    print(f"[startup] python     : {sys.version.split()[0]}", flush=True)
    print(f"[startup] cwd        : {os.getcwd()}", flush=True)
    print(f"[startup] impala host: {os.getenv('IMPALA_HOST', '<UNSET>')}", flush=True)
    print(f"[startup] database   : {os.getenv('IMPALA_DATABASE', '<UNSET>')}", flush=True)
    print(f"[startup] user       : {os.getenv('IMPALA_USER', '<UNSET>')}", flush=True)
    print(
        f"[startup] serving on http://{HOST}:{PORT}{PATH} "
        f"(auth: {'bearer' if BEARER_TOKEN else 'none'})",
        flush=True,
    )

    server = uvicorn.Server(
        uvicorn.Config(app, host=HOST, port=PORT, log_level=os.getenv("LOG_LEVEL", "info"))
    )

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop is not None and loop.is_running():
        print("[startup] event loop already running (PBJ runtime); serving on it", flush=True)
        loop.create_task(server.serve())
    else:
        server.run()


# Not guarded by `if __name__ == "__main__"`. Under a PBJ runtime the module
# name is not reliably "__main__", and a guard that silently does nothing would
# leave the application reporting healthy with nothing on $CDSW_APP_PORT.
serve()
