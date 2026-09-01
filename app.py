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
from starlette.responses import JSONResponse, PlainTextResponse

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

PUBLIC_PATHS = ("/healthz", "/")

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


@mcp.custom_route("/", methods=["GET"])
async def index(request):
    return JSONResponse(
        {
            "service": "Cloudera Iceberg MCP Server",
            "mcp_endpoint": PATH,
            "transport": "streamable-http",
            "auth": "bearer" if BEARER_TOKEN else "none",
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
