"""Cloudera AI Application entrypoint: Iceberg/Impala MCP server over HTTP.

Serves two read-only tools over MCP Streamable HTTP so that a SaaS MCP client
(Mistral Vibe Work) can reach it at the workbench's public application URL.

Derived from cloudera/iceberg-mcp-server (Apache-2.0); see NOTICE.
"""

import asyncio
import hmac
import os
import sys

import uvicorn
from dotenv import load_dotenv
from fastmcp import FastMCP
from starlette.middleware import Middleware
from starlette.responses import JSONResponse, PlainTextResponse

load_dotenv()

# Put the project root on sys.path before importing our own package.
# Cloudera AI PBJ runtimes execute this file through an IPython kernel rather
# than as `python app.py`, and a kernel does not add the script's directory to
# sys.path the way the interpreter does -- so `import iceberg_mcp` fails with
# ModuleNotFoundError unless we do it ourselves. `__file__` is also not
# guaranteed to exist under a kernel, hence the fallback to cwd.
_HERE = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
for _candidate in (_HERE, os.getcwd()):
    if os.path.isdir(os.path.join(_candidate, "iceberg_mcp")):
        if _candidate not in sys.path:
            sys.path.insert(0, _candidate)
        break

from iceberg_mcp import impala  # noqa: E402  (must follow the sys.path bootstrap)

# Bind host: Cloudera AI documents binding applications to 127.0.0.1 on
# $CDSW_APP_PORT; the ingress reaches the process inside the pod's network
# namespace. Overridable so local testing can use 0.0.0.0.
HOST = os.getenv("MCP_HOST", "127.0.0.1")
PORT = int(os.getenv("CDSW_APP_PORT") or os.getenv("MCP_PORT") or "8100")
PATH = os.getenv("MCP_PATH", "/mcp")

# Optional shared secret. Unset (the default) means the endpoint is open, which
# is what a Cloudera AI application with "Enable Unauthenticated Access" gives
# you. Set it and paste the same value into the Mistral connector's bearer
# token to lock the endpoint down without any other change.
BEARER_TOKEN = os.getenv("MCP_BEARER_TOKEN") or None

PUBLIC_PATHS = ("/healthz", "/")

mcp = FastMCP(
    name="Cloudera Iceberg MCP Server",
    instructions=(
        "Read-only access to Iceberg tables in a Cloudera Data Warehouse via "
        "Impala SQL. Call get_schema first to discover tables, then DESCRIBE a "
        "table before querying it."
    ),
)


# The docstrings below become the tool descriptions the model sees when
# choosing a tool, so they carry the usage guidance rather than the code
# comments doing it.
@mcp.tool()
def get_schema() -> str:
    """List the tables available in the Cloudera Iceberg database.

    Call this first when you don't yet know what data exists. Takes no
    arguments. Returns a JSON array of table names.
    """
    return impala.get_schema()


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
    return impala.execute_query(query)


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

    def __init__(self, app, token: str | None):
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


# stateless_http + json_response are deliberate: every call becomes a plain
# POST -> JSON with no mcp-session-id stickiness and no long-lived SSE stream,
# which is what survives the Cloudera ingress proxy in front of the app.
app = mcp.http_app(
    path=PATH,
    transport="http",
    stateless_http=True,
    json_response=True,
    middleware=[Middleware(BearerAuthMiddleware, token=BEARER_TOKEN)],
)


def serve() -> None:
    """Start the server, whether or not an event loop is already running.

    uvicorn.run() calls asyncio.run(), which raises RuntimeError inside a
    Cloudera AI PBJ runtime because the IPython kernel already has a loop
    running. In that case, schedule the server on the existing loop instead;
    the kernel process stays alive and keeps serving.
    """
    print(
        f"Starting Iceberg MCP Server on http://{HOST}:{PORT}{PATH} "
        f"(auth: {'bearer' if BEARER_TOKEN else 'none'})",
        flush=True,
    )
    config = uvicorn.Config(app, host=HOST, port=PORT, log_level=os.getenv("LOG_LEVEL", "info"))
    server = uvicorn.Server(config)

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop is not None and loop.is_running():
        print("Event loop already running (PBJ runtime); serving on it.", flush=True)
        loop.create_task(server.serve())
    else:
        server.run()


# Not guarded by `if __name__ == "__main__"`. Under a PBJ runtime the module
# name is not always "__main__", and a guard that silently does nothing would
# leave the application "running" with nothing listening on $CDSW_APP_PORT.
serve()
