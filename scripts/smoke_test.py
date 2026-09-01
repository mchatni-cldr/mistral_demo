#!/usr/bin/env python3
"""End-to-end check of a deployed Iceberg MCP endpoint.

Drives the real MCP protocol over the same Streamable HTTP transport Mistral
Vibe uses, so a pass here means "the server is fine" and any remaining problem
is connector configuration.

Usage:
    python scripts/smoke_test.py http://localhost:8000/mcp
    python scripts/smoke_test.py https://iceberg-mcp.<workbench-domain>/mcp
    MCP_BEARER_TOKEN=... python scripts/smoke_test.py <url>
    python scripts/smoke_test.py <url> --query "SELECT * FROM my_table LIMIT 5"
"""

import argparse
import asyncio
import os
import sys

from contextlib import asynccontextmanager

from mcp import ClientSession

# mcp 2.x renamed the transport and moved headers onto an httpx client;
# mcp 1.x took a `headers` kwarg directly. Support both.
try:
    from mcp.client.streamable_http import httpx2
    from mcp.client.streamable_http import streamable_http_client as _transport

    _MCP2 = True
except ImportError:
    from mcp.client.streamable_http import streamablehttp_client as _transport

    _MCP2 = False


@asynccontextmanager
async def connect(url: str, headers: dict):
    if _MCP2:
        async with httpx2.AsyncClient(headers=headers, timeout=60) as http_client:
            async with _transport(url, http_client=http_client) as streams:
                yield streams[0], streams[1]
    else:
        async with _transport(url, headers=headers) as streams:
            yield streams[0], streams[1]


def render(result) -> tuple[str, bool]:
    """Return (text, failed). A tool can fail two ways: an MCP-level error
    (result.isError, e.g. an uncaught exception) or these tools' convention of
    returning a plain "Error: ..." string from their own except block."""
    parts = [getattr(block, "text", repr(block)) for block in result.content]
    text = "\n".join(parts)
    # mcp 1.x used camelCase, 2.x uses snake_case.
    is_error = getattr(result, "is_error", None)
    if is_error is None:
        is_error = getattr(result, "isError", False)
    failed = bool(is_error) or text.startswith("Error")
    limit = int(os.getenv("SMOKE_MAX_CHARS", "600"))
    if len(text) > limit:
        text = text[:limit] + f"... [{len(text)} chars total]"
    return text, failed


async def run(url: str, token: str | None, query: str | None) -> int:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    failures = []

    async with connect(url, headers) as (read, write):
        async with ClientSession(read, write) as session:
            init = await session.initialize()
            info = getattr(init, "server_info", None) or getattr(init, "serverInfo")
            proto = getattr(init, "protocol_version", None) or getattr(
                init, "protocolVersion", "?"
            )
            print(f"[ok]   initialize  -> {info.name} (protocol {proto})")

            tools = (await session.list_tools()).tools
            names = [t.name for t in tools]
            print(f"[ok]   tools/list  -> {names}")
            for expected in ("get_schema", "execute_query"):
                if expected not in names:
                    failures.append(f"tool {expected!r} missing")

            schema, failed = render(await session.call_tool("get_schema", {}))
            print(f"[{'!!' if failed else 'ok'}]   get_schema  -> {schema}")
            if failed:
                failures.append("get_schema failed (check the IMPALA_* env vars)")

            if query:
                rows, failed = render(
                    await session.call_tool("execute_query", {"query": query})
                )
                print(f"[{'!!' if failed else 'ok'}]   execute_query -> {rows}")
                if failed:
                    failures.append("execute_query failed")

            # The empty-query guard must return a message, not blow up the tool.
            guard, _ = render(await session.call_tool("execute_query", {"query": "   "}))
            if guard == "Only read-only queries are allowed.":
                print("[ok]   empty-query guard -> refused cleanly")
            else:
                print(f"[!!]   empty-query guard -> {guard}")
                failures.append("empty-query guard did not refuse cleanly")

    if failures:
        print("\nFAILED:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("\nAll checks passed.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url", help="Full MCP endpoint URL, including /mcp")
    parser.add_argument("--token", default=os.getenv("MCP_BEARER_TOKEN"),
                        help="Bearer token (defaults to $MCP_BEARER_TOKEN)")
    parser.add_argument("--query", default=None,
                        help="Optional SQL to run through execute_query")
    args = parser.parse_args()
    return asyncio.run(run(args.url, args.token, args.query))


if __name__ == "__main__":
    sys.exit(main())
