"""Read-only Impala access for the MCP tools.

Derived from cloudera/iceberg-mcp-server (Apache-2.0), pinned at upstream commit
38a7f3914934ab851254291ed3a2934bcd95f6cd. See NOTICE for attribution and for
what was changed. Rewritten here so this project has no dependency on the
upstream source tree, whose pyproject requires Python >= 3.13 -- higher than any
current Cloudera AI ML Runtime provides.
"""

import json
import os

from impala.dbapi import connect

# Prefix allowlist. Deliberately conservative: this is a demo endpoint and the
# warehouse account should be read-only in its own right, so this is a guard
# rail rather than a security boundary. It does not stop stacked statements or
# a WITH ... INSERT, which is why it is not the only control.
READONLY_PREFIXES = ("select", "show", "describe", "with")

REFUSAL = "Only read-only queries are allowed."


def _env_flag(name: str, default: str) -> bool:
    """Parse a boolean-ish env var.

    Upstream passed these through as raw strings, so any non-empty value --
    including the string "false" -- came out truthy and SSL could not actually
    be turned off. Parse them properly instead.
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
    raises AttributeError from inside thrift. Raised from a `finally` block that
    would replace the real exception with a useless
    "'NoneType' object has no attribute 'close'".
    """
    if conn is None:
        return
    try:
        conn.close()
    except Exception:
        pass


def execute_query(query: str) -> str:
    """Run a read-only SQL query, returning rows as a JSON array of objects."""
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


def get_schema() -> str:
    """List the table names in the configured database as a JSON array."""
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
