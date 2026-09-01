#!/usr/bin/env python3
"""Register the MCP server with Mistral via the API, bypassing the UI.

Use this when the Connectors UI leaves the Create button greyed out. The API
returns an actual error message -- plan restriction, permission, validation --
where the disabled button tells you nothing.

Needs the `mistralai` SDK and an API key from https://console.mistral.ai/

    pip install mistralai
    export MISTRAL_API_KEY=...
    python scripts/register_connector.py https://<subdomain>.<domain>/mcp

Add --list to see existing connectors, or --token to send a bearer token with
every call to your server (matching MCP_BEARER_TOKEN on the application).
"""

import argparse
import os
import sys

try:
    # mistralai 2.x
    from mistralai.client import Mistral
except ImportError:  # pragma: no cover - mistralai 1.x
    from mistralai import Mistral

DEFAULT_NAME = "clouderaiceberg"  # no spaces or special characters
DEFAULT_DESCRIPTION = (
    "Read-only access to patient readmission-risk data in a Cloudera Data "
    "Warehouse: patients, admissions, vitals/labs and clinical notes. Use it "
    "to answer questions about readmission risk, diagnoses and follow-up care."
)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("url", nargs="?", help="MCP endpoint URL, including /mcp")
    ap.add_argument("--name", default=DEFAULT_NAME)
    ap.add_argument("--description", default=DEFAULT_DESCRIPTION)
    ap.add_argument("--token", default=os.getenv("MCP_BEARER_TOKEN"),
                    help="Bearer token for your MCP server, if it requires one")
    ap.add_argument("--visibility", default="private",
                    choices=["private", "shared_workspace", "shared_org"])
    ap.add_argument("--list", action="store_true", help="List connectors and exit")
    args = ap.parse_args()

    key = os.getenv("MISTRAL_API_KEY")
    if not key:
        print("MISTRAL_API_KEY is not set. Create one at https://console.mistral.ai/")
        return 1

    client = Mistral(api_key=key)

    if args.list:
        result = client.beta.connectors.list()
        for c in getattr(result, "data", None) or getattr(result, "connectors", []) or []:
            print(f"  {getattr(c, 'name', '?'):<24} {getattr(c, 'server', '?')}")
        return 0

    if not args.url:
        ap.error("the MCP endpoint URL is required unless --list is given")

    kwargs = dict(
        name=args.name,
        description=args.description,
        server=args.url,
        visibility=args.visibility,
    )
    if args.token:
        kwargs["headers"] = {"Authorization": f"Bearer {args.token}"}

    print(f"Registering {args.name!r} -> {args.url}")
    try:
        connector = client.beta.connectors.create(**kwargs)
    except Exception as e:
        # This is the point of the script: surface the real reason.
        print(f"\nFAILED: {type(e).__name__}: {e}")
        body = getattr(e, "body", None) or getattr(e, "raw_response", None)
        if body:
            print(f"Response: {body}")
        return 1

    print(f"OK. id={getattr(connector, 'id', '?')} name={getattr(connector, 'name', '?')}")
    print("It should now appear under Connectors -> My Connectors.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
