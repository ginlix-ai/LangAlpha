"""
Test script for Robinhood MCP OAuth flow.

Run with:
    uv run python scripts/utils/test_robinhood_oauth.py

Flow:
1. Dynamically register a client (redirect_uri = http://localhost:9876/callback)
2. Generate PKCE verifier + S256 challenge
3. Open authorize URL in browser
4. Local HTTP server captures the callback with code + state
5. Exchange code for tokens
6. Make a test call to the Robinhood MCP endpoint
"""

import asyncio
import base64
import hashlib
import json
import secrets
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Event, Thread
from urllib.parse import parse_qs, urlencode, urlparse

import httpx

# ── Constants ────────────────────────────────────────────────────────────────

REGISTER_URL   = "https://agent.robinhood.com/oauth/trading/register"
AUTHORIZE_URL  = "https://robinhood.com/oauth"
TOKEN_URL      = "https://api.robinhood.com/oauth2/token/"
MCP_URL        = "https://agent.robinhood.com/mcp/trading"
REDIRECT_URI   = "http://localhost:9876/callback"
SCOPE          = "internal"

# ── PKCE ─────────────────────────────────────────────────────────────────────

def pkce_pair() -> tuple[str, str]:
    verifier = secrets.token_urlsafe(32)
    digest = hashlib.sha256(verifier.encode()).digest()
    challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
    return verifier, challenge

# ── Local callback server ─────────────────────────────────────────────────────

_callback_result: dict = {}
_callback_event = Event()


class _CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/callback":
            qs = parse_qs(parsed.query)
            _callback_result["code"]  = qs.get("code",  [None])[0]
            _callback_result["state"] = qs.get("state", [None])[0]
            _callback_result["error"] = qs.get("error", [None])[0]

            if _callback_result.get("error"):
                body = f"<h2>Error: {_callback_result['error']}</h2><p>Check the terminal.</p>"
            else:
                body = "<h2>Authorization received!</h2><p>You can close this tab.</p>"

            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(body.encode())
            _callback_event.set()
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *args):
        pass  # suppress default access log


def _run_server(server: HTTPServer):
    server.handle_request()  # handle exactly one request then stop


# ── OAuth steps ───────────────────────────────────────────────────────────────

async def register_client() -> str:
    print("Registering client with Robinhood MCP...")
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(
            REGISTER_URL,
            json={
                "redirect_uris": [REDIRECT_URI],
                "token_endpoint_auth_method": "none",
            },
        )
        resp.raise_for_status()
        data = resp.json()
    print(f"  client_id : {data['client_id']}")
    print(f"  client_name: {data['client_name']}")
    return data["client_id"]


async def exchange_code(code: str, verifier: str, client_id: str) -> dict:
    print("\nExchanging authorization code for tokens...")
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            TOKEN_URL,
            data={
                "grant_type":    "authorization_code",
                "code":          code,
                "client_id":     client_id,
                "redirect_uri":  REDIRECT_URI,
                "code_verifier": verifier,
            },
        )
        if resp.status_code >= 400:
            print(f"  ERROR {resp.status_code}: {resp.text}")
        resp.raise_for_status()
        return resp.json()


async def test_mcp_call(token: str):
    """Fire a real MCP initialize to verify the token works."""
    print("\nTesting token against MCP endpoint...")
    from mcp import ClientSession
    from mcp.client.streamable_http import streamable_http_client

    async with httpx.AsyncClient(
        headers={"Authorization": f"Bearer {token}"},
        timeout=30.0,
    ) as http_client:
        async with streamable_http_client(MCP_URL, http_client=http_client) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tools = await session.list_tools()
                print(f"  MCP connected! {len(tools.tools)} tools available.")
                print(f"  First few: {[t.name for t in tools.tools[:5]]}")


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    # 1. Register
    client_id = await register_client()

    # 2. PKCE
    verifier, challenge = pkce_pair()
    state = secrets.token_urlsafe(16)

    # 3. Build authorize URL
    params = {
        "response_type":         "code",
        "client_id":             client_id,
        "redirect_uri":          REDIRECT_URI,
        "scope":                 SCOPE,
        "state":                 state,
        "code_challenge":        challenge,
        "code_challenge_method": "S256",
    }
    authorize_url = f"{AUTHORIZE_URL}?{urlencode(params)}"

    # 4. Start local callback server before opening browser
    server = HTTPServer(("localhost", 9876), _CallbackHandler)
    Thread(target=_run_server, args=(server,), daemon=True).start()

    print(f"\nOpening browser for Robinhood authorization...")
    print(f"URL: {authorize_url}\n")
    webbrowser.open(authorize_url)

    # 5. Wait for callback (timeout 120s)
    print("Waiting for callback on http://localhost:9876/callback ...")
    got_callback = _callback_event.wait(timeout=120)
    if not got_callback:
        print("Timeout waiting for callback.")
        return

    if _callback_result.get("error"):
        print(f"Authorization error: {_callback_result['error']}")
        return

    code      = _callback_result["code"]
    got_state = _callback_result["state"]

    # 6. Verify state
    if got_state != state:
        print(f"State mismatch! Expected {state}, got {got_state}")
        return
    print(f"  State verified OK")
    print(f"  code: {code[:20]}...")

    # 7. Exchange code for tokens
    tokens = await exchange_code(code, verifier, client_id)
    print(f"  access_token  : {tokens.get('access_token', '')[:30]}...")
    print(f"  refresh_token : {tokens.get('refresh_token', '')[:30]}...")
    print(f"  expires_in    : {tokens.get('expires_in')} seconds")

    # Save to local file for inspection
    output = {
        "client_id":     client_id,
        "access_token":  tokens.get("access_token"),
        "refresh_token": tokens.get("refresh_token"),
        "expires_in":    tokens.get("expires_in"),
    }
    with open("/tmp/robinhood_tokens.json", "w") as f:
        json.dump(output, f, indent=2)
    print("\n  Tokens saved to /tmp/robinhood_tokens.json")

    # 8. Test MCP call
    try:
        await test_mcp_call(tokens["access_token"])
    except Exception as e:
        print(f"  MCP test failed: {e}")

    print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
