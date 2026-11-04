"""The setup token a user's Claude Code turns sign in with on the backend:
saving, removing, and whether it works, for Settings and the model picker.

- GET    /api/v1/claude-code/status          Whether the user's Claude Code models can run
- POST   /api/v1/claude-code/sign-in         Start a sign-in that makes a setup token
- POST   /api/v1/claude-code/sign-in/code    Finish it with the code Claude's page shows
- PUT    /api/v1/claude-code/token           Save a setup token the user made themselves
- DELETE /api/v1/claude-code/token           Remove the saved token
- GET    /api/v1/claude-code/custody         How turns on cloud computers run, where the user chooses it
- PUT    /api/v1/claude-code/custody         Choose it

The flow and the token store are ``services.claude_code`` (``flow``, ``saved``),
and the choice for cloud computers ``hosted``.
"""

from fastapi import APIRouter, Response

from src.server.models.claude_code import ClaudeCodeCustody, ClaudeCodeStatus, CodeBody, CustodyBody, TokenBody
from src.server.services.claude_code import flow, hosted, saved
from src.server.services.claude_code.flow import backend_on, pasted_code, pasted_token, require
from src.server.services.claude_code.saved import SavedHome
from src.server.services.oauth_signin import OAuthRefusal
from src.server.utils.api import CurrentUserId

router = APIRouter(prefix="/api/v1/claude-code", tags=["claude-code"])


@router.get("/status", response_model=ClaudeCodeStatus)
async def get_status(x_user_id: CurrentUserId):
    return await saved.status(x_user_id)


@router.post("/sign-in")
async def start_sign_in(x_user_id: CurrentUserId):
    require(await backend_on(x_user_id))
    return await flow.start_sign_in(SavedHome(x_user_id))


@router.post("/sign-in/code", response_model=ClaudeCodeStatus)
async def finish_sign_in(body: CodeBody, x_user_id: CurrentUserId):
    require(await backend_on(x_user_id))
    return await flow.finish_sign_in(SavedHome(x_user_id), pasted_code(body))


@router.put("/token", response_model=ClaudeCodeStatus)
async def save_token(body: TokenBody, x_user_id: CurrentUserId):
    require(await backend_on(x_user_id))
    token, rejected = pasted_token(body)
    return await flow.paste(SavedHome(x_user_id), token, rejected)


@router.delete("/token", status_code=204)
async def remove_token(x_user_id: CurrentUserId):
    await flow.remove(SavedHome(x_user_id))
    return Response(status_code=204)


_NO_CHOICE = "This server offers no choice of how Claude Code runs on cloud computers."


@router.get("/custody", response_model=ClaudeCodeCustody)
async def get_custody(x_user_id: CurrentUserId):
    current = await hosted.custody(x_user_id) if await flow.rows_on(x_user_id) else None
    require(current is not None, _NO_CHOICE)
    return current


#: Why a choice is refused, by the reason ``hosted.choose`` gives.
_CUSTODY_REFUSED = {
    "unavailable": (409, "This server doesn't offer that choice right now."),
    "stored_blocked": (403, "Storing a setup token isn't available for your account."),
    "disclosure": (400, "Accept the current terms to store your setup token."),
}


@router.put("/custody", response_model=ClaudeCodeCustody)
async def put_custody(body: CustodyBody, x_user_id: CurrentUserId):
    try:
        chosen = await hosted.choose(x_user_id, body.mode, body.disclosure) if await flow.rows_on(x_user_id) else None
    except hosted.CustodyRefused as refused:
        status, message = _CUSTODY_REFUSED[refused.reason]
        raise OAuthRefusal(status, refused.reason, message, retryable=False) from None
    except hosted.TurnsNotStopped:
        raise flow.turns_running() from None
    require(chosen is not None, _NO_CHOICE)
    return chosen
