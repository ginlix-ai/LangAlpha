"""The setup token a user's Claude Code turns sign in with on the backend:
saving, removing, and whether it works, for Settings and the model picker.

- GET    /api/v1/claude-code/status          Whether the user's Claude Code models can run
- POST   /api/v1/claude-code/sign-in         Start a sign-in that makes a setup token
- POST   /api/v1/claude-code/sign-in/code    Finish it with the code Claude's page shows
- PUT    /api/v1/claude-code/token           Save a setup token the user made themselves
- DELETE /api/v1/claude-code/token           Remove the saved token

The flow and the token store are ``services.claude_code`` (``flow``, ``saved``).
"""

from fastapi import APIRouter, Response

from src.server.models.claude_code import ClaudeCodeStatus, CodeBody, TokenBody
from src.server.services.claude_code import flow, saved
from src.server.services.claude_code.flow import backend_on, pasted_code, pasted_token, require
from src.server.services.claude_code.saved import SavedHome
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
