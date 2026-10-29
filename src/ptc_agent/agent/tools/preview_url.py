"""Get preview URLs for services running in the sandbox."""

from collections.abc import Awaitable, Callable
from typing import Any

import structlog
from langchain_core.tools import BaseTool, tool

from ptc_agent.agent.backends.sandbox import SandboxBackend
from ptc_agent.agent.tools.utils import mark_sandbox_raised
from ptc_agent.core.sandbox.runtime import PreviewInfo
from src.observability import TOOL_CHANNEL, TOOL_PROVIDER

logger = structlog.get_logger(__name__)

# (sandbox_id, port, preview) -> None. The whole preview rather than its URL,
# because only the server can tell what a browser may be handed for it.
OnSignedUrl = Callable[[str, int, PreviewInfo], Awaitable[None]]


def create_preview_url_tool(
    backend: SandboxBackend,
    *,
    workspace_id: str = "",
    on_signed_url: OnSignedUrl | None = None,
) -> BaseTool:
    """Factory function to create GetPreviewUrl tool with injected dependencies.

    Args:
        backend: SandboxBackend wrapping the sandbox
        workspace_id: Workspace ID for preview URL generation
        on_signed_url: Optional async callback that records the started
            preview, so the panel opens it without a second resolve

    Returns:
        Configured GetPreviewUrl tool function
    """

    @tool(response_format="content_and_artifact")
    async def GetPreviewUrl(
        port: int,
        command: str,
        title: str | None = None,
        path: str | None = None,
    ) -> tuple[str, dict[str, Any]]:
        """Get a preview URL for a service running on the given port in the sandbox.

        This tool starts the given command in the background AND generates a preview URL.
        Always provide the command used to start the server — it will be persisted so the
        server can be restarted automatically when the user reopens the preview later.

        Args:
            port: Port number (3000-9999) the command will listen on
            command: The shell command to start the server (e.g. "python -m http.server 8080")
            title: Optional display title for the preview (default: "App on port {port}")
            path: Optional URL path suffix appended to the preview URL
                  (e.g. "/timeline.html" to open a specific file instead of the default index)
        """
        try:
            from langgraph.config import get_stream_writer

            writer = get_stream_writer()
        except Exception:
            writer = None

        if not workspace_id:
            return "ERROR: No workspace ID available — cannot generate preview URL", {}

        from src.server.database.share_codes import share_url
        from src.server.database.share_links import app_display_title, describe_app_link

        try:
            # Start the server process and wait for it to be ready
            preview_info = await backend.astart_preview_url(command, port)
            display_title = app_display_title(port, title)

            # Record the preview so the frontend resolves it instantly
            if on_signed_url and backend.sandbox_id:
                try:
                    await on_signed_url(backend.sandbox_id, port, preview_info)
                except Exception:
                    logger.debug("Failed to cache signed URL for port %s", port, exc_info=True)

            # Persist command to DB so the preview can auto-restart on workspace reopen
            try:
                from src.server.database.workspace import save_preview_command
                await save_preview_command(workspace_id, port, command)
            except Exception:
                logger.debug("Failed to persist preview command for port %s", port, exc_info=True)

            logger.info(
                "Generated preview URL",
                port=port,
                title=display_title,
                workspace_id=workspace_id,
            )

            normalized_path = ""
            if path:
                # Reject traversal attempts at the tool layer (defense in depth)
                from urllib.parse import unquote
                clean = unquote(path).lstrip("/")
                # Strip any ".." segments (server-side also independently rejects them)
                segments = [s for s in clean.split("/") if s and s != ".."]
                normalized_path = "/" + "/".join(segments) if segments else ""

            artifact = {
                "type": "preview_url",
                "port": port,
                "title": display_title,
                "command": command,
                **({"path": normalized_path} if normalized_path else {}),
            }

            # Emit SSE artifact so the frontend auto-opens the preview panel
            if writer:
                writer({
                    "artifact_type": "preview_url",
                    "artifact_id": f"preview_{port}",
                    "payload": artifact,
                })

            # The link the user gets is the item's private ``/a/`` page, which
            # resolves the signed URL for the signed-in owner. Nothing in it
            # names the workspace, so it is safe in a reply, a channel relay
            # and a public replay of this thread. The page rides in the URL:
            # a later call on this port moves the link's own entry path, and
            # this reply should keep opening what it announced.
            entry = normalized_path.lstrip("/") or None
            try:
                link = await describe_app_link(
                    workspace_id, port, title=title, path=entry
                )
                content = (
                    f"Preview URL for {display_title} (opens only for the "
                    f"owner, signed in): {share_url(link.code, entry)}"
                )
            except Exception:
                logger.warning(
                    "Failed to create the app link for port %s", port, exc_info=True
                )
                content = (
                    f"Preview for {display_title} is running on port {port} and "
                    "open in the preview panel; a link could not be created."
                )
            return content, artifact

        except NotImplementedError:
            return (
                "ERROR: Preview URLs are not supported by the current sandbox provider",
                {},
            )
        except Exception as e:
            mark_sandbox_raised(e)
            error_msg = f"Failed to generate preview URL for port {port}: {e!s}"
            logger.error(error_msg, port=port, error=str(e), exc_info=True)
            return f"ERROR: {error_msg}", {}

    # Every run, as for Bash: a preview is always the sandbox's to serve. With
    # no workspace each call is refused before it reaches the sandbox, so none
    # is filed under its provider.
    if workspace_id:
        GetPreviewUrl.metadata = {
            TOOL_PROVIDER: backend.provider,
            TOOL_CHANNEL: backend.channel,
        }
    return GetPreviewUrl
