"""Get preview URLs for services running in the sandbox."""

from collections.abc import Awaitable, Callable
from typing import Any

import structlog
from langchain_core.tools import BaseTool, tool

logger = structlog.get_logger(__name__)

# Callback signature: (sandbox_id, port, signed_url) -> None
OnSignedUrl = Callable[[str, int, str], Awaitable[None]]


def create_preview_url_tool(
    sandbox: Any,
    *,
    workspace_id: str = "",
    on_signed_url: OnSignedUrl | None = None,
) -> BaseTool:
    """Factory function to create GetPreviewUrl tool with injected dependencies.

    Args:
        sandbox: PTCSandbox instance for preview URL generation
        workspace_id: Workspace ID for preview URL generation
        on_signed_url: Optional async callback to cache signed URLs

    Returns:
        Configured GetPreviewUrl tool function
    """

    @tool(response_format="content_and_artifact")
    async def GetPreviewUrl(
        port: int,
        command: str,
        title: str | None = None,
    ) -> tuple[str, dict[str, Any]]:
        """Get a preview URL for a service running on the given port in the sandbox.

        This tool starts the given command in the background AND generates a preview URL.
        Always provide the command used to start the server — it will be persisted so the
        server can be restarted automatically when the user reopens the preview later.

        Args:
            port: Port number (3000-9999) the service is listening on
            command: The shell command to start the server (e.g. "python -m http.server 8080")
            title: Optional display title for the preview (default: "Port {port}")

        Returns:
            The signed preview URL that can be used to access the service
        """
        try:
            from langgraph.config import get_stream_writer

            writer = get_stream_writer()
        except Exception:
            writer = None

        if not workspace_id:
            return "ERROR: No workspace ID available — cannot generate preview URL", {}

        try:
            # Start the server process and wait for it to be ready
            preview_info = await sandbox.start_and_get_preview_url(command, port)
            display_title = title or f"Port {port}"

            # Cache the fresh signed URL so frontend resolves it instantly
            if on_signed_url:
                try:
                    await on_signed_url(sandbox.sandbox_id, port, preview_info.url)
                except Exception:
                    logger.debug("Failed to cache signed URL for port %s", port, exc_info=True)

            logger.info(
                "Generated preview URL",
                port=port,
                title=display_title,
                workspace_id=workspace_id,
            )

            # Stable URL: {base}/api/v1/preview/{workspace_id}/{port}
            from src.config.env import SERVER_BASE_URL

            stable_url = f"{SERVER_BASE_URL.rstrip('/')}/api/v1/preview/{workspace_id}/{port}"

            artifact = {
                "type": "preview_url",
                "port": port,
                "title": display_title,
                "command": command,
            }

            # Emit SSE artifact so the frontend auto-opens the preview panel
            if writer:
                writer({
                    "artifact_type": "preview_url",
                    "artifact_id": f"preview_{port}",
                    "payload": artifact,
                })

            content = f"Preview URL for {display_title}: {stable_url}"
            return content, artifact

        except NotImplementedError:
            return (
                "ERROR: Preview URLs are not supported by the current sandbox provider",
                {},
            )
        except Exception as e:
            error_msg = f"Failed to generate preview URL for port {port}: {e!s}"
            logger.error(error_msg, port=port, error=str(e), exc_info=True)
            return f"ERROR: {error_msg}", {}

    return GetPreviewUrl
