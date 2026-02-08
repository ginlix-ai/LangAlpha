"""Execute code tool for running Python code in the PTC sandbox."""

import base64
import binascii
from pathlib import Path
from typing import Any

import structlog
from langchain_core.tools import BaseTool, tool

# Import storage upload functions (supports R2, S3, OSS, or none via STORAGE_PROVIDER env var)
from ptc_agent.utils.storage.storage_uploader import get_public_url, is_storage_enabled, upload_bytes

logger = structlog.get_logger(__name__)

# Image extensions to detect for cloud storage upload
IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".svg", ".gif", ".webp", ".bmp", ".tiff"}


def create_execute_code_tool(sandbox: Any, mcp_registry: Any) -> BaseTool:
    """Factory function to create execute_code tool with injected dependencies.

    Args:
        sandbox: PTCSandbox instance for code execution
        mcp_registry: MCPRegistry instance with available MCP tools

    Returns:
        Configured execute_code tool function
    """

    @tool("ExecuteCode")
    async def execute_code(code: str) -> str:
        """Execute Python code in the sandbox.

        Use for: Complex operations, data processing, MCP tool calls
        Import MCP tools: from tools.{server} import {tool}

        Args:
            code: Python code to execute. Print summary to stdout.

        Returns:
            SUCCESS with stdout/files, or ERROR with stderr

        Paths: Use RELATIVE paths (results/, data/). Never /results/ or /workspace/.
        """
        if not sandbox:
            return "ERROR: Sandbox not initialized"

        try:
            logger.info("Executing code in sandbox", code_length=len(code))

            # Execute code in sandbox
            result = await sandbox.execute(code)

            if result.success:
                # Format success response
                parts = ["SUCCESS"]

                if result.stdout:
                    parts.append(result.stdout)

                if result.files_created:
                    # Extract file names from file objects
                    files = [
                        f.name if hasattr(f, "name") else str(f)
                        for f in result.files_created
                    ]
                    if files:
                        parts.append(f"Files created: {', '.join(files)}")

                # Upload images to cloud storage (if enabled via STORAGE_PROVIDER)
                uploaded_images = []

                if is_storage_enabled():
                    # 1. Upload charts from artifacts (matplotlib plt.show())
                    if hasattr(result, "charts") and result.charts:
                        for i, chart in enumerate(result.charts):
                            if chart.png_base64:
                                try:
                                    # Decode base64 and upload
                                    png_bytes = base64.b64decode(chart.png_base64)
                                    storage_key = f"charts/{result.execution_id}/chart_{i}.png"
                                    if upload_bytes(storage_key, png_bytes):
                                        url = get_public_url(storage_key)
                                        title = chart.title if chart.title else f"chart_{i}"
                                        uploaded_images.append(f"![{title}]({url})")
                                        logger.info(f"Uploaded chart to storage: {storage_key}")
                                except (OSError, ValueError, binascii.Error):
                                    logger.exception("Failed to upload chart artifact")

                    # 2. Upload saved image files from results/
                    if result.files_created:
                        for file_path in result.files_created:
                            file_str = file_path.name if hasattr(file_path, "name") else str(file_path)
                            ext = Path(file_str).suffix.lower()
                            if ext in IMAGE_EXTENSIONS:
                                try:
                                    # Download from sandbox
                                    sandbox_path = sandbox.normalize_path(file_str)
                                    file_bytes = await sandbox.adownload_file_bytes(sandbox_path)
                                    if file_bytes:
                                        # Upload to cloud storage
                                        filename = Path(file_str).name
                                        storage_key = f"charts/{result.execution_id}/{filename}"
                                        if upload_bytes(storage_key, file_bytes):
                                            url = get_public_url(storage_key)
                                            uploaded_images.append(f"![{file_str}]({url})")
                                            logger.info(f"Uploaded image to storage: {storage_key}")
                                except (OSError, ValueError):
                                    logger.exception(f"Failed to upload saved image {file_str}")

                    # 3. Fallback: Check /results/ (absolute path) for images
                    # LLMs sometimes use absolute paths despite prompt instructions
                    if not uploaded_images:
                        try:
                            # Call Daytona SDK directly to bypass path validation.
                            # LLMs sometimes use absolute /results despite prompt instructions.
                            assert sandbox.sandbox is not None
                            root_results_raw = await sandbox.sandbox.fs.list_files("/results")
                            for f in root_results_raw or []:
                                file_name = str(f.name) if hasattr(f, "name") else str(f)
                                ext = Path(file_name).suffix.lower()
                                if ext not in IMAGE_EXTENSIONS:
                                    continue

                                try:
                                    file_bytes = await sandbox.sandbox.fs.download_file(f"/results/{file_name}")
                                    if not file_bytes:
                                        continue

                                    storage_key = f"charts/{result.execution_id}/{file_name}"
                                    if upload_bytes(storage_key, file_bytes):
                                        url = get_public_url(storage_key)
                                        uploaded_images.append(f"![{file_name}]({url})")
                                        logger.info(f"Uploaded image from /results/ fallback: {storage_key}")
                                except Exception as e:
                                    logger.debug("Failed to upload /results fallback image", file_name=file_name, error=str(e))
                        except Exception:  # noqa: S110 - /results/ fallback should fail silently
                            pass

                    # Add uploaded images to response
                    if uploaded_images:
                        parts.append("\nUploaded images:")
                        parts.extend(uploaded_images)

                response = "\n".join(parts)
                logger.info(
                    "Code executed successfully",
                    stdout_length=len(result.stdout),
                    images_uploaded=len(uploaded_images)
                )
                return response
            # Format error response
            # Python tracebacks often go to stdout in some environments
            # Show stderr if available, otherwise show stdout
            error_output = result.stderr if result.stderr else result.stdout

            logger.warning(
                "Code execution failed",
                stderr_length=len(result.stderr),
                stdout_length=len(result.stdout),
            )

            return f"ERROR\n{error_output}"

        except Exception as e:
            logger.error("Code execution exception", error=str(e), exc_info=True)
            return f"ERROR: {e!s}"

    return execute_code
