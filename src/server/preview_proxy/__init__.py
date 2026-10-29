"""Serves previews of private sandboxes under the deployment's preview base."""

from src.server.preview_proxy.app import install_preview_proxy

__all__ = ["install_preview_proxy"]
