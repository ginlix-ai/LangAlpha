"""Content extractors for specialised URL types."""

from .base import ContentExtractor, ExtractorError, get_extractor_registry, register_extractor

# Each import triggers @register_extractor — wrapped for resilience
try:
    from . import pdf  # noqa: F401
except ImportError:
    pass
try:
    from . import youtube  # noqa: F401
except ImportError:
    pass
try:
    from . import twitter  # noqa: F401
except ImportError:
    pass

__all__ = [
    "ContentExtractor",
    "ExtractorError",
    "get_extractor_registry",
    "register_extractor",
]
