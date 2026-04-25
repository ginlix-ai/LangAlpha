"""User-managed memo store — schema, PDF extraction, index rebuild, metadata generation."""

from ptc_agent.agent.memo.pdf import MemoPdfExtractionError, extract_pdf_text
from ptc_agent.agent.memo.schema import (
    ACCEPTED_MIME_TYPES,
    MEMO_CONTENT_MIN_CHARS,
    MEMO_MAX_CONTENT_BYTES,
    MEMO_MAX_UPLOAD_BYTES,
    METADATA_PLACEHOLDER_DESCRIPTION,
    MemoMetadata,
)
from ptc_agent.agent.memo.slug import (
    MAX_COLLISION_SUFFIX,
    candidate_slug,
    random_collision_slug,
    slug_components,
    slugify_filename,
)

__all__ = [
    "ACCEPTED_MIME_TYPES",
    "MAX_COLLISION_SUFFIX",
    "MEMO_CONTENT_MIN_CHARS",
    "MEMO_MAX_CONTENT_BYTES",
    "MEMO_MAX_UPLOAD_BYTES",
    "METADATA_PLACEHOLDER_DESCRIPTION",
    "MemoMetadata",
    "MemoPdfExtractionError",
    "candidate_slug",
    "extract_pdf_text",
    "random_collision_slug",
    "slug_components",
    "slugify_filename",
]
