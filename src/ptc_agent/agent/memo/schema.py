"""Memo value shape, response pydantic models, and configuration constants."""

from __future__ import annotations

from typing import Literal, TypedDict

from pydantic import BaseModel, Field

# --- Size limits -----------------------------------------------------------

# Max raw upload size. 5 MB covers most research memos; larger deferred.
MEMO_MAX_UPLOAD_BYTES: int = 5 * 1024 * 1024

# Max extracted/stored text. 1 MB accommodates long PDFs after text extraction
# while keeping the LLM metadata call and the agent's `read_file` responsive.
MEMO_MAX_CONTENT_BYTES: int = 1 * 1024 * 1024

# Minimum characters after whitespace strip before we accept the extraction.
# Scanned PDFs with no text layer fall below this.
MEMO_CONTENT_MIN_CHARS: int = 50

# Head of the extracted text fed to the metadata LLM call.
METADATA_LLM_CONTENT_CHARS: int = 16000

# Hard caps on LLM-generated metadata. memo.md sanitizes display, but the
# raw value also flows back through /read and the next prompt cache, so a
# misbehaving model returning megabytes would bloat both. Keep generous
# room over the prompt-stated targets so we don't truncate good output.
METADATA_DESCRIPTION_MAX_CHARS: int = 600
METADATA_SUMMARY_MAX_CHARS: int = 4000


# --- Accepted MIME types ---------------------------------------------------

ACCEPTED_MIME_TYPES: frozenset[str] = frozenset({
    "text/markdown",
    "text/plain",
    "text/csv",
    "application/json",
    "application/pdf",
})


# --- Placeholders ----------------------------------------------------------

METADATA_PLACEHOLDER_DESCRIPTION: str = "Summary generating…"
METADATA_PLACEHOLDER_SUMMARY: str = ""


# --- Store value shape -----------------------------------------------------

MetadataStatus = Literal["pending", "ready", "failed"]
SourceKind = Literal["upload", "sandbox"]


class BinaryRef(TypedDict):
    """Reference to an original binary stashed in object storage."""

    storage: str           # e.g. "r2", "s3", "cos"
    key: str               # provider-internal object key
    content_type: str      # original MIME type


class MemoValue(TypedDict, total=False):
    """The jsonb value stored under (user_id, "memos", key)."""

    content: str                       # extracted/plain text
    encoding: str                      # always "utf-8" for MVP
    mime_type: str
    original_filename: str             # display name (user's upload)
    key: str                           # slug used as the store key
    size_bytes: int                    # size of stored content (not original)
    sha256: str                        # hash of content (for dedup short-circuit)
    description: str
    summary: str
    metadata_status: MetadataStatus
    metadata_error: str | None
    binary_ref: BinaryRef | None       # set when object storage configured
    original_bytes_b64: str | None     # fallback when object storage not configured
    created_at: str
    modified_at: str
    metadata_generated_at: str | None
    # Source provenance — set when memo was created from a tracked location
    # (e.g. a sandbox file). Used to detect duplicate adds and replace in
    # place rather than allocating a new slug.
    source_kind: SourceKind
    source_workspace_id: str | None
    source_path: str | None


# --- LLM response schema --------------------------------------------------


class MemoMetadata(BaseModel):
    """Structured response from the metadata generation LLM call."""

    description: str = Field(
        ...,
        max_length=METADATA_DESCRIPTION_MAX_CHARS,
        description="One clear sentence (≤ 30 words) stating what the document is.",
    )
    summary: str = Field(
        ...,
        max_length=METADATA_SUMMARY_MAX_CHARS,
        description="2-3 paragraphs summarizing the key content, themes, and data points.",
    )
