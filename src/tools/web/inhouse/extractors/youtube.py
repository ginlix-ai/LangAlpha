"""YouTube content extractor using oEmbed metadata and transcript API."""

import asyncio
import logging
import re
from urllib.parse import parse_qs, urlparse

from ..backend import CrawlOutput
from .base import ContentExtractor, _validate_url, register_extractor

logger = logging.getLogger(__name__)


def _parse_video_id(url: str) -> str | None:
    """Extract YouTube video ID from various URL formats."""
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower().removeprefix("www.")

    if host in ("youtube.com", "m.youtube.com"):
        # /watch?v=ID
        if parsed.path == "/watch":
            qs = parse_qs(parsed.query)
            ids = qs.get("v")
            return ids[0] if ids else None
        # /shorts/ID, /live/ID, /embed/ID
        for prefix in ("/shorts/", "/live/", "/embed/"):
            if parsed.path.startswith(prefix):
                segment = parsed.path[len(prefix) :].split("/")[0].split("?")[0]
                return segment or None
    elif host == "youtu.be":
        # youtu.be/ID
        segment = parsed.path.lstrip("/").split("/")[0].split("?")[0]
        return segment or None

    return None


def _format_timestamp(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    return f"{m:02d}:{s:02d}"


@register_extractor
class YouTubeExtractor(ContentExtractor):
    name = "youtube"
    url_patterns = [
        re.compile(r"(?:youtube\.com/watch\?.*v=|youtu\.be/|youtube\.com/shorts/|youtube\.com/live/)", re.IGNORECASE),
    ]

    async def extract(self, url: str) -> CrawlOutput | None:
        _validate_url(url)

        video_id = _parse_video_id(url)
        if not video_id:
            return None

        # Fetch metadata and transcript in parallel
        (title, author), transcript_text = await asyncio.gather(
            self._fetch_oembed(url),
            self._fetch_transcript(video_id),
        )

        # Build markdown
        lines = [f"# {title}", ""]
        if author:
            lines.append(f"**Channel:** {author}")
            lines.append("")

        if transcript_text:
            lines.append("## Transcript")
            lines.append("")
            lines.append(transcript_text)
        else:
            lines.append("*No transcript available for this video.*")

        lines.append("")
        lines.append(f"[Watch on YouTube]({url})")

        markdown = "\n".join(lines)
        full_title = f"{title} - {author}" if author else title
        return CrawlOutput(title=full_title, html="", markdown=markdown)

    async def _fetch_oembed(self, url: str) -> tuple[str, str]:
        """Fetch oEmbed metadata. Returns (title, author_name)."""
        try:
            resp = await self._client.get(
                "https://www.youtube.com/oembed",
                params={"url": url, "format": "json"},
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("title", "YouTube Video"), data.get("author_name", "")
        except Exception as e:
            logger.debug(f"oEmbed fetch failed: {e}")
            return "YouTube Video", ""

    async def _fetch_transcript(self, video_id: str) -> str | None:
        """Fetch transcript via youtube_transcript_api."""
        try:
            from youtube_transcript_api import YouTubeTranscriptApi

            transcript = await asyncio.to_thread(YouTubeTranscriptApi.fetch, video_id)
            segments = []
            for entry in transcript:
                ts = _format_timestamp(entry.get("start", 0))
                text = entry.get("text", "")
                segments.append(f"[{ts}] {text}")
            return "\n".join(segments) if segments else None
        except Exception as e:
            logger.debug(f"Transcript fetch failed for {video_id}: {e}")
            return None
