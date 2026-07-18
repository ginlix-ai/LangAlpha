"""Twitter/X content extractor using FixTweet API."""

import logging
import re
from urllib.parse import urlparse

from ..backend import CrawlOutput
from .base import ContentExtractor, _validate_url, register_extractor

logger = logging.getLogger(__name__)


def _parse_tweet_url(url: str) -> tuple[str, str] | None:
    """Extract (username, tweet_id) from a Twitter/X status URL."""
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower().removeprefix("www.")
    if host not in ("x.com", "twitter.com"):
        return None

    # Path: /<user>/status/<id>
    parts = [p for p in parsed.path.split("/") if p]
    if len(parts) >= 3 and parts[1] == "status":
        username = parts[0]
        tweet_id = parts[2].split("?")[0]
        if tweet_id.isdigit():
            return username, tweet_id

    return None


@register_extractor
class TwitterExtractor(ContentExtractor):
    name = "twitter"
    url_patterns = [
        re.compile(r"(?:x\.com|twitter\.com)/\w+/status/\d+", re.IGNORECASE),
    ]

    async def extract(self, url: str) -> CrawlOutput | None:
        _validate_url(url)

        parsed = _parse_tweet_url(url)
        if not parsed:
            return None

        username, tweet_id = parsed

        try:
            resp = await self._client.get(f"https://api.fxtwitter.com/{username}/status/{tweet_id}")
        except Exception as e:
            logger.warning(f"FixTweet request failed: {e}")
            return None

        if resp.status_code != 200:
            logger.debug(f"FixTweet returned {resp.status_code} for tweet {tweet_id}")
            return None

        try:
            data = resp.json()
        except Exception:
            return None

        tweet = data.get("tweet", data)
        return self._format_tweet(tweet, url)

    def _format_tweet(self, tweet: dict, url: str) -> CrawlOutput:
        author = tweet.get("author", {})
        handle = author.get("screen_name", "") or tweet.get("author_screen_name", "")
        name = author.get("name", "") or tweet.get("author_name", handle)
        text = tweet.get("text", "")
        created_at = tweet.get("created_at", "")

        likes = tweet.get("likes", 0)
        retweets = tweet.get("retweets", 0)
        replies = tweet.get("replies", 0)

        # Build markdown
        lines = [f"**@{handle}** ({name})", ""]
        if created_at:
            lines.append(f"*{created_at}*")
            lines.append("")
        lines.append(text)

        # Media
        media = tweet.get("media", {})
        images = media.get("photos", media.get("images", []))
        videos = media.get("videos", [])

        if images:
            lines.append("")
            for img in images:
                img_url = img.get("url", "") if isinstance(img, dict) else str(img)
                if img_url:
                    lines.append(f"![image]({img_url})")

        if videos:
            lines.append("")
            for vid in videos:
                thumb = vid.get("thumbnail_url", "") if isinstance(vid, dict) else ""
                vid_url = vid.get("url", "") if isinstance(vid, dict) else str(vid)
                if thumb:
                    lines.append(f"![video thumbnail]({thumb})")
                elif vid_url:
                    lines.append(f"[Video]({vid_url})")

        # Engagement
        lines.append("")
        lines.append(f"**Likes:** {likes} | **Retweets:** {retweets} | **Replies:** {replies}")
        lines.append("")
        lines.append(f"[View on X]({url})")

        markdown = "\n".join(lines)

        # Title
        truncated = text[:60] + "..." if len(text) > 60 else text
        title = f"@{handle}: {truncated}"

        return CrawlOutput(title=title, html="", markdown=markdown)
