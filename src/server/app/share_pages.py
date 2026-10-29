"""The branded page a browser gets when a shared report link no longer serves.

Separate from the routes so both the file half and the replay half can render
it without either importing the other.
"""

from __future__ import annotations

from fastapi import Request
from fastapi.responses import Response


def wants_html(request: Request) -> bool:
    """True when the caller is a browser/iframe (Accept includes text/html), so a
    failed serve should render a page instead of raw JSON. API clients (Accept
    ``*/*`` etc.) keep the JSON error."""
    return "text/html" in request.headers.get("accept", "").lower()


def prefers_chinese(request: Request) -> bool:
    """Whether the browser's top Accept-Language tag is Chinese."""
    primary = request.headers.get("accept-language", "").split(",")[0].strip().lower()
    return primary.startswith("zh")


def unavailable_response(request: Request, status_code: int) -> Response | None:
    """The branded page for a failed share serve, or None to keep the JSON error.

    Shared by both failure routes into this page. An ``HTTPException`` raised
    inside one ``except`` clause is not caught by a sibling clause of the same
    ``try``, so the sandbox path cannot reach the ``HTTPException`` handler by
    raising and has to render through here itself.
    """
    if status_code not in (403, 404) or not wants_html(request):
        return None
    return Response(
        content=unavailable_page(status_code, chinese=prefers_chinese(request)),
        status_code=status_code,
        media_type="text/html; charset=utf-8",
        headers={"Cache-Control": "no-store", "X-Robots-Tag": "noindex"},
    )


def unavailable_page(status_code: int, *, chinese: bool) -> str:
    """Self-contained, theme-aware HTML page shown when a browser opens a shared
    report link that has been revoked (404) or lacks file access (403)."""
    if chinese:
        lang = "zh-CN"
        heading = "此分享报告不可用"
        detail = (
            "创建者尚未为此链接开启文件访问权限。"
            if status_code == 403
            else "该链接可能已被创建者关闭，或报告已不存在。"
        )
        link_text = "前往 LangAlpha"
    else:
        lang = "en"
        heading = "This shared report isn’t available"
        detail = (
            "The owner hasn’t enabled file access for this link."
            if status_code == 403
            else "The link may have been turned off by its owner, or the report no longer exists."
        )
        link_text = "Go to LangAlpha"
    return f"""<!DOCTYPE html>
<html lang="{lang}">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="light dark">
<meta name="robots" content="noindex">
<title>{heading}</title>
<style>
  :root {{
    --bg: #F3EEE8; --bg-glow: rgba(55, 82, 139, 0.06);
    --card: #FFFCF9; --border: rgba(0, 0, 0, 0.08);
    --text: #2D2B28; --muted: #7A756F; --accent: #37528B;
  }}
  @media (prefers-color-scheme: dark) {{
    :root {{
      --bg: #000000; --bg-glow: rgba(65, 97, 164, 0.12);
      --card: #0A0A0A; --border: rgba(255, 255, 255, 0.08);
      --text: #FFFFFF; --muted: #999999; --accent: #4161A4;
    }}
  }}
  * {{ box-sizing: border-box; }}
  html, body {{ height: 100%; margin: 0; }}
  body {{
    display: flex; align-items: center; justify-content: center;
    min-height: 100%; padding: 24px;
    background: radial-gradient(circle at 50% 32%, var(--bg-glow), transparent 60%), var(--bg);
    color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    line-height: 1.6;
  }}
  .card {{
    max-width: 30rem; width: 100%; text-align: center;
    background: var(--card); border: 1px solid var(--border);
    border-radius: 16px; padding: 40px 32px;
  }}
  .badge {{
    font-size: 13px; font-weight: 600; letter-spacing: .01em;
    color: var(--muted); margin-bottom: 20px;
  }}
  h1 {{ font-size: 20px; font-weight: 600; margin: 0 0 10px; }}
  p {{ font-size: 14px; color: var(--muted); margin: 0 0 24px; }}
  a {{ font-size: 14px; font-weight: 500; color: var(--accent); text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
</style>
</head>
<body>
  <div class="card">
    <div class="badge">LangAlpha</div>
    <h1>{heading}</h1>
    <p>{detail}</p>
    <a href="/">{link_text}</a>
  </div>
</body>
</html>"""
