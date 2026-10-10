"""Email delivery for automation results.

Sends the run's final assistant message over SMTP. The subject is the report's
own first heading (e.g. "MSFT Trading Day Update — Tuesday, October 6") because
the model dates it correctly, falling back to the automation name, behind a
fixed source prefix.
"""

import asyncio
import html
import logging
import re
import smtplib
import ssl
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Any, Dict
from zoneinfo import ZoneInfo

from psycopg.rows import dict_row

from src.server.database import pool

logger = logging.getLogger(__name__)

# Per socket operation (connect, STARTTLS, login, send). Short because delivery is
# awaited while an automation settles, and a mail outage must not stall that.
_SMTP_TIMEOUT = 10

# Marks mail as automation output so it's recognisable (and filterable) in an inbox.
SUBJECT_PREFIX = "[LangAlpha]"

_HEADING_RE = re.compile(r"^\s{0,3}#{1,3}\s+(.+?)\s*#*\s*$", re.M)


async def _final_report_text(thread_id: str, run_id: str | None = None) -> str:
    """Last non-empty main-agent text message of the run (default: the thread's latest).

    Earlier text chunks in a run are progress narration before tool calls;
    only the last one is the report.
    """
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT ev->'data'->>'content' AS content
                FROM (
                    SELECT sse_events FROM conversation_responses
                    WHERE conversation_thread_id = %s AND sse_events IS NOT NULL
                      AND (%s::uuid IS NULL OR conversation_response_id = %s::uuid)
                    ORDER BY run_seq DESC LIMIT 1
                ) r, jsonb_array_elements(r.sse_events) WITH ORDINALITY AS t(ev, n)
                WHERE ev->>'event' = 'message_chunk'
                  AND ev->'data'->>'content_type' = 'text'
                  AND ev->'data'->>'role' = 'assistant'
                  AND COALESCE(ev->'data'->>'content', '') <> ''
                  AND ev->'data'->>'agent' LIKE 'model:%%'
                ORDER BY n DESC LIMIT 1
                """,
                (thread_id, run_id, run_id),
            )
            row = await cur.fetchone()
    return (row or {}).get("content") or ""


# Inline styles throughout: mail clients (Outlook especially) drop or rewrite <style> blocks.
_INK, _MUTED, _RULE, _LINK = "#1f2328", "#6b7280", "#e5e7eb", "#1a56db"
_FONT = "-apple-system,'Segoe UI',Helvetica,Arial,sans-serif"
_TD = f"border:1px solid {_RULE};padding:7px 10px;text-align:left;vertical-align:top"


def _inline(text: str) -> str:
    text = html.escape(text)
    text = re.sub(r"`([^`]+)`", r'<code style="background:#f3f4f6;padding:1px 4px;border-radius:3px">\1</code>', text)
    text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)
    text = re.sub(r"(?<![*\w])\*(?!\s)(.+?)(?<!\s)\*(?!\w)", r"<em>\1</em>", text)
    return re.sub(
        r"\[([^\]]+)\]\((https?://[^)\s]+)\)",
        rf'<a href="\2" style="color:{_LINK}">\1</a>',
        text,
    )


def _table(lines: list[str]) -> str:
    def cells(line: str) -> list[str]:
        return [c.strip() for c in line.strip().strip("|").split("|")]

    rows_ = [cells(ln) for ln in lines if not re.fullmatch(r"[\s|:-]+", ln)]
    if not rows_:  # only separator lines: nothing to tabulate
        return ""
    head, *body = rows_
    th = "".join(f'<th style="{_TD};background:#f3f4f6">{_inline(c)}</th>' for c in head)
    rows = "".join(
        "<tr>"
        + "".join(
            f'<td style="{_TD}{";background:#fafafa" if n % 2 else ""}">{_inline(c)}</td>'
            for c in r
        )
        + "</tr>"
        for n, r in enumerate(body)
    )
    return (
        f'<table style="border-collapse:collapse;width:100%;margin:4px 0 16px;font-size:14px">'
        f"<thead><tr>{th}</tr></thead><tbody>{rows}</tbody></table>"
    )


def markdown_to_html(md: str) -> str:
    """Small markdown subset (headings, lists, tables, emphasis, links, rules).

    Hand-rolled rather than adding a dependency: a new package would bust the
    backend image's dependency layer, and reports only use this subset. The
    first heading is the report title; later h1-h3 are section headings.
    Code fences, blockquotes and nested lists are not handled and render as plain
    paragraphs.
    """
    out: list[str] = []
    lines = md.splitlines()
    seen_title = False
    i = 0
    while i < len(lines):
        line = lines[i]
        if not line.strip():
            i += 1
        elif m := re.match(r"^\s{0,3}(#{1,6})\s+(.*?)\s*#*\s*$", line):
            if not seen_title:
                out.append(
                    f'<h1 style="font-size:22px;line-height:1.3;margin:0 0 16px;color:{_INK}">'
                    f"{_inline(m.group(2))}</h1>"
                )
                seen_title = True
            elif len(m.group(1)) <= 3:
                out.append(
                    f'<h2 style="font-size:17px;margin:26px 0 10px;padding-bottom:6px;'
                    f'border-bottom:1px solid {_RULE};color:{_INK}">{_inline(m.group(2))}</h2>'
                )
            else:
                out.append(
                    f'<h3 style="font-size:15px;margin:18px 0 6px;color:{_INK}">{_inline(m.group(2))}</h3>'
                )
            i += 1
        elif re.fullmatch(r"\s*([-*_])(\s*\1){2,}\s*", line):
            i += 1
            nxt = next((ln for ln in lines[i:] if ln.strip()), "")
            # A rule right above a heading would stack with the heading's own underline.
            if not re.match(r"^\s{0,3}#{1,6}\s", nxt):
                out.append(f'<hr style="border:0;border-top:1px solid {_RULE};margin:20px 0">')
        elif "|" in line and i + 1 < len(lines) and re.fullmatch(r"[\s|:-]+", lines[i + 1]) and "-" in lines[i + 1]:
            j = i
            while j < len(lines) and "|" in lines[j]:
                j += 1
            out.append(_table(lines[i:j]))
            i = j
        elif re.match(r"^\s*([-*\u2022]|\d+[.)])\s+", line):
            ordered = bool(re.match(r"^\s*\d", line))
            items = []
            while i < len(lines) and (m := re.match(r"^\s*(?:[-*\u2022]|\d+[.)])\s+(.*)", lines[i])):
                items.append(f'<li style="margin:0 0 5px">{_inline(m.group(1))}</li>')
                i += 1
            tag = "ol" if ordered else "ul"
            out.append(f'<{tag} style="margin:0 0 14px;padding-left:22px">{"".join(items)}</{tag}>')
        else:
            para = []
            while i < len(lines) and lines[i].strip() and not re.match(
                r"^\s{0,3}#{1,6}\s|^\s*([-*\u2022]|\d+[.)])\s+", lines[i]
            ):
                para.append(_inline(lines[i]))
                i += 1
            out.append(f'<p style="margin:0 0 14px">{"<br>".join(para)}</p>')
    return "".join(out)


def wrap_email(body_html: str, source: str, kind: str, sent_at: str) -> str:
    """Brand header and source footer around the rendered report."""
    name = html.escape(source)
    return (
        f'<html><body style="margin:0;padding:0;background:#ffffff">'
        f'<div style="font-family:{_FONT};font-size:15px;line-height:1.6;color:{_INK};'
        f'max-width:680px;padding:20px 16px">'
        f'<div style="font-size:11px;letter-spacing:.12em;text-transform:uppercase;color:{_MUTED};'
        f'padding-bottom:10px;margin-bottom:18px;border-bottom:1px solid {_RULE}">'
        f"LangAlpha &middot; {name}</div>"
        f"{body_html}"
        f'<div style="margin-top:28px;padding-top:12px;border-top:1px solid {_RULE};'
        f'font-size:12px;color:{_MUTED}">Sent by the LangAlpha {html.escape(kind)} '
        f"&ldquo;{name}&rdquo; &middot; {html.escape(sent_at)}. AI-generated research; "
        f"not investment advice.</div>"
        f"</div></body></html>"
    )


def _sent_at(timezone_name: str | None) -> str:
    try:
        tz = ZoneInfo(timezone_name or "UTC")
    except Exception:
        tz = timezone.utc
    return datetime.now(tz).strftime("%a %d %b %Y, %I:%M %p %Z")


def _send(msg: EmailMessage) -> dict:
    """Send over verified TLS; returns the recipients the server refused."""
    from src.config import settings

    # Explicit context: the credentials must only ever go to a verified server.
    ctx = ssl.create_default_context()
    if settings.SMTP_PORT == 465:  # implicit TLS; 587 upgrades with STARTTLS
        smtp_cm = smtplib.SMTP_SSL(settings.SMTP_HOST, settings.SMTP_PORT, timeout=_SMTP_TIMEOUT, context=ctx)
    else:
        smtp_cm = smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT, timeout=_SMTP_TIMEOUT)
    with smtp_cm as smtp:
        if settings.SMTP_PORT != 465:
            smtp.starttls(context=ctx)
        smtp.login(settings.SMTP_USER, settings.SMTP_PASSWORD)
        return smtp.send_message(msg)


async def send_report_email(
    subject: str, body_md: str, source: str, kind: str, timezone_name: str | None
) -> Dict[str, Any]:
    """Render and send one report to the configured recipients. Never raises."""
    from src.config import settings

    result: Dict[str, Any] = {"method": "email", "success": False}
    # One operator-configured recipient list: in a multi-user deployment any
    # user choosing "Email" would mail their report to it.
    if settings.HOST_MODE != "oss":
        result["error"] = "email delivery is only available in single-user (HOST_MODE=oss) deployments"
        logger.warning(f"[EMAIL] refused: {result['error']}")
        return result
    recipients = [a.strip() for a in settings.AUTOMATION_EMAIL_TO.split(",") if a.strip()]
    if not (settings.SMTP_USER and settings.SMTP_PASSWORD and recipients):
        result["error"] = "SMTP_USER, SMTP_PASSWORD and AUTOMATION_EMAIL_TO must be set"
        logger.warning(f"[EMAIL] not configured, skipping: {result['error']}")
        return result
    try:
        subject = re.sub(r"[*_`]", "", subject).strip()
        sent_at = _sent_at(timezone_name)
        msg = EmailMessage()
        msg["Subject"] = f"{SUBJECT_PREFIX} {subject}"
        msg["From"] = settings.SMTP_FROM or settings.SMTP_USER
        msg["To"] = ", ".join(recipients)
        msg.set_content(f"{body_md}\n\n--\nSent by the LangAlpha {kind} \"{source}\" · {sent_at}")
        msg.add_alternative(
            wrap_email(markdown_to_html(body_md), source, kind, sent_at), subtype="html"
        )
        refused = await asyncio.to_thread(_send, msg)
        # send_message accepts the mail if any recipient is accepted; surface the rest.
        result["success"] = len(refused) < len(recipients)
        if refused:
            result["error"] = f"refused by server: {', '.join(refused)}"
            logger.warning(f"[EMAIL] '{subject}' refused for {len(refused)} recipient(s)")
        else:
            logger.info(f"[EMAIL] sent '{subject}' to {len(recipients)} recipient(s)")
    except Exception as e:
        logger.error(f"[EMAIL] delivery failed: {e}")
        result["error"] = str(e)
    return result


async def deliver_automation_email(
    automation: Dict[str, Any], thread_id: str | None, run_id: str | None = None
) -> Dict[str, Any]:
    """Email an automation run's final report. Never raises; returns a delivery_result row."""
    name = automation.get("name") or "Automation"
    try:
        body = await _final_report_text(thread_id, run_id) if thread_id else ""
    except Exception as e:
        logger.error(f"[EMAIL] could not read report: {e}")
        return {"method": "email", "success": False, "error": str(e)}
    if not body.strip():
        return {"method": "email", "success": False, "error": "run produced no report text"}
    heading = _HEADING_RE.search(body)
    return await send_report_email(
        heading.group(1) if heading else name, body, name, "automation", automation.get("timezone")
    )


_BRIEF_LABELS = {
    "pre_market": "Pre-market brief",
    "market_update": "Market update",
    "post_market": "Post-market recap",
}


async def deliver_insight_email(
    job_type: str, parsed: Dict[str, Any], timezone_name: str | None
) -> Dict[str, Any]:
    """Email a scheduled market brief from its structured output. Never raises."""
    label = _BRIEF_LABELS.get(job_type, "Market brief")
    try:
        headline = parsed["headline"]
        lines = [f"# {label}: {headline}", "", parsed.get("summary") or "", ""]
        if parsed.get("topics"):
            lines += ["**Topics:** " + " · ".join(t["text"] for t in parsed["topics"]), ""]
        if parsed.get("news_items"):
            lines += ["## Top stories", ""]
            for item in parsed["news_items"]:
                lines += [f"### {item['title']}", "", item.get("body") or ""]
                if item.get("url"):
                    lines += ["", "[Source](" + item["url"].replace("(", "%28").replace(")", "%29") + ")"]
                lines.append("")
    except Exception as e:
        logger.error(f"[EMAIL] could not assemble {job_type} brief: {e}")
        return {"method": "email", "success": False, "error": f"malformed brief: {e}"}
    return await send_report_email(
        f"{label}: {headline}", "\n".join(lines), label, "market brief", timezone_name
    )
