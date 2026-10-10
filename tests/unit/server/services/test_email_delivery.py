"""Pins the markdown subset the report email renders and the recipient gate."""

import pytest

from src.server.services import email_delivery
from src.server.services.email_delivery import markdown_to_html


def test_renders_headings_tables_and_lists():
    html = markdown_to_html("## Title\n\n### Section\n\n| A | B |\n|---|---|\n| 1 | 2 |\n\n- one\n- two\n")
    assert "<h1 " in html and ">Title</h1>" in html  # first heading is the title
    assert "<h2 " in html and ">Section</h2>" in html  # h1-h3 after the title are sections
    assert ">A</th>" in html and ">2</td>" in html
    assert html.count("<li ") == 2


def test_rule_above_a_heading_is_dropped():
    html = markdown_to_html("# T\n\npara\n\n---\n\n### Next\n\nmore\n\n---\n\nend\n")
    assert html.count("<hr") == 1  # only the one not followed by a heading


def test_wrap_names_the_source_automation():
    html = email_delivery.wrap_email("<p>x</p>", "MSFT daily summary", "automation", "Wed 07 Oct 2026")
    assert "LangAlpha" in html and "MSFT daily summary" in html


def test_link_cannot_break_out_of_href():
    html = markdown_to_html('[x](https://a.test/"onmouseover="alert(1))')
    assert 'href="https://a.test/"onmouseover' not in html
    assert "&quot;" in html


def test_model_text_is_escaped():
    assert "<script>" not in markdown_to_html("<script>alert(1)</script>")


@pytest.mark.asyncio
async def test_refused_outside_single_user_mode(monkeypatch):
    from src.config import settings

    monkeypatch.setattr(settings, "HOST_MODE", "platform")
    result = await email_delivery.send_report_email("s", "# s", "x", "automation", None)
    assert result["success"] is False
    assert "single-user" in result["error"]


@pytest.mark.asyncio
async def test_brief_email_is_assembled_from_structured_output(monkeypatch):
    sent = {}

    async def fake_send(subject, body_md, source, kind, tz):
        sent.update(subject=subject, body=body_md, kind=kind)
        return {"method": "email", "success": True}

    monkeypatch.setattr(email_delivery, "send_report_email", fake_send)
    parsed = {
        "headline": "Stocks climb",
        "summary": "Broad gains.",
        "topics": [{"text": "AI", "trend": "up"}],
        "news_items": [{"title": "Chip rally", "body": "Details.", "url": "https://a.test/x"}],
    }
    await email_delivery.deliver_insight_email("pre_market", parsed, "America/New_York")
    assert sent["subject"] == "Pre-market brief: Stocks climb"
    assert sent["kind"] == "market brief"
    assert "### Chip rally" in sent["body"] and "[Source](https://a.test/x)" in sent["body"]


def test_separator_only_table_does_not_lose_the_report():
    html = markdown_to_html("|\n---\n\nstill here")
    assert "still here" in html
