# Verification Templates

Playwright + Chromium are pre-installed in the sandbox. Use these templates for Step 5 Tier 2 verification.

## verify.py — Universal Template

```python
"""Dashboard verification script. Run after GetPreviewUrl starts the server."""
import sys

PORT = 8050  # Change to match your dashboard port

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("SKIP: Playwright not available")
    sys.exit(0)

js_errors = []
api_errors = []


def on_page_error(exc):
    js_errors.append(str(exc))


def on_response(response):
    if "/api/" in response.url and response.status >= 400:
        api_errors.append(f"{response.status} {response.url}")


with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page()
    page.on("pageerror", on_page_error)
    page.on("response", on_response)  # Remove this line for simple tier

    # Load page
    page.goto(f"http://127.0.0.1:{PORT}/", wait_until="networkidle", timeout=20000)

    # Check page is not blank
    body_text = page.locator("body").inner_text().strip()
    assert len(body_text) > 20, f"Page appears blank (body text: {body_text[:50]!r})"

    # Check for chart libraries
    has_plotly = page.locator(".plotly, .js-plotly-plot").count() > 0
    has_chartjs = page.locator("canvas").count() > 0
    has_lw_charts = page.locator(".tv-lightweight-charts").count() > 0
    has_recharts = page.locator(".recharts-wrapper").count() > 0
    charts_found = has_plotly or has_chartjs or has_lw_charts or has_recharts
    print(f"Charts detected: plotly={has_plotly} chartjs={has_chartjs} lw={has_lw_charts} recharts={has_recharts}")

    # --- Optional: click primary action button ---
    # Uncomment for interactive dashboards with buttons/filters/tabs
    #
    # errors_before_click = len(js_errors)
    # btn = page.locator("button").first
    # if btn.count() > 0:
    #     btn.click()
    #     page.wait_for_timeout(1000)
    #     new_errors = js_errors[errors_before_click:]
    #     assert not new_errors, f"JS errors after click: {new_errors}"
    # --- End optional section ---

    # Screenshot
    page.screenshot(path="work/dashboard/verify-screenshot.png", full_page=True)

    browser.close()

# Results
passed = True
if js_errors:
    print(f"FAIL: {len(js_errors)} JS error(s): {js_errors}")
    passed = False
if api_errors:
    print(f"FAIL: {len(api_errors)} API error(s): {api_errors}")
    passed = False
if not charts_found:
    print("WARN: No chart elements detected (may be OK for table-only dashboards)")

if passed:
    print("PASS: Dashboard verification succeeded")
else:
    sys.exit(1)
```

## Customization Guide

Add assertions based on your dashboard type:

| Dashboard type | Additional assertions |
|---|---|
| Stock tracker | `assert page.locator("text=AAPL").count() > 0` — ticker appears |
| Sector heatmap | `assert page.locator("svg, .js-plotly-plot").count() > 0` — treemap rendered |
| Portfolio monitor | `assert page.locator("table tr").count() > 1` — holdings table has rows |
| Multi-stock comparison | `assert page.locator("canvas").count() >= 2` — multiple charts rendered |
| Earnings tracker | `assert page.locator("text=EPS").count() > 0` — earnings data present |

## Quick Inline Alternative

For simple dashboards where a full `verify.py` is overkill — paste this directly after `GetPreviewUrl`:

```python
from playwright.sync_api import sync_playwright
with sync_playwright() as p:
    page = p.chromium.launch().new_page()
    errors = []; page.on("pageerror", lambda e: errors.append(str(e)))
    page.goto("http://127.0.0.1:8050/", wait_until="networkidle", timeout=15000)
    page.screenshot(path="work/dashboard/verify.png", full_page=True)
    assert not errors, f"JS errors: {errors}"
    print("PASS")
```
