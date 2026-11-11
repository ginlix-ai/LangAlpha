---
name: dashboard-page
description: "The user's own LangAlpha Dashboard: build or change the page they see when they open LangAlpha, with live market data and custom data refreshed on a schedule"
---

# Dashboard page

The page the user sees first when they open LangAlpha. Build it as one HTML page in the workspace, which LangAlpha keeps a copy of and shows. Do not build it as a running app: the user's computer may be off or asleep when they open LangAlpha, and the page must still open at once with current numbers.

## When to use

- The user asks for their dashboard, home page or start page, what they see when they open LangAlpha, or a change to the one they have.
- It stops at that page: an app the user opens on its own, a tool, anything that needs a server answering requests, and a report on a snapshot of data are other work. When the user wants such an app as their Dashboard anyway, build it as an app: the file panel can set a running app as the Dashboard too, though it opens only while its computer is on.

## How the Dashboard shows the page

These facts decide the design:

- The user sets the page as their Dashboard with the house icon on the file in the file panel. LangAlpha then keeps a private copy of the page and of every file it loads, and the Dashboard shows that copy.
- When a turn in this workspace ends, LangAlpha copies the page again if any of its files changed. Your edits reach the Dashboard when your turn ends. There is nothing to restart.
- The copy holds at most 200 files and 25 MB. Past either, the Dashboard stops updating with no error, so keep `dashboard/` to the page, `kit.js` and the data files it reads; keep history and raw data elsewhere.
- The copy is static. No server runs for it, so the page cannot call a local port or a backend. It can read a public API that answers any page (see Data the kit doesn't have in `references/dashboard-kit.md`).
- Market data and the user's own data come live through the kit, with no computer: quotes, charts, company profiles, watchlists, holdings, news, the daily brief, earnings and recent threads. Read `references/dashboard-kit.md` before you write the page.
- Anything else (a screen, your own metrics, data the kit does not have) is a JSON file the page reads, refreshed on a schedule (see Custom data), unless it must stay live and a public API the page can read has it.

## Build

1. **Agree on the contents.** Unless the user said what they want, propose one screen they take in at a glance, for example: the major indexes, their watchlist, their holdings, news on what they hold, upcoming earnings for their names, and one or two panels only you can compute. Ask before building a large page.
2. **Lay out the folder** in this workspace: `dashboard/index.html`, `dashboard/kit.js` (copied from `references/kit.js`), `dashboard/data/` for custom data, and `dashboard/refresh.py` if there is custom data. `references/starter.html` and `references/refresh.py` are a starting point.
3. **Take every number the kit has from the kit**, as a component, a restyled one, or your own markup from its data; `references/dashboard-kit.md` covers each. A number you fetch yourself and write into the page is stale tomorrow, and the kit's is live.
4. **Combine where it stays fresh.** A figure built from data the kit has (holdings by sector, a list's average move, a spread between two names) is computed in the page from the kit's reads, so it moves with the prices. Only what the kit cannot give, or what is too slow to compute in a browser, goes to a data file, and then only that part: a scheduled run writes each holding's factor score, and the page multiplies it by live prices.
5. **Read custom data with a relative fetch whose path is written out**, `fetch('data/screen.json')`. The copy finds a file by its path in the page's text, or by the page loading it while the user has it open; a path built from variables is found only the second way, so it can be missing.
6. **Date every data file.** Each has an `as_of` (an ISO time), and its panel carries `<la-updated src="data/screen.json" due="..." timezone="...">` with the cron and timezone of the run that writes it, so the user can tell yesterday's data from this morning's and sees when a run was missed.
7. **Fit the Dashboard.** The page fills the area beside LangAlpha's sidebar, under its header, down to a phone's width, in light and dark: lay it out with `<la-grid>` and color it with the kit's theme variables.
8. **Check it** with Playwright against `python -m http.server` in `dashboard/`: no script errors, and every data file loads. Outside LangAlpha, kit components show a short note instead of data; that is expected.
9. **Hand it over.** If the page is not yet the user's Dashboard, tell them to open `dashboard/index.html` in the file panel and click the house icon. If it already is, say the Dashboard updates when this turn ends.

## Custom data

- `refresh.py` computes each panel's data and writes `data/<name>.json` with its `as_of`. It imports the data tools your code uses (`from tools.<module> import ...`), so run it with ExecuteCode, not Bash, whose Python cannot import the data tools: `import runpy; runpy.run_path("dashboard/refresh.py", run_name="__main__")`. Run it once before handing the page over.
- Offer an automation that runs it on a schedule, following the README in `.agents/user/automations/` (confirm with the user first). A good default is weekdays before the open and after the close: `"cron_expression": "30 8,16 * * 1-5"`, `"timezone": "America/New_York"`. Give it a self-contained instruction such as "Run dashboard/refresh.py with ExecuteCode. Reply with one line: done, or what failed.", in this workspace (the default), with `"thread": "persistent"` and `"delivery": []`. A run is a turn here, so the Dashboard has the new data when it ends.
- A run happens only while this workspace's computer can run it. If the workspace is on the user's own machine, a run that falls while it is asleep is missed, and the page shows the last data with its age. Say so when you set the schedule.
- Keep each file small: what the panel draws, not the raw data behind it. A data file can hold the slow half of a live figure (a sector or score per symbol) for the page to combine with live prices.

## Moving an existing app onto the Dashboard

When the user asks to make an app they already run (a server and its page) their Dashboard, keep what it shows and drop the server. Sort each thing the server does:

- What it polls during the session moves into the page: kit reads for what the kit has, otherwise `la.source` on the public API the server called, if that API answers any page.
- What it builds daily or slowly (history, baselines, anything joined from many calls) becomes a data file written by `refresh.py` on a schedule, with the page combining it with the live half.
- Reuse the app's own data code in `refresh.py` rather than rewriting it.

## Changing an existing Dashboard

Edit the files in place. Keep `dashboard/index.html` at the same path: the Dashboard follows that file, and a page saved under another name is a new page the user would have to set again.

Copy `references/kit.js` over `dashboard/kit.js` again before you edit. The page keeps the copy it was built with, and the current one carries fixes.
