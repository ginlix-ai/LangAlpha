# LangAlpha dashboard kit

Use the kit when a page or an app shows the user's market data, lists or work: prices, charts, watchlists, news, the daily brief, earnings, portfolio, recent threads. Inside LangAlpha (the user's Dashboard, the file panel, a private link), LangAlpha reads that data as the signed-in user and keeps it fresh. The page holds no key and writes no data code. Opened anywhere else, kit components show a short note instead of data.

Copy the kit next to the page that loads it; never link it from elsewhere:

```bash
cp .agents/skills/dashboard-page/references/kit.js <folder>/kit.js
```

```html
<script src="kit.js"></script>
```

A page made only of kit components needs no backend at all: `python -m http.server <port>` serves it.

## Components

| Element | Attributes | Shows |
|---|---|---|
| `<la-quote>` | `symbol`, `kind="stock\|index"` | Name, price, change. Live while the market is open. Click opens the symbol in LangAlpha. |
| `<la-sparkline>` | `symbol`, `interval` (default `5min`), `kind`, `height` (40) | Today's intraday line. |
| `<la-chart>` | `symbol`, `range` (`1D 5D 1M 3M 6M YTD 1Y 5Y`, default `6M`), `interval`, `kind`, `height` (240) | Price chart with hover. |
| `<la-watchlist>` | `list` (name or id; default the first list), `columns` | The user's watchlist with live prices. |
| `<la-news>` | `tickers="AAPL,MSFT"` (optional), `limit` (10) | Headlines with source and time. |
| `<la-brief>` | | Today's daily brief. |
| `<la-earnings>` | `days` (7), `symbols` (default: the names on the user's watchlists) | Upcoming earnings by date. |
| `<la-portfolio>` | `columns` | Holdings with live value and P/L. |
| `<la-threads>` | `limit` (8) | The user's recent threads. Click opens one. |
| `<la-grid>` | `cols` (3); children take `span="2"` | Layout grid, one column on narrow screens. |
| `<la-card>` | `heading` | A card around any content. |
| `<la-updated>` | `src`, `due`, `timezone`, `grace` (15), `field` (`as_of`) | How old one of your data files is, and whether a scheduled update was missed (see Data the kit doesn't have). |

Indexes use `kind="index"` and the platform's spelling: `GSPC`, `IXIC`, `DJI`, `RUT`, `VIX`. Other markets work the same way with their exchange suffix: `600519.SH`, `159915.SZ`, `0700.HK`, and indexes such as `000001.SH` or `HSI`.

Prices, lists and charts say how current they are, in LangAlpha's own words and the user's language: a small tag saying whether the price is real-time, delayed or the last close, in the card's heading when the component sits in a `<la-card>`. A list carries one tag for the state most rows share and marks a row that differs with a dot. When a refresh fails, the component keeps the data it has, dims it once it is old, and says so beneath it. You add nothing for this.

## Changing how components look

Components follow LangAlpha's theme, light or dark, through CSS variables the kit sets on `<html>` (`--color-bg-page`, `--color-bg-card`, `--color-text-primary`, `--color-text-secondary`, `--color-up`, `--color-down`, `--color-warning`, `--color-border-muted`, `--font-ui`, …). Style your own markup with the same variables so the page matches.

Each component draws inside its own shadow root, so ordinary CSS does not reach in. Two ways to change one:

- **`columns`** picks and orders the columns of a list. `<la-watchlist>`: `symbol`, `name`, `price`, `change`, `changePercent` (default `symbol,name,price,changePercent`). `<la-portfolio>`: `symbol`, `name`, `quantity`, `price`, `value`, `pl`, `cost` (default `symbol,name,quantity,price,value,pl`).
- **`::part()`** styles the pieces. Not every component has every part: lists have `row`, `symbol`, `name`, `price`, `change`; every component has `tag`, `stale`, `note`; the portfolio adds `header`, `quantity`, `value`, `pl`, `cost`, `total`; `<la-quote>` has `quote`, `<la-chart>` `chart`, `<la-card>` `heading`; news, the brief and threads have `item`, `title`, `meta`; earnings has `day`; `<la-updated>` has `updated`.

```css
la-watchlist::part(row) { padding: 3px 0; }
la-watchlist::part(symbol) { color: var(--color-accent-primary); }
la-card::part(heading) { text-transform: uppercase; letter-spacing: .04em; }
```

When the user wants a view no component gives (their own chart, one big number, a figure combined from several sources), draw it yourself from the data (next sections). Use a component where one fits, style it where the data is right but the look is not, and draw your own where neither does.

## Access

Portfolio and threads are private: the user is asked once before the app may read them, and until then those components say so. Everything else is granted on request. Ask only for what the page shows. A component asks for its own data. Your own `la.get`, `la.watch` and `la.stream` calls need their scope declared in the head. The kit reads the declaration once, when the page loads, so it must also name the scopes of components your script adds later:

```html
<meta name="la-scopes" content="quotes news">
```

Scopes: `quotes`, `watchlists`, `news`, `brief`, `earnings`, `portfolio`, `threads`.

## Your own markup: `window.la`

The same data the components show, as plain rows, for markup and charts of your own. Load any chart library you like; the page may load scripts from anywhere.

```js
await la.ready;                                     // la.connected is false outside LangAlpha
const quotes = await la.get('quotes.get', { symbols: ['AAPL', 'MSFT'] });
const stop = la.watch('news.list', { tickers: ['NVDA'], limit: 5 }, (items, err) => render(items));
la.stream(['AAPL'], (quotes) => update(quotes.AAPL)); // quotes.get rows by symbol, kept live; { kind: 'index' } for indexes
la.source('https://api.example.com/screen', (rows) => draw(rows), { every: 300000 }); // a public API (see Data the kit doesn't have), or a running app's own backend
la.open({ symbol: 'AAPL' });                        // or { thread, workspace } / { workspace }
la.format.price(1234.5, 'USD'); la.format.percent(1.2); la.format.compact(3.4e9); la.format.ago(iso);
```

**Combine data in the page when it must stay live.** Values joined from several reads and recomputed on each change stay as current as their inputs, with no server and no schedule. For example, holdings by sector:

```js
let holdings = [], sectors = {}, quotes = {}, stopQuotes = () => {};
la.watch('portfolio.holdings', {}, (rows) => {
  holdings = rows;
  const symbols = rows.map((h) => h.symbol);
  la.get('profiles.get', { symbols: symbols.slice(0, 50) })       // ≤50 per call; split a longer list
    .then((ps) => { sectors = Object.fromEntries(ps.map((p) => [p.symbol, p.sector])); draw(); });
  stopQuotes();                                     // holdings re-read on their cadence; follow only the current names
  stopQuotes = la.stream(symbols, (q) => { quotes = q; draw(); });
});
function draw() {
  const bySector = {};
  for (const h of holdings) {
    const q = quotes[h.symbol];
    if (q && q.price != null) bySector[sectors[h.symbol] || 'Other'] = (bySector[sectors[h.symbol] || 'Other'] || 0) + q.price * h.quantity;
  }
  render(bySector);
}
```

A stream calls back on every tick; coalesce repaints (`requestAnimationFrame`) when a draw is costly. Stop a watch or stream you replace by calling the function it returned.

**Say how current your numbers are.** Every quote carries `freshness`, and `bars.batch` gives each series one: `{state, label, tone, asOf, source}` or null. Show `label` (already in the user's language) beside numbers you draw, colored by `tone`: `live` with `var(--color-up)`, `neutral` with `var(--color-text-tertiary)`, `warning` with `var(--color-warning)`. A tag in a card's heading is one event away: `el.dispatchEvent(new CustomEvent('la-status', { detail: freshness, bubbles: true, composed: true }))` from inside the `<la-card>`.

| Method | Params | Returns |
|---|---|---|
| `quotes.get` | `symbols` (≤50), `kind` | `[{symbol, name, price, change, changePercent, previousClose, open, high, low, volume, currency, freshness}]` |
| `bars.get` | `symbol`, `interval`, `from`, `to` (YYYY-MM-DD), `kind` (`index` for an index) | `[{time, open, high, low, close, volume}]`; `time` is market wall-clock seconds, format it in UTC |
| `bars.batch` | `symbols` (≤50), `interval`, `from`, `to`, `kind` | `[{symbol, bars, freshness, error}]` in the order asked; one symbol's `error` leaves the rest. One call for returns or correlations across a list, where `bars.get` per symbol would hit the rate limit |
| `profiles.get` | `symbols` (≤50) | `[{symbol, name, sector, industry, marketCap, currency, exchange, country}]`; a symbol with no profile is left out. Scope `quotes` |
| `watchlists.list` | | `[{id, name, items: [{symbol, name}]}]` |
| `news.list` | `tickers` (≤20), `limit` (≤50) | `[{id, title, url, source, publishedAt, tickers, imageUrl}]` |
| `brief.today` | | `[{id, title, summary, createdAt}]` |
| `earnings.calendar` | `from`, `to` (default the next 7 days), `symbols` (without it, every US reporter), `limit` | `[{symbol, date, time, epsEstimated, revenueEstimated}]`, soonest first |
| `portfolio.holdings` | | `[{symbol, name, quantity, averageCost, currency, account}]`; `currency` is the one `averageCost` is in, which can differ from the quote's: compute P/L only when they match |
| `threads.recent` | `limit` (≤50) | `[{id, title, workspaceId, updatedAt}]` |

`changePercent` is in percent (1.5 means 1.5%). A failed call rejects with `err.code`: `scope` (not granted), `rate` (slow down), `bad_request`, `unavailable` (outside LangAlpha), `failed`. Calls are limited to a burst of 40, then 2 a second: ask for many symbols in one call, never one call per symbol. `la.stream` follows up to 200 symbols.

Refresh is handled: `la.watch` and every component re-read on a cadence suited to the data (quotes 30s, intraday bars 1m and daily bars 5m, news 5m, the brief 15m, earnings and profiles 1h), pause while the tab is hidden, and catch up when the user comes back or the machine wakes; prices stream live where the market stream is available. Don't add timers of your own for this data.

## Data the kit doesn't have

Anything else (fundamentals, screens, estimates, options, your own computations) comes from your own code, in one of these ways:

- **A public API the page reads itself**, for data that must stay live while the page is open. It works when the API answers requests from any page. Depending on the deployment, LangAlpha serves the page at origin `null` or on a host of its own, so check both: `curl -sI -H 'Origin: null' '<url>'` and `curl -sI -H 'Origin: https://example.com' '<url>'` must each carry `Access-Control-Allow-Origin` of `*` or the origin you sent. Poll it with `la.source('<url>', callback, { every: 60000 })`, which pauses while the tab is hidden and catches up after. A scheduled file is only as fresh as its last run, and each run is a turn, so don't schedule runs every few minutes to imitate a live feed.

- **The user's Dashboard page** has no server: a scheduled run writes a JSON file beside the page, which the page reads (Custom data in `.agents/skills/dashboard-page/SKILL.md`). `<la-updated>` shows the file's age and a missed run; to draw that yourself, `la.overdue(asOf, cron, timezone)` returns the missed run's time or null, and `la.lastRun(cron, timezone)` the latest scheduled time.
- **A running app** serves it from its backend, which imports the same data tools your code does (`from tools.<module> import ...` works inside the server `GetPreviewUrl` starts, though not inside a Docker image). Poll it from the page with `la.source`, and cache what you compute: the server runs only while the computer is on.
