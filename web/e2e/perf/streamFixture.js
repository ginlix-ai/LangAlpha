/**
 * Deterministic long reply for the streaming smoothness benchmark.
 *
 * Mixed constructs on purpose: headings, emphasis, inline code, links, lists,
 * fenced code in three languages, a table, math and a blockquote. Each one
 * exercises a different remark/rehype path, so a renderer change that only
 * helps paragraphs shows up as a smaller gain here, not as a false win.
 */

import { sseEvents } from '../helpers/mockResponses.js';

export const END_MARKER = 'END-OF-REPLY';

const PARA = [
  'Revenue growth **accelerated to 18% year over year**, driven by the data-center segment, while gross margin expanded 140 bps on mix. Management guided *above* consensus for the next quarter and reiterated the capex envelope.',
  'The balance sheet remains net cash positive at `$4.2B`, and free cash flow conversion improved to 92% of adjusted EBITDA. See the [10-Q filing](https://example.com/10q) for segment detail and the reconciliation tables.',
  'Risks include customer concentration (top three customers are 41% of revenue), export-control exposure in two regions, and a lumpy services backlog that can move quarterly recognition by several points.',
  'Valuation sits at 24x forward earnings against a five-year median of 19x; the premium is roughly explained by the growth differential versus peers, but it leaves little room for a guidance miss.',
];

function codePython(i) {
  return `\`\`\`python
import pandas as pd

def load_prices_${i}(symbol: str) -> pd.DataFrame:
    """Load daily closes and compute a ${20 + i}-day rolling mean."""
    df = pd.read_parquet(f"data/{symbol}.parquet")
    df["ma"] = df["close"].rolling(${20 + i}).mean()
    return df.dropna()

prices = load_prices_${i}("NVDA")
print(prices.tail())
\`\`\``;
}

const CODE_JSON = `\`\`\`json
{
  "symbol": "NVDA",
  "quarter": "Q2 FY27",
  "revenue_bn": 46.7,
  "gross_margin": 0.751,
  "segments": [{ "name": "Data Center", "share": 0.88 }, { "name": "Gaming", "share": 0.08 }]
}
\`\`\``;

const CODE_BASH = `\`\`\`bash
uv run python scripts/backtest.py --symbol NVDA --window 20 --start 2024-01-01
\`\`\``;

const TABLE = `| Metric | Q1 | Q2 | Q3 | Q4 |
|---|---:|---:|---:|---:|
| Revenue ($B) | 26.0 | 30.0 | 35.1 | 39.3 |
| Gross margin | 78.4% | 75.1% | 74.6% | 73.0% |
| Op. margin | 64.9% | 62.3% | 62.3% | 61.1% |
| FCF ($B) | 14.9 | 13.5 | 16.8 | 15.5 |`;

const MATH = 'The implied growth rate solves $g = \\frac{r \\cdot P - D}{P}$ where $P$ is price and $D$ the dividend, which gives $g \\approx 7.8\\%$ at the current quote.';

const QUOTE = '> "We see demand visibility extending through next year and are supply constrained in two product lines.": CFO, Q2 call';

function list(i) {
  return `- **Thesis ${i}.1**: the installed base compounds services revenue at ~30% with minimal incremental capex.
- **Thesis ${i}.2**: pricing power holds while the competitor roadmap slips by two quarters.
  - Sub-point: channel checks in ${['Taiwan', 'Korea', 'Germany', 'Texas'][i % 4]} corroborate lead times.
- **Thesis ${i}.3**: buybacks absorb dilution; the share count is flat since 2023.`;
}

function numbered(i) {
  return `1. Pull the last ${8 + i} quarters of segment data.
2. Normalize for the fiscal-year shift in ${2020 + i}.
3. Regress margin on mix and utilization; report the residual.`;
}

/** The whole reply as one markdown string (about 14 KB). */
export function buildReply() {
  const parts = ['# NVDA earnings deep dive\n'];
  for (let i = 0; i < 6; i++) {
    parts.push(`## Section ${i + 1}: ${['Growth', 'Margins', 'Cash', 'Risks', 'Valuation', 'Setup'][i]}\n`);
    parts.push(PARA[i % PARA.length]);
    parts.push(list(i));
    parts.push(PARA[(i + 1) % PARA.length]);
    if (i % 2 === 0) parts.push(codePython(i));
    if (i === 1) parts.push(TABLE);
    if (i === 3) parts.push(CODE_JSON);
    if (i === 4) parts.push(MATH);
    if (i === 5) parts.push(CODE_BASH);
    parts.push(numbered(i));
    parts.push(QUOTE);
  }
  parts.push(`Summary: the setup is constructive into the print. ${END_MARKER}`);
  return parts.join('\n\n');
}

/**
 * The whole turn as SSE events: reasoning, four tool calls, then the reply,
 * every text event split to `chunkChars` so the stream arrives at token cadence.
 */
export function buildEvents(chunkChars = 8) {
  const events = [];
  events.push(sseEvents.messageChunk('start', 'reasoning_signal'));
  for (const c of chunk(buildReasoning(), chunkChars)) events.push(sseEvents.messageChunk(c, 'reasoning'));
  events.push(sseEvents.messageChunk('complete', 'reasoning_signal'));
  const calls = ['toolu_p1', 'toolu_p2', 'toolu_p3', 'toolu_p4'];
  events.push(sseEvents.toolCalls(calls.map((id, i) => ({ name: 'bash', args: { command: `echo ${i}` }, id }))));
  events.push(sseEvents.finishToolCalls());
  for (const id of calls) events.push({ ...sseEvents.toolCallResult(id, 'ok'), delayAfter: 150 });
  for (const c of chunk(buildReply(), chunkChars)) events.push(sseEvents.messageChunk(c));
  events.push(sseEvents.finishStop());
  events.push(sseEvents.creditUsage());
  return events;
}

/** A reasoning block with enough prose to fill a live row several times over. */
export function buildReasoning() {
  return [
    '**Framing the question**',
    'The user wants an earnings deep dive. I should cover growth, margins, cash generation, the main risks and valuation, and close with a positioning summary.',
    'I will pull the last eight quarters from the filings tool, compute margin trends, and cross-check the guidance commentary from the call transcript before writing.',
    'Structure: six sections, each with a short thesis list, a code sample where a reader may want to reproduce the number, and one table for the quarterly view.',
  ].join('\n\n');
}

/** Split text into token-sized chunks (about `size` chars, never inside a surrogate pair). */
export function chunk(text, size = 8) {
  // A zero or negative size never advances the loop; fail loudly rather than
  // let PERF_CHUNK_CHARS=0 hang the benchmark until its timeout.
  if (!Number.isSafeInteger(size) || size < 1) {
    throw new RangeError(`chunk size must be a positive integer, got ${size}`);
  }
  const out = [];
  let i = 0;
  while (i < text.length) {
    let end = Math.min(text.length, i + size);
    if (end < text.length && /[\uD800-\uDBFF]/.test(text[end - 1])) end += 1;
    out.push(text.slice(i, end));
    i = end;
  }
  return out;
}
