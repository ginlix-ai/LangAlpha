import type { CSSProperties, ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { Globe } from 'lucide-react';
import { ease, useSceneTime } from './clock';
import { Appear, rv } from './kit';
import { FileCrumbs, HTML_FILE_ACTIONS, PanelStrip, PanelTab } from './panelParts';
import { HEADER_H } from './transcriptParts';

/* The Analyst's dashboard, capex-tracker.html, as the file panel previews it
   in the capex thread (capexThread.tsx): written top to bottom as the
   workflow's build step runs, then filtered to META by a click on its chip.
   The chat intro's ptcSandbox scene plays that; the share scene opens on it
   finished, and shows the page again as its link's visitor sees it. The
   page is laid out as the html-report skill writes one: a title and its
   basis, filter chips, KPI tiles, a stacked column chart, and a timeline of
   what each call said, in the app's own tokens so it themes with the app.
   It is the user's file, so its text stays English in every locale. */
/* eslint-disable react-refresh/only-export-components -- scene part:
   the panel plus the layout constants the scenes' cameras read */

/** How long (ms) after `at` the build step takes to write the whole page. */
export const DASH_BUILD = 2300;

/** The META filter chip's centre, in px from the panel's top-left corner,
    for the pointer that clicks it: 58px under the tab strip. */
export const DASH_META_CHIP = { x: 158, y: HEADER_H + 58 };

type Company = 'MSFT' | 'GOOGL' | 'AMZN' | 'META';

/** The companies in the order the chips and the legend list them. */
const COMPANIES: Company[] = ['MSFT', 'GOOGL', 'AMZN', 'META'];

/** Each company's colour: the app's identity hues, which hold their contrast
    as text in either theme, for its tag on the timeline. */
const INK: Record<Company, string> = {
  MSFT: 'var(--color-identity-0-fg)',
  GOOGL: 'var(--color-identity-1-fg)',
  AMZN: 'var(--color-identity-4-fg)',
  META: 'var(--color-identity-6-fg)',
};

/** The same hues as the chart fills them: mixed toward the page, so the
    bars sit at a mid tone in either theme, solid. */
const FILL = Object.fromEntries(
  COMPANIES.map((c) => [c, `color-mix(in srgb, ${INK[c]} 72%, var(--color-bg-page))`])
) as Record<Company, string>;

const QUARTERS = ["Q3'24", "Q4'24", "Q1'25", "Q2'25", "Q3'25", "Q4'25", "Q1'26", "Q2'26"];

/** Capex per quarter, USD bn, as the 32 calls and their 10-Qs report it. */
const CAPEX: Record<Company, number[]> = {
  MSFT: [20.0, 22.6, 21.4, 24.2, 34.9, 37.5, 38.2, 41.6],
  GOOGL: [13.1, 14.3, 17.2, 22.4, 24.0, 27.9, 31.4, 34.8],
  AMZN: [22.6, 27.8, 25.0, 32.2, 34.2, 39.5, 41.0, 44.3],
  META: [9.2, 14.8, 13.7, 17.0, 19.4, 22.1, 24.6, 27.9],
};

/** All four's capex per quarter: 64.9 up to 148.6. */
const TOTALS = QUARTERS.map((_, i) => COMPANIES.reduce((sum, c) => sum + CAPEX[c][i], 0));

/** The chart's value axis runs to here, leaving room over the tallest
    column for its label. */
const AXIS_MAX = 165;
const AXIS_TICKS = [0, 50, 100, 150];

/** The stack, bottom up: META on the baseline, so filtered to META its
    columns still rise from the axis while the rest fade back above them. */
const STACK: Company[] = ['META', 'AMZN', 'GOOGL', 'MSFT'];

/** A KPI tile's figure in both views (all four, then META), how it prints,
    and the line under it in each. */
interface Kpi {
  label: string;
  all: number;
  meta: number;
  print: (v: number) => string;
  sub: [string, string];
}

const KPIS: Kpi[] = [
  {
    label: 'Q2 2026 capex',
    all: 148.6,
    meta: 27.9,
    print: (v) => `$${v.toFixed(1)}bn`,
    sub: ['Q2 2025: $95.8bn', 'Q2 2025: $17.0bn'],
  },
  {
    label: 'Year on year',
    all: 55,
    meta: 64,
    print: (v) => `+${Math.round(v)}%`,
    sub: ['Q1 2026: +75%', 'Q1 2026: +80%'],
  },
  {
    label: '2026 guidance',
    all: 610,
    meta: 115,
    print: (v) => `$${Math.round(v)}bn`,
    sub: ['Sum of midpoints', 'Range $112–118bn'],
  },
];

/** One call on the timeline: when, whose, what it said about capex, and
    the quarter's capex it reported. */
interface Call {
  date: string;
  co: Company;
  text: string;
  capex: string;
}

/** The calls that moved the outlook, newest first: each company's July
    call, then META's last four. Both lists run four deep, so the page keeps
    its height when the filter swaps them. */
const CALLS: { all: Call[]; meta: Call[] } = {
  all: [
    {
      date: 'Jul 31, 2026',
      co: 'AMZN',
      text: '2026 capex near $185bn, most of it for AWS.',
      capex: 'Q2 $44.3bn',
    },
    {
      date: 'Jul 30, 2026',
      co: 'META',
      text: 'Raised 2026 capex to $112–118bn, from $100–110bn.',
      capex: 'Q2 $27.9bn',
    },
    {
      date: 'Jul 29, 2026',
      co: 'MSFT',
      text: 'Capex growth in FY27 to outpace FY26.',
      capex: 'Q2 $41.6bn',
    },
    {
      date: 'Jul 23, 2026',
      co: 'GOOGL',
      text: 'Raised 2026 capex to $140bn, from $125bn.',
      capex: 'Q2 $34.8bn',
    },
  ],
  meta: [
    {
      date: 'Jul 30, 2026',
      co: 'META',
      text: 'Raised 2026 capex to $112–118bn, from $100–110bn.',
      capex: 'Q2 $27.9bn',
    },
    {
      date: 'Apr 29, 2026',
      co: 'META',
      text: 'Raised 2026 capex to $100–110bn, from $95–105bn.',
      capex: 'Q1 $24.6bn',
    },
    {
      date: 'Jan 28, 2026',
      co: 'META',
      text: 'Set 2026 capex at $95–105bn.',
      capex: 'Q4 $22.1bn',
    },
    {
      date: 'Oct 29, 2025',
      co: 'META',
      text: 'Raised 2025 capex to $71–73bn, from $66–72bn.',
      capex: 'Q3 $19.4bn',
    },
  ],
};

/** When each part is written, in ms after `at`: the title and chips, the
    tiles, the chart a quarter at a time, then the calls one by one. */
const WRITE = {
  title: 0,
  basis: 100,
  chips: 220,
  kpi: 400,
  kpiPer: 90,
  count: 650,
  chart: 720,
  bars: 860,
  barPer: 80,
  rise: 420,
  calls: 1560,
  callFirst: 1660,
  callPer: 120,
};

/** How long the page takes to settle into a filter once it is clicked. */
const FILTER_MS = 360;

/** A filter chip: `All`, or a company with its colour dot; the active one
    is filled in ink. */
function Chip({ co, active, children }: { co?: Company; active: boolean; children: ReactNode }) {
  return (
    <span
      className="flex h-[11px] items-center gap-[2.5px] rounded-[3px] border px-[4px] text-[5.5px] font-medium leading-none transition-colors duration-quick"
      style={
        active
          ? {
              background: 'var(--color-btn-primary-bg)',
              borderColor: 'var(--color-btn-primary-bg)',
              color: 'var(--color-btn-primary-text)',
            }
          : { borderColor: 'var(--color-border-default)', color: 'var(--color-text-secondary)' }
      }
    >
      {co && (
        <span className="h-[4px] w-[4px] shrink-0 rounded-full" style={{ background: FILL[co] }} />
      )}
      {children}
    </span>
  );
}

/** A KPI tile: its label, its figure counting up from the moment it is
    written (`at`) and over to META's once filtered (`f` from 0 to 1), and
    the line under it for the view in front. */
function KpiTile({ kpi, at, f, meta }: { kpi: Kpi; at: number; f: number; meta: boolean }) {
  const t = useSceneTime();
  const v = (kpi.all + (kpi.meta - kpi.all) * f) * ease(t, at, WRITE.count);
  return (
    <Appear
      at={at}
      reserve
      className="flex min-w-0 flex-1 flex-col gap-[3px] rounded-[3px] border px-[5px] py-[4px] leading-none"
      style={{ background: 'var(--color-bg-card)', borderColor: 'var(--color-border-default)' }}
    >
      <span className="truncate text-[5px]" style={{ color: 'var(--color-text-tertiary)' }}>
        {kpi.label}
      </span>
      <span
        className="text-[8px] font-semibold tabular-nums"
        style={{ color: 'var(--color-text-primary)' }}
      >
        {kpi.print(v)}
      </span>
      <span
        className="truncate text-[5px] tabular-nums"
        style={{ color: 'var(--color-text-secondary)' }}
      >
        {kpi.sub[meta ? 1 : 0]}
      </span>
    </Appear>
  );
}

/** A value's height on the chart, as a share of the plot. */
const onAxis = (v: number) => `${(v / AXIS_MAX) * 100}%`;

/** The stacked column chart: gridlines and the value axis, a column per
    quarter rising one after another from `at`, its total over it. Filtered
    to META (`f` from 0 to 1), the other companies fade back and each label
    glides down onto META's segment, counting down to its value. */
function CapexChart({ at, f }: { at: number; f: number }) {
  const t = useSceneTime();
  return (
    <div className="relative mt-[6px] min-h-0 flex-1">
      {/* gridlines, each with its value at the left */}
      {AXIS_TICKS.map((v) => (
        <div
          key={v}
          className="absolute left-0 right-0 flex items-center"
          style={{ bottom: onAxis(v), transform: 'translateY(50%)' }}
        >
          <span
            className="w-[11px] shrink-0 text-right text-[5px] leading-none tabular-nums"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {v}
          </span>
          <span
            className="ml-[3px] h-px flex-1"
            style={{
              background: v === 0 ? 'var(--color-border-default)' : 'var(--color-border-muted)',
            }}
          />
        </div>
      ))}
      {/* the columns, over the gridlines */}
      <div className="absolute bottom-0 left-[14px] right-0 top-0 flex">
        {QUARTERS.map((q, i) => {
          const p = ease(t, at + i * WRITE.barPer, WRITE.rise);
          const total = TOTALS[i];
          const label = total + (CAPEX.META[i] - total) * f;
          return (
            <div key={q} className="relative flex flex-1 justify-center">
              <div
                className="absolute bottom-0 flex w-[58%] flex-col-reverse overflow-hidden rounded-t-[1px]"
                style={{ height: `${(total / AXIS_MAX) * p * 100}%` }}
              >
                {STACK.map((co) => (
                  <span
                    key={co}
                    className="block shrink-0"
                    style={{
                      height: `${(CAPEX[co][i] / total) * 100}%`,
                      background: FILL[co],
                      opacity: co === 'META' ? 1 : 1 - 0.78 * f,
                      // a hairline of page between segments, as the chart library draws one
                      boxShadow:
                        co === STACK.at(-1) ? undefined : 'inset 0 0.5px 0 var(--color-bg-page)',
                    }}
                  />
                ))}
              </div>
              <span
                className="absolute text-[5px] font-medium leading-none tabular-nums"
                style={{
                  bottom: `calc(${onAxis(label)} + 1.5px)`,
                  color: 'var(--color-text-secondary)',
                  opacity: p > 0.85 ? 1 : 0,
                  transition: 'opacity var(--motion-quick) var(--ease-out)',
                }}
              >
                {label.toFixed(1)}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

/** One call on the timeline: a dot in the company's colour on the rail,
    the date, the company's tag and the quarter's capex, then what was said. */
function CallRow({ call, last }: { call: Call; last: boolean }) {
  return (
    <div className="relative flex gap-[5px]">
      <span className="relative flex w-[5px] shrink-0 justify-center pt-[0.5px]">
        <span className="h-[4px] w-[4px] rounded-full" style={{ background: FILL[call.co] }} />
        {!last && (
          <span
            className="absolute bottom-[-6px] top-[6px] w-px"
            style={{ background: 'var(--color-border-default)' }}
          />
        )}
      </span>
      <div className="flex min-w-0 flex-1 flex-col gap-[3px]">
        <div className="flex items-center gap-[4px] leading-none">
          <span
            className="font-mono text-[5px] tabular-nums"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {call.date}
          </span>
          <span
            className="rounded-[2px] px-[2px] py-[1px] text-[5px] font-semibold"
            style={{ background: 'var(--color-bg-tag)', color: INK[call.co] }}
          >
            {call.co}
          </span>
          <span
            className="ml-auto font-mono text-[5px] tabular-nums"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {call.capex}
          </span>
        </div>
        <span
          className="whitespace-nowrap text-[6px] leading-none"
          style={{ color: 'var(--color-text-primary)' }}
        >
          {call.text}
        </span>
      </div>
    </div>
  );
}

/** A section's heading: its name, with a note or a legend at the right. */
function SectionHead({ title, children }: { title: string; children?: ReactNode }) {
  return (
    <div className="flex shrink-0 items-center gap-[4px] leading-none">
      <span className="text-[6px] font-semibold" style={{ color: 'var(--color-text-primary)' }}>
        {title}
      </span>
      <span className="ml-auto flex items-center gap-[5px]">{children}</span>
    </div>
  );
}

/** The page's body: blank until the build step starts writing at `at`, then
    written top to bottom within `DASH_BUILD`, and filtered to META at
    `filterAt`; without one it stays on All, as the share scene shows it. */
export function Dashboard({ at, filterAt }: { at: number; filterAt?: number }) {
  const t = useSceneTime();
  const meta = filterAt !== undefined && t >= filterAt;
  const f = filterAt === undefined ? 0 : ease(t, filterAt, FILTER_MS);
  const w = (ms: number) => at + ms;
  const rule: CSSProperties = { borderColor: 'var(--color-border-muted)' };
  const calls = meta ? CALLS.meta : CALLS.all;
  return (
    <div className="flex min-h-0 flex-1 flex-col overflow-hidden px-2 pb-2 pt-[9px]">
      {/* title and basis */}
      <Appear at={w(WRITE.title)} reserve className="shrink-0">
        <span
          className="block text-[8px] font-semibold leading-none"
          style={{ color: 'var(--color-text-primary)' }}
        >
          Hyperscaler AI capex
        </span>
      </Appear>
      <Appear at={w(WRITE.basis)} reserve className="mt-[4px] shrink-0">
        <span
          className="block text-[5.5px] leading-none"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          MSFT, GOOGL, AMZN, META · 32 calls · USD bn
        </span>
      </Appear>
      {/* filter chips */}
      <Appear at={w(WRITE.chips)} reserve className="mt-[7px] flex shrink-0 items-center gap-[3px]">
        <Chip active={!meta}>All</Chip>
        {COMPANIES.map((co) => (
          <Chip key={co} co={co} active={meta && co === 'META'}>
            {co}
          </Chip>
        ))}
      </Appear>
      {/* KPI tiles */}
      <div className="mt-[7px] flex shrink-0 gap-[4px]">
        {KPIS.map((kpi, i) => (
          <KpiTile
            key={kpi.label}
            kpi={kpi}
            at={w(WRITE.kpi + i * WRITE.kpiPer)}
            f={f}
            meta={meta}
          />
        ))}
      </div>
      {/* the chart: heading and legend, plot, quarters */}
      <Appear
        at={w(WRITE.chart)}
        reserve
        className="mt-[8px] flex min-h-0 flex-1 flex-col border-t pt-[6px]"
        style={rule}
      >
        <SectionHead title="Capex by quarter">
          {COMPANIES.map((co) => (
            <span
              key={co}
              className="flex items-center gap-[2px] text-[5px]"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              <span className="h-[4px] w-[4px] rounded-[1px]" style={{ background: FILL[co] }} />
              {co}
            </span>
          ))}
        </SectionHead>
        <CapexChart at={w(WRITE.bars)} f={f} />
        <div className="ml-[14px] mt-[3px] flex shrink-0">
          {QUARTERS.map((q) => (
            <span
              key={q}
              className="flex-1 text-center text-[5px] leading-none tabular-nums"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              {q}
            </span>
          ))}
        </div>
      </Appear>
      {/* the calls, newest first */}
      <div className="mt-[8px] flex shrink-0 flex-col">
        <Appear at={w(WRITE.calls)} reserve className="border-t pt-[6px]" style={rule}>
          <SectionHead title="On the calls">
            <span className="text-[5px]" style={{ color: 'var(--color-text-tertiary)' }}>
              Newest first
            </span>
          </SectionHead>
        </Appear>
        <div className="mt-[6px] flex flex-col gap-[6px]">
          {calls.map((call, i) => (
            <Appear
              key={`${meta ? 'meta' : 'all'}-${i}`}
              at={w(WRITE.callFirst + i * WRITE.callPer)}
              reserve
              style={meta ? rv(i * 0.04) : undefined}
            >
              <CallRow call={call} last={i === calls.length - 1} />
            </Appear>
          ))}
        </div>
      </div>
    </div>
  );
}

/** The file panel open on the dashboard (`FilePanel` previewing an HTML
    file): the tab strip, level with the chat header, the file's header
    with its line count and actions, then the page. */
export function DashboardPanel({ at, filterAt }: { at: number; filterAt?: number }) {
  const { t } = useTranslation();
  return (
    <div className="flex h-full min-w-0 flex-1 flex-col">
      <PanelStrip
        tabs={<PanelTab icon={Globe} label="capex-tracker.html" active />}
        tree={false}
        className="border-b"
        style={{ height: HEADER_H, borderColor: 'var(--color-border-muted)' }}
      />
      <FileCrumbs
        path={['results', 'capex-tracker.html']}
        meta={t('filePanel.metaLines', { count: 412 })}
        actions={HTML_FILE_ACTIONS}
      />
      <Dashboard at={at} filterAt={filterAt} />
    </div>
  );
}
