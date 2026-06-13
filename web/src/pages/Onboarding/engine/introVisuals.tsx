import type { CSSProperties, ReactNode } from 'react';
import {
  ArrowLeft,
  ArrowUp,
  ChevronDown,
  ChevronRight,
  Clock,
  FileText,
  Folder,
  MousePointer2,
  Paperclip,
  Plus,
  Search,
  Upload,
  X,
  Zap,
} from 'lucide-react';
import type { IntroVisualId } from '../registry';
import './pageIntro.css';

/*
 * Mockup scenes for the page-intro visual panel: zoomed-out wireframes of the
 * WHOLE page (app rail + topbar + skeleton regions), with the region the
 * current step talks about highlighted. Landing-page blueprint style; all
 * data is invented placeholder skeleton.
 */
/* eslint-disable react-refresh/only-export-components -- visual library: many
   private scene components, one Record export */

const rv = (s: number): CSSProperties => ({ '--rv': `${s}s` } as CSSProperties);

/** Grey skeleton bar standing in for text. */
function Ln({ w, h = 3, c }: { w: string; h?: number; c?: string }) {
  return (
    <span
      className="block rounded-full"
      style={{ width: w, height: h, background: c ?? 'var(--iv-line)' }}
    />
  );
}

function Tag({ children, color = 'var(--iv-blue)' }: { children: ReactNode; color?: string }) {
  return (
    <span
      className="w-fit shrink-0 rounded px-1 py-0.5 font-mono text-[8px] leading-none"
      style={{ background: `color-mix(in srgb, ${color} 18%, transparent)`, color }}
    >
      {children}
    </span>
  );
}

/** One region of the page wireframe; `hot` = what this step is about. */
function Region({
  hot = false,
  dashed = false,
  delay,
  className = '',
  style,
  children,
}: {
  hot?: boolean;
  dashed?: boolean;
  delay?: number;
  className?: string;
  style?: CSSProperties;
  children?: ReactNode;
}) {
  return (
    <div
      className={`intro-region ${hot ? 'intro-region--hot' : ''} ${
        dashed ? 'intro-region--dashed' : ''
      } ${delay != null ? 'intro-rv' : ''} ${className}`}
      style={{ ...(delay != null ? rv(delay) : null), ...style }}
    >
      {children}
    </div>
  );
}

/** The whole-app chrome: icon rail + topbar + content. */
function Frame({ children, topbar }: { children: ReactNode; topbar?: ReactNode }) {
  return (
    <div className="intro-mk intro-rv w-full max-w-[400px] overflow-hidden" style={rv(0)}>
      <div className="flex">
        {/* icon rail */}
        <div
          className="flex w-7 shrink-0 flex-col items-center gap-2 border-r py-2.5"
          style={{ borderColor: 'var(--iv-border)' }}
        >
          <span className="h-2.5 w-2.5 rounded" style={{ background: 'var(--iv-blue)' }} />
          {[0, 1, 2].map((i) => (
            <span key={i} className="h-2.5 w-2.5 rounded" style={{ background: 'var(--iv-fill)' }} />
          ))}
        </div>
        <div className="min-w-0 flex-1">
          <div
            className="flex h-7 items-center gap-2 border-b px-2.5"
            style={{ borderColor: 'var(--iv-border)' }}
          >
            {topbar ?? <Ln w="34%" />}
          </div>
          <div className="flex min-h-[300px] flex-col p-2.5">{children}</div>
        </div>
      </div>
    </div>
  );
}

/** The chat input dock: composer line + plus / plan / model / send row. */
function InputDock({ hot = false, delay, chip }: { hot?: boolean; delay?: number; chip?: ReactNode }) {
  return (
    <Region hot={hot} delay={delay} className="mt-auto flex flex-col gap-1.5 p-1.5">
      {chip}
      <Ln w="46%" c="var(--iv-line-3)" />
      <div className="flex items-center gap-1.5">
        <Plus className="h-2.5 w-2.5 shrink-0" style={{ color: 'var(--iv-text-3)' }} />
        <span className="font-mono text-[8px]" style={{ color: 'var(--iv-text-3)' }}>
          plan
        </span>
        <span className="ml-auto">
          <Ln w="30px" h={3} c="var(--iv-line-3)" />
        </span>
        <span
          className="flex h-4 w-4 shrink-0 items-center justify-center rounded-full"
          style={{ background: 'var(--iv-blue-deep)' }}
        >
          <ArrowUp className="h-2 w-2 text-white" />
        </span>
      </div>
    </Region>
  );
}

/** A workspace card in the gallery wireframe: title, blurb, updated-at.
    The Flash card is fully color-filled — that's how users tell it apart
    from regular (unfilled) workspace cards in the real gallery. */
function WsCard({ hot = false, flash = false, delay }: { hot?: boolean; flash?: boolean; delay: number }) {
  return (
    <Region
      hot={hot}
      delay={delay}
      className="flex flex-col gap-1 p-2"
      style={
        flash
          ? {
              background:
                'linear-gradient(135deg, rgba(90,130,216,0.22), rgba(90,130,216,0.08))',
              borderColor: 'rgba(90,130,216,0.3)',
            }
          : undefined
      }
    >
      <div className="flex items-center gap-1">
        {flash && <Zap className="h-2 w-2 shrink-0" style={{ color: 'var(--iv-blue)' }} />}
        <Ln w="52%" h={4} c="var(--iv-line-2)" />
      </div>
      <Ln w="88%" />
      <Ln w="64%" />
      <div className="mt-auto pt-0.5">
        <Ln w="32%" h={2} c="var(--iv-line-3)" />
      </div>
    </Region>
  );
}

/* ── chat · step 1: the agent's two modes, side by side ────────────────── */

/** Split scene: Flash (instant answer) vs PTC (subagents → deliverable). */
function TwoModes() {
  return (
    <Frame topbar={<Ln w="26%" />}>
      <div className="flex flex-1 gap-2">
        {/* Flash half: coordinating the desk, with quick answers on the side */}
        <Region hot delay={0.12} className="flex flex-1 flex-col gap-1.5 p-2">
          <div className="flex items-center gap-1.5" style={{ color: 'var(--iv-blue)' }}>
            <Zap className="h-2.5 w-2.5" />
            <span className="font-mono text-[8px]">Flash</span>
          </div>
          <OrchRow
            icon={<Folder className="h-2 w-2 shrink-0" style={{ color: 'var(--iv-blue)' }} />}
            label="workspace created"
            delay={0.22}
          />
          <OrchRow
            icon={<ChevronRight className="h-2 w-2 shrink-0" style={{ color: 'var(--iv-blue)' }} />}
            label="dispatched to PTC"
            delay={0.3}
          />
          <div className="grid grid-cols-2 gap-1.5 pt-0.5">
            <Ln w="90%" h={2} c="var(--iv-line-3)" />
            <Ln w="74%" h={2} c="var(--iv-line-3)" />
          </div>
          <span className="mt-auto font-mono text-[7px]" style={{ color: 'var(--iv-text-3)' }}>
            coordination · quick answers
          </span>
        </Region>
        {/* PTC half: subagents fanning out toward a deliverable. Plain region
            + teal tag — the blue accent belongs to Flash in this comparison */}
        <Region delay={0.28} className="flex flex-1 flex-col gap-1.5 p-2">
          <div className="flex items-center">
            <Tag color="var(--iv-teal)">PTC</Tag>
          </div>
          <SubagentCard name="data-prep" tools="23 tools" delay={0.4} />
          <SubagentCard name="valuation" tools="17 tools" delay={0.48} />
          <div className="intro-rv flex items-center gap-1" style={rv(0.56)}>
            <FileText className="h-2 w-2 shrink-0" style={{ color: 'var(--iv-teal)' }} />
            <Ln w="48%" h={2} c="var(--iv-line-3)" />
          </div>
          <span className="mt-auto font-mono text-[7px]" style={{ color: 'var(--iv-text-3)' }}>
            deep analysis &amp; deliverables
          </span>
        </Region>
      </div>
      <div className="mt-2.5 flex items-center gap-1.5 font-mono text-[8px]" style={{ color: 'var(--iv-text-3)' }}>
        <span className="intro-pulse" style={{ width: 5, height: 5 }} />
        one agent · two modes
      </div>
    </Frame>
  );
}

/* ── chat · step 4: the workspace gallery page ─────────────────────────── */

function WorkspaceGrid() {
  return (
    <Frame topbar={<Ln w="20%" />}>
      <div className="flex flex-1 flex-col gap-2">
        {/* page title + new-workspace button */}
        <div className="intro-rv flex items-center justify-between" style={rv(0.08)}>
          <Ln w="22%" h={5} c="var(--iv-line-2)" />
          <span
            className="flex items-center gap-1 rounded px-1.5 py-1"
            style={{ background: 'var(--iv-blue-deep)' }}
          >
            <Plus className="h-2 w-2 text-white" />
            <Ln w="30px" h={3} c="rgba(255,255,255,0.8)" />
          </span>
        </div>
        {/* search */}
        <Region delay={0.16} className="flex items-center gap-1.5 p-1.5">
          <Search className="h-2 w-2 shrink-0" style={{ color: 'var(--iv-text-3)' }} />
          <Ln w="30%" c="var(--iv-line-3)" />
        </Region>
        {/* no hot card here — the step is about the gallery as a whole, and a
            highlighted card would read as a second Flash card */}
        <div className="grid flex-1 auto-rows-fr grid-cols-2 gap-2">
          <WsCard flash delay={0.24} />
          <WsCard delay={0.32} />
          <WsCard delay={0.4} />
          <WsCard delay={0.48} />
        </div>
      </div>
      <div className="mt-2.5 flex items-center gap-1.5 font-mono text-[8px]" style={{ color: 'var(--iv-text-3)' }}>
        <span className="intro-pulse" style={{ width: 5, height: 5 }} />
        one desk per company, sector, or strategy
      </div>
    </Frame>
  );
}

/* ── chat · step 2: a Flash chat page ──────────────────────────────────── */

/** Compact live-quote card, echoing the real quote tool-result card. */
function QuoteCard({
  ticker,
  price,
  change,
  up,
  delay,
}: {
  ticker: string;
  price: string;
  change: string;
  up: boolean;
  delay: number;
}) {
  return (
    <Region delay={delay} className="flex flex-col gap-1 p-1.5">
      <div className="flex items-center gap-1">
        <span className="font-mono text-[8px] leading-none" style={{ color: 'var(--iv-text)' }}>
          {ticker}
        </span>
        <Ln w="26%" h={2} c="var(--iv-line-3)" />
      </div>
      <div className="flex items-baseline gap-1">
        <span
          className="font-mono text-[9px] font-semibold leading-none"
          style={{ color: 'var(--iv-text)' }}
        >
          {price}
        </span>
        <span
          className="font-mono text-[7px] leading-none"
          style={{ color: up ? 'var(--iv-teal)' : 'var(--iv-red)' }}
        >
          {change}
        </span>
      </div>
      {/* open / range / volume stat grid stays skeleton */}
      <div className="grid grid-cols-2 gap-x-2 gap-y-1 pt-0.5">
        <Ln w="80%" h={2} c="var(--iv-line-3)" />
        <Ln w="64%" h={2} c="var(--iv-line-3)" />
        <Ln w="56%" h={2} c="var(--iv-line-3)" />
        <Ln w="72%" h={2} c="var(--iv-line-3)" />
      </div>
    </Region>
  );
}

/** One line of Flash's orchestration run: icon · action · ✓. */
function OrchRow({
  icon,
  label,
  delay,
}: {
  icon: ReactNode;
  label: string;
  delay: number;
}) {
  return (
    <div className="intro-rv flex items-center gap-1.5" style={rv(delay)}>
      {icon}
      <span className="font-mono text-[8px] leading-none" style={{ color: 'var(--iv-text-2)' }}>
        {label}
      </span>
      <span className="ml-auto font-mono text-[7px] leading-none" style={{ color: 'var(--iv-teal)' }}>
        ✓
      </span>
    </div>
  );
}

/** Orchestration first: Flash sets up a workspace, dispatches the heavy task
    to PTC, and schedules an automation; a quick price check follows so fast
    QA reads as part of the job, not the whole job. */
function FlashAnswer() {
  return (
    <Frame topbar={<><Ln w="22%" /><Tag>Flash</Tag></>}>
      <div className="flex flex-1 flex-col gap-2">
        {/* mr keeps the bubbles clear of the bleed crop (scene anchors left) */}
        <div className="intro-rv mr-24 flex justify-end" style={rv(0.12)}>
          <div
            className="rounded-md rounded-br-sm px-2 py-1.5"
            style={{ background: 'rgba(90,130,216,0.22)' }}
          >
            <span
              className="block font-mono text-[8px] leading-none"
              style={{ color: 'var(--iv-text)' }}
            >
              Set up a deep dive on NVDA earnings
            </span>
          </div>
        </div>
        <Region hot delay={0.2} className="flex flex-col gap-1.5 p-2">
          <div className="flex items-center gap-1.5" style={{ color: 'var(--iv-blue)' }}>
            <Zap className="h-2.5 w-2.5" />
            <span className="font-mono text-[8px]">flash · orchestrating</span>
          </div>
          <OrchRow
            icon={<Folder className="h-2 w-2 shrink-0" style={{ color: 'var(--iv-blue)' }} />}
            label="workspace created · NVDA deep dive"
            delay={0.3}
          />
          <OrchRow
            icon={<ChevronRight className="h-2 w-2 shrink-0" style={{ color: 'var(--iv-blue)' }} />}
            label="task dispatched to PTC"
            delay={0.38}
          />
          <OrchRow
            icon={<Clock className="h-2 w-2 shrink-0" style={{ color: 'var(--iv-blue)' }} />}
            label="automation · daily earnings brief"
            delay={0.46}
          />
        </Region>
        {/* quick QA rides along below */}
        <div className="intro-rv mr-24 flex justify-end" style={rv(0.54)}>
          <div
            className="rounded-md rounded-br-sm px-2 py-1.5"
            style={{ background: 'rgba(90,130,216,0.22)' }}
          >
            <span
              className="block font-mono text-[8px] leading-none"
              style={{ color: 'var(--iv-text)' }}
            >
              NVDA price?
            </span>
          </div>
        </div>
        <Region delay={0.62} className="flex flex-col gap-1.5 p-2">
          <div className="flex items-center gap-1.5" style={{ color: 'var(--iv-blue)' }}>
            <Zap className="h-2.5 w-2.5" />
            <span className="font-mono text-[8px]">flash · 0.9s</span>
          </div>
          <div className="grid grid-cols-2 gap-1.5">
            <QuoteCard ticker="NVDA" price="$142.10" change="+1.8%" up delay={0.7} />
            <QuoteCard ticker="AVGO" price="$310.51" change="-0.4%" up={false} delay={0.78} />
          </div>
        </Region>
        <InputDock delay={0.88} />
      </div>
    </Frame>
  );
}

/* ── chat · step 3: a PTC thread page running code ─────────────────────── */

/** A subagent task card, echoing the real run cards (name · ✓ · n tools). */
function SubagentCard({ name, tools, delay }: { name: string; tools: string; delay: number }) {
  return (
    <Region delay={delay} className="flex flex-col gap-1 p-1.5">
      <div className="flex items-center">
        <span className="font-mono text-[8px] leading-none" style={{ color: 'var(--iv-text-2)' }}>
          {name}
        </span>
        <span
          className="ml-auto font-mono text-[7px] leading-none"
          style={{ color: 'var(--iv-teal)' }}
        >
          ✓ completed
        </span>
      </div>
      <Ln w="72%" h={2} c="var(--iv-line-3)" />
      <span className="font-mono text-[7px] leading-none" style={{ color: 'var(--iv-text-3)' }}>
        {tools}
      </span>
    </Region>
  );
}

/** No user bubble here — the scene is the agent mid-run: parallel subagents,
    an inline chart, deliverables, and the file panel previewing the Excel
    they produced (the panel bleeds off the cropped right edge). */
function PtcSandbox() {
  const bars = [38, 56, 46, 86, 60, 70];
  const accent = 3;
  return (
    <Frame topbar={<><Ln w="22%" /><Tag>PTC</Tag></>}>
      <div className="flex flex-1 gap-2">
        {/* chat column */}
        <div className="flex min-w-0 flex-[1.5] flex-col gap-2">
          {/* parallel subagents (Flash can't do this) */}
          <SubagentCard name="data-prep" tools="23 tools" delay={0.1} />
          <SubagentCard name="report-builder" tools="10 tools" delay={0.18} />
          {/* narration, blurred */}
          <Ln w="58%" />
          {/* inline visualization in the reply */}
          <Region hot delay={0.32} className="flex flex-col gap-1 p-2">
            <Ln w="34%" h={2} c="var(--iv-line-3)" />
            <div className="mt-0.5 flex h-9 items-end gap-1">
              {bars.map((h, i) => (
                <span
                  key={i}
                  className="intro-bar flex-1 rounded-t-sm"
                  style={{
                    ...rv(0.42 + i * 0.05),
                    height: `${h}%`,
                    background: i === accent ? 'var(--iv-teal)' : 'rgba(90,130,216,0.7)',
                  }}
                />
              ))}
            </div>
          </Region>
          {/* the deliverables */}
          <div className="intro-rv flex flex-wrap items-center gap-1.5" style={rv(0.52)}>
            <Region className="flex items-center gap-1 px-1.5 py-1">
              <FileText className="h-2 w-2 shrink-0" style={{ color: 'var(--iv-text-3)' }} />
              <span className="font-mono text-[7px] leading-none" style={{ color: 'var(--iv-text-2)' }}>
                comps.xlsx
              </span>
              <Tag color="var(--iv-teal)">✓</Tag>
            </Region>
            <Region className="flex items-center gap-1 px-1.5 py-1">
              <FileText className="h-2 w-2 shrink-0" style={{ color: 'var(--iv-text-3)' }} />
              <span className="font-mono text-[7px] leading-none" style={{ color: 'var(--iv-text-2)' }}>
                report.pdf
              </span>
              <Tag color="var(--iv-teal)">✓</Tag>
            </Region>
          </div>
          <InputDock delay={0.6} />
        </div>
        {/* file panel previewing the Excel deliverable */}
        <Region delay={0.44} className="flex flex-1 flex-col gap-1.5 p-1.5">
          <div className="flex items-center gap-1">
            <FileText className="h-2 w-2 shrink-0" style={{ color: 'var(--iv-text-3)' }} />
            <span className="font-mono text-[7px] leading-none" style={{ color: 'var(--iv-text-2)' }}>
              comps.xlsx
            </span>
          </div>
          {/* sheet tabs */}
          <div className="flex gap-1">
            <Tag>AI Semi Comps</Tag>
            <Tag color="var(--iv-text-3)">Mega-Cap</Tag>
          </div>
          {/* spreadsheet grid: hairline-divided cells */}
          <div
            className="grid flex-1 auto-rows-fr grid-cols-3 gap-px overflow-hidden rounded border"
            style={{ background: 'var(--iv-border)', borderColor: 'var(--iv-border)' }}
          >
            {Array.from({ length: 21 }, (_, i) => (
              <span
                key={i}
                className="flex items-center px-1"
                style={{ background: i < 3 ? 'var(--iv-fill)' : 'var(--iv-mk-bg)' }}
              >
                <Ln w={i < 3 ? '80%' : `${[64, 40, 52][i % 3]}%`} h={2} c="var(--iv-line-3)" />
              </span>
            ))}
          </div>
        </Region>
      </div>
    </Frame>
  );
}

/* ── chat · step 5: the new-workspace modal over the gallery ───────────── */

function CreateWorkspace() {
  return (
    <div className="relative w-full max-w-[400px]">
      {/* this scene is center-anchored: the dimmed gallery fades into the
          panel at both edges instead of hard-cropping on one side */}
      <div
        style={{
          opacity: 0.45,
          WebkitMaskImage:
            'linear-gradient(90deg, transparent 0, black 18%, black 82%, transparent 100%)',
          maskImage:
            'linear-gradient(90deg, transparent 0, black 18%, black 82%, transparent 100%)',
        }}
      >
        <Frame>
          <div className="flex flex-col gap-2">
            {/* the new-workspace button the scripted cursor clicks */}
            <div className="flex items-center">
              <Ln w="22%" h={5} c="var(--iv-line-2)" />
              <span
                className="ml-auto mr-16 flex items-center gap-1 rounded px-1.5 py-1"
                style={{ background: 'var(--iv-blue-deep)' }}
              >
                <Plus className="h-2 w-2 text-white" />
                <Ln w="30px" h={3} c="rgba(255,255,255,0.8)" />
              </span>
            </div>
            <div className="grid grid-cols-2 gap-2">
              <WsCard flash delay={0} />
              <WsCard delay={0} />
              <WsCard delay={0} />
              <WsCard delay={0} />
            </div>
          </div>
        </Frame>
      </div>
      {/* scripted click: cursor slides onto the button, ripple fires, then
          the modal pops. Lives outside the dimmed layer so the gesture reads
          at full strength; coordinates match the button above (fixed-size
          mockup, so they're deterministic). */}
      <div className="pointer-events-none absolute" style={{ left: 299, top: 46 }}>
        <span className="intro-click absolute -left-3 -top-3 h-6 w-6" style={rv(0.62)} />
        <MousePointer2
          className="intro-rv absolute h-3 w-3"
          style={{ ...rv(0.28), color: 'var(--iv-text)', fill: 'currentColor' }}
        />
      </div>
      {/* the modal the click opens: name, description, drop-zone, create.
          Centering transform lives on a static wrapper — the pop animates
          `transform` and would clobber translate utilities on itself. */}
      <div className="absolute left-1/2 top-1/2 w-[72%] -translate-x-1/2 -translate-y-1/2">
        <div className="intro-mk intro-mk--accent intro-pop p-2.5" style={rv(0.95)}>
          <div className="flex items-center justify-between">
            <span className="font-mono text-[9px]">New workspace</span>
            <X className="h-2.5 w-2.5" style={{ color: 'var(--iv-text-3)' }} />
          </div>
          <div className="mt-2 flex flex-col gap-1.5">
            {/* name field */}
            <span className="font-mono text-[8px]" style={{ color: 'var(--iv-text-3)' }}>
              name
            </span>
            <Region className="flex items-center px-1.5 py-1" style={{ borderRadius: 5 }}>
              <span className="font-mono text-[8px] leading-none" style={{ color: 'var(--iv-text)' }}>
                NVDA earnings deep-dive
              </span>
            </Region>
            {/* description */}
            <span className="font-mono text-[8px]" style={{ color: 'var(--iv-text-3)' }}>
              description
            </span>
            <span className="font-mono text-[8px]" style={{ color: 'var(--iv-text-2)' }}>
              Q1 print vs. guidance, data-center demand…
            </span>
            {/* files queued for the new workspace */}
            <span className="font-mono text-[8px]" style={{ color: 'var(--iv-text-3)' }}>
              files
            </span>
            <Region dashed className="flex items-center justify-center gap-1.5 p-1.5">
              <Upload className="h-2.5 w-2.5" style={{ color: 'var(--iv-text-3)' }} />
              <Ln w="38%" />
            </Region>
            <div className="intro-rv flex items-center gap-1.5" style={rv(0.45)}>
              <FileText className="h-2 w-2 shrink-0" style={{ color: 'var(--iv-text-3)' }} />
              <Ln w="34%" c="var(--iv-line-2)" />
              <span className="ml-auto">
                <Tag color="var(--iv-teal)">ready</Tag>
              </span>
            </div>
            <div className="intro-rv flex items-center gap-1.5" style={rv(0.55)}>
              <FileText className="h-2 w-2 shrink-0" style={{ color: 'var(--iv-text-3)' }} />
              <Ln w="26%" c="var(--iv-line-2)" />
              <span className="ml-auto w-8">
                <Ln w="64%" h={2} c="var(--iv-teal)" />
              </span>
            </div>
            <div className="flex items-center justify-end gap-2">
              <span className="font-mono text-[8px]" style={{ color: 'var(--iv-text-3)' }}>
                cancel
              </span>
              <span
                className="flex items-center px-2 py-1"
                style={{ background: 'var(--iv-blue-deep)', borderRadius: 5 }}
              >
                <span className="font-mono text-[8px] leading-none text-white">create</span>
              </span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

/* ── thread page base: chat column + tabbed right panel ────────────────── */

function PanelTabs({ active }: { active: 'files' | 'memory' | 'memos' }) {
  const tabs = ['files', 'memory', 'memos'] as const;
  return (
    <div className="flex items-center gap-1">
      {tabs.map((t) => (
        <span
          key={t}
          className="rounded px-1.5 py-0.5 font-mono text-[8px] leading-none"
          style={
            t === active
              ? { background: 'var(--iv-blue-deep)', color: '#fff' }
              : { color: 'var(--iv-text-3)' }
          }
        >
          {t}
        </span>
      ))}
    </div>
  );
}

/** A row in the workspace file tree. */
function TreeRow({
  depth = 0,
  kind = 'file',
  open = false,
  w,
  dot = false,
  delay,
}: {
  depth?: number;
  kind?: 'file' | 'folder';
  open?: boolean;
  w: string;
  dot?: boolean;
  delay: number;
}) {
  const Chevron = open ? ChevronDown : ChevronRight;
  return (
    <div className="intro-rv flex items-center gap-1" style={{ ...rv(delay), paddingLeft: depth * 8 }}>
      {kind === 'folder' ? (
        <>
          <Chevron className="h-2 w-2 shrink-0" style={{ color: 'var(--iv-text-3)' }} />
          <Folder className="h-2 w-2 shrink-0" style={{ color: 'var(--iv-text-3)' }} />
        </>
      ) : (
        <FileText className="h-2 w-2 shrink-0" style={{ color: 'var(--iv-text-3)' }} />
      )}
      <Ln w={w} />
      {dot && <span className="ml-auto h-1 w-1 shrink-0 rounded-full" style={{ background: 'var(--iv-teal)' }} />}
    </div>
  );
}

function ThreadFrame({
  tab,
  panel,
  bubble,
}: {
  tab: 'files' | 'memory' | 'memos';
  panel: ReactNode;
  bubble?: ReactNode;
}) {
  return (
    <Frame
      topbar={
        <>
          <ArrowLeft className="h-2.5 w-2.5 shrink-0" style={{ color: 'var(--iv-text-3)' }} />
          <Ln w="24%" />
          <span className="ml-auto">
            <Tag>PTC</Tag>
          </span>
        </>
      }
    >
      <div className="flex flex-1 gap-2">
        <div className="flex min-w-0 flex-1 flex-col gap-1.5">
          <Ln w="70%" />
          <Ln w="86%" />
          <Ln w="52%" />
          {bubble}
          <Ln w="78%" />
          <Ln w="84%" />
          <Ln w="60%" />
          <Ln w="72%" />
          <Ln w="40%" />
          <InputDock delay={0.55} />
        </div>
        {/* the panel is the subject — give it the wider column (the chat
            side bleeds off the left edge anyway) */}
        <Region hot delay={0.2} className="flex flex-[1.25] flex-col gap-1.5 p-1.5">
          <div className="flex items-center justify-between gap-1">
            <PanelTabs active={tab} />
            <span className="intro-pulse" style={{ width: 5, height: 5 }} />
          </div>
          {panel}
        </Region>
      </div>
    </Frame>
  );
}

/* ── thread · step 1: file panel ───────────────────────────────────────── */

function FilePanel() {
  return (
    <ThreadFrame
      tab="files"
      panel={
        <>
          <TreeRow w="34%" dot delay={0.3} />
          <TreeRow kind="folder" open w="30%" delay={0.38} />
          <TreeRow depth={1} w="58%" dot delay={0.46} />
          <TreeRow depth={1} w="48%" dot delay={0.54} />
          <TreeRow kind="folder" w="26%" delay={0.62} />
          <Tag color="var(--iv-teal)">deliverables → results/</Tag>
        </>
      }
    />
  );
}

/* ── thread · step 2: long-term memory ─────────────────────────────────── */

function Memory() {
  return (
    <ThreadFrame
      tab="memory"
      bubble={
        <div className="intro-rv flex justify-end" style={rv(0.1)}>
          <div className="rounded-md rounded-br-sm px-1.5 py-1" style={{ background: 'rgba(90,130,216,0.22)' }}>
            <span className="block font-mono text-[8px] leading-none" style={{ color: 'var(--iv-text-2)' }}>
              remember this…
            </span>
          </div>
        </div>
      }
      panel={
        <>
          {['82%', '68%', '74%'].map((w, i) => (
            <div key={i} className="intro-rv flex items-center gap-1.5" style={rv(0.3 + i * 0.1)}>
              <span className="h-1 w-1 shrink-0 rounded-full" style={{ background: 'var(--iv-teal)' }} />
              <Ln w={w} />
            </div>
          ))}
          <Tag color="var(--iv-teal)">applies to every thread</Tag>
        </>
      }
    />
  );
}

/* ── thread · step 3: memos ────────────────────────────────────────────── */

function Memo() {
  return (
    <ThreadFrame
      tab="memos"
      panel={
        <>
          <Region dashed className="intro-rv flex items-center justify-center gap-1 p-1.5" style={rv(0.3)}>
            <Upload className="h-2.5 w-2.5" style={{ color: 'var(--iv-text-3)' }} />
            <Ln w="42%" />
          </Region>
          {['76%', '60%'].map((w, i) => (
            <div key={i} className="intro-rv flex items-center gap-1.5" style={rv(0.42 + i * 0.1)}>
              <span className="h-2 w-2 shrink-0 rounded-sm" style={{ background: 'var(--iv-blue)' }} />
              <Ln w={w} />
              <span className="ml-auto">
                <Tag color="var(--iv-teal)">ready</Tag>
              </span>
            </div>
          ))}
        </>
      }
    />
  );
}

/* ── dashboard base: ticker strip + widget grid ────────────────────────── */

function TickerStrip({ delay = 0.1 }: { delay?: number }) {
  const chips = ['var(--iv-teal)', 'var(--iv-teal)', 'var(--iv-red)', 'var(--iv-teal)', 'var(--iv-red)'];
  return (
    <div className="intro-rv flex gap-1.5" style={rv(delay)}>
      {chips.map((c, i) => (
        <Region key={i} className="flex flex-1 flex-col gap-1 p-1.5">
          <Ln w="70%" h={3} c="var(--iv-line-2)" />
          <Ln w="50%" h={3} c={`color-mix(in srgb, ${c} 65%, transparent)`} />
        </Region>
      ))}
    </div>
  );
}

function Watchlist({
  hot = false,
  delay = 0,
  className = '',
}: {
  hot?: boolean;
  delay?: number;
  className?: string;
}) {
  const rows = ['var(--iv-teal)', 'var(--iv-red)', 'var(--iv-teal)'];
  return (
    <Region hot={hot} delay={delay} className={`flex flex-col gap-1.5 p-1.5 ${className}`}>
      <Ln w="46%" h={4} c="var(--iv-line-2)" />
      {rows.map((c, i) => (
        <div key={i} className="flex items-center justify-between gap-2">
          <Ln w="38%" />
          <Ln w="22%" c={`color-mix(in srgb, ${c} 65%, transparent)`} />
        </div>
      ))}
    </Region>
  );
}

/* ── dashboard · step 1: the whole page ────────────────────────────────── */

function DashboardGrid() {
  return (
    <Frame topbar={<Ln w="30%" />}>
      <div className="flex flex-1 flex-col gap-1.5">
        <TickerStrip />
        <div className="flex flex-1 gap-1.5">
          {/* brief / news canvas */}
          <Region delay={0.22} className="flex flex-[1.5] flex-col gap-1.5 p-2">
            <Ln w="36%" h={4} c="var(--iv-line-2)" />
            <Ln w="92%" />
            <Ln w="84%" />
            <Ln w="88%" />
            <Ln w="58%" />
            <Ln w="76%" />
            <Ln w="90%" />
            <Ln w="68%" />
            <Ln w="82%" />
            <Ln w="40%" />
          </Region>
          <div className="flex flex-1 flex-col gap-1.5">
            <Watchlist delay={0.32} />
            <Region delay={0.42} className="flex flex-1 flex-col justify-center p-1.5">
              <svg viewBox="0 0 100 30" className="h-8 w-full" aria-hidden>
                <polyline
                  className="intro-wire"
                  points="0,24 16,20 32,25 48,13 64,17 80,8 100,4"
                  fill="none"
                  stroke="var(--iv-teal)"
                  strokeWidth="1.6"
                />
              </svg>
            </Region>
          </div>
        </div>
      </div>
    </Frame>
  );
}

/* ── dashboard · step 2: customize (Custom mode + add/drag widgets) ────── */

function DashboardCustomize() {
  return (
    <Frame
      topbar={
        <>
          <Ln w="22%" />
          <span className="ml-auto flex items-center gap-1 rounded p-0.5" style={{ background: 'var(--iv-line-3)' }}>
            <span className="rounded px-1.5 py-0.5 font-mono text-[8px]" style={{ color: 'var(--iv-text-3)' }}>
              Classic
            </span>
            <span className="rounded px-1.5 py-0.5 font-mono text-[8px] text-white" style={{ background: 'var(--iv-blue-deep)' }}>
              Custom
            </span>
          </span>
          <span className="intro-pulse" style={{ width: 5, height: 5 }} />
        </>
      }
    >
      <div className="flex flex-1 flex-col gap-1.5">
        <TickerStrip />
        <div className="grid flex-1 auto-rows-fr grid-cols-3 gap-1.5">
          <Region delay={0.22} className="flex flex-col gap-1.5 p-1.5">
            <Ln w="50%" h={4} c="var(--iv-line-2)" />
            <Ln w="84%" />
            <Ln w="62%" />
          </Region>
          {/* widget mid-drag — rotation sits on a static wrapper because
              intro-rv animates `transform` and would override it */}
          <div className="rotate-[-2deg]">
            <Region hot delay={0.32} className="flex h-full flex-col gap-1.5 p-1.5">
              <div className="flex items-center justify-between">
                <Ln w="44%" h={4} c="var(--iv-line-2)" />
                <Tag>drag</Tag>
              </div>
              <Ln w="78%" />
              <Ln w="56%" />
            </Region>
          </div>
          <Region dashed delay={0.42} className="flex items-center justify-center gap-1 p-1.5">
            <Plus className="h-3 w-3" style={{ color: 'var(--iv-text-3)' }} />
            <Ln w="36%" />
          </Region>
        </div>
      </div>
    </Frame>
  );
}

/* ── dashboard · step 3: clip a row into chat context ──────────────────── */

function DashboardAttach() {
  return (
    <Frame topbar={<Ln w="30%" />}>
      <div className="flex flex-1 flex-col gap-1.5">
        <div className="flex flex-1 gap-1.5">
          {/* news widget with the paperclip row */}
          <Region hot delay={0.12} className="flex flex-[1.5] flex-col gap-1.5 p-2">
            <Ln w="34%" h={4} c="var(--iv-line-2)" />
            <div className="flex items-center gap-2">
              <div className="flex min-w-0 flex-1 flex-col gap-1">
                <Ln w="92%" />
                <Ln w="64%" />
              </div>
              <span className="relative flex h-5 w-5 shrink-0 items-center justify-center rounded-full" style={{ background: 'rgba(90,130,216,0.25)' }}>
                <Paperclip className="h-2.5 w-2.5" style={{ color: 'var(--iv-blue)' }} />
                <span className="intro-pulse intro-pulse--blue absolute -right-0.5 -top-0.5" style={{ width: 5, height: 5 }} />
              </span>
            </div>
            <Ln w="84%" />
            <Ln w="70%" />
          </Region>
          <Watchlist delay={0.24} className="flex-1" />
        </div>
        {/* chat dock receiving the clipped row */}
        <InputDock
          hot
          delay={0.36}
          chip={
            /* right-aligned: this scene bleeds off the left edge */
            <span className="ml-auto flex w-fit items-center gap-1 rounded px-1.5 py-0.5" style={{ background: 'rgba(90,130,216,0.2)' }}>
              <Paperclip className="h-2 w-2" style={{ color: 'var(--iv-blue)' }} />
              <span className="font-mono text-[8px]" style={{ color: 'var(--iv-blue)' }}>
                attached
              </span>
            </span>
          }
        />
      </div>
    </Frame>
  );
}

export const INTRO_VISUALS: Record<IntroVisualId, () => ReactNode> = {
  twoModes: TwoModes,
  workspaceGrid: WorkspaceGrid,
  flashAnswer: FlashAnswer,
  ptcSandbox: PtcSandbox,
  createWorkspace: CreateWorkspace,
  filePanel: FilePanel,
  memory: Memory,
  memo: Memo,
  dashboardGrid: DashboardGrid,
  dashboardCustomize: DashboardCustomize,
  dashboardAttach: DashboardAttach,
};

/**
 * The mockup is rendered oversized and bleeds off one edge of the visual
 * panel (landing-page style). The anchored side stays fully visible, so each
 * scene anchors the side its hot region lives on; the other side crops.
 */
export const INTRO_VISUAL_ANCHOR: Record<IntroVisualId, 'left' | 'right' | 'center'> = {
  // centered: symmetric comparison, fades its own edges
  twoModes: 'center',
  workspaceGrid: 'left',
  flashAnswer: 'left',
  ptcSandbox: 'left',
  // centered: the scene fades its own edges instead of bleeding one side
  createWorkspace: 'center',
  filePanel: 'right',
  memory: 'right',
  memo: 'right',
  dashboardGrid: 'left',
  dashboardCustomize: 'right',
  dashboardAttach: 'right',
};
