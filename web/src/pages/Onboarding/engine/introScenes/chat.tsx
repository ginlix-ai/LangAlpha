import { useTranslation } from 'react-i18next';
import {
  ArrowUpRight,
  Brain,
  ChevronRight,
  FileSpreadsheet,
  FileText,
  Folder,
  Inbox,
  Search,
  Send,
  TrendingUp,
  Zap,
} from 'lucide-react';
import { HomeRowIcon } from '@/hooks/useHomeWorkspace';
import { messagingAppName } from '@/pages/ChatAgent/components/messaging/messageDelivery';
import { useAgentNames } from '../introAgents';
import type { IntroVisualId } from '../../registry';
import { BarChart, StockPriceCard, type Bar } from './charts';
import { PAST, useSceneTime } from './clock';
import { CLOSE, Camera, REST, type Shot, VIEW, shot, wide } from './camera';
import { Appear, InputDock, Region, UserBubble, pointerScript } from './kit';
import type { IntroScene } from './types';
import {
  type FlowAgent,
  type Icon,
  ChartCard,
  DeliveryPill,
  FileStack,
  FromChiefOfStaff,
  HandoffSlip,
  ViaApp,
  WorkflowCard,
  WorkspaceLine,
} from './chatParts';
import { Reply, ThreadHeader, ToolRow, Transcript } from './transcriptParts';
import { DASH_META_CHIP } from './chatDashboard';
import { CapexThread, PTC, PTC_CHAT } from './capexThread';

/* Scenes for the chat intro: the Chief of Staff answering a quick question
   itself and handing the deeper follow-up to an Analyst, who takes it up in
   its own workspace, the Chief of Staff's own thread coordinating
   Analysts, and an Analyst's thread running the work it was handed. The
   Chief of Staff stays brief and talks to the user; the Analyst runs a
   workflow and writes the long answer with its charts and files. Each part
   is a small copy of the component the app draws it with (named on each),
   in the same tokens and labels. The figures are invented, and agree with
   each other across the scenes. */
/* eslint-disable react-refresh/only-export-components -- scene module:
   private scene components, one record export */

/* ── chat · step 1: one hand-off, from both sides ──────────────────────── */

/** An Analyst's report-back, drawn in place of its hidden message as a line
    of its own (`ReportRow`), right-aligned and shown at `at`: the inbox a
    report arrives in, the Analyst, how its run ended, and the way into its
    thread. Only the chat scenes show one, so it stays here, clear of the
    parts the thread intro loads. */
function ReportBack({ at, workspace }: { at: number; workspace: string }) {
  const { t } = useTranslation();
  return (
    <Appear at={at} className="flex justify-end">
      <span
        className="flex min-w-0 items-center gap-[3.5px] text-[7px] leading-none"
        style={{ color: 'var(--color-text-secondary)' }}
      >
        <Inbox className="h-[7px] w-[7px] shrink-0" />
        <span className="truncate font-medium" style={{ color: 'var(--color-text-primary)' }}>
          {t('chat.provenance.analyst', { name: workspace })}
        </span>
        <span className="shrink-0 whitespace-nowrap">{t('chat.provenance.reportedBack')}</span>
        <span
          className="ml-[4.5px] flex shrink-0 items-center gap-[2px] font-medium"
          style={{ color: 'var(--color-text-primary)' }}
        >
          {t('chat.ptcCard.open')}
          <ArrowUpRight className="h-[7px] w-[7px]" />
        </span>
      </span>
    </Appear>
  );
}

/** A pane's heading, as the sidebar names a desk: its glyph, then its name. */
function PaneHeader({ icon: Glyph, name }: { icon: Icon; name: string }) {
  return (
    <div className="flex items-center gap-1">
      <Glyph className="h-2.5 w-2.5 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
      <span
        className="text-[8px] font-medium leading-none"
        style={{ color: 'var(--color-text-primary)' }}
      >
        {name}
      </span>
    </div>
  );
}

/** NVDA's closes over six months, for the Chief of Staff's quick answer:
    the high is the August print's gap to $186.90, as in the chart intro. */
const NVDA_6M = [
  141.6, 138.2, 133.9, 132.1, 136.8, 142.5, 147.3, 145.0, 151.2, 156.8, 153.4, 159.9, 164.1, 161.7,
  168.5, 172.3, 186.9, 179.8, 173.4, 171.2, 175.6, 178.9, 180.3, 182.42,
];

/** The Analyst's DCF: value per share by case, against spot. */
const DCF_CASES: Bar[] = [
  { label: 'Bear', value: 128, shown: '$128' },
  { label: 'Base', value: 171, shown: '$171', mark: true },
  { label: 'Bull', value: 226, shown: '$226' },
];

/** The NVDA valuation the Analyst's workflow runs: three phases, the agents
    each sends out, and the log line it prints as each phase starts. */
const NVDA_FLOW = {
  phases: [
    { name: 'Research', at: 4650 },
    { name: 'Value', at: 5700 },
    { name: 'Review', at: 6650 },
  ],
  agents: [
    { label: 'Data-center demand', at: 4700, done: 5600, took: '2m 31s' },
    { label: 'DCF, three cases', at: 5750, done: 6550, took: '2m 05s' },
    { label: 'Model check', at: 6700, done: 7100, took: '44.1s' },
  ] satisfies FlowAgent[],
  logs: [
    { text: 'reading the Q2 call and consensus', at: 4700 },
    { text: 'building the DCF', at: 5750 },
    { text: 'checking the model', at: 6700 },
  ],
};

/** Split scene: the user asks the Chief of Staff (left) a quick question,
    which it answers itself, then a deeper follow-up, which it hands to an
    Analyst (right) in a new workspace. The Analyst takes the task, runs a
    workflow on it and answers at length, with a chart and its files; the
    Chief of Staff reports back to the user in a line. */
const MODES = {
  quick: 'NVDA, last 6 months?',
  ask: 100,
  fetch: 350,
  card: 700,
  answered: 1300,
  request: 'Is it still worth owning into earnings?',
  follow: 1900,
  workspace: 'NVDA deep dive',
  created: [2200, 2600],
  handoff: 2650,
  said: 2950,
  task: 4350,
  flow: 4600,
  flowDone: 7200,
  say: 7350,
  chart: 8100,
  files: [8600, 8800],
  report: 9450,
  reply: 9650,
  length: 12000,
};

/** The camera follows the work at rest, a pane whole with the edge of the
    other: the Chief of Staff answering the quick question, down its pane
    as the follow-up is handed off, across to the Analyst taking the task
    and starting the workflow, down to its answer, chart and files, back to
    the report, and out to both panes. */
const MODES_SHOTS: [Shot, ...Shot[]] = [
  shot(0, 160, 132, REST),
  shot(2800, 160, 198, REST, 700),
  shot(4300, 240, 132, REST, 800),
  shot(7500, 240, 198, REST, 700),
  shot(9500, 160, 198, REST, 600),
  wide(11400, 900),
];

function TwoModes() {
  const { t } = useTranslation();
  const names = useAgentNames();
  const [made, created] = MODES.created;
  return (
    <Camera shots={MODES_SHOTS}>
      <div className="flex gap-2 p-2" style={VIEW}>
        {/* the Chief of Staff: answers the quick question itself, hands the
            deeper follow-up off, and reports back in a line when the Analyst
            is done */}
        <Region delay={0.12} className="flex min-w-0 flex-1 flex-col gap-2 p-2">
          <PaneHeader icon={HomeRowIcon} name={names.home} />
          <Transcript>
            <UserBubble at={MODES.ask}>{MODES.quick}</UserBubble>
            <ToolRow
              dense
              icon={TrendingUp}
              running={{
                label: t('toolArtifact.inProgress.fetchingSymbolPrices', { symbol: 'NVDA' }),
              }}
              settled={{ label: t('toolArtifact.tool.stockPrices'), pill: 'NVDA' }}
              at={MODES.fetch}
              done={MODES.card}
            />
            <Appear at={MODES.card} pop>
              <StockPriceCard
                sym="NVDA"
                price="$182.42"
                change={28.83}
                period="2026-04-20, 2026-10-20"
                series={NVDA_6M}
                high="$186.90"
                low="$132.10"
                vol="176.8M"
                at={MODES.card + 100}
              />
            </Appear>
            <Reply text="**Up 29% in six months,** 2% off its August high." at={MODES.answered} />
            <UserBubble at={MODES.follow}>{MODES.request}</UserBubble>
            <WorkspaceLine name={MODES.workspace} at={made} done={created} />
            <HandoffSlip
              workspace={MODES.workspace}
              title="NVDA valuation into earnings"
              at={MODES.handoff}
              started={MODES.task}
              finished={MODES.files[1]}
            />
            <Reply
              text="**An Analyst in NVDA deep dive is on it.** I'll report back when it's done."
              at={MODES.said}
            />
            <ReportBack at={MODES.report} workspace={MODES.workspace} />
            <Reply
              text="**NVDA looks fully priced.** Only the bull case clears today's $182; the base case is $171. The model is in valuation.xlsx."
              at={MODES.reply}
            />
          </Transcript>
        </Region>
        {/* the Analyst: takes the task, runs a workflow on it, and answers
            at length with a chart and its files */}
        <Region delay={0.28} className="flex min-w-0 flex-1 flex-col gap-2 p-2">
          <PaneHeader icon={Folder} name={names.worker} />
          <Transcript>
            <UserBubble at={MODES.task} caption={<FromChiefOfStaff />}>
              Value NVDA three ways before Q3.
            </UserBubble>
            <WorkflowCard
              name="nvda-valuation"
              title="Is NVDA worth owning into earnings?"
              {...NVDA_FLOW}
              at={MODES.flow}
              done={MODES.flowDone}
              took="6m 48s"
              tokens={412_000}
            />
            <Reply
              text="**Base case $171 a share, 6% below spot.** Only the bull case clears $182, and it needs data-center revenue to compound 40% a year to 2028."
              at={MODES.say}
            />
            <Appear at={MODES.chart}>
              <ChartCard title="DCF value per share" legend="spot $182.42">
                <BarChart
                  bars={DCF_CASES}
                  max={240}
                  median={182.42}
                  at={MODES.chart + 120}
                  per={90}
                  labelWidth={18}
                />
              </ChartCard>
            </Appear>
            <FileStack
              top={{ name: 'valuation.xlsx', kind: 'spreadsheet', icon: FileSpreadsheet }}
              under={{ name: 'nvda-earnings.pdf', kind: 'document', icon: FileText }}
              at={MODES.files[0]}
              next={MODES.files[1]}
            />
          </Transcript>
        </Region>
      </div>
    </Camera>
  );
}

/* ── chat · step 2: a Chief of Staff thread ────────────────────────────── */

/** The Chief of Staff's turn, played out under earlier quick answers, all in
    a thread the user writes to from iMessage: a request comes in from the
    phone, the work is handed to the workspace's Analyst, a line saying so;
    then the Analyst's report comes back and the Chief
    of Staff sends it on to iMessage, where the request came from, with the
    file. */
const COS = {
  app: 'imessage',
  ask: 'Build me a capex tracker before NVDA reports.',
  askAt: 1500,
  think: 1850,
  handoff: 2350,
  started: 2850,
  say: 3150,
  finished: 5400,
  report: 5650,
  send: [5950, 6650],
  delivered: 6700,
  reply: 7000,
  length: 9800,
};

/** The chat column's width (px), centered on the page as the app sets it. */
const COS_COL = 240;

/** The earlier exchange's chart: today's move in the names it was asked
    about, against the index. */
const SEMIS: Bar[] = [
  { label: 'AVGO', value: 2.3, shown: '+2.3%' },
  { label: 'NVDA', value: 1.6, shown: '+1.6%', mark: true },
  { label: 'TSM', value: 1.4, shown: '+1.4%' },
  { label: 'AMD', value: 0.9, shown: '+0.9%' },
  { label: 'MU', value: 0.6, shown: '+0.6%' },
  { label: 'ARM', value: 0.3, shown: '+0.3%' },
];

/** The whole thread first, its header naming the Chief of Staff, then down
    to the newest lines as the request comes in and the turn builds, in on
    the send to iMessage as the report goes back out, and out again. */
const COS_SHOTS: [Shot, ...Shot[]] = [
  wide(0),
  shot(1300, 200, 198, REST, 900),
  shot(6700, 200, 220, CLOSE, 800),
  shot(9000, 200, 198, REST, 700),
];

function ChiefOfStaffAnswer() {
  const { t } = useTranslation();
  const names = useAgentNames();
  const now = useSceneTime();
  const [send, sent] = COS.send;
  const via = <ViaApp app={COS.app} />;
  const app = messagingAppName(COS.app) ?? '';
  return (
    <Camera shots={COS_SHOTS}>
      <div className="flex flex-col" style={VIEW}>
        {/* the thread's header: back, its title, the agent it runs with */}
        <ThreadHeader title="Semis this week" agent={names.home} />
        <div
          className="mx-auto flex min-h-0 flex-1 flex-col gap-1.5 pb-1 pt-[18px]"
          style={{ width: COS_COL }}
        >
          {/* the transcript settles on the composer, as a thread with history
              does: the newest lines just above it, the oldest running off the
              top as they come in */}
          <Transcript settled>
            {/* earlier today, from the phone: the week ahead, then a quick
                market question, both answered here and relayed back */}
            <UserBubble at={PAST} caption={via} className="ml-12">
              Morning. What&apos;s on this week?
            </UserBubble>
            <div className="mr-12 flex flex-col gap-1.5">
              <ToolRow
                dense
                icon={Search}
                settled={{ label: t('toolArtifact.tool.webSearch'), pill: 'earnings this week' }}
              />
              <Reply
                text="**A busy week.** CPI prints Wednesday at 8:30, and NVDA reports after Thursday's close. Its guide rests on hyperscaler capex."
                at={PAST}
              />
            </div>
            <UserBubble at={PAST} caption={via} className="ml-12">
              What&apos;s moving in semis?
            </UserBubble>
            <div className="mr-12 flex flex-col gap-1.5">
              <ToolRow
                dense
                icon={Zap}
                settled={{
                  label: t('toolArtifact.tool.liveQuotes'),
                  pill: 'SOXX, AVGO, NVDA, TSM, AMD, MU',
                }}
              />
              <ChartCard title="Semis today, % change" legend="SOXX +1.2%">
                <BarChart bars={SEMIS} max={2.6} median={1.2} at={PAST} labelWidth={22} />
              </ChartCard>
              <Reply
                text="**SOXX is up 1.2%.** AVGO leads; NVDA holds above $182 into earnings."
                at={PAST}
              />
            </div>
            {/* the request, texted in rather than typed here */}
            <UserBubble at={COS.askAt} caption={via} className="ml-8">
              {COS.ask}
            </UserBubble>
            {/* the turn: reasoning, the hand-off, a line on it */}
            {now >= COS.think && (
              <div className="mr-12 flex flex-col gap-1.5">
                <ToolRow
                  dense
                  icon={Brain}
                  running={{ label: t('toolArtifact.thinkingFor', { duration: '1s' }) }}
                  settled={{ label: t('toolArtifact.thoughtFor', { duration: '1s' }) }}
                  after={
                    // centered on the line, as the app's chevron is, not
                    // sat on the label's baseline
                    <ChevronRight
                      className="h-2 w-2 shrink-0 self-center"
                      style={{ color: 'var(--color-icon-muted)' }}
                    />
                  }
                  at={COS.think}
                  done={COS.handoff}
                />
                <HandoffSlip
                  workspace="AI capex"
                  title="Build a hyperscaler capex tracker"
                  at={COS.handoff}
                  started={COS.started}
                  finished={COS.finished}
                />
                <Reply
                  text="**The AI capex Analyst is on it.** I'll send you the tracker once it's built."
                  at={COS.say}
                />
              </div>
            )}
            {/* the Analyst's report comes back, and goes on to the phone the
                request came from, the dashboard attached */}
            <ReportBack at={COS.report} workspace="AI capex" />
            {now >= send && (
              <div className="mr-12 flex flex-col gap-1.5">
                <ToolRow
                  dense
                  icon={Send}
                  running={{ label: t('toolArtifact.inProgress.sendingTo', { app }) }}
                  settled={{ label: t('toolArtifact.tool.sendMessage'), pill: app }}
                  at={send}
                  done={sent}
                />
                <Appear at={COS.delivered} pop>
                  <DeliveryPill app={COS.app} files={1} />
                </Appear>
                <Reply
                  text="**The capex tracker is on your iMessage.** Spending hit $149bn last quarter, up 55% on a year ago, and all four raised their outlook in July."
                  at={COS.reply}
                />
              </div>
            )}
          </Transcript>
          <InputDock />
        </div>
      </div>
    </Camera>
  );
}

/* ── chat · step 3: an Analyst thread running a workflow ───────────────── */

/** The META chip on the page, where the pointer clicks it. */
const PTC_CHIP = { x: PTC_CHAT + DASH_META_CHIP.x, y: DASH_META_CHIP.y };

/** The pointer comes in from the page's right edge, above the tiles, once
    the page is written, clicks the META chip, and rests at the empty end of
    the chip row so the filtered page shows whole. */
const PTC_POINTER = pointerScript(
  { at: 5900, x: VIEW.width - 8, y: PTC_CHIP.y - 18 },
  {
    chip: { ...PTC_CHIP, arrive: 6350, click: PTC.filter },
    rest: { x: PTC_CHIP.x + 36, y: PTC_CHIP.y + 2, arrive: 6900 },
  }
);

/** The camera rests on the chat as the task arrives and the workflow works
    through the calls, and holds as the file panel opens beside it. Once the
    build step writes the page it pushes in on the totals and the chart,
    then down to the calls' timeline as it fills, back out to the whole page
    for the click on META, then back to the chat for the answer and its
    files, and out to the whole page. */
const PTC_SHOTS: [Shot, ...Shot[]] = [
  shot(0, 160, 132, REST),
  shot(4100, 267, 144, CLOSE, 900),
  shot(5300, 267, 219, CLOSE, 700),
  shot(6100, 240, 179, REST, 600),
  shot(7700, 160, 198, REST, 700),
  wide(9600, 900),
];

function PtcSandbox() {
  return (
    <Camera shots={PTC_SHOTS} pointer={PTC_POINTER}>
      <CapexThread filterAt={PTC.filter} />
    </Camera>
  );
}

export const chatScenes = {
  // centered: a side-by-side comparison, so neither column may crop
  twoModes: { Scene: TwoModes, length: MODES.length, anchor: 'center' },
  chiefOfStaffAnswer: { Scene: ChiefOfStaffAnswer, length: COS.length, anchor: 'center' },
  ptcSandbox: { Scene: PtcSandbox, length: PTC.length, anchor: 'center' },
} satisfies Partial<Record<IntroVisualId, IntroScene>>;
