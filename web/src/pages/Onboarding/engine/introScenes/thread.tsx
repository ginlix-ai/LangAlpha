import { useTranslation } from 'react-i18next';
import {
  ArrowLeft,
  BookMarked,
  BookText,
  Building2,
  CheckSquare,
  FileBarChart,
  FileText,
  Info,
  RefreshCw,
  ScrollText,
  TrendingUp,
  Upload,
} from 'lucide-react';
import { cn } from '@/lib/utils';
import { formatBytes, paddedClockTime, shortMonthDay } from '@/lib/format';
import { useLocale } from '@/hooks/useLocale';
import type { IntroVisualId } from '../../registry';
import { CLOSE, Camera, REST, type Shot, VIEW, shot } from './camera';
import { PAST, useSceneTime, useTween } from './clock';
import { Appear, Loader, UserBubble, pointAt, pointerScript } from './kit';
import { PanelTab } from './panelParts';
import { ChromeIcon, PANEL_W, ThreadPage } from './threadParts';
import { Reply, Timeline, ToolRow } from './transcriptParts';
import type { IntroScene } from './types';

/* Scenes for the thread intro's memory and memo steps: the chat column
   beside the file panel, Memory and Memo open as tabs of their own, drawn
   the way the thread page draws them. The page is drawn whole and a camera
   frames the part each beat is about, resting where a pane reads whole.
   Labels are the app's own strings. The file panel step lives in
   `threadFiles`. */
/* eslint-disable react-refresh/only-export-components -- scene module:
   private scene components, one record export */

/* ── thread · step 2: long-term memory ─────────────────────────────────── */

/** "Today" on the scene's clock: when a file saved in the scene changed. */
const SAVED_AT = new Date(2026, 9, 21, 14, 5);

/** A standing preference, not a fact about a trade: under an answer that
    leaned on the Street's targets, the user asks the agent to remember how
    they want write-ups done. The agent writes it to its own topic file in
    user memory and adds a line for it to the index, `memory.md`, then says
    what changes; the Memory tab lists the new file and shows the index as
    changed today. The pointer opens the file to show what was kept.
    Positions are px on the thread page: report_style.md is listed fourth
    once it lands. The thread is sized so that, once the turn is in, its top
    edge falls between two messages. */
const MEM = {
  /** The thread so far, the oldest exchange first: it runs off the top as
      the new turn comes in. */
  close: {
    ask: 'How did AVGO close today?',
    answer: '**$349.80**, up 1.9% on the day, on volume a third above its average.',
  },
  date: { ask: 'When does AVGO report?', answer: '**Thursday, Dec 10**, after the close.' },
  earlier: 'Is AVGO worth adding before earnings?',
  street:
    "**The Street's average target is $410**, 17% above spot, and 31 of 38 analysts " +
    'rate it a buy. Most expect AI revenue to beat again, and two brokers raised their ' +
    'targets this week.',
  ask: 'Remember: bottom line first, one page, no sell-side targets.',
  typeAsk: 250,
  cps: 30,
  sendAsk: 2550,
  save: 2850,
  saved: 3400,
  /** The index edit that follows the write. */
  index: 3450,
  indexed: 3850,
  file: 'report_style.md',
  /** What the app calls the file: its name with underscores as spaces. */
  topic: 'report style',
  reply:
    'Saved. Every write-up will open with the call, fit on a page, and leave Street targets out.',
  replyAt: 4000,
  listed: 5500,
  click: 6600,
  open: 6680,
  length: 9400,
};

/** To the new file's row in the listing and a click on it, then out of
    the way to the page's corner. */
const MEM_POINTER = pointerScript(
  { at: 5900, x: 352, y: 236 },
  {
    file: { x: 252, y: 166, arrive: 6500, click: MEM.click, leave: 6800 },
    rest: { x: 372, y: 300, arrive: 7400 },
  }
);

/** The camera: on the chat with the composer and the answer above it as
    the ask is typed, held there as the save lands and the reply comes in;
    across to the Memory tab as its listing picks up the new file; then in
    on the file once it opens. */
const MEM_SHOTS: [Shot, ...Shot[]] = [
  shot(0, 160, 198, REST),
  shot(5700, 240, 132, REST, 800),
  shot(7600, 267, 110, CLOSE, 800),
];

/** One file in the memory list: its name, then size and when it changed
    (the time of day when that was today). */
function MemoryFile({
  name,
  bytes,
  modified,
  today = false,
  hover = false,
}: {
  name: string;
  bytes: number;
  modified: Date;
  today?: boolean;
  hover?: boolean;
}) {
  const locale = useLocale();
  const when = today ? paddedClockTime(modified, locale) : shortMonthDay(modified, locale);
  return (
    <div
      className="flex min-w-0 items-center gap-1.5 px-1.5 py-1.5"
      style={{ background: hover ? 'var(--color-bg-hover)' : undefined }}
    >
      <FileText
        className="h-[9px] w-[9px] shrink-0"
        style={{ color: 'var(--color-text-tertiary)' }}
      />
      <span className="flex min-w-0 flex-col gap-[3px]">
        <span
          className="truncate text-[8px] leading-none"
          style={{ color: 'var(--color-text-primary)' }}
        >
          {name}
        </span>
        <span className="text-[6px] leading-none" style={{ color: 'var(--color-text-tertiary)' }}>
          {formatBytes(bytes, locale)} · {when}
        </span>
      </span>
    </div>
  );
}

/** The Memory tab's list (`MemoryPanel`): the User / Workspace switch, the
    folder it lists, and the files in it, memory.md first and the rest A to
    Z. Once the save lands the new file takes its place in that order and
    the index reads as changed today. */
function MemoryList() {
  const { t } = useTranslation();
  const now = useSceneTime();
  const saved = now >= MEM.listed;
  const tiers = [t('memoryPanel.tierUser'), t('memoryPanel.tierWorkspace')];
  return (
    <>
      <div
        className="flex items-center justify-between border-b px-1.5 py-1"
        style={{ borderColor: 'var(--color-border-muted)' }}
      >
        <span
          className="flex gap-0.5 rounded-[4px] p-[1.5px]"
          style={{ background: 'var(--color-border-muted)' }}
        >
          {tiers.map((tier, i) => (
            <span
              key={tier}
              className="rounded-[3px] px-1.5 py-[3px] text-[7px] font-medium leading-none"
              style={
                i === 0
                  ? { background: 'var(--color-bg-page)', color: 'var(--color-text-primary)' }
                  : { color: 'var(--color-text-tertiary)' }
              }
            >
              {tier}
            </span>
          ))}
        </span>
        <ChromeIcon icon={RefreshCw} />
      </div>
      <span
        className="px-1.5 pb-0.5 pt-1.5 text-[6px] leading-none"
        style={{ color: 'var(--color-text-tertiary)' }}
      >
        .agents/user/memory/
      </span>
      <MemoryFile
        name="memory.md"
        bytes={saved ? 1342 : 1108}
        modified={saved ? SAVED_AT : new Date(2026, 9, 9)}
        today={saved}
      />
      <MemoryFile name="data_sources.md" bytes={486} modified={new Date(2026, 8, 30)} />
      <MemoryFile name="investor_profile.md" bytes={912} modified={new Date(2026, 7, 14)} />
      <Appear at={MEM.listed}>
        <MemoryFile
          name={MEM.file}
          bytes={418}
          modified={SAVED_AT}
          today
          hover={now >= MEM_POINTER.beats.file.arrive}
        />
      </Appear>
      <MemoryFile name="trade_preferences.md" bytes={655} modified={new Date(2026, 8, 12)} />
    </>
  );
}

/** A memory file open in the Memory tab: back, its path, then the file as
    Markdown, its frontmatter left out as the app leaves it out. The body is
    the rule first, then why it holds. */
function MemoryViewer() {
  const rules = [
    'Lead with the bottom line, then the evidence.',
    'One page at most.',
    'No sell-side price targets. Give your own view.',
  ];
  return (
    <Appear at={MEM.open} className="flex flex-1 flex-col">
      <div
        className="flex items-center gap-1 border-b px-1.5 py-[5px]"
        style={{ borderColor: 'var(--color-border-muted)' }}
      >
        <ArrowLeft className="h-2 w-2 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
        <span
          className="truncate text-[7px] font-semibold leading-none"
          style={{ color: 'var(--color-text-primary)' }}
        >
          .agents/user/memory/{MEM.file}
        </span>
      </div>
      <div className="flex flex-col gap-2 px-2.5 py-2.5">
        <span
          className="text-[12px] font-semibold leading-none"
          style={{ color: 'var(--color-text-primary)' }}
        >
          Report style
        </span>
        <span className="text-[8px] leading-snug" style={{ color: 'var(--color-text-secondary)' }}>
          How every write-up should read.
        </span>
        <ul className="flex flex-col gap-1.5 pl-3">
          {rules.map((rule) => (
            <li
              key={rule}
              className="list-disc text-[8px] leading-snug marker:text-[var(--color-text-tertiary)]"
              style={{ color: 'var(--color-text-secondary)' }}
            >
              {rule}
            </li>
          ))}
        </ul>
        <span className="text-[8px] leading-snug" style={{ color: 'var(--color-text-secondary)' }}>
          <b className="font-semibold" style={{ color: 'var(--color-text-primary)' }}>
            Why:
          </b>{' '}
          wants a judgement, not the Street&apos;s consensus.
        </span>
      </div>
    </Appear>
  );
}

function Memory() {
  const t = useSceneTime();
  const { t: tr } = useTranslation();
  const overview = tr('toolArtifact.tool.companyOverview');
  return (
    <Camera shots={MEM_SHOTS} pointer={MEM_POINTER}>
      <ThreadPage
        title="AVGO earnings"
        stack="bottom"
        tabs={
          <>
            <PanelTab icon={FileText} label="notes.md" />
            <PanelTab icon={BookMarked} label={tr('filePanel.tabs.memory')} active />
          </>
        }
        ask={{ text: MEM.ask, at: MEM.typeAsk, send: MEM.sendAsk, cps: MEM.cps }}
        chat={
          <>
            <UserBubble at={PAST}>{MEM.close.ask}</UserBubble>
            <Reply text={MEM.close.answer} />
            <UserBubble at={PAST}>{MEM.date.ask}</UserBubble>
            <Reply text={MEM.date.answer} />
            <UserBubble at={PAST}>{MEM.earlier}</UserBubble>
            <Timeline>
              <ToolRow icon={Building2} settled={{ label: overview, pill: 'AVGO' }} />
            </Timeline>
            <Reply text={MEM.street} />
            <UserBubble at={MEM.sendAsk + 80} wrap>
              {MEM.ask}
            </UserBubble>
            <Timeline>
              <ToolRow
                icon={BookMarked}
                running={{
                  label: tr('toolArtifact.inProgress.addingMemoryTopic', { topic: MEM.topic }),
                }}
                settled={{ label: tr('toolArtifact.completed.addedMemory'), pill: MEM.topic }}
                at={MEM.save}
                done={MEM.saved}
              />
              {/* the index names no topic, so its row carries no pill */}
              <ToolRow
                icon={BookMarked}
                running={{ label: tr('toolArtifact.inProgress.updatingMemory') }}
                settled={{ label: tr('toolArtifact.completed.updatedMemory') }}
                at={MEM.index}
                done={MEM.indexed}
              />
            </Timeline>
            <Reply text={MEM.reply} at={MEM.replyAt} cps={110} />
          </>
        }
        panel={t >= MEM.open ? <MemoryViewer /> : <MemoryList />}
      />
    </Camera>
  );
}

/* ── thread · step 3: memos ────────────────────────────────────────────── */

/** The user's own thesis, dragged in from Finder and dropped on the Memo
    tab below the memos already there: the drop overlay takes the drag,
    Upload reads "Uploading…", then the memo is listed generating its summary
    and turns Ready with its description. Then a question typed under the
    thread's last exchange: the agent reads the memo, fetches the filing it
    is checked against on its own, and answers with one point backed and one
    weaker. Positions are px on the thread page; the overlay shows while the
    pointer is over the panel, and the drag holds above the overlay's label
    so both read. */
const MEMO = {
  drop: 1500,
  uploaded: 2300,
  /** Listed oldest first, as the tab sorts them. */
  held: [
    {
      name: 'semis-valuation-framework.md',
      description: 'How I value semis: EV/Sales against growth, margins through the cycle',
    },
    {
      name: 'expert-call-notes.md',
      description: 'Notes from a call with a former hyperscaler procurement lead',
    },
  ],
  upload: {
    name: 'ai-capex-thesis.md',
    at: 2300,
    ready: 3200,
    description:
      'Thesis, Aug 2026: hyperscaler capex keeps rising through 2027; power is the constraint',
  },
  /** The thread so far, the oldest exchange first: what the Street
      expected, then how the quarter came in against it. The oldest lines
      run off the top as the new turn comes in. */
  consensus: {
    ask: 'What does the Street expect from MSFT this quarter?',
    answer:
      'Consensus has revenue at **$75.9B**, up 14%, and EPS of $3.68. Azure is ' +
      'expected to grow 37%, and the stock is up 9% since the last print.',
  },
  earlier: {
    ask: "How did MSFT's quarter land?",
    answer: 'Revenue of **$77.4B**, up 16%. Azure grew 39%.',
  },
  ask: 'Does their capex guide change my thesis?',
  typeAsk: 4500,
  cps: 30,
  askAt: 6150,
  readMemo: { at: 6500, done: 6900 },
  filing: { at: 6950, done: 7550 },
  answer:
    'It backs point 2 of your thesis: capex was **$41.2B** for the quarter, and they now ' +
    'expect FY27 spend to grow faster than FY26. Point 3 looks weaker. The 10-Q flags ' +
    'power and site delays, the constraint you expected to ease by mid-2027.',
  answerAt: 7700,
  cite: 'From your memo `ai-capex-thesis.md` and the MSFT 10-Q.',
  citeAt: 9300,
  length: 11200,
};

/** The drag from Finder onto the Memo tab, held there over the drop, then
    the empty pointer off the page's foot. */
const MEMO_POINTER = pointerScript(
  { at: 300, x: 118, y: 110 },
  {
    drop: { x: 300, y: 96, arrive: 1300, leave: 1650 },
    off: { x: 350, y: 380, arrive: 2300 },
  }
);

/** The camera: on the Memo tab as the drag comes over it, held while the
    drop uploads and the memo turns Ready; across to the chat as the
    question is typed and the two reads come in; then in on the answer. */
const MEMO_SHOTS: [Shot, ...Shot[]] = [
  shot(0, 240, 132, REST),
  shot(4700, 160, 198, REST, 900),
  shot(9900, 94, 190, CLOSE, 1000),
];

/** What the OS drags out of Finder: the file's icon, marked with its kind,
    and its name. It shrinks into the drop. */
function DragChip() {
  const t = useSceneTime();
  const sink = useTween(MEMO.drop, 240);
  if (t < MEMO_POINTER.enter || sink >= 1) return null;
  const { x, y } = pointAt(MEMO_POINTER.path, t);
  return (
    <div
      className="pointer-events-none absolute z-[9] flex flex-col items-center gap-[3px]"
      style={{
        left: x + 9,
        top: y + 10,
        opacity: 1 - sink,
        transform: `translateX(-50%) scale(${1 - 0.25 * sink})`,
      }}
    >
      <span
        className={cn(
          'flex h-[18px] w-[14px] items-end justify-center',
          'rounded-[1.5px] border pb-[2px]'
        )}
        style={{ background: '#fff', borderColor: 'rgba(0, 0, 0, 0.2)' }}
      >
        <span className="text-[4px] font-semibold leading-none" style={{ color: '#8a8a8a' }}>
          MD
        </span>
      </span>
      <span
        className={cn(
          'whitespace-nowrap rounded-[2px] px-[3px] py-[1.5px]',
          'text-[6px] leading-none text-white'
        )}
        style={{ background: '#0a64d8' }}
      >
        {MEMO.upload.name}
      </span>
    </div>
  );
}

/** A memo's status pill: generating with the loader, then Ready. */
function MemoStatus({ ready }: { ready: number }) {
  const t = useSceneTime();
  const { t: tr } = useTranslation();
  const pill =
    'inline-flex shrink-0 whitespace-nowrap rounded-full px-1 py-[2px] text-[6px] font-medium leading-none';
  if (t < ready) {
    return (
      <span
        className={cn(pill, 'items-center gap-0.5')}
        style={{ background: 'var(--color-border-muted)', color: 'var(--color-text-tertiary)' }}
      >
        <Loader />
        {tr('memoPanel.status.pending')}
      </span>
    );
  }
  return (
    <Appear at={ready} pop className="flex">
      <span
        className={pill}
        style={{ background: 'var(--color-accent-soft)', color: 'var(--color-accent-primary)' }}
      >
        {tr('memoPanel.status.ready')}
      </span>
    </Appear>
  );
}

/* the columns a narrow Memo tab keeps: name, status, description. Status
   is as wide as a Ready pill; a memo still generating has no description
   yet, so its wider pill runs into the empty cell beside it. */
const COL_NAME = 'flex w-[88px] min-w-0 shrink-0 items-center gap-1 pl-1.5';
const COL_STATUS = 'flex w-[34px] shrink-0 px-1';

/** One memo in the table, listed from `at`; its description, the summary
    the agent writes of it, fills in once it is ready. A narrow tab gives the
    description four lines where a wide one gives it two. */
function MemoRow({
  name,
  at,
  ready,
  description,
}: {
  name: string;
  at: number;
  ready: number;
  description: string;
}) {
  return (
    <Appear at={at} className="flex items-center py-1">
      <span className={COL_NAME}>
        <FileText className="h-2 w-2 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
        <span
          className="truncate text-[7px] leading-none"
          style={{ color: 'var(--color-text-primary)' }}
        >
          {name}
        </span>
      </span>
      <span className={COL_STATUS}>
        <MemoStatus ready={ready} />
      </span>
      <Appear
        at={ready}
        className="line-clamp-4 min-w-0 flex-1 pr-1.5 text-[6px] leading-[1.35]"
        style={{ color: 'var(--color-text-secondary)' }}
      >
        {description}
      </Appear>
    </Appear>
  );
}

/** The Memo tab: its title bar with Upload, the table, and the drop overlay
    while a drag is over it. */
function MemoTab() {
  const t = useSceneTime();
  const { t: tr } = useTranslation();
  const over =
    t < MEMO.drop &&
    t >= MEMO_POINTER.enter &&
    pointAt(MEMO_POINTER.path, t).x >= VIEW.width - PANEL_W;
  const uploading = t >= MEMO.drop && t < MEMO.uploaded;
  return (
    <>
      <div
        className="flex items-center justify-between border-b px-1.5 py-1"
        style={{ borderColor: 'var(--color-border-muted)' }}
      >
        <span className="flex items-center gap-1">
          <span
            className="text-[8px] font-semibold leading-none"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {tr('memoPanel.title')}
          </span>
          <Info className="h-[7px] w-[7px]" style={{ color: 'var(--color-text-tertiary)' }} />
        </span>
        <span className="flex items-center gap-1.5">
          <ChromeIcon icon={RefreshCw} />
          <ChromeIcon icon={CheckSquare} />
          <span
            className={cn(
              'flex items-center gap-1 rounded-[4px] px-1.5 py-[3px]',
              'text-[7px] font-medium leading-none'
            )}
            style={{
              background: 'var(--color-btn-primary-bg)',
              color: 'var(--color-btn-primary-text)',
            }}
          >
            {uploading ? <Loader /> : <Upload className="h-[7px] w-[7px] shrink-0" />}
            {uploading ? tr('memoPanel.uploading') : tr('memoPanel.uploadButton')}
          </span>
        </span>
      </div>
      <div className="relative flex flex-1 flex-col">
        <div
          className="flex py-1 text-[7px] font-medium leading-none"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          <span className={COL_NAME}>{tr('memoPanel.columns.name')}</span>
          <span className={COL_STATUS}>{tr('memoPanel.columns.status')}</span>
          <span>{tr('memoPanel.columns.description')}</span>
        </div>
        {MEMO.held.map((r) => (
          <MemoRow key={r.name} {...r} at={PAST} ready={PAST} />
        ))}
        <MemoRow {...MEMO.upload} />
        {over && (
          <div
            className={cn(
              'absolute inset-1 z-[5] flex flex-col items-center justify-center gap-1',
              'rounded-[5px] border-[1.5px] border-dashed text-[8px] font-medium leading-none'
            )}
            style={{
              background: 'var(--color-bg-overlay-strong)',
              borderColor: 'var(--color-accent-overlay)',
              color: 'var(--color-text-tertiary)',
            }}
          >
            <Upload className="h-4 w-4" style={{ color: 'var(--color-accent-primary)' }} />
            {tr('memoPanel.dragOverlayText')}
          </div>
        )}
      </div>
    </>
  );
}

function Memo() {
  const { t: tr } = useTranslation();
  const overview = tr('toolArtifact.tool.companyOverview');
  const prices = tr('toolArtifact.tool.stockPrices');
  const statements = tr('toolArtifact.tool.financialStatements');
  const filing = tr('toolArtifact.tool.secFiling');
  const memo = MEMO.upload.name;
  return (
    <Camera shots={MEMO_SHOTS} pointer={MEMO_POINTER}>
      <ThreadPage
        tabs={
          <>
            <PanelTab icon={BookMarked} label={tr('filePanel.tabs.memory')} />
            <PanelTab icon={ScrollText} label={tr('filePanel.tabs.memo')} active />
          </>
        }
        title="MSFT capex"
        stack="bottom"
        chat={
          <>
            <UserBubble at={PAST} wrap>
              {MEMO.consensus.ask}
            </UserBubble>
            <Timeline>
              <ToolRow icon={Building2} settled={{ label: overview, pill: 'MSFT' }} />
              <ToolRow icon={TrendingUp} settled={{ label: prices, pill: 'MSFT' }} />
            </Timeline>
            <Reply text={MEMO.consensus.answer} />
            <UserBubble at={PAST}>{MEMO.earlier.ask}</UserBubble>
            <Timeline>
              <ToolRow icon={FileBarChart} settled={{ label: statements, pill: 'MSFT' }} />
            </Timeline>
            <Reply text={MEMO.earlier.answer} />
            <UserBubble at={MEMO.askAt} wrap>
              {MEMO.ask}
            </UserBubble>
            <Timeline>
              <ToolRow
                icon={BookText}
                running={{ label: tr('toolArtifact.inProgress.readingMemoSlug', { slug: memo }) }}
                settled={{ label: tr('toolArtifact.completed.readMemo'), pill: memo }}
                at={MEMO.readMemo.at}
                done={MEMO.readMemo.done}
              />
              {/* the filing comes from the platform, not the user: a running
                  call reads as its name and what it fetches, as the app's
                  live label joins them, and settles on the symbol */}
              <ToolRow
                icon={FileBarChart}
                running={{
                  label: `${filing} ${tr('toolArtifact.inProgress.fetchingSymbolFiling', { symbol: 'MSFT' })}`,
                }}
                settled={{ label: filing, pill: 'MSFT' }}
                at={MEMO.filing.at}
                done={MEMO.filing.done}
              />
            </Timeline>
            <Reply text={MEMO.answer} at={MEMO.answerAt} cps={170} />
            <Reply text={MEMO.cite} at={MEMO.citeAt} cps={110} />
          </>
        }
        panel={<MemoTab />}
        ask={{ text: MEMO.ask, at: MEMO.typeAsk, send: MEMO.askAt, cps: MEMO.cps }}
      />
      <DragChip />
    </Camera>
  );
}

export const threadScenes = {
  memory: { Scene: Memory, length: MEM.length, anchor: 'right' },
  memo: { Scene: Memo, length: MEMO.length, anchor: 'right' },
} satisfies Partial<Record<IntroVisualId, IntroScene>>;
