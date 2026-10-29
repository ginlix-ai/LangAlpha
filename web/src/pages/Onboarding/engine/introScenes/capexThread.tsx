import { FileSpreadsheet, Globe } from 'lucide-react';
import { useAgentNames } from '../introAgents';
import { ease, useSceneTime } from './clock';
import { REST, VIEW } from './camera';
import { InputDock, UserBubble } from './kit';
import { type FlowAgent, FileStack, FromChiefOfStaff, WorkflowCard } from './chatParts';
import { Reply, ThreadHeader, Transcript } from './transcriptParts';
import { DASH_BUILD, DashboardPanel } from './chatDashboard';
import { ChatHeaderButtons } from './panelParts';

/* The Analyst's thread on the capex tracker, which two scenes play: the chat
   intro's third step runs it, and the thread intro's share step opens on it
   finished. A module of its own so each intro downloads it without the
   other's scenes. */
/* eslint-disable react-refresh/only-export-components -- shared parts:
   the thread plus the script and sizes its scenes place things by */

/** The calls the Analyst's workflow reads: the four hyperscalers' last
    eight earnings calls, a quarter at a time. */
const CALLS = [
  'Oct 2024',
  'Jan 2025',
  'Apr 2025',
  'Jul 2025',
  'Oct 2025',
  'Jan 2026',
  'Apr 2026',
  'Jul 2026',
].flatMap((month) => ['MSFT', 'GOOGL', 'AMZN', 'META'].map((sym) => `${sym} ${month} call`));

/** The workflow runs eight agents at once and queues the rest. */
const FLOW_SLOTS = 8;

/** The read step: an agent per call, each sent as one of the eight running
    finishes, so the run works through the calls in waves. The times are
    the scene's; `took` is what each row reads once it settles. */
function readAgents(start: number): FlowAgent[] {
  const free = Array.from({ length: FLOW_SLOTS }, (_, i) => start + i * 50);
  return CALLS.map((label, i) => {
    const at = Math.min(...free);
    const done = at + 300 + ((i * 89) % 130);
    free[free.indexOf(at)] = done + 20;
    const secs = 52 + ((i * 37) % 70);
    const took =
      secs < 60 ? `${secs}.${(i * 7) % 10}s` : `1m ${String(secs - 60).padStart(2, '0')}s`;
    return { label, at, done, took };
  });
}

/** The task from the Chief of Staff arriving, the Analyst saying what it
    will run, the workflow reading the calls eight at a time, then building
    the dashboard as the file panel shows it written and checking it; the
    dashboard's META filter clicked; then the Analyst's answer and files. */
export const PTC = {
  task: 100,
  plan: 300,
  flow: 800,
  read: 850,
  build: 2750,
  panel: 2850,
  dash: 3050,
  check: 5450,
  flowDone: 5950,
  filter: 6400,
  say: 7400,
  files: [8200, 8400],
  length: 10000,
};

/** The capex tracker's run: read every call, build the page, check its
    figures and quotes against the sources. */
const CAPEX_FLOW = {
  phases: [
    { name: 'Read', at: PTC.read },
    { name: 'Build', at: PTC.build },
    { name: 'Check', at: PTC.check },
  ],
  agents: [
    ...readAgents(PTC.read),
    {
      label: 'Build capex-tracker.html',
      at: PTC.build,
      done: PTC.dash + DASH_BUILD,
      took: '3m 12s',
    },
    {
      label: 'Check figures and quotes',
      at: PTC.check + 50,
      done: PTC.flowDone - 100,
      took: '58.4s',
    },
  ] satisfies FlowAgent[],
  logs: [
    { text: 'reading 32 calls, 8 at a time', at: PTC.read },
    { text: 'writing capex-tracker.html', at: PTC.build },
    { text: 'checking 96 figures and 14 quotes', at: PTC.check + 50 },
  ],
};

/** The chat's width (px) once the file panel opens beside it and takes the
    rest of the page, wider than the chat, as a file preview opens. */
export const PTC_CHAT = 176;

/** The chat's width (px) before the panel opens: what the resting camera
    shows. At this width the app's centred column (max-w-3xl) fills the chat,
    so the transcript runs edge to edge in it and the agent's cards start at
    its left; the panel then slides in from the frame's right edge. */
const PTC_CHAT_START = VIEW.width / REST;

/** The Analyst's thread on the capex tracker: the chat, and the file panel
    that opens beside it on the dashboard, filtered to META at `filterAt`.
    The share scene opens on it finished and unfiltered, and draws the
    header's share button `sharing` while its popover is open and `shared`
    once the conversation is. */
export function CapexThread({
  filterAt,
  sharing = false,
  shared = false,
}: {
  filterAt?: number;
  sharing?: boolean;
  shared?: boolean;
}) {
  const names = useAgentNames();
  const now = useSceneTime();
  const open = ease(now, PTC.panel, 450);
  return (
    <div className="flex" style={VIEW}>
      {/* the chat: header, the transcript, the composer; it gives way to
            the file panel when the dashboard opens */}
      <div
        className="flex shrink-0 flex-col"
        style={{ width: PTC_CHAT_START - (PTC_CHAT_START - PTC_CHAT) * open }}
      >
        {/* the header's share and panel toggle at its right; the toggle
              moves to the panel's corner once the panel is open */}
        <ThreadHeader title="Capex tracker" agent={names.worker}>
          <ChatHeaderButtons panelOpen={now >= PTC.panel} sharing={sharing} shared={shared} />
        </ThreadHeader>
        <div className="flex min-h-0 w-full flex-1 flex-col gap-1.5 p-2.5">
          <Transcript>
            <UserBubble at={PTC.task} caption={<FromChiefOfStaff />}>
              Build a capex tracker from the hyperscaler calls.
            </UserBubble>
            {/* the Analyst's turn opens with a line saying what it will run,
                  so the run card reads as part of that turn */}
            <Reply
              text="I'll read all 32 calls, eight at a time, then build the tracker."
              at={PTC.plan}
            />
            <WorkflowCard
              name="capex-tracker"
              title="Hyperscaler capex, 32 earnings calls"
              {...CAPEX_FLOW}
              at={PTC.flow}
              done={PTC.flowDone}
              took="14m 22s"
              tokens={1_340_000}
            />
            {/* the long answer, then the files */}
            <Reply
              text="**Capex hit $149bn last quarter, up 55% on a year ago.** All four raised their outlook in July; 2026 guidance now sums to about $610bn, with GOOGL up most, to $140bn. Every figure checks against the filings."
              at={PTC.say}
            />
            <FileStack
              top={{ name: 'capex-tracker.html', kind: 'page', icon: Globe }}
              under={{ name: 'capex-calls.csv', kind: 'spreadsheet', icon: FileSpreadsheet }}
              at={PTC.files[0]}
              next={PTC.files[1]}
            />
          </Transcript>
          <InputDock delay={0.6} />
        </div>
      </div>
      {/* file panel previewing the dashboard, written as the workflow runs;
            it slides in at its full width, so the page never reflows */}
      {now >= PTC.panel && (
        <div
          className="flex min-w-0 flex-1 overflow-hidden border-l"
          style={{ background: 'var(--color-bg-page)', borderColor: 'var(--color-border-muted)' }}
        >
          <div className="flex shrink-0" style={{ width: VIEW.width - PTC_CHAT }}>
            <DashboardPanel at={PTC.dash} filterAt={filterAt} />
          </div>
        </div>
      )}
    </div>
  );
}
