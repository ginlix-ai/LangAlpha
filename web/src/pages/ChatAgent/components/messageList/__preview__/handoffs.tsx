/* eslint-disable react-refresh/only-export-components -- entry file, not a module */
import { StrictMode, useEffect, useState, type ReactNode } from 'react';
import { createRoot } from 'react-dom/client';
import { MemoryRouter, Route, Routes, useLocation, useNavigate } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import i18n, { initI18n } from '@/i18n';
import { switchLocale } from '@/lib/locale';
import { ThemeProvider, useTheme } from '@/contexts/ThemeContext';
import '@/index.css';
import type { MessageProvenance } from '@/types/chat';
import { DispatchStatusProvider } from '@/pages/ChatAgent/hooks/usePTCDispatchStatus';
import type { DispatchLiveness, DispatchRunLiveness } from '@/pages/ChatAgent/utils/api';
import { HandoffApproval } from '../../analystHandoff/HandoffApproval';
import { HandoffDeclined } from '../../analystHandoff/HandoffDeclined';
import { HandoffGroup } from '../../analystHandoff/HandoffGroup';
import { HandoffSlip } from '../../analystHandoff/HandoffSlip';
import type { HandoffPhase, HandoffView, ProposalData } from '../../analystHandoff/handoffModel';
import { HandoffPlacesContext, type HandoffPlace } from '../../analystHandoff/reportLinks';
import { AnalystHandoffRun, type AnalystHandoffMember } from '../AnalystHandoffRun';
import { MessageActionsProvider } from '../MessageActionsContext';
import { ReportRow } from '../ReportRow';

/**
 * Dev-only harness for the Chief of Staff's hand-offs to workspace analysts:
 * every slip state, the approval card, a declined row, a group, the same in a
 * narrow column, and the report rows, drawn from fixtures; then the same parts
 * wired to a stubbed liveness read, as a reply puts them, and a run of cards
 * to answer by hand, to watch each answer resolve in place. Open
 * /handoffs-preview.html on the dev server.
 *
 *   ?theme=dark&locale=zh-CN&bar=0
 */

const MIN = 60_000;
const NOW = Date.now();
const noop = () => {};

const BRIEF_SHORT = 'Refresh the NVDA DCF with the Q3 data-center margin and say what moves the target.';
const BRIEF_LONG = [
  '## Goal',
  'Rebuild the **NVDA** discounted cash flow model with the Q3 FY2027 results, then explain what moved the per-share value since the last version.',
  '',
  '## Inputs',
  '- The 10-Q filed last week and the earnings call transcript',
  '- The existing model in `models/nvda_dcf.xlsx`',
  '- Consensus revisions since the print',
  '',
  '## Deliverable',
  'A one-page summary with the new target, a bridge from the old one, and the three assumptions the answer is most sensitive to.',
].join('\n');

// --- Gallery fixtures: one view per state, no reads ------------------------

const GALLERY_REPORT = 'msg-gallery-report';

// The host's `onRevealMessage`, minus the transcript scroller this page lacks.
function revealMessage(id: string): boolean {
  const el = document.querySelector<HTMLElement>(`[data-message-id="${id}"]`);
  if (!el) return false;
  el.scrollIntoView({ block: 'center', behavior: 'smooth' });
  el.focus({ preventScroll: true });
  return true;
}
const REVEAL = { onRevealMessage: revealMessage };
const seeGalleryReport = () => {
  revealMessage(GALLERY_REPORT);
};

function view(phase: HandoffPhase, over: Partial<HandoffView> = {}): HandoffView {
  return {
    id: `${phase}:${over.task ?? ''}`,
    phase,
    task: 'NVDA DCF refresh after Q3 data-center margin',
    workspaceName: 'NVDA Deep Dive',
    steered: false,
    read: true,
    onOpen: noop,
    ...over,
  };
}

const SLIPS: Array<{ label: string; view: HandoffView }> = [
  { label: 'starting', view: view('starting') },
  { label: 'working', view: view('working', { startedAt: NOW - 4 * MIN - 12_000 }) },
  {
    label: 'working, a task too long for the line',
    view: view('working', {
      startedAt: NOW - 38 * MIN,
      task: 'Rebuild the NVDA discounted cash flow model with the Q3 FY2027 results and bridge the per-share value from the last version',
    }),
  },
  { label: 'steered into a run already going', view: view('working', { startedAt: NOW - 6 * MIN, steered: true }) },
  { label: 'needs your answer', view: view('needs_input', { task: 'Which peer set should the comps use?', workspaceName: 'Semis Comps' }) },
  {
    label: 'finished, reported back',
    view: view('finished', {
      startedAt: NOW - 19 * MIN,
      finishedAt: NOW - MIN,
      reportId: GALLERY_REPORT,
      onSeeReport: seeGalleryReport,
    }),
  },
  { label: 'finished, no report', view: view('finished', { startedAt: NOW - 12 * MIN, finishedAt: NOW - MIN }) },
  { label: "couldn't finish", view: view('failed', { workspaceName: 'Macro Rates', task: 'Rebuild the rates curve from the Fed dot plot' }) },
  { label: 'stopped', view: view('stopped', { startedAt: NOW - 7 * MIN, finishedAt: NOW - MIN }) },
  { label: "couldn't start", view: view('not_started', { onOpen: undefined, task: 'Screen small-cap biotech for upcoming PDUFA dates', workspaceName: 'Biotech Catalysts' }) },
];

// The finished run's report is in, but a group links its reports only once
// every run has one, so it waits on the other two.
const GROUP: HandoffView[] = [
  view('working', { task: 'NVDA DCF refresh after Q3 data-center margin', workspaceName: 'NVDA Deep Dive', startedAt: NOW - 4 * MIN - 12_000 }),
  view('working', { task: 'AMD vs NVDA accelerator share, 2025 to 2027', workspaceName: 'Semis Comps', startedAt: NOW - 2 * MIN - 40_000, steered: true }),
  view('finished', {
    task: 'TSMC CoWoS capacity and what it caps',
    workspaceName: 'Supply Chain',
    startedAt: NOW - 18 * MIN,
    finishedAt: NOW - MIN,
    reportId: GALLERY_REPORT,
  }),
];

// Where a group comes to rest, and so how it reads most often.
const SETTLED_GROUP: HandoffView[] = [
  view('finished', { startedAt: NOW - 18 * MIN, finishedAt: NOW - MIN }),
  view('finished', { task: 'AMD vs NVDA accelerator share, 2025 to 2027', workspaceName: 'Semis Comps', startedAt: NOW - 9 * MIN, finishedAt: NOW - MIN }),
  view('finished', { task: 'TSMC CoWoS capacity and what it caps', workspaceName: 'Supply Chain', startedAt: NOW - 25 * MIN, finishedAt: NOW - MIN }),
];

// Every glyph the column can hold, led on the header by the one waiting on the
// reader.
const MIXED_GROUP: HandoffView[] = [
  view('needs_input', { task: 'Which peer set should the comps use?', workspaceName: 'Semis Comps' }),
  view('starting', { task: 'Screen small-cap biotech for upcoming PDUFA dates', workspaceName: 'Biotech Catalysts' }),
  view('failed', { task: 'Rebuild the rates curve from the Fed dot plot', workspaceName: 'Macro Rates' }),
  view('stopped', { task: 'TSMC CoWoS capacity and what it caps', workspaceName: 'Supply Chain', startedAt: NOW - 7 * MIN, finishedAt: NOW - MIN }),
  view('finished', { startedAt: NOW - 18 * MIN, finishedAt: NOW - MIN }),
];

// What a narrow column (a side panel open, a phone) keeps of each.
const NARROW = SLIPS.filter((s) =>
  ['working, a task too long for the line', 'needs your answer', 'reported back', "couldn't finish", 'stopped'].includes(s.label),
);

const PENDING_LONG: ProposalData = { status: 'pending', question: BRIEF_LONG, workspace_name: 'NVDA Deep Dive', report_back: true };
const PENDING_SHORT: ProposalData = { status: 'pending', question: BRIEF_SHORT, workspace_name: 'Semis Comps', report_back: true };
const DECLINED: ProposalData = { status: 'rejected', question: BRIEF_LONG, workspace_name: 'NVDA Deep Dive' };

type AnalystReport = Extract<MessageProvenance, { kind: 'analyst_report' }>;
const report = (over: Partial<AnalystReport>): AnalystReport => ({
  kind: 'analyst_report',
  threadId: 'th-report',
  workspaceId: 'ws-nvda',
  workspaceName: 'NVDA Deep Dive',
  ...over,
});

const REPORTS: Array<{ label: string; id: string; report: AnalystReport }> = [
  { label: 'reported back', id: GALLERY_REPORT, report: report({ outcome: 'completed' }) },
  { label: "couldn't finish", id: 'msg-error', report: report({ outcome: 'error' }) },
  { label: 'stopped', id: 'msg-stopped', report: report({ outcome: 'cancelled' }) },
  { label: 'before outcomes (legacy)', id: 'msg-legacy', report: report({}) },
  { label: 'no workspace name', id: 'msg-unnamed', report: report({ workspaceName: null, outcome: 'completed' }) },
];

// --- Wired: real hand-off run over a stubbed liveness read -----------------

const iso = (ms: number) => new Date(ms).toISOString();

const LIVENESS: Record<string, Omit<DispatchLiveness, 'thread_id' | 'run_id' | 'can_reconnect'>> = {
  'th-a': { status: 'running', run_started_at: iso(NOW - 4 * MIN - 12_000), title: 'NVDA DCF refresh after Q3 data-center margin', workspace_name: 'NVDA Deep Dive' },
  'th-b': { status: 'running', run_started_at: iso(NOW - 2 * MIN - 40_000), title: 'AMD vs NVDA accelerator share, 2025 to 2027', workspace_name: 'Semis Comps' },
  'th-c': {
    status: 'completed',
    run_started_at: iso(NOW - 19 * MIN),
    run_finished_at: iso(NOW - MIN),
    title: 'TSMC CoWoS capacity and what it caps',
    workspace_name: 'Supply Chain',
  },
  'th-d': { status: 'interrupted', run_started_at: iso(NOW - 6 * MIN), title: 'Which peer set should the comps use?', workspace_name: 'Semis Comps' },
  // Handed work twice: the first run ended, and a later turn is running now.
  'th-e': { status: 'running', run_started_at: iso(NOW - 90_000), title: 'NVDA supply checks', workspace_name: 'NVDA Deep Dive' },
  // Its latest run is older than the hand-off whose outcome is unknown.
  'th-f': {
    status: 'completed',
    run_started_at: iso(NOW - 40 * MIN),
    run_finished_at: iso(NOW - 31 * MIN),
    title: 'Biotech catalysts',
    workspace_name: 'Biotech Catalysts',
  },
};

// Each hand-off's own run, as the dispatch named it.
const RUNS: Record<string, Omit<DispatchRunLiveness, 'run_id'>> = {
  'run-e1': { thread_id: 'th-e', status: 'completed', run_started_at: iso(NOW - 9 * MIN), run_finished_at: iso(NOW - 9 * MIN + 7_500) },
  'run-e2': { thread_id: 'th-e', status: 'running', run_started_at: iso(NOW - 90_000) },
};

const realFetch = window.fetch.bind(window);
window.fetch = async (input, init) => {
  const href = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
  const url = new URL(href, window.location.origin);
  if (!url.pathname.startsWith('/api/')) return realFetch(input, init);
  let body: unknown = {};
  if (url.pathname === '/api/v1/threads/dispatches/liveness') {
    const ids = (url.searchParams.get('ids') ?? '').split(',').filter(Boolean);
    const runs = (url.searchParams.get('runs') ?? '').split(',').filter(Boolean);
    body = {
      liveness: ids
        .filter((id) => id in LIVENESS)
        .map((id) => ({ thread_id: id, run_id: `run-${id}`, can_reconnect: false, ...LIVENESS[id] })),
      runs: runs.filter((id) => id in RUNS).map((id) => ({ run_id: id, ...RUNS[id] })),
    };
  }
  await new Promise((r) => setTimeout(r, 200));
  return new Response(JSON.stringify(body), { status: 200, headers: { 'Content-Type': 'application/json' } });
};

const approved = (key: string, threadId: string | undefined, workspace: string, extra: Partial<ProposalData> = {}): AnalystHandoffMember => ({
  key,
  proposalId: `p-${key}`,
  proposal: { status: 'approved', question: BRIEF_SHORT, thread_id: threadId, workspace_name: workspace, workspace_id: `ws-${key}`, tool_call_id: `tc-${key}`, ...extra },
});

const RUN_GROUP: AnalystHandoffMember[] = [
  approved('a', 'th-a', 'NVDA Deep Dive'),
  approved('b', 'th-b', 'Semis Comps'),
  approved('c', 'th-c', 'Supply Chain'),
  {
    key: 'p',
    proposalId: 'p-p',
    proposal: { status: 'pending', question: BRIEF_LONG, workspace_name: 'Macro Rates', report_back: true, interruptId: 'i-p' },
  },
];
const RUN_SINGLE: AnalystHandoffMember[] = [approved('d', 'th-d', 'Semis Comps')];
const RUN_FAILED: AnalystHandoffMember[] = [approved('x', undefined, 'Biotech Catalysts', { dispatch_failed: true })];
// Two hand-offs to one thread: each slip reads its own run, not the latest.
const RUN_PINNED: AnalystHandoffMember[] = [
  approved('e1', 'th-e', 'NVDA Deep Dive', { run_id: 'run-e1' }),
  approved('e2', 'th-e', 'NVDA Deep Dive', { run_id: 'run-e2' }),
];
const RUN_UNKNOWN: AnalystHandoffMember[] = [approved('f', 'th-f', 'Biotech Catalysts', { dispatch_failed: true, dispatch_unknown: true })];
const WIRED_REPORT = 'msg-report-c';

// Four cards to answer by hand. Approved, each reads one of the stubbed
// threads, so the slip it becomes has a run to show.
const ASKING: AnalystHandoffMember[] = [
  ['q1', 'NVDA Deep Dive'],
  ['q2', 'Semis Comps'],
  ['q3', 'Supply Chain'],
  ['q4', 'Macro Rates'],
].map(([key, workspace]) => ({
  key,
  proposalId: `p-${key}`,
  proposal: {
    status: 'pending',
    question: BRIEF_LONG,
    workspace_name: workspace,
    workspace_id: `ws-${key}`,
    tool_call_id: `tc-${key}`,
    report_back: true,
    interruptId: `i-${key}`,
  },
}));
const ASKING_THREADS: Record<string, string> = Object.fromEntries(
  [['p-q1', 'th-a'], ['p-q2', 'th-b'], ['p-q3', 'th-c'], ['p-q4', 'th-d']],
);
const WIRED_PLACES: ReadonlyMap<string, HandoffPlace> = new Map([
  ['tc-c', { reportId: WIRED_REPORT }],
  ['tc-f', { madeAt: NOW - 2 * MIN }],
]);

// --- Harness -------------------------------------------------------------------

interface Knobs {
  theme: 'light' | 'dark';
  locale: 'en-US' | 'zh-CN';
  bar: boolean;
}

function readKnobs(): Knobs {
  const p = new URLSearchParams(window.location.search);
  return {
    theme: p.get('theme') === 'dark' ? 'dark' : 'light',
    locale: p.get('locale') === 'zh-CN' ? 'zh-CN' : 'en-US',
    bar: p.get('bar') !== '0',
  };
}

function writeKnobs(k: Knobs) {
  const p = new URLSearchParams({ theme: k.theme, locale: k.locale, bar: k.bar ? '1' : '0' });
  window.history.replaceState(null, '', `?${p}`);
}

function Section({ id, title, children }: { id: string; title: string; children: ReactNode }) {
  return (
    <section id={id} data-preview-section={id} className="space-y-3">
      <h2 className="font-mono text-[0.6875rem] uppercase tracking-wider" style={{ color: 'var(--color-text-quaternary)' }}>
        {title}
      </h2>
      {children}
    </section>
  );
}

function Labeled({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="space-y-1">
      <div className="font-mono text-[0.625rem]" style={{ color: 'var(--color-text-quaternary)' }}>
        {label}
      </div>
      {children}
    </div>
  );
}

function Prose({ children }: { children: string }) {
  return (
    <p className="text-[0.9375rem] leading-relaxed" style={{ color: 'var(--color-text-primary)' }}>
      {children}
    </p>
  );
}

function Gallery() {
  return (
    <>
      <Section id="slips" title="Slips">
        {SLIPS.map((s) => (
          <Labeled key={s.label} label={s.label}>
            <HandoffSlip view={s.view} />
          </Labeled>
        ))}
      </Section>
      <Section id="approval" title="Approval">
        <Labeled label="long brief">
          <HandoffApproval proposal={PENDING_LONG} onApprove={noop} onReject={noop} />
        </Labeled>
        <Labeled label="short brief">
          <HandoffApproval proposal={PENDING_SHORT} onApprove={noop} onReject={noop} />
        </Labeled>
      </Section>
      <Section id="declined" title="Declined">
        <HandoffDeclined proposal={DECLINED} />
      </Section>
      <Section id="group" title="Group">
        <HandoffGroup views={GROUP} />
        <Labeled label="settled">
          <HandoffGroup views={SETTLED_GROUP} />
        </Labeled>
        <Labeled label="every phase">
          <HandoffGroup views={MIXED_GROUP} />
        </Labeled>
      </Section>
      <Section id="narrow" title="Narrow (340px)">
        <div className="max-w-[340px] space-y-3">
          {NARROW.map((s) => (
            <Labeled key={s.label} label={s.label}>
              <HandoffSlip view={s.view} />
            </Labeled>
          ))}
          <Labeled label="declined">
            <HandoffDeclined proposal={DECLINED} />
          </Labeled>
          <Labeled label="group">
            <HandoffGroup views={GROUP} />
          </Labeled>
        </div>
      </Section>
      <Section id="reports" title="Report rows">
        {REPORTS.map((r) => (
          <Labeled key={r.label} label={r.label}>
            <ReportRow id={r.id} provenance={r.report} />
          </Labeled>
        ))}
        <Labeled label="read-only">
          <ReportRow id="msg-read-only" provenance={REPORTS[0].report} readOnly />
        </Labeled>
      </Section>
    </>
  );
}

function Wired() {
  return (
    <Section id="wired" title="In a reply (stubbed liveness)">
      <DispatchStatusProvider>
        <MessageActionsProvider actions={REVEAL}>
          <HandoffPlacesContext value={WIRED_PLACES}>
            <div className="sibling-space-y-6">
              <div className="sibling-space-y-3">
                <Prose>I split the semis question three ways and have one more to propose.</Prose>
                <AnalystHandoffRun members={RUN_GROUP} onApprove={noop} onReject={noop} />
              </div>
              <div className="sibling-space-y-3">
                <Prose>The comps analyst has a question before it goes on.</Prose>
                <AnalystHandoffRun members={RUN_SINGLE} />
              </div>
              <AnalystHandoffRun members={RUN_FAILED} />
              <div className="sibling-space-y-3">
                <Prose>Two hand-offs to one thread: the first finished in 8s, the second is still going.</Prose>
                <AnalystHandoffRun members={RUN_PINNED.slice(0, 1)} />
                <AnalystHandoffRun members={RUN_PINNED.slice(1)} />
              </div>
              <div className="sibling-space-y-3">
                <Prose>Outcome unknown, and the thread has run nothing since.</Prose>
                <AnalystHandoffRun members={RUN_UNKNOWN} />
              </div>
              <ReportRow
                id={WIRED_REPORT}
                provenance={report({ threadId: 'th-c', workspaceName: 'Supply Chain', outcome: 'completed' })}
              />
              <Prose>CoWoS capacity roughly doubles next year, which lifts the cap on accelerator units by about 40%.</Prose>
            </div>
          </HandoffPlacesContext>
        </MessageActionsProvider>
      </DispatchStatusProvider>
    </Section>
  );
}

function Answering() {
  const [round, setRound] = useState(0);
  const [members, setMembers] = useState(ASKING);
  const answer = (proposalId: string, status: 'approved' | 'rejected') => setMembers((ms) => ms.map((m) => (
    m.proposalId === proposalId
      ? { ...m, proposal: { ...m.proposal, status, ...(status === 'approved' ? { thread_id: ASKING_THREADS[proposalId] } : {}) } }
      : m
  )));
  return (
    <Section id="answering" title="Answered live">
      <DispatchStatusProvider>
        <MessageActionsProvider actions={REVEAL}>
          <div className="sibling-space-y-3">
            <Prose>Four hand-offs to answer in any order: one alone, one beside a slip, one beside a group, one declined.</Prose>
            <AnalystHandoffRun
              key={round}
              members={members}
              onApprove={(_proposal, _overrides, proposalId) => answer(proposalId, 'approved')}
              onReject={(_proposal, proposalId) => answer(proposalId, 'rejected')}
            />
            <Prose>The reply goes on under the run, and glides once with each answer.</Prose>
            <button
              type="button"
              onClick={() => {
                setRound((r) => r + 1);
                setMembers(ASKING);
              }}
              className="cursor-pointer rounded-full border border-(--color-border-default) px-2.5 py-0.5 text-[0.6875rem]"
              style={{ color: 'var(--color-text-secondary)' }}
            >
              ask again
            </button>
          </div>
        </MessageActionsProvider>
      </DispatchStatusProvider>
    </Section>
  );
}

function Btn({ on, onClick, children }: { on: boolean; onClick: () => void; children: string }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="cursor-pointer rounded-full border border-(--color-border-default) px-2.5 py-0.5 text-[0.6875rem]"
      style={{
        background: on ? 'var(--color-accent-primary)' : 'var(--color-bg-card)',
        color: on ? 'var(--color-text-on-accent)' : 'var(--color-text-secondary)',
      }}
    >
      {children}
    </button>
  );
}

function Landed() {
  const { pathname, state } = useLocation();
  const navigate = useNavigate();
  return (
    <div className="p-8 font-mono text-xs" style={{ color: 'var(--color-text-primary)' }}>
      <p>navigated to {pathname}</p>
      <pre>{JSON.stringify(state, null, 2)}</pre>
      <button type="button" className="underline" onClick={() => navigate('/', { replace: true })}>
        back
      </button>
    </div>
  );
}

function Preview() {
  const [knobs, setKnobs] = useState(readKnobs);
  const [client] = useState(() => new QueryClient({ defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } } }));
  const { setTheme } = useTheme();
  useEffect(() => setTheme(knobs.theme), [knobs.theme, setTheme]);
  useEffect(() => {
    void switchLocale(i18n, knobs.locale);
  }, [knobs.locale]);
  const set = (patch: Partial<Knobs>) => {
    const next = { ...knobs, ...patch };
    writeKnobs(next);
    setKnobs(next);
  };

  return (
    <QueryClientProvider client={client}>
      <MemoryRouter>
        <Routes>
          <Route
            path="/"
            element={
              <div data-preview-scroll className="h-full overflow-y-auto px-3 py-6 sm:px-6" style={{ background: 'var(--color-bg-page)' }}>
                <main className="font-content mx-auto w-full max-w-3xl space-y-10">
                  <Gallery />
                  <Wired />
                  <Answering />
                </main>
              </div>
            }
          />
          <Route path="*" element={<Landed />} />
        </Routes>
      </MemoryRouter>
      {knobs.bar && (
        <div
          className="fixed right-3 bottom-3 z-50 flex gap-1 rounded-xl border border-(--color-border-default) p-2"
          style={{ background: 'var(--color-bg-card)' }}
        >
          <Btn on={knobs.theme === 'dark'} onClick={() => set({ theme: knobs.theme === 'dark' ? 'light' : 'dark' })}>
            dark
          </Btn>
          <Btn on={knobs.locale === 'zh-CN'} onClick={() => set({ locale: knobs.locale === 'zh-CN' ? 'en-US' : 'zh-CN' })}>
            zh-CN
          </Btn>
        </div>
      )}
    </QueryClientProvider>
  );
}

void initI18n().then(() =>
  createRoot(document.getElementById('root')!).render(
    <StrictMode>
      <ThemeProvider>
        <Preview />
      </ThemeProvider>
    </StrictMode>,
  ),
);
