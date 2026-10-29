import type { ComponentType, CSSProperties, ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import {
  ArrowUpRight,
  Check,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  Folder,
  FolderPlus,
  Bot,
  PanelRight,
  Paperclip,
  type LucideProps,
} from 'lucide-react';
import { LangAlphaMark } from '@/components/brand/LangAlphaMark';
import { useLocale } from '@/hooks/useLocale';
import { compactNumber } from '@/lib/format';
import { cn } from '@/lib/utils';
import { Favicon } from '@/pages/ChatAgent/components/Favicon';
import {
  messagingAppDomain,
  messagingAppName,
} from '@/pages/ChatAgent/components/messaging/messageDelivery';
import { progress, useSceneTime } from './clock';
import { Appear, Loader, rv } from './kit';

/* The chat intro's own transcript parts, shared by its scenes and the share
   scene that opens on one of its threads: the workspace line, the hand-off
   row, a workflow run, the caption on a message from another agent, a
   chart in the reply, and the files a turn produced. The parts every
   transcript draws (header, column, prose, tool rows) are in
   transcriptParts.tsx. Each is a small copy of the component the app draws
   it with (named on each). */

export type Icon = ComponentType<LucideProps>;

/** A solid card in the chat transcript (`--color-bg-tool-card`), as tool results sit. */
const toolCard: CSSProperties = {
  background: 'var(--color-bg-tool-card)',
  borderColor: 'var(--color-border-muted)',
};

/* ── the chat transcript ───────────────────────────────────────────────── */

/** The Chief of Staff creating a workspace (`CreateWorkspaceCard`): the
    card's header with its loader until `done`, then the one line it settles
    to. */
export function WorkspaceLine({ name, at, done }: { name: string; at: number; done: number }) {
  const { t } = useTranslation();
  const pending = useSceneTime() < done;
  return (
    <Appear at={at} className="flex min-w-0 items-center gap-1">
      {pending ? (
        <>
          <FolderPlus className="h-2 w-2 shrink-0" style={{ color: 'var(--color-accent-light)' }} />
          <span
            className="truncate text-[8px] font-medium leading-none"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {t('chat.createWorkspaceCard.title')}
          </span>
          <span className="ml-auto flex">
            <Loader color="var(--color-icon-muted)" className="text-[7px]" />
          </span>
        </>
      ) : (
        <>
          <ChevronRight className="h-2 w-2 shrink-0" style={{ color: 'var(--color-icon-muted)' }} />
          <Check className="h-2 w-2 shrink-0" style={{ color: 'var(--color-accent-light)' }} />
          <span
            className="truncate text-[8px] leading-none"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {t('chat.createWorkspaceCard.created', { name })}
          </span>
        </>
      )}
    </Appear>
  );
}

/** A hand-off to an Analyst once it is handed off (`HandoffSlip`): our
    mark on a round avatar, the task, and under it how the run stands, then
    the workspace it runs in. No card: it sits in the reply as the turn's
    other rows do, at their left edge, keeping the app's padding above and
    under its two lines. At the scenes' scale the chat column is a narrow one, so
    the line takes the app's narrow form, without the Analyst's name or how
    long the run took. Starting until `started`, working until `finished`,
    then finished, where the spinner going away is the done state. */
export function HandoffSlip({
  workspace,
  title,
  at,
  started,
  finished,
}: {
  workspace: string;
  title: string;
  at: number;
  started: number;
  finished: number;
}) {
  const { t } = useTranslation();
  const now = useSceneTime();
  const [glyph, label] =
    now < started
      ? [
          <span
            key="pending"
            className="block h-[4.5px] w-[4.5px] rounded-full border"
            style={{ borderColor: 'var(--color-text-tertiary)' }}
          />,
          'chat.ptcCard.statusStarting',
        ]
      : now < finished
        ? [
            <Loader key="live" color="var(--color-accent-primary)" className="text-[7px]" />,
            'chat.ptcCard.statusWorking',
          ]
        : [null, 'chat.ptcCard.finished'];
  return (
    <Appear at={at} className="grid grid-cols-[16px_minmax(0,1fr)] items-center gap-x-[7px] py-[3px]">
      <span
        className="flex h-4 w-4 items-center justify-center rounded-full border"
        style={{ borderColor: 'var(--color-border-default)', color: 'var(--color-text-primary)' }}
      >
        <LangAlphaMark className="h-[8.5px] w-[8.5px]" />
      </span>
      <span className="min-w-0">
        <span
          className="block truncate text-[8px] font-medium leading-[1.3]"
          style={{ color: 'var(--color-text-primary)' }}
        >
          {title}
        </span>
        <span
          className="mt-[2px] flex min-w-0 items-center gap-[3.5px] text-[7px] leading-[1.3]"
          style={{ color: 'var(--color-text-secondary)' }}
        >
          <span
            className="flex shrink-0 items-center gap-[2px] whitespace-nowrap"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {glyph}
            {t(label)}
          </span>
          <span style={{ color: 'var(--color-text-tertiary)' }}>·</span>
          <Folder className="h-[7px] w-[7px] shrink-0" />
          <span className="truncate">{workspace}</span>
        </span>
      </span>
    </Appear>
  );
}

/** The caption over a message nobody typed in this thread
    (`MessageProvenanceLabel`): who sent it, by its glyph or a chat app's
    `logo`, then the arrow that opens where it came from. A chat app's
    message has nowhere to open, so it goes without. It sits over the
    bubble's right edge, or alone in place of a report-back's hidden
    message. */
export function Provenance({
  icon: Glyph,
  logo,
  children,
}: {
  icon?: Icon;
  logo?: ReactNode;
  children: ReactNode;
}) {
  return (
    <span
      className="inline-flex min-w-0 items-center gap-1 text-[7px] leading-none"
      style={{ color: 'var(--color-text-tertiary)' }}
    >
      {logo ?? (Glyph && <Glyph className="h-[7px] w-[7px] shrink-0" />)}
      <span className="truncate">{children}</span>
      {!logo && <ArrowUpRight className="h-[7px] w-[7px] shrink-0" />}
    </span>
  );
}

/** The caption over a task the Chief of Staff handed down
    (`MessageProvenanceLabel`): `From the Chief of Staff`. */
export function FromChiefOfStaff() {
  const { t } = useTranslation();
  return <Provenance icon={Bot}>{t('chat.provenance.fromChiefOfStaff')}</Provenance>;
}

/** A chat app's mark, as the app draws it: the favicon of the app's site. */
function AppLogo({ app, size = 7 }: { app: string; size?: number }) {
  return <Favicon domain={messagingAppDomain(app) ?? ''} size={size} />;
}

/** The caption over a message that came in from a chat app: its logo and
    `Via iMessage`. */
export function ViaApp({ app }: { app: string }) {
  const { t } = useTranslation();
  return (
    <Provenance logo={<AppLogo app={app} />}>
      {t('chat.provenance.viaChannel', { platform: messagingAppName(app) })}
    </Provenance>
  );
}

/** A message the agent sent to a chat app (`MessageDeliveryCard`), as its
    pill: the app, how many files went with it, and the send's status. */
export function DeliveryPill({ app, files }: { app: string; files: number }) {
  const { t } = useTranslation();
  return (
    <span
      className="inline-flex w-fit items-center gap-1 rounded-full border py-[3px] pl-1.5 pr-[3px] text-[7px] leading-none"
      style={{
        borderColor: 'var(--color-border-elevated)',
        background: 'var(--color-bg-card)',
        color: 'var(--color-text-primary)',
      }}
    >
      <AppLogo app={app} size={8} />
      <span className="font-medium">{messagingAppName(app)}</span>
      <span className="flex items-center gap-0.5" style={{ color: 'var(--color-text-secondary)' }}>
        <span style={{ color: 'var(--color-text-tertiary)' }}>·</span>
        <Paperclip className="h-[7px] w-[7px]" />
        {files}
      </span>
      <span
        className="flex items-center gap-0.5 rounded-full px-1 py-0.5 font-medium"
        style={{ background: 'var(--color-success-soft)', color: 'var(--color-success)' }}
      >
        <CheckCircle2 className="h-[7px] w-[7px]" />
        {t('toolArtifact.messageDelivery.status.sent')}
      </span>
    </span>
  );
}

/** One agent a workflow's script sends out: its label, when it is sent,
    and when it settles, with the time the row then reads. */
export interface FlowAgent {
  label: string;
  at: number;
  done: number;
  took: string;
}

/** Agent rows the run card keeps in view; earlier ones fold into a count. */
const FLOW_ROWS = 8;

/**
 * A workflow run (`WorkflowRunCard` in `TaskCardShell`): `workflow · name`
 * and the run's status over a rule, the run's title, the phase rail with the
 * phase under way in bold, a row per agent the script sends out (the loader
 * while it runs, a check and its time once done), the latest log line while
 * the run goes, and its totals in a band under a rule. Each phase starts at
 * its `at`, the tokens count up from `at`, and the run settles at `done`.
 */
export function WorkflowCard({
  name,
  title,
  phases,
  agents,
  logs,
  at,
  done,
  took,
  tokens,
}: {
  name: string;
  title: string;
  phases: { name: string; at: number }[];
  agents: FlowAgent[];
  logs: { text: string; at: number }[];
  at: number;
  done: number;
  took: string;
  tokens: number;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useSceneTime();
  const running = now < done;
  const current = phases.filter((p) => p.at <= now).length - 1;
  const sent = agents.filter((a) => a.at <= now);
  const settled = sent.filter((a) => a.done <= now).length;
  const hidden = Math.max(0, sent.length - FLOW_ROWS);
  const log = logs.filter((l) => l.at <= now).at(-1);
  const spent = Math.round(tokens * progress(now, at, done - at));
  const meta: string[] = [];
  if (sent.length > 0) {
    const count = t('chat.workflowRun.agents', { count: sent.length });
    meta.push(running ? `${count} · ${t('chat.workflowRun.done', { count: settled })}` : count);
  }
  if (!running) meta.push(took);
  if (spent > 0) {
    meta.push(t('chat.workflowRun.tokens', { value: compactNumber(spent, locale) }));
  }
  const small = 'text-[6px] leading-none';
  return (
    <Appear at={at}>
      <div className="overflow-hidden rounded-lg border font-mono" style={toolCard}>
        <div
          className="flex items-center gap-1 border-b px-1.5 pb-1 pt-1.5"
          style={{ borderColor: 'var(--color-border-subtle)' }}
        >
          <span
            className="truncate text-[7px] font-medium lowercase leading-none"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            workflow · {name}
          </span>
          <span
            className={cn(
              'ml-auto flex shrink-0 items-center gap-0.5',
              'text-[6px] font-medium leading-none tracking-[0.04em]'
            )}
            style={{ color: running ? 'var(--color-warning)' : 'var(--color-success)' }}
          >
            {running ? (
              <Loader color="currentColor" className="text-[7px]" />
            ) : (
              <Check className="intro-pop h-1.5 w-1.5" strokeWidth={2.5} style={rv(0)} />
            )}
            {t(running ? 'chat.taskCard.statusRunning' : 'chat.taskCard.statusCompleted')}
          </span>
          <ChevronRight
            className="h-2 w-2 shrink-0"
            style={{ color: 'var(--color-text-quaternary)' }}
          />
        </div>
        <div className="flex flex-col gap-1.5 px-1.5 py-1.5">
          <span
            className="truncate font-sans text-[8px] font-medium leading-none"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {title}
          </span>
          {/* the phase rail: done phases dim, the one under way bold */}
          <span className={cn('flex flex-wrap items-center gap-x-1', small)}>
            {phases.map((phase, i) => (
              <span key={phase.name} className="flex items-center gap-1">
                {i > 0 && <span style={{ color: 'var(--color-text-quaternary)' }}>─</span>}
                <span
                  style={
                    running && i === current
                      ? { color: 'var(--color-text-primary)', fontWeight: 600 }
                      : {
                          color:
                            !running || i < current
                              ? 'var(--color-text-tertiary)'
                              : 'var(--color-text-quaternary)',
                          fontWeight: 500,
                        }
                  }
                >
                  {phase.name}
                </span>
              </span>
            ))}
          </span>
          {sent.length > 0 && (
            <span className="flex flex-col gap-[3px]">
              {hidden > 0 && (
                <span className={small} style={{ color: 'var(--color-text-quaternary)' }}>
                  {t('chat.workflowRun.earlier', { count: hidden })}
                </span>
              )}
              {sent.slice(hidden).map((agent) => {
                const live = now < agent.done;
                return (
                  <span key={agent.label} className={cn('flex items-center gap-1', small)}>
                    {live ? (
                      <Loader color="var(--color-warning)" className="text-[6px]" />
                    ) : (
                      <Check
                        className="h-[6px] w-[6px] shrink-0"
                        strokeWidth={2.5}
                        style={{ color: 'var(--color-success)' }}
                      />
                    )}
                    <span
                      className="min-w-0 flex-1 truncate"
                      style={{ color: 'var(--color-text-secondary)' }}
                    >
                      {agent.label}
                    </span>
                    <span className="shrink-0" style={{ color: 'var(--color-text-tertiary)' }}>
                      {live ? '' : agent.took}
                    </span>
                  </span>
                );
              })}
            </span>
          )}
          {running && log && (
            <span className={cn('truncate', small)} style={{ color: 'var(--color-text-tertiary)' }}>
              › {log.text}
            </span>
          )}
        </div>
        {meta.length > 0 && (
          <div
            className={cn('border-t px-1.5 py-1 tracking-[0.02em]', small)}
            style={{
              borderColor: 'var(--color-border-subtle)',
              color: 'var(--color-text-tertiary)',
            }}
          >
            {meta.join(' · ')}
          </div>
        )}
      </div>
    </Appear>
  );
}

/** A chart the agent drew into its reply: a solid card with the chart's
    title and, across from it, its legend for the dashed rule, then the
    chart. */
export function ChartCard({
  title,
  legend,
  children,
}: {
  title: string;
  legend: string;
  children: ReactNode;
}) {
  return (
    <div className="flex flex-col gap-1 rounded-md border px-2 py-1.5" style={toolCard}>
      <span className="flex items-baseline gap-1 leading-none">
        <span
          className="truncate text-[7px] font-semibold"
          style={{ color: 'var(--color-text-primary)' }}
        >
          {title}
        </span>
        <span
          className="ml-auto flex shrink-0 items-center gap-0.5 text-[5px]"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          <span className="w-1.5 border-t border-dashed" style={{ borderColor: 'currentColor' }} />
          {legend}
        </span>
      </span>
      {children}
    </div>
  );
}

/** A file's thumbnail on its card: a page over a tilted sheet, the kind's
    glyph on it, and under the glyph its `label`, set as the app's
    `.turn-file-ext` sets it: a file's extension, or a chart's interval. */
function FileThumb({ icon: Glyph, label }: { icon: Icon; label?: string }) {
  return (
    <span className="relative h-[15px] w-3 shrink-0">
      <span
        className="absolute inset-0 rounded-sm border"
        style={{
          background: 'var(--color-bg-elevated)',
          borderColor: 'var(--color-border-muted)',
          transform: 'translate(-1px, 1px) rotate(-5deg)',
        }}
      />
      <span
        className={cn(
          'absolute inset-0 flex items-center justify-center rounded-sm border',
          label && 'flex-col gap-[1px]'
        )}
        style={{ background: 'var(--color-bg-page)', borderColor: 'var(--color-border-elevated)' }}
      >
        <Glyph className="h-1.5 w-1.5" style={{ color: 'var(--color-accent-primary)' }} />
        {label && (
          <span
            className="font-mono text-[3px] leading-none tracking-[0.04em]"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {label}
          </span>
        )}
      </span>
    </span>
  );
}

/**
 * A file the turn produced (`TurnFileCards`): thumbnail, name, what it is
 * (`meta`), and the Open button with its menu, or the deck's count once
 * `count` files are stacked. The thumbnail carries `label` under its glyph:
 * a file's extension, or, on a chart the agent drew on
 * (`InlineChartAnnotationCard`), its interval, that card having Open on its
 * own (`solo`), there being no menu to open it otherwise.
 */
export function FileCard({
  name,
  meta,
  icon,
  label,
  count = 1,
  solo = false,
}: {
  name: string;
  meta: ReactNode;
  icon: Icon;
  label?: string;
  count?: number;
  solo?: boolean;
}) {
  const { t } = useTranslation();
  const open = (
    <>
      <PanelRight className="h-1.5 w-1.5" />
      {t('chat.turnFiles.open')}
    </>
  );
  return (
    <div
      className="flex h-6 items-center gap-1.5 rounded-lg border px-1.5"
      style={{ borderColor: 'var(--color-border-muted)', background: 'var(--color-bg-card)' }}
    >
      <FileThumb icon={icon} label={label} />
      <span className="flex min-w-0 flex-col gap-0.5">
        <span
          className="truncate text-[8px] leading-none"
          style={{ color: 'var(--color-text-primary)' }}
        >
          {name}
        </span>
        <span
          className="truncate text-[6px] leading-none"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {meta}
        </span>
      </span>
      {count > 1 ? (
        <span className="intro-pop ml-auto flex items-center gap-0.5" style={rv(0)}>
          <span
            className={cn(
              'flex h-2.5 min-w-2.5 items-center justify-center rounded-full px-0.5',
              'text-[6px] font-medium leading-none'
            )}
            style={{ background: 'var(--color-bg-tag)', color: 'var(--color-text-secondary)' }}
          >
            {count}
          </span>
          <ChevronDown className="h-2 w-2" style={{ color: 'var(--color-text-tertiary)' }} />
        </span>
      ) : solo ? (
        <span
          className="ml-auto flex shrink-0 items-center gap-0.5 rounded-[4px] border px-1 py-0.5 text-[6px] leading-none"
          style={{ borderColor: 'var(--color-border-muted)', color: 'var(--color-text-tertiary)' }}
        >
          {open}
        </span>
      ) : (
        <span
          className="ml-auto flex shrink-0 items-stretch text-[6px] leading-none"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          <span
            className="flex items-center gap-0.5 rounded-l-[4px] border px-1 py-0.5"
            style={{ borderColor: 'var(--color-border-muted)' }}
          >
            {open}
          </span>
          <span
            className="flex items-center rounded-r-[4px] border border-l-0 px-0.5"
            style={{ borderColor: 'var(--color-border-muted)' }}
          >
            <ChevronDown className="h-1.5 w-1.5" />
          </span>
        </span>
      )}
    </div>
  );
}

/** A file for `FileStack`: its name, its kind, and its card's glyph. */
interface TurnFile {
  name: string;
  kind: 'spreadsheet' | 'document' | 'page';
  icon: Icon;
}

/** The files a turn produced, stacked as the app stacks them: the first
    card lands at `at`, and once the second lands at `next` it peeks out
    under the first, which takes the count. */
export function FileStack({
  top,
  under,
  at,
  next,
}: {
  top: TurnFile;
  under: TurnFile;
  at: number;
  next: number;
}) {
  const { t } = useTranslation();
  const stacked = useSceneTime() >= next;
  /* the extension goes under the thumbnail's glyph, as the app's card puts
     it, and the kind line reads as the card's does: the kind, then the
     extension, kept as separate pieces of text, since one joined string sets
     the line a hair differently */
  const card = ({ name, kind, icon }: TurnFile) => {
    const ext = name.slice(name.lastIndexOf('.') + 1).toUpperCase();
    return {
      name,
      icon,
      label: ext,
      meta: (
        <>
          {t(`chat.turnFiles.kind.${kind}`)} · {ext}
        </>
      ),
    };
  };
  return (
    <Appear at={at}>
      <div className="relative" style={{ height: stacked ? 27 : 24 }}>
        {stacked && (
          <div
            className="absolute inset-x-0 top-0"
            style={{ transform: 'translateY(3px) scale(0.98)' }}
          >
            <FileCard {...card(under)} />
          </div>
        )}
        <div className="absolute inset-x-0 top-0">
          <FileCard {...card(top)} count={stacked ? 2 : 1} />
        </div>
      </div>
    </Appear>
  );
}
