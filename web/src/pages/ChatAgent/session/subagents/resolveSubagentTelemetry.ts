/**
 * Pure resolver for what an inline subagent card shows about a task it does
 * not own: its telemetry, and why it stopped.
 *
 * Two writers feed the inline subagent card: the live `cards[...]` state
 * (driven by SSE events) and the post-refresh subagent history
 * (driven by history replay). Either can be present, both can be present,
 * or neither. The resolver picks the right source so the card renders
 * the same numbers in every reconnect/refresh permutation.
 *
 * Extracted as a pure function so the namespace-race fallback (history
 * fills in when the live card hasn't been hydrated yet) and the
 * post-refresh ZERO_USAGE seeding can be regression-tested without
 * mounting `ChatView`.
 */
import { countToolCalls } from './subagentMetrics';
import { isTerminalStatus, normalizeWireStatus } from './subagentStatus';
import { ZERO_USAGE, type SubagentTokenUsage } from '../../utils/tokenUsage';
import { latestRun, type SubagentModel, type TaskRun } from './taskRuns';

export interface SubagentTelemetry {
  toolCalls: number;
  tokenUsage: SubagentTokenUsage;
  /** Why a settled task ended, when it ended with a reason. */
  stopReason?: string;
  /** That reason's machine spelling, so a surface can offer the remedy for the
   *  one kind of stop that has one. */
  stopReasonType?: string;
  /** When the run started and settled, from the run ledger (epoch ms), not
   *  from when this view happened to see it. `finishedAtMs` is absent while
   *  the run is live. */
  startedAtMs?: number;
  finishedAtMs?: number;
  /** The call running now, for a step label. Live only. */
  currentToolCall?: { toolName: string; args?: Record<string, unknown> };
  /** The task's closing prose, for a settled row's result line. */
  lastText?: string;
  /** The model the run used, present only when it was not the model of the
   *  agent that launched it: that is the one time the model is news. */
  model?: SubagentModel;
  /** Present while the run this launch draws is the task's live one, once
   *  the ledger has said it started. */
  running?: true;
}

interface MessageLike {
  toolCallProcesses?: Record<string, unknown>;
  [key: string]: unknown;
}

export interface SubagentDataLike {
  messages?: MessageLike[];
  tokenUsage?: SubagentTokenUsage;
  error?: string;
  errorType?: string;
  status?: string;
}

export interface SubagentHistoryLike {
  messages?: MessageLike[];
  tokenUsage?: SubagentTokenUsage;
  toolCalls?: number;
  error?: string;
  errorType?: string;
  status?: string;
}

/** The fields of a tool call process the current step reads. */
interface CallProcess {
  toolName?: string;
  toolCall?: { args?: Record<string, unknown> } | null;
  isInProgress?: boolean;
  isComplete?: boolean;
  order?: number;
}

/** The run a launch's row draws: the one it launched, else the task's newest
 *  (a launch the ledger kept no run for, or one whose run has not named its
 *  launch yet). */
function runOfLaunch(runs: readonly TaskRun[], launchId: string | undefined): TaskRun | undefined {
  const own = launchId ? latestRun(runs.filter((run) => run.launchToolCallId === launchId)) : undefined;
  return own ?? latestRun(runs);
}

/** The stretch of the task's clock a run owns, `from` inclusive. */
interface RunWindow {
  from: number;
  to: number;
}

/** From the run's start to the next run's, not to its finish: the writer
 *  stamps frames on its own clock and the ledger bounds runs on the
 *  database's, so a message can open a little past either edge, and a past
 *  run whose close this page never saw has no finish at all. The earliest
 *  run also owns anything before it, since no other run can. */
function runWindow(runs: readonly TaskRun[], run: TaskRun | undefined): RunWindow {
  const start = run?.startedAtMs;
  if (start === undefined) return { from: -Infinity, to: Infinity };
  let earlier = false;
  let next = Infinity;
  for (const other of runs) {
    const s = other.startedAtMs;
    if (s === undefined) continue;
    if (s < start) earlier = true;
    else if (s > start && s < next) next = s;
  }
  return { from: earlier ? start : -Infinity, to: next };
}

function processesOf(message: MessageLike): CallProcess[] {
  const procs = message.toolCallProcesses;
  if (!procs || typeof procs !== 'object') return [];
  return (Object.values(procs) as CallProcess[]).sort((a, b) => (a.order ?? 0) - (b.order ?? 0));
}

/** A notice the view adds to a run's transcript (an instruction it never
 *  read, a send that failed) is drawn as the run's, but the run wrote none. */
const isNotice = (message: MessageLike): boolean => String(message.id).startsWith('task-notice-');

function lastAssistant(messages: MessageLike[] | undefined): MessageLike | undefined {
  if (!messages) return undefined;
  for (let i = messages.length - 1; i >= 0; i--) {
    const message = messages[i];
    if (message?.role === 'assistant' && !isNotice(message)) return message;
  }
  return undefined;
}

// Keyed by the process map, not its message: the live handlers write a
// message's fields in place, but every tool frame swaps in a new map.
const currentCallByProcesses = new WeakMap<object, SubagentTelemetry['currentToolCall'] | null>();

/** The call the run is in now: the newest unanswered call of the run's own
 *  message (each run opens one). */
function currentToolCallOf(messages: MessageLike[] | undefined): SubagentTelemetry['currentToolCall'] {
  const message = lastAssistant(messages);
  const procs = message?.toolCallProcesses;
  if (!message || !procs || typeof procs !== 'object') return undefined;
  const cached = currentCallByProcesses.get(procs);
  if (cached !== undefined) return cached ?? undefined;
  const open = processesOf(message).filter((p) => p.isInProgress && !p.isComplete).at(-1);
  const call = open?.toolName
    ? { toolName: open.toolName, ...(open.toolCall?.args ? { args: open.toolCall.args } : {}) }
    : null;
  currentCallByProcesses.set(procs, call);
  return call ?? undefined;
}

/** The text after a message's last tool call, as written. */
function tailTextOf(message: MessageLike | undefined): string | undefined {
  const segments = message?.contentSegments;
  if (!Array.isArray(segments)) return undefined;
  const tail: string[] = [];
  for (let i = segments.length - 1; i >= 0; i--) {
    const segment = segments[i] as { type?: unknown; content?: unknown } | null;
    if (segment?.type === 'tool_call') break;
    if (segment?.type === 'text' && typeof segment.content === 'string') tail.unshift(segment.content);
  }
  const text = tail.join('');
  return text.trim() ? text : undefined;
}

/** When the writer opened a message, as the stream stamped it. */
function messageAtMs(message: MessageLike): number | undefined {
  return typeof message.openedAtMs === 'number' ? message.openedAtMs : undefined;
}

/** The prose a settled run closed on. The newest run's message is the last
 *  one. An earlier run's is the last message the stream stamped inside its
 *  window: each run opens messages of its own, and the newest belongs to
 *  whatever ran since. Replay stamps none, so an earlier launch drawn from it
 *  names no prose and its row says its status. */
function closingTextOf(messages: MessageLike[] | undefined, owned: RunWindow): string | undefined {
  if (!messages) return undefined;
  if (owned.to === Infinity) return tailTextOf(lastAssistant(messages));
  for (let i = messages.length - 1; i >= 0; i--) {
    const message = messages[i];
    if (message?.role !== 'assistant' || isNotice(message)) continue;
    const at = messageAtMs(message);
    if (at !== undefined && at >= owned.from && at < owned.to) return tailTextOf(message);
  }
  return undefined;
}

/**
 * `runs` are the task's runs (`taskRuns.ts`) and `launchId` the Task call the
 * asking row stands for. A resumed task has one row per launch, all over one
 * task, so each row draws the run its own launch started.
 */
export function resolveSubagentTelemetry(
  subagentData: SubagentDataLike | undefined,
  history: SubagentHistoryLike | undefined,
  runs: readonly TaskRun[] = [],
  launchId?: string,
): SubagentTelemetry | undefined {
  const sdMessages = subagentData?.messages;
  const sdTokenUsage = subagentData?.tokenUsage;

  // The reason has its own precedence, deliberately unlike the numbers: those
  // pick a source and read it whole, but only one of the two writers ever
  // holds a reason for a given task — the live error frame stamps the card,
  // replay stamps history from the ledger — so the reason takes whichever has
  // one rather than whichever won the count.
  const reason = ((): Pick<SubagentTelemetry, 'stopReason' | 'stopReasonType'> => {
    // One source for both fields, picked once. Read independently they can
    // pair a reason from the card with a type from history, and the type is
    // what decides whether a stop is offered back to the user with plan
    // links — so a mismatch does not garble the copy, it changes what the
    // surface claims happened.
    const source = subagentData?.error ? subagentData : history;
    const error = source?.error;
    if (!error) return {};
    const errorType = source?.errorType;
    return { stopReason: error, ...(errorType ? { stopReasonType: errorType } : {}) };
  })();

  // The run's own fields: its bounds and model from the ledger, its current
  // call from the card the stream writes, whichever source the numbers below
  // come from.
  const run = runOfLaunch(runs, launchId);
  const owned = runWindow(runs, run);
  // Live only while no run of the task started after it (a drawn run whose
  // own start is not known yet counts as the newest), and by the card's
  // status when there is a card; with history alone, by the status the ledger
  // replayed, so a run from before it kept a finish still settles.
  const live =
    owned.to === Infinity
    && run?.finishedAtMs === undefined
    && !isTerminalStatus(normalizeWireStatus(subagentData ? subagentData.status : history?.status));
  const currentToolCall = live ? currentToolCallOf(sdMessages) : undefined;
  // No closing prose while the run is live: it would change with every chunk
  // and say nothing the row's status does not.
  const runFields = (messages: MessageLike[] | undefined): Partial<SubagentTelemetry> => {
    const lastText = live ? undefined : closingTextOf(messages, owned);
    return {
      ...(run?.startedAtMs !== undefined ? { startedAtMs: run.startedAtMs } : {}),
      ...(run?.finishedAtMs !== undefined ? { finishedAtMs: run.finishedAtMs } : {}),
      ...(run?.model ? { model: run.model } : {}),
      ...(currentToolCall ? { currentToolCall } : {}),
      ...(lastText !== undefined ? { lastText } : {}),
      ...(live && run?.startedAtMs !== undefined ? { running: true as const } : {}),
    };
  };

  // Card path: prefer live state, but only when the card has actually been
  // populated. A click-created card with empty messages and zero tokens
  // should still pull from history below — the bug we hit when post-refresh
  // resolution returned zero even though history had the real total.
  if (subagentData && (sdMessages?.length || (sdTokenUsage?.total ?? 0) > 0)) {
    return {
      toolCalls: countToolCalls(sdMessages),
      tokenUsage: sdTokenUsage ?? ZERO_USAGE,
      ...reason,
      ...runFields(sdMessages),
    };
  }

  // History fallback: post-refresh path before the user opens the card,
  // and the namespace-race fallback when SSE hydration hasn't caught up.
  if (history) {
    return {
      toolCalls: history.toolCalls ?? countToolCalls(history.messages),
      tokenUsage: history.tokenUsage ?? ZERO_USAGE,
      ...reason,
      ...runFields(history.messages),
    };
  }

  if (!subagentData) return undefined;

  return {
    toolCalls: countToolCalls(sdMessages),
    tokenUsage: sdTokenUsage ?? ZERO_USAGE,
    ...reason,
    ...runFields(undefined),
  };
}

/** The same keys, each holding a value `same` matches. */
function sameFields(a: object, b: object, same: (x: unknown, y: unknown) => boolean): boolean {
  const left = a as Record<string, unknown>;
  const right = b as Record<string, unknown>;
  const keys = Object.keys(left);
  return keys.length === Object.keys(right).length
    && keys.every((key) => key in right && same(left[key], right[key]));
}

/**
 * Whether two answers draw the same: each field equal, or an object (the
 * usage, the model, the current call) of equal fields. The resolver hands out
 * new objects on every call, and a streaming card is rewritten per chunk, so
 * a surface holding answers compares them by value.
 */
export function sameTelemetry(a: SubagentTelemetry | undefined, b: SubagentTelemetry | undefined): boolean {
  if (a === b) return true;
  if (!a || !b) return false;
  return sameFields(a, b, (x, y) =>
    x === y || (typeof x === 'object' && typeof y === 'object' && !!x && !!y && sameFields(x, y, Object.is)));
}
