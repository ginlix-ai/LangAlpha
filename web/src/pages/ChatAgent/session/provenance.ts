/**
 * Where a message came from when nobody typed it in this view: read off a
 * replayed query row's metadata, a live report-back's first frame, or a
 * subagent instruction's sender. Pure, so the mapping is pinned without
 * rendering anything; the label that draws it is
 * `components/messageList/MessageProvenance.tsx`.
 *
 * Every key is optional on the wire: rows written before a key existed, and a
 * share's replay, which drops `report_source`, must keep today's rendering.
 */
import type { BotMessageKind, BotMessageSource, ReportSource, ThreadOrigin } from '@/types/api';
import type { ChatMessage, MessageProvenance, UserMessage } from '@/types/chat';
import { messagingAppDomain } from '../components/messaging/messageDelivery';

/** Every instruction the main agent writes to a subagent carries the same label. */
export const MAIN_AGENT_PROVENANCE: MessageProvenance = Object.freeze({ kind: 'main_agent' });

/** The head-word each message kind frames its text with, as delivery.py writes
 *  it; the label beside the sender's name is this word plus "from". */
const FRAME_LABEL: Record<BotMessageKind, string> = {
  message: 'Message',
  request: 'Request',
  reply: 'Reply',
  report: 'Report',
  question: 'Question',
  notice: 'Notice',
};

const FRAME_KIND: Record<string, BotMessageKind> = Object.fromEntries(
  Object.entries(FRAME_LABEL).map(([kind, label]) => [label, kind as BotMessageKind]),
);

/** The kinds whose frame carries one model-facing instruction line under its
 *  head (delivery.py's `_TAILS`); a reader sees the sender's words only. */
const FRAMED_TAIL: ReadonlySet<BotMessageKind> = new Set(['request', 'reply', 'report', 'question']);

const FRAME_HEAD = /^\[(Message|Request|Reply|Report|Question|Notice) from (.+?)\](?:\r?\n|$)/;

/** Narrow an untrusted metadata value to the bot-message source the server
 *  writes. A row without a readable sender name names nothing. */
export function botMessageSourceFromMetadata(value: unknown): BotMessageSource | null {
  if (!value || typeof value !== 'object') return null;
  const raw = value as Partial<BotMessageSource> & { kind?: unknown };
  if (raw.kind !== 'bot_message' || typeof raw.from_name !== 'string' || !raw.from_name.trim()) {
    return null;
  }
  const kind = typeof raw.message_kind === 'string' && raw.message_kind in FRAME_LABEL
    ? (raw.message_kind as BotMessageKind)
    : 'message';
  return {
    kind: 'bot_message',
    ...(typeof raw.message_id === 'string' ? { message_id: raw.message_id } : {}),
    from_bot_id: typeof raw.from_bot_id === 'string' ? raw.from_bot_id : null,
    from_name: raw.from_name.trim(),
    message_kind: kind,
    ...(typeof raw.body === 'string' ? { body: raw.body } : {}),
  };
}

/** The provenance a bot-message source draws the row as. */
export function botMessageProvenance(source: BotMessageSource): MessageProvenance {
  return {
    kind: 'bot_message',
    fromBotId: source.from_bot_id ?? null,
    fromName: source.from_name,
    messageKind: source.message_kind,
    ...(source.message_id ? { messageId: source.message_id } : {}),
  };
}

/**
 * The sender's own words from the framed text the model reads. The frame is
 * written server-side, so it is recognised by its shape rather than by any
 * text the sender chose; the instruction line under the head of the kinds
 * that carry one is the server's, not the sender's, and goes too.
 */
export function stripSenderFrame(content: string, source: BotMessageSource): string {
  const head = `[${FRAME_LABEL[source.message_kind]} from ${source.from_name}]`;
  let rest = content;
  let framed = false;
  if (rest.startsWith(head)) {
    rest = rest.slice(head.length);
    framed = true;
  } else {
    const match = FRAME_HEAD.exec(rest);
    if (match) {
      rest = rest.slice(match[0].length);
      framed = true;
    }
  }
  if (!framed) return content;
  if (rest.startsWith('\n')) rest = rest.slice(1);
  if (FRAMED_TAIL.has(source.message_kind)) {
    const newline = rest.indexOf('\n');
    rest = newline === -1 ? '' : rest.slice(newline + 1);
  }
  return rest.trim();
}

/** A framed bot message read without its metadata: an older row's words still
 *  name the sender and kind in their first line, which is what draws it. */
export function parseSenderFrame(content: string): { source: BotMessageSource; body: string } | null {
  const match = FRAME_HEAD.exec(content);
  if (!match) return null;
  const source: BotMessageSource = {
    kind: 'bot_message',
    from_bot_id: null,
    from_name: match[2].trim(),
    message_kind: FRAME_KIND[match[1]] ?? 'message',
  };
  return { source, body: stripSenderFrame(content, source) };
}

/** What each place a received bot message is drawn sets on its row. */
type BotMessageRowFields = Pick<UserMessage, 'id' | 'timestamp'>
  & Partial<Pick<UserMessage, 'turnIndex' | 'runId' | 'isHistory'>>;

/** A received bot message's row: the sender line and the sender's words. */
export function botMessageRow(
  source: BotMessageSource,
  fields: BotMessageRowFields,
  content = '',
): UserMessage {
  return {
    role: 'user',
    content: stripSenderFrame(content, source),
    contentType: 'text',
    isStreaming: false,
    provenance: botMessageProvenance(source),
    ...fields,
  };
}

/**
 * A synthetic bot-message turn attached live: the stream's first frame names
 * the sender, and no user message comes with it, so the row is placed ahead of
 * the bubble the attach opened. History replaces it by its run id (the replay's
 * row carries `report_run_id`), so a reload draws one row and a reconnect finds
 * this one rather than drawing a second.
 */
export function withLiveBotMessageRow(
  messages: ChatMessage[],
  runId: string,
  source: BotMessageSource,
  beforeId: string | null,
  turnIndex?: number,
  content = '',
): ChatMessage[] {
  if (messages.some((m) => m.role === 'user' && m.runId === runId)) return messages;
  const row = botMessageRow(source, {
    id: `bot-message-${runId}`,
    timestamp: new Date(),
    runId,
    ...(turnIndex !== undefined ? { turnIndex } : {}),
  }, content);
  const found = beforeId ? messages.findIndex((m) => m.id === beforeId) : -1;
  const at = found === -1 ? messages.length : found;
  return [...messages.slice(0, at), row, ...messages.slice(at)];
}


/** The keys a query row's metadata names its sender by. */
export interface SenderMetadata {
  /** Who sent the turn; absent when the user did. */
  origin?: ThreadOrigin;
  /** The surface it arrived on, raw (`web`, `market_view:AAPL`, `telegram`, ...). */
  platform?: string;
}

/**
 * A report-back's row, named by what it reports on. Null without a source
 * (a share's replay, an older row), which keeps the row hidden as it always was.
 */
export function reportRowProvenance(source?: ReportSource): MessageProvenance | null {
  switch (source?.kind) {
    case 'analyst': {
      const runIds = source.run_ids ?? [];
      return {
        kind: 'analyst_report',
        threadId: source.thread_id,
        workspaceId: source.workspace_id ?? null,
        workspaceName: source.workspace_name ?? null,
        // Left out, not undefined, when an older report has none.
        ...(source.outcome !== undefined ? { outcome: source.outcome } : {}),
        ...(runIds.length > 0 ? { runIds } : {}),
        // Kept when empty: a report that lists none says it answers no
        // steered hand-off, which one whose steers are unknown cannot.
        ...(source.steer_ids ? { steerIds: source.steer_ids } : {}),
      };
    }
    case 'task':
      return {
        kind: 'task_report',
        taskId: source.task_id,
        displayId: source.display_id,
        description: source.description ?? null,
        failed: source.failed,
        ...(source.subagent_type ? { subagentType: source.subagent_type } : {}),
      };
    case 'command_report':
      return {
        kind: 'command_report',
        tool: source.tool,
        description: source.description ?? null,
        status: source.status,
        failureReason: source.failure_reason ?? null,
        toolCallId: source.tool_call_id ?? null,
      };
    default:
      return null;
  }
}

/**
 * Who sent a turn, or null when the user did. Who sent it (`origin`) wins over
 * where it arrived (`platform`): an automation can post through a channel, and
 * the automation is the answer. Only a chat app this build lists is labelled;
 * `web` and `market_view:*` are this app.
 */
export function senderProvenance(metadata?: SenderMetadata): MessageProvenance | null {
  const origin = metadata?.origin;
  if (origin?.type === 'agent') {
    return { kind: 'agent', agent: origin.agent ?? null, threadId: origin.id ?? null };
  }
  if (origin?.type === 'automation') {
    return { kind: 'automation', automationId: origin.id ?? null, name: origin.name ?? null };
  }
  const platform = metadata?.platform;
  if (platform && messagingAppDomain(platform)) return { kind: 'channel', platform };
  return null;
}

/**
 * A report-back attached live: its stream's first frame names the report, and
 * no user message comes with it, so the row is placed ahead of the bubble the
 * attach opened. The run's own row is kept wherever it is: a re-attach replays
 * the stream from its start, and history draws the row of a run that was
 * still going, or had just ended, when the page loaded. A row for another run
 * is not this one's, even right above it: a report-back that failed keeps its
 * row. The same frame names the run's turn, which the bubble takes too, so the
 * row is stamped with it and a reload draws it on the same turn. A backend
 * that names none leaves both unstamped, and an unstamped user row takes the
 * turn the assistant bubble after it opens.
 */
export function withLiveReportRow(
  messages: ChatMessage[],
  runId: string,
  source: ReportSource,
  beforeId: string | null,
  turnIndex?: number,
): ChatMessage[] {
  const row = reportRow(source, {
    id: `report-${runId}`,
    timestamp: new Date(),
    runId,
    ...(turnIndex !== undefined ? { turnIndex } : {}),
  });
  if (!row) return messages;
  if (messages.some((m) => m.role === 'user' && m.runId === runId)) return messages;
  const found = beforeId ? messages.findIndex((m) => m.id === beforeId) : -1;
  const at = found === -1 ? messages.length : found;
  return [...messages.slice(0, at), row, ...messages.slice(at)];
}

/** What each place a report is drawn sets on its row beside the caption. */
type ReportRowFields = Pick<UserMessage, 'id' | 'timestamp'>
  & Partial<Pick<UserMessage, 'turnIndex' | 'runId' | 'isHistory' | 'steeringDelivered'>>;

/**
 * A report's row: the caption alone, whether the report opened its own turn
 * or a live turn took it in. Null for a kind of report this build does not
 * know, which stays hidden as a report with no source does.
 */
export function reportRow(source: ReportSource, fields: ReportRowFields): UserMessage | null {
  const provenance = reportRowProvenance(source);
  if (!provenance) return null;
  return { role: 'user', content: '', contentType: 'text', isStreaming: false, provenance, ...fields };
}
