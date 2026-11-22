import { registerAuthReset } from '@/lib/authResets';
import type { PendingToolCallChunk } from '@/types/chat';
import type { ChartKey } from '@/pages/MarketView/utils/chartInstanceKey';
import type { ChartLayersCard } from '../chartLayerGrouping';
import { buildRenderBlocks, groupSegments, HIDDEN_TOOL_CALL_NAMES, textBlockKey, type RenderBlock } from './buildRenderBlocks';
import { preparingCallsOf, samePreparing } from './pendingToolCalls';
import { retainsNotice } from './notices';
import { ANSWER_SCORE_HELD, ANSWER_SCORE_SHOWN, answerScore } from './answerText';
import type { ActivityItem, PreparingToolCallData } from './activityTypes';
import { EMPTY_OBJ, type ContentSegmentRecord, type FoldState, type MessageRecord, type ToolCallProcessRecord } from './types';
import type { ProjectedMessage } from './turnProjection';

export interface ContentInput {
  segments: ContentSegmentRecord[];
  reasoningProcesses: Record<string, Record<string, unknown>>;
  toolCallProcesses: Record<string, ToolCallProcessRecord>;
  pendingToolCallChunks?: Record<string, PendingToolCallChunk>;
  isStreaming?: boolean;
  isSubagentView?: boolean;
  /** See `buildRenderBlocks`; the current time when omitted. */
  now?: number;
  /** The computer's own root, read as a sandbox root by the path checks. */
  root?: string | null;
  /** See `buildRenderBlocks`. */
  cardedCharts?: ReadonlySet<ChartKey>;
}

type FoldRole = 'retain' | 'text' | 'process';
export type TextFollow = 'nothing' | 'work' | 'ask';
export interface ContentProjection {
  blocks: RenderBlock[];
  roles: ReadonlyMap<string, FoldRole>;
  nextExpiry: number | null;
  preparingToolCall: PreparingToolCallData | null;
  lastTextKey?: string;
  textCount: number;
  /** What the agent did after each text, by block key, before its next text:
   *  nothing, more work, or a card asking the user. Structure only; whether a
   *  text reads as an answer is scored on demand (`shownTexts`). */
  textsFollowed: ReadonlyMap<string, TextFollow>;
  hasRetained: boolean;
  hasProcess: boolean;
  /** A background task is still running under a turn whose stream has ended. */
  hasPinnedLive: boolean;
  /** When the last such task stopped, once none is running. The turn was live
   *  until then, so this is what its fold measures to. */
  pinnedSettledAt: number | null;
  /** The charts it was built leaving to a report row, so a rebuild for the
   *  clock leaves out the same. */
  cardedCharts?: ReadonlySet<ChartKey>;
}

const RETAINED_TYPES = new Set<RenderBlock['type']>([
  'subagent_task', 'html_widget', 'user_question', 'create_workspace', 'start_question', 'ptc_agent',
  'delete_workspace', 'stop_workspace', 'delete_thread', 'credit_pause', 'tool_approval', 'past_plan',
  // A chart's published layers are a thing the reader works with.
  'chart_layers',
]);

/** Outcomes stay; lookups fold. A collapsed turn shows the answer and what the
 *  turn produced, and an inline card the answer restates in a sentence is part
 *  of the working, not part of the result: a research turn that consulted a
 *  dozen tools would otherwise collapse to a stack of a dozen cards and the
 *  fold would buy nothing. These four are not lookups. A preview is a
 *  deliverable, the approval only records consent while the receipt owns the
 *  order outcome and the link to its ledger entry, and a sent message has
 *  already left for another app, where the turn cannot take it back. An
 *  answer to a file comment has left for the file in the same way, and its
 *  line is the chat's only way there. Which notices stay is said where each
 *  is drawn (`NOTICES`).
 *
 *  Every other entry in `INLINE_ARTIFACT_MAP` is a lookup and folds. Adding a
 *  card type here is a product decision, so `contentProjection.foldRole.test.ts`
 *  pins the split rather than letting it drift with the map. */
const RETAINED_ARTIFACTS = new Set(['preview_url', 'order_receipt', 'message_delivery', 'artifact_comment']);

function foldRole(block: RenderBlock): FoldRole {
  if (block.type === 'text') return 'text';
  if (RETAINED_TYPES.has(block.type)) return 'retain';
  if (block.type === 'compact_artifact' && block.card && RETAINED_ARTIFACTS.has(block.card.type)) return 'retain';
  if (block.type === 'notification' && retainsNotice(block.segment)) return 'retain';
  return 'process';
}

function sameFields(a: object, b: object): boolean {
  const keys = Object.keys(a);
  if (keys.length !== Object.keys(b).length) return false;
  for (const key of keys) {
    if ((a as Record<string, unknown>)[key] !== (b as Record<string, unknown>)[key]) return false;
  }
  return true;
}

/** A card is rebuilt on every build, so it is compared by what it lists: an
 *  entry is the wire's own object, kept until its call's result changes. */
function sameCard(a: ChartLayersCard, b: ChartLayersCard): boolean {
  if (a.id !== b.id || a.key !== b.key || a.workspaceId !== b.workspaceId
    || a.symbol !== b.symbol || a.timeframe !== b.timeframe || a.layers.length !== b.layers.length) return false;
  return a.layers.every((layer, i) => layer === b.layers[i] || sameFields(layer, b.layers[i]));
}

function shareItems(next: ActivityItem[], prev: ActivityItem[]): ActivityItem[] {
  let unchanged = next.length === prev.length;
  const items = next.map((item, i) => {
    const old = prev[i]?.id === item.id ? prev[i] : prev.find((p) => p.id === item.id);
    const shared = old && sameFields(old, item) ? old : item;
    if (shared !== prev[i]) unchanged = false;
    return shared;
  });
  return unchanged ? prev : items;
}

function shareBlock(next: RenderBlock, old: RenderBlock | undefined): RenderBlock {
  if (!old || old.type !== next.type) return next;
  if (next.type === 'activity' && old.type === 'activity') {
    const items = shareItems(next.items, old.items);
    return items === old.items ? old : { ...next, items };
  }
  if (next.type === 'text' && old.type === 'text') {
    return sameFields(next.segment, old.segment) ? old : next;
  }
  if (next.type === 'compact_artifact' && old.type === 'compact_artifact') {
    // Its artifact and card are read off the call, and the card is a new
    // object on every build.
    return next.proc === old.proc && next.toolCallId === old.toolCallId ? old : next;
  }
  if (next.type === 'chart_layers' && old.type === 'chart_layers') {
    // Fed by every call that published on the chart, not by one.
    return sameCard(next.card, old.card) ? old : next;
  }
  return sameFields(next, old) ? old : next;
}

function sameKeys(a: ReadonlySet<ChartKey> | undefined, b: ReadonlySet<ChartKey> | undefined): boolean {
  if (a === b) return true;
  const sizeA = a?.size ?? 0;
  if (sizeA !== (b?.size ?? 0)) return false;
  return sizeA === 0 || [...a!].every((key) => b!.has(key));
}

/** `next` with every block and activity item that did not change taken from
 *  `prev`. A streamed chunk rebuilds the whole projection, and the rebuild
 *  hands every block new objects: the memoized activity blocks above the
 *  prose, and each of their rows, would redraw on every chunk for data that
 *  did not move. Only what is field-for-field equal is reused, so this can
 *  never show stale data; that holds because the stream replaces a tool call's
 *  record on every update rather than editing it in place. */
function shareProjection(next: ContentProjection, prev: ContentProjection | undefined): ContentProjection {
  if (!prev) return next;
  const prevByKey = new Map(prev.blocks.map((block) => [block.key, block]));
  let unchanged = next.blocks.length === prev.blocks.length;
  const blocks = next.blocks.map((block, i) => {
    const shared = shareBlock(block, prevByKey.get(block.key));
    if (shared !== prev.blocks[i]) unchanged = false;
    return shared;
  });
  const preparingToolCall = next.preparingToolCall && prev.preparingToolCall
    && samePreparing(next.preparingToolCall, prev.preparingToolCall) ? prev.preparingToolCall : next.preparingToolCall;
  if (unchanged && preparingToolCall === prev.preparingToolCall && next.nextExpiry === prev.nextExpiry
    && next.hasPinnedLive === prev.hasPinnedLive && next.pinnedSettledAt === prev.pinnedSettledAt
    && sameKeys(next.cardedCharts, prev.cardedCharts)
    // A hidden call after a text changes how it folds and no block with it.
    && sameFollows(next.textsFollowed, prev.textsFollowed)) return prev;
  return { ...next, blocks: unchanged ? prev.blocks : blocks, preparingToolCall };
}

const NON_BLANK = /\S/;

/** A text group or block with something in it; a blank one is never drawn. */
function hasText(content: string | undefined): content is string {
  return !!content && NON_BLANK.test(content);
}

/** Cards that wait on the user. Each stays in a folded turn, and the text
 *  leading into one is what it asks about. */
const ASKS_USER = new Set([
  'user_question', 'tool_approval', 'create_workspace', 'start_question', 'ptc_agent',
  'delete_workspace', 'stop_workspace', 'delete_thread', 'credit_pause',
]);
/** Calls that record the work rather than do it. An answer often ends on one,
 *  ticking the plan, so it says nothing about whether the turn went on. An
 *  older thread's plan is a `TodoWrite`. */
const BOOKKEEPING_TOOLS = new Set(['UpdatePlan', 'TodoWrite']);

/** What the agent did after each text before its next one. Reasoning opens the
 *  next model call and a notice is not the agent's, so neither counts. */
function readFollows(
  groups: readonly ContentSegmentRecord[],
  toolCallProcesses: Record<string, ToolCallProcessRecord>,
): Map<string, TextFollow> {
  const follows = new Map<string, TextFollow>();
  let key: string | null = null;
  for (const group of groups) {
    if (group.type === 'text') {
      if (!hasText(group.content)) continue;
      key = textBlockKey(group.order);
      follows.set(key, 'nothing');
    } else if (key === null || group.type === 'reasoning' || group.type === 'notification') {
      continue;
    } else if (ASKS_USER.has(group.type)) {
      follows.set(key, 'ask');
    } else if (follows.get(key) === 'nothing' && !isBookkeeping(group, toolCallProcesses)) {
      follows.set(key, 'work');
    }
  }
  return follows;
}

function isBookkeeping(group: ContentSegmentRecord, toolCallProcesses: Record<string, ToolCallProcessRecord>): boolean {
  const name = group.type === 'tool_call' && group.toolCallId ? toolCallProcesses[group.toolCallId]?.toolName : undefined;
  return name !== undefined && BOOKKEEPING_TOOLS.has(name);
}

function sameFollows(a: ReadonlyMap<string, TextFollow>, b: ReadonlyMap<string, TextFollow>): boolean {
  if (a === b) return true;
  if (a.size !== b.size) return false;
  for (const [key, follow] of a) if (b.get(key) !== follow) return false;
  return true;
}

/** Rendering and fold decisions consume this same projection, including the
 * builder's hidden tools, chart grouping, and actual text blocks. `prev`, an
 * earlier projection of the same message, lends it every part that did not
 * change (see `shareProjection`). */
export function projectContent(input: ContentInput, prev?: ContentProjection): ContentProjection {
  // A hidden tool is dropped here too, or it would show as preparing until
  // it landed and vanished.
  const preparingToolCall = preparingCallsOf(input.pendingToolCallChunks, input.toolCallProcesses, HIDDEN_TOOL_CALL_NAMES);
  const groups = groupSegments(input.segments);
  const { blocks, nextExpiry, pinnedLive, pinnedSettledAt } = buildRenderBlocks(groups, {
    ...input, preparing: preparingToolCall !== null,
  });
  const roles = new Map<string, FoldRole>();
  let lastTextKey: string | undefined;
  let textCount = 0;
  let hasRetained = false;
  let hasProcess = false;
  for (const block of blocks) {
    const role = foldRole(block);
    roles.set(block.key, role);
    if (block.type === 'text' && hasText(block.segment.content)) {
      lastTextKey = block.key;
      textCount++;
    }
    if (role === 'retain') hasRetained = true;
    if (role === 'process') hasProcess = true;
  }
  return shareProjection({
    blocks, roles, nextExpiry, preparingToolCall, lastTextKey, textCount, textsFollowed: readFollows(groups, input.toolCallProcesses),
    hasRetained, hasProcess, hasPinnedLive: pinnedLive, pinnedSettledAt, cardedCharts: input.cardedCharts,
  }, prev);
}

/** What the projection was built from, so a hit can prove it is still current.
 *  Message identity is not enough on its own: the subagent tool-call and
 *  tool-call-result handlers assign `contentSegments` and `toolCallProcesses`
 *  onto the existing record and hand React a new array around the same object,
 *  so a cache keyed on the record alone answers a tool call with the
 *  text-only projection that preceded it, and `nextExpiry` is null on that one,
 *  which means forever. Comparing the inputs by reference costs five checks and
 *  holds for any writer, including ones that mutate. */
interface CacheEntry {
  subagent: boolean;
  segments: unknown;
  reasoning: unknown;
  tools: unknown;
  pending: unknown;
  streaming: boolean;
  root: string | null | undefined;
  carded: ReadonlySet<ChartKey> | undefined;
  projection: ContentProjection;
}

const cache = new WeakMap<MessageRecord, CacheEntry>();

/** The last projection built per message, by id: a streamed chunk arrives as a
 *  new record, so the cache above never holds the previous chunk's projection
 *  to share with. Bounded, because it is only an optimization: a message that
 *  fell out rebuilds whole, which is what every build did before. It holds
 *  transcript text, so a sign-out or account switch empties it. */
const latestById = new Map<string, ContentProjection>();
const LATEST_MAX = 32;
registerAuthReset(() => latestById.clear());

/** `cardedCharts` is the list's (`planReportChartCards`), rebuilt on every
 *  frame, so a hit compares it by content. */
export function projectMessageContent(
  message: MessageRecord,
  isSubagentView = false,
  root?: string | null,
  cardedCharts?: ReadonlySet<ChartKey>,
): ContentProjection {
  const hit = cache.get(message);
  if (hit && hit.subagent === isSubagentView
    && hit.root === root
    && sameKeys(hit.carded, cardedCharts)
    && hit.segments === message.contentSegments
    && hit.reasoning === message.reasoningProcesses
    && hit.tools === message.toolCallProcesses
    && hit.pending === message.pendingToolCallChunks
    && hit.streaming === (message.isStreaming === true)
    && (hit.projection.nextExpiry === null || hit.projection.nextExpiry > Date.now())) return hit.projection;
  const segments = message.contentSegments as ContentSegmentRecord[] | undefined;
  const id = typeof message.id === 'string' ? `${isSubagentView ? 'subagent' : 'main'}:${message.id}` : null;
  const prev = hit?.subagent === isSubagentView ? hit.projection : id ? latestById.get(id) : undefined;
  const projection = projectContent({
    segments: segments?.length ? segments : typeof message.content === 'string'
      ? [{ type: 'text', content: message.content, order: 0 }] : [],
    reasoningProcesses: (message.reasoningProcesses ?? EMPTY_OBJ) as ContentInput['reasoningProcesses'],
    toolCallProcesses: (message.toolCallProcesses ?? EMPTY_OBJ) as ContentInput['toolCallProcesses'],
    pendingToolCallChunks: (message.pendingToolCallChunks ?? EMPTY_OBJ) as ContentInput['pendingToolCallChunks'],
    isStreaming: message.isStreaming === true,
    isSubagentView,
    root,
    cardedCharts,
  }, prev);
  if (id) {
    latestById.delete(id);
    latestById.set(id, projection);
    if (latestById.size > LATEST_MAX) latestById.delete(latestById.keys().next().value!);
  }
  cache.set(message, {
    subagent: isSubagentView,
    segments: message.contentSegments,
    reasoning: message.reasoningProcesses,
    tools: message.toolCallProcesses,
    pending: message.pendingToolCallChunks,
    streaming: message.isStreaming === true,
    root,
    carded: cardedCharts,
    projection,
  });
  return projection;
}

/**
 * An assistant bubble that settled with nothing to paint. Some turns legitimately
 * finalize empty in STATE (a HITL resume whose content landed on another bubble,
 * a history turn whose only event was a re-raised interrupt deduped by
 * interrupt_id, a turn whose only call was a hidden tool) and they must stay in
 * state because edit/regenerate map UI position → backend turn_index by counting
 * assistant bubbles. But painting them shows an orphan avatar + action row, so
 * the list skips rendering them.
 *
 * Empty is judged on the render blocks the bubble would draw, not on its raw
 * segments: the builder drops hidden tool calls, so a turn of only those has
 * segments and still paints a blank column. The plan is no segment at all: it
 * shows under the message that asked for the work. Beyond the blocks, the Sources
 * pill, the Stopped chip, an error, and a live stream keep the bubble.
 *
 * INVARIANT: everything an assistant bubble can render must surface through
 * its content projection / provenanceRecords / error / stopped / isStreaming.
 * A future assistant field that renders OUTSIDE those (e.g. assistant-side
 * attachments) must be added to this guard or its bubbles will be hidden.
 * `isSubagentView`, `root` and `cardedCharts` only key the projection cache the
 * bubble itself reads, so they must be the ones the bubble passes or each
 * render rebuilds it.
 */
export function isOrphanAssistantMessage(
  message: MessageRecord,
  isSubagentView = false,
  root?: string | null,
  cardedCharts?: ReadonlySet<ChartKey>,
): boolean {
  if (message.role !== 'assistant') return false;
  if (message.isStreaming) return false;
  const provenance = message.provenanceRecords as Record<string, unknown> | undefined;
  if (provenance && Object.keys(provenance).length > 0) return false;
  if (message.error || message.stopped) return false;
  const projection = projectMessageContent(message, isSubagentView, root, cardedCharts);
  return projection.textCount === 0 && projection.blocks.every((block) => block.type === 'text');
}

/** Drop bubbles the list must not paint (see `isOrphanAssistantMessage`). */
export function visibleProjection(
  projected: ProjectedMessage[],
  isSubagentView = false,
  root?: string | null,
  cardedOf?: (message: MessageRecord) => ReadonlySet<ChartKey> | undefined,
): ProjectedMessage[] {
  return projected.filter((p) => !isOrphanAssistantMessage(p.message, isSubagentView, root, cardedOf?.(p.message)));
}

/**
 * The text block still being written, or -1. Only the last one is, and only
 * with no activity after it and no tool call running: prose the model turned
 * away from is finished and shows whole. The renderer gates this block alone,
 * and the streaming indicator waits on it alone, so both read it from here.
 */
export function streamingTextBlockIndex(blocks: RenderBlock[]): number {
  let text = -1;
  let activity = -1;
  for (let i = 0; i < blocks.length; i++) {
    const b = blocks[i];
    if (b.type === 'text') text = i;
    if (b.type === 'activity') {
      if (b.items.some((item) => item._liveState === 'active' && item.type === 'tool_call')) return -1;
      activity = i;
    }
  }
  return activity < text ? text : -1;
}

/** The text that stands as a reply whatever it says: the last of a bubble that
 *  closes one, unless the turn went on working after it. Structure only, so
 *  it is known while the turn streams. */
function closingTextKey(projection: ContentProjection, endsReply: boolean): string | undefined {
  const key = projection.lastTextKey;
  return endsReply && key !== undefined && projection.textsFollowed.get(key) !== 'work' ? key : undefined;
}

/** The texts a collapsed turn keeps from this bubble, by `endsReply`. Scored on
 *  first read, so a turn still streaming, which shows every text, never pays. */
const shownByProjection = new WeakMap<ContentProjection, Map<boolean, ReadonlySet<string>>>();

function shownTexts(projection: ContentProjection, endsReply: boolean): ReadonlySet<string> {
  let byEnd = shownByProjection.get(projection);
  const known = byEnd?.get(endsReply);
  if (known) return known;
  const closing = closingTextKey(projection, endsReply);
  const shown = new Set<string>();
  for (const block of projection.blocks) {
    if (block.type !== 'text') continue;
    const follow = projection.textsFollowed.get(block.key);
    if (follow === undefined) continue;
    if (block.key === closing || follow === 'ask') {
      shown.add(block.key);
      continue;
    }
    // A reply's last text, which the turn went on working after, needs less to
    // stand as its answer than an earlier text does to join it.
    const threshold = endsReply && block.key === projection.lastTextKey ? ANSWER_SCORE_HELD : ANSWER_SCORE_SHOWN;
    if (answerScore(block.segment.content ?? '') >= threshold) shown.add(block.key);
  }
  if (!byEnd) shownByProjection.set(projection, byEnd = new Map());
  byEnd.set(endsReply, shown);
  return shown;
}

/** Whether a block belongs to the turn's working, which a fold row can put
 *  away. Position only, never what follows: a text still typing when a call
 *  lands after it must keep its wrapper, or it remounts and skips to its end. */
export function blockIsProcess(projection: ContentProjection, block: RenderBlock, endsReply: boolean): boolean {
  const role = projection.roles.get(block.key);
  if (role === 'retain') return false;
  return role !== 'text' || !(endsReply && block.key === projection.lastTextKey);
}

export function blockVisible(projection: ContentProjection, block: RenderBlock, fold: FoldState, endsReply: boolean): boolean {
  if (fold !== 'collapsed') return true;
  const role = projection.roles.get(block.key);
  return role === 'retain' || (role === 'text' && shownTexts(projection, endsReply).has(block.key));
}

/** Where a turn-end landing puts the viewport top: the first text a collapsed
 *  turn keeps, so an answer kept above a closing line is read from its start.
 *  Unfolded, every text shows and the reply is the last. */
export function replyStartKey(projection: ContentProjection, fold: FoldState, endsReply: boolean): string | undefined {
  if (fold !== 'collapsed') return projection.lastTextKey;
  const shown = shownTexts(projection, endsReply);
  return projection.blocks.find((block) => shown.has(block.key))?.key;
}

/** The bubble keeps a text when its turn is collapsed. */
export function holdsAnswerText(projection: ContentProjection, endsReply: boolean): boolean {
  return closingTextKey(projection, endsReply) !== undefined || shownTexts(projection, endsReply).size > 0;
}
