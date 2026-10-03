import { registerAuthReset } from '@/lib/authResets';
import { buildRenderBlocks, groupSegments, type RenderBlock } from './buildRenderBlocks';
import type { ActivityItem, PreparingToolCallData } from './activityTypes';
import { EMPTY_OBJ, type ContentSegmentRecord, type FoldState, type MessageRecord, type ToolCallProcessRecord } from './types';
import type { ProjectedMessage } from './turnProjection';

export interface ContentInput {
  segments: ContentSegmentRecord[];
  reasoningProcesses: Record<string, Record<string, unknown>>;
  toolCallProcesses: Record<string, ToolCallProcessRecord>;
  pendingToolCallChunks?: Record<string, Record<string, unknown>>;
  isStreaming?: boolean;
  isSubagentView?: boolean;
  /** See `buildRenderBlocks`; the current time when omitted. */
  now?: number;
}

type FoldRole = 'retain' | 'text' | 'process';
export interface ContentProjection {
  blocks: RenderBlock[];
  roles: ReadonlyMap<string, FoldRole>;
  nextExpiry: number | null;
  preparingToolCall: PreparingToolCallData | null;
  lastTextKey?: string;
  textCount: number;
  hasRetained: boolean;
  hasProcess: boolean;
  /** A background task is still running under a turn whose stream has ended. */
  hasPinnedLive: boolean;
  /** When the last such task stopped, once none is running. The turn was live
   *  until then, so this is what its fold measures to. */
  pinnedSettledAt: number | null;
}

const RETAINED_TYPES = new Set<RenderBlock['type']>([
  'subagent_task', 'html_widget', 'user_question', 'create_workspace', 'start_question', 'ptc_agent',
  'delete_workspace', 'stop_workspace', 'delete_thread', 'credit_pause', 'tool_approval', 'past_plan',
]);

/** Outcomes stay; lookups fold. A collapsed turn shows the answer and what the
 *  turn produced, and an inline card the answer restates in a sentence is part
 *  of the working, not part of the result: a research turn that consulted a
 *  dozen tools would otherwise collapse to a stack of a dozen cards and the
 *  fold would buy nothing. These four are not lookups. A preview is a
 *  deliverable, an annotated chart is a thing the reader works with, the
 *  approval only records consent while the receipt owns the order outcome and
 *  the link to its ledger entry, and a sent message has already left for
 *  another app, where the turn cannot take it back.
 *
 *  Every other entry in `INLINE_ARTIFACT_MAP` is a lookup and folds. Adding a
 *  card type here is a product decision, so `contentProjection.foldRole.test.ts`
 *  pins the split rather than letting it drift with the map. */
const RETAINED_ARTIFACTS = new Set(['preview_url', 'chart_annotation', 'order_receipt', 'message_delivery']);

function foldRole(block: RenderBlock): FoldRole {
  if (block.type === 'text') return 'text';
  if (RETAINED_TYPES.has(block.type)) return 'retain';
  if (block.type === 'compact_artifact') {
    const artifact = (block.proc.toolCallResult as { artifact?: { type?: string } } | undefined)?.artifact;
    if (RETAINED_ARTIFACTS.has(artifact?.type as string)) return 'retain';
  }
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
  return sameFields(next, old) ? old : next;
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
    && sameFields(next.preparingToolCall, prev.preparingToolCall) ? prev.preparingToolCall : next.preparingToolCall;
  if (unchanged && preparingToolCall === prev.preparingToolCall && next.nextExpiry === prev.nextExpiry
    && next.hasPinnedLive === prev.hasPinnedLive && next.pinnedSettledAt === prev.pinnedSettledAt) return prev;
  return { ...next, blocks: unchanged ? prev.blocks : blocks, preparingToolCall };
}

/** Rendering and fold decisions consume this same projection, including the
 * builder's hidden tools, chart grouping, and actual text blocks. `prev`, an
 * earlier projection of the same message, lends it every part that did not
 * change (see `shareProjection`). */
export function projectContent(input: ContentInput, prev?: ContentProjection): ContentProjection {
  const chunks = Object.values(input.pendingToolCallChunks ?? EMPTY_OBJ);
  const name = chunks.find((chunk) => typeof chunk.toolName === 'string')?.toolName;
  const preparingToolCall = chunks.length ? {
    toolName: typeof name === 'string' ? name : undefined,
    argsLength: chunks.reduce((sum, chunk) => sum + (typeof chunk.argsLength === 'number' ? chunk.argsLength : 0), 0),
  } : null;
  const { blocks, nextExpiry, pinnedLive, pinnedSettledAt } = buildRenderBlocks(groupSegments(input.segments), {
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
    if (block.type === 'text' && block.segment.content?.trim()) {
      lastTextKey = block.key;
      textCount++;
    }
    if (role === 'retain') hasRetained = true;
    if (role === 'process') hasProcess = true;
  }
  return shareProjection({
    blocks, roles, nextExpiry, preparingToolCall, lastTextKey, textCount,
    hasRetained, hasProcess, hasPinnedLive: pinnedLive, pinnedSettledAt,
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

export function projectMessageContent(message: MessageRecord, isSubagentView = false): ContentProjection {
  const hit = cache.get(message);
  if (hit && hit.subagent === isSubagentView
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
 * segments: the builder drops hidden tool calls (TodoWrite's list floats
 * outside the bubble) and todo-list segments, so a turn of only those has
 * segments and still paints a blank column. Beyond the blocks, the Sources
 * pill, the Stopped chip, an error, and a live stream keep the bubble.
 *
 * INVARIANT: everything an assistant bubble can render must surface through
 * its content projection / provenanceRecords / error / stopped / isStreaming.
 * A future assistant field that renders OUTSIDE those (e.g. assistant-side
 * attachments) must be added to this guard or its bubbles will be hidden.
 * `isSubagentView` only keys the projection cache the bubble itself reads.
 */
export function isOrphanAssistantMessage(message: MessageRecord, isSubagentView = false): boolean {
  if (message.role !== 'assistant') return false;
  if (message.isStreaming) return false;
  const provenance = message.provenanceRecords as Record<string, unknown> | undefined;
  if (provenance && Object.keys(provenance).length > 0) return false;
  if (message.error || message.stopped) return false;
  const projection = projectMessageContent(message, isSubagentView);
  return projection.textCount === 0 && projection.blocks.every((block) => block.type === 'text');
}

/** Drop bubbles the list must not paint (see `isOrphanAssistantMessage`). */
export function visibleProjection(projected: ProjectedMessage[], isSubagentView = false): ProjectedMessage[] {
  return projected.filter((p) => !isOrphanAssistantMessage(p.message, isSubagentView));
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

export function blockIsProcess(projection: ContentProjection, block: RenderBlock, isTurnTail: boolean): boolean {
  const role = projection.roles.get(block.key);
  return role !== 'retain' && !(role === 'text' && isTurnTail && block.key === projection.lastTextKey);
}

export function blockVisible(projection: ContentProjection, block: RenderBlock, fold: FoldState, isTurnTail: boolean): boolean {
  return fold !== 'collapsed' || !blockIsProcess(projection, block, isTurnTail);
}
