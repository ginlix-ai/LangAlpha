import {
  categorizeTool,
  getActiveLabel,
  getCompletedRowTitle,
  getCompletedSummary,
  type ToolCategory,
} from '../toolDisplayConfig';
import { formatThoughtFor, type TFn } from '@/lib/elapsed';

import type { ActivityItem, ToolActivityItem } from './activityTypes';
export type { ActivityItem, LiveState } from './activityTypes';

/**
 * Slot = what the header emits. `memory`, `profile` and `automations` each
 * collapse read+write into a single fragment whose verb flips on any write
 * (each is conceptually one surface: the user's memory, profile data,
 * automations). `fileRead`/
 * `fileEdit` and `memo`/`memoWrite` stay separate: distinct file/memo paths
 * shouldn't collide under one label, and any memo modification is surfaced
 * distinctly so a future regression letting the agent mutate a memo is visible.
 */
export type SummarySlot =
  | 'skill' | 'memory' | 'memo' | 'memoWrite' | 'profile' | 'automations' | 'code'
  | 'web' | 'search' | 'fileRead' | 'fileEdit' | 'reasoning' | 'generic';

export interface SummaryFragment {
  slot: SummarySlot;
  /** Reasoning only: the summed duration, when every thought knew its own. */
  thoughtMs?: number;
  count: number;
  modified?: boolean;
}

const SLOT_ORDER: SummarySlot[] = [
  'skill', 'memory', 'memo', 'memoWrite', 'profile', 'automations', 'code',
  'web', 'search', 'fileRead', 'fileEdit', 'reasoning', 'generic',
];

/** The slots that collapse a read and a write category into one fragment. */
const READ_WRITE_SLOTS: Partial<Record<SummarySlot, readonly [ToolCategory, ToolCategory]>> = {
  memory: ['memoryRead', 'memoryWrite'],
  profile: ['profileRead', 'profileWrite'],
  automations: ['automationsRead', 'automationsWrite'],
};

/** When folded, cap the breakdown so the header stays scannable on a turn with
 *  many tool categories; expanding reveals the full list. */
const FOLDED_MAX = 3;

/** The counting bucket one item falls in: its tool category, or `reasoning`. */
export function classifyToolCall(item: ActivityItem): ToolCategory | 'reasoning' {
  return item.type === 'reasoning'
    ? 'reasoning'
    : categorizeTool(item.toolName || '', item.toolCall, item.root);
}

/** Per-category counts for a run, in the order the header reads them. */
export function summaryFragments(items: ActivityItem[]): SummaryFragment[] {
  const counts = new Map<ToolCategory | 'reasoning', number>();
  for (const item of items) {
    const key = classifyToolCall(item);
    counts.set(key, (counts.get(key) ?? 0) + 1);
  }

  const out: SummaryFragment[] = [];
  for (const slot of SLOT_ORDER) {
    const readWrite = READ_WRITE_SLOTS[slot];
    if (readWrite) {
      const reads = counts.get(readWrite[0]) ?? 0;
      const writes = counts.get(readWrite[1]) ?? 0;
      if (reads + writes > 0) out.push({ slot, count: reads + writes, modified: writes > 0 });
    } else if (slot === 'reasoning' && counts.has('reasoning')) {
      out.push({ slot, count: counts.get('reasoning')!, thoughtMs: thoughtDuration(items) });
    } else if (counts.has(slot as ToolCategory | 'reasoning')) {
      out.push({ slot, count: counts.get(slot as ToolCategory | 'reasoning')! });
    }
  }
  return out;
}

/** Summed thinking time of a run's reasoning items, or undefined when any of
 *  them (a checkpoint-sourced replay, an older stored turn) has no duration:
 *  a partial sum would read as the whole. */
export function thoughtDuration(items: ActivityItem[]): number | undefined {
  let total = 0;
  for (const item of items) {
    if (item.type !== 'reasoning') continue;
    const ms = item.reasoningElapsedMs;
    if (typeof ms !== 'number') return undefined;
    total += ms;
  }
  return total;
}

function fragmentLabel(f: SummaryFragment, t: TFn): string {
  if (f.slot === 'reasoning') {
    return f.thoughtMs !== undefined
      ? t('toolArtifact.thoughtFor', { duration: formatThoughtFor(f.thoughtMs, t) })
      : t('toolArtifact.nReasoning', { count: f.count });
  }
  if (f.slot === 'skill') return t('toolArtifact.categoryCount.skill', { count: f.count });
  // No count for memory, profile or automations: each is one surface, and any
  // write/edit overrules the pure-read framing.
  if (f.slot === 'memory') {
    return t(f.modified ? 'toolArtifact.categoryCount.memoryUpdated' : 'toolArtifact.categoryCount.memoryRead');
  }
  if (f.slot === 'profile') {
    return t(f.modified ? 'toolArtifact.categoryCount.profileUpdated' : 'toolArtifact.categoryCount.profileRead');
  }
  if (f.slot === 'automations') {
    return t(f.modified ? 'toolArtifact.categoryCount.automationsUpdated' : 'toolArtifact.categoryCount.automationsRead');
  }
  if (f.slot === 'fileRead') return t('toolArtifact.categoryCount.fileRead', { count: f.count });
  if (f.slot === 'fileEdit') return t('toolArtifact.categoryCount.fileEdit', { count: f.count });
  if (f.slot === 'memo') return t('toolArtifact.categoryCount.memo', { count: f.count });
  if (f.slot === 'memoWrite') return t('toolArtifact.categoryCount.memoWrite', { count: f.count });
  if (f.slot === 'code') return t('toolArtifact.categoryCount.code', { count: f.count });
  if (f.slot === 'web') return t('toolArtifact.categoryCount.web', { count: f.count });
  if (f.slot === 'search') return t('toolArtifact.categoryCount.search', { count: f.count });
  return t('toolArtifact.categoryCount.generic', { count: f.count });
}

function failedCall(item: ActivityItem): boolean {
  return item.type === 'tool_call' && (item.isFailed === true || item._liveState === 'failed');
}

/**
 * The run's one-line account of itself, e.g. "Ran 2 searches · read 3 files and
 * more". `expanded` drops the fold cap, since the rows themselves are on screen.
 * `countFailures` adds "1 failed", never cut by the cap: on the status line a
 * failed call has no row of its own, and the summary is where it stays findable.
 */
export function summarizeCompletedItems(
  items: ActivityItem[],
  t: TFn,
  options?: { expanded?: boolean; countFailures?: boolean },
): string | undefined {
  const fragments = summaryFragments(items);
  const failures = options?.countFailures ? items.filter(failedCall).length : 0;
  const failed = failures > 0 ? [t('toolArtifact.categoryCount.failed', { count: failures })] : [];
  let label: string | undefined;

  const reasoning = fragments.find((f) => f.slot === 'reasoning');
  if (fragments.length > 0 && fragments.some((f) => f.slot !== 'reasoning' && f.slot !== 'generic')) {
    // High-signal fragments (memory writes, memo writes) get priority: they are
    // information the user specifically needs and shouldn't drop into the
    // "and more …" tail when 4+ categories are present. Hoisted to the front of
    // the visible slice, preserving the relative order of everything else.
    const isPriority = (f: SummaryFragment) =>
      f.slot === 'memoWrite'
      || (f.slot === 'memory' && f.modified === true)
      || (f.slot === 'profile' && f.modified === true)
      || (f.slot === 'automations' && f.modified === true);
    const ordered = [...fragments.filter(isPriority), ...fragments.filter((f) => !isPriority(f))];
    const overflowing = !options?.expanded && ordered.length > FOLDED_MAX;
    const visible = overflowing ? ordered.slice(0, FOLDED_MAX) : ordered;
    // "and more" ends the counts it cuts, before the failures: after them it
    // read as more calls having failed.
    const shown = visible.map((f) => fragmentLabel(f, t)).join(' · ');
    label = [overflowing ? `${shown} ${t('toolArtifact.andMore')}` : shown, ...failed].join(' · ');
  } else if (fragments.length === 1 && reasoning?.thoughtMs !== undefined) {
    // A run that was only thought: say how long, not how many.
    label = fragmentLabel(reasoning, t);
  } else if (items.length > 0) {
    label = [t('toolArtifact.nStepsCompleted', { count: items.length }), ...failed].join(' · ');
  }

  // Capitalize the first character only — embedded proper nouns and casing
  // stay intact. Applies to the fragment label and the step-count fallback.
  if (label && label.length > 0) label = label.charAt(0).toUpperCase() + label.slice(1);
  return label;
}

/** Present tense, for a call that is still running: "Reading memory about …". */
export function liveToolLabel(item: ToolActivityItem, t: TFn): string {
  return getActiveLabel(item.toolName || '', item.toolCall, t, item.root);
}

/** Past tense, for a settled row: "Read memory", "Activated skill". */
export function completedToolTitle(item: ToolActivityItem, t: TFn): string {
  return getCompletedRowTitle(item.toolName || '', item.toolCall, t, { artifact: item.toolCallResult?.artifact, root: item.root });
}

/** The object a settled row names, rendered as its pill; null when there is none. */
export function completedToolSummary(item: ToolActivityItem, t: TFn): string | null {
  return getCompletedSummary(item.toolName || '', item.toolCall, t, item.root);
}
