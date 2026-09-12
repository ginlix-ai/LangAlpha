import type { SegmentedOption } from '@/components/ui/segmented-control';
import type {
  McpServerBindingPatch,
  McpToolBinding,
  McpToolSummary,
} from '@/pages/ChatAgent/utils/api';
import type { CapabilityGroup } from '../brokerages';

/**
 * Which of a server's tools a bulk change can reach, and what one write looks
 * like. A brokerage publishes up to 88 tools, so "what does this checkbox
 * stand for" and "what does Apply send" are answers worth having without a
 * dialog on screen -- every rule here is pure and the components only draw it.
 */

/** The kinds of section a tool list is read in, in the order they are drawn. */
export type ToolSectionKind = 'group' | 'never' | 'unclassified' | 'flat';

export interface ToolSection {
  /** Capability key on a group section, the kind itself on the others. */
  key: string;
  kind: ToolSectionKind;
  tools: McpToolSummary[];
  /** Nothing here is callable, so it draws as chips: no control, no checkbox. */
  dimmed: boolean;
  /** The connection was offered this group and refused it. */
  declined: boolean;
}

/**
 * The tool list bucketed the way it is read, shared by the renderer and the
 * selection above it so both agree about which tools are reachable.
 *
 * Two trailing buckets rather than one, because a tool outside every group is
 * in one of two opposite states: `always_denied` is one we read and withheld,
 * refused whatever is granted, while the rest are unclassified and the policy
 * permits them by design. Absent, not merely falsy, is a server that predates
 * the field, and that server's policy was an allowlist -- under which an
 * ungrouped tool was refused.
 *
 * With no groups to read against (any server that is not a brokerage) the whole
 * list is one flat section, so a caller never branches on the shape.
 */
export function toolSections(
  groups: readonly CapabilityGroup[],
  granted: string[] | null | undefined,
  tools: readonly McpToolSummary[],
): ToolSection[] {
  if (groups.length === 0) {
    return tools.length === 0
      ? []
      : [{ key: 'flat', kind: 'flat', tools: [...tools], dimmed: false, declined: false }];
  }

  const settled = granted != null;
  const byGroup = new Map<string, McpToolSummary[]>();
  const never: McpToolSummary[] = [];
  const unclassified: McpToolSummary[] = [];
  for (const tool of tools) {
    if (tool.capability) {
      const bucket = byGroup.get(tool.capability);
      if (bucket) bucket.push(tool);
      else byGroup.set(tool.capability, [tool]);
    } else if (tool.always_denied === false) {
      unclassified.push(tool);
    } else {
      never.push(tool);
    }
  }

  const sections: ToolSection[] = [];
  for (const group of groups) {
    const bucket = byGroup.get(group.key);
    if (!bucket || bucket.length === 0) continue;
    const on = !settled || granted.includes(group.key);
    sections.push({
      key: group.key,
      kind: 'group',
      tools: bucket,
      dimmed: settled && !on,
      declined: settled && !on,
    });
  }
  if (never.length > 0) {
    sections.push({ key: 'never', kind: 'never', tools: never, dimmed: true, declined: false });
  }
  if (unclassified.length > 0) {
    // Not dimmed once there is a connection: these are the ones the agent can
    // actually call. Without one, nothing is reachable yet.
    sections.push({
      key: 'unclassified',
      kind: 'unclassified',
      tools: unclassified,
      dimmed: !settled,
      declined: false,
    });
  }
  return sections;
}

/** Substring, case-insensitive, on the tool's name. An empty query matches all. */
export function matchesToolFilter(tool: McpToolSummary, query: string): boolean {
  const needle = query.trim().toLowerCase();
  if (!needle) return true;
  return tool.name.toLowerCase().includes(needle);
}

/** The same sections with each bucket narrowed to the query; empty ones drop. */
export function filterSections(sections: ToolSection[], query: string): ToolSection[] {
  if (!query.trim()) return sections;
  const out: ToolSection[] = [];
  for (const section of sections) {
    const tools = section.tools.filter((tool) => matchesToolFilter(tool, query));
    if (tools.length > 0) out.push({ ...section, tools });
  }
  return out;
}

/**
 * Whether a bulk change can move this tool at all. `policy` is the server
 * pinning it -- an order tool held to direct -- and it answers 422 to anything
 * else, so it never enters a selection and draws its pinned label instead.
 */
export function isSelectable(tool: McpToolSummary): boolean {
  return tool.binding_source !== 'policy';
}

/** Every tool a checkbox may be drawn on: reachable section, movable tool. */
export function selectableIn(sections: readonly ToolSection[]): McpToolSummary[] {
  return sections
    .filter((section) => !section.dimmed)
    .flatMap((section) => section.tools.filter(isSelectable));
}

/**
 * Whether this tool takes this binding. `allowed` is the whole answer: the
 * server lists it for every tool it restricts, an order tool included, and an
 * absent list is a response from before the field rather than a locked tool.
 */
export function permitsBinding(tool: McpToolSummary, binding: McpToolBinding): boolean {
  return tool.allowed == null || tool.allowed.includes(binding);
}

const BINDINGS: McpToolBinding[] = ['ptc', 'direct', 'both'];

/**
 * The three bindings as segments, for a tool row and for the bulk bar. Two
 * call sites and one declaration, so the answers a row offers and the answers
 * a selection offers cannot drift apart.
 */
export function bindingOptions(
  t: (key: string) => string,
  /** Absent = every value is offered. */
  isAllowed?: (binding: McpToolBinding) => boolean,
): SegmentedOption<McpToolBinding>[] {
  return BINDINGS.map((binding) => ({
    value: binding,
    label: t(`plugins.detail.binding_${binding}`),
    disabled: isAllowed ? !isAllowed(binding) : false,
  }));
}

/** Tri-state of a header checkbox over the rows it stands for. */
export type CheckState = 'none' | 'some' | 'all';

/** What a list needs to draw checkboxes; absent = the list is read-only. */
export interface ToolListSelection {
  selected: ReadonlySet<string>;
  onToggle: (name: string) => void;
  /** A header box: every row it stands for moves to the same state at once. */
  onToggleMany: (names: string[], on: boolean) => void;
}

export const NO_SELECTION: ReadonlySet<string> = new Set<string>();

export function checkStateOf(
  names: readonly string[],
  selected: ReadonlySet<string>,
): CheckState {
  if (names.length === 0) return 'none';
  let hit = 0;
  for (const name of names) if (selected.has(name)) hit += 1;
  if (hit === 0) return 'none';
  return hit === names.length ? 'all' : 'some';
}

export interface BulkBindingPlan {
  /** One patch for the whole selection, or null when nothing may move. */
  patch: McpServerBindingPatch | null;
  applied: string[];
  /** Selected tools the value is not allowed on; counted, never sent. */
  skipped: string[];
}

/**
 * One write for the whole selection. A value a tool does not take is dropped
 * rather than sent: the server refuses the entire patch on the first bad pair,
 * so sending one would lose the other eighty-seven changes to a single order
 * tool. The count comes back so the bar can say how many stayed put.
 */
export function planBulkBinding(
  tools: readonly McpToolSummary[],
  value: McpToolBinding,
): BulkBindingPlan {
  const applied: string[] = [];
  const skipped: string[] = [];
  for (const tool of tools) {
    if (permitsBinding(tool, value)) applied.push(tool.name);
    else skipped.push(tool.name);
  }
  const set: Record<string, McpToolBinding> = {};
  for (const name of applied) set[name] = value;
  return {
    patch: applied.length > 0 ? { tool_binding_set: set } : null,
    applied,
    skipped,
  };
}

/** Reset names the tools rather than writing a value: precedence is the
 *  server's, so removing the override is the only honest way back. */
export function planBulkReset(tools: readonly McpToolSummary[]): {
  patch: McpServerBindingPatch | null;
  names: string[];
} {
  const names = tools.map((tool) => tool.name);
  return { patch: names.length > 0 ? { tool_binding_unset: names } : null, names };
}

/** The binding the whole selection already sits on, or null when they differ,
 *  so the bar's segments read as state rather than three fire-once buttons. */
export function commonBinding(
  tools: readonly McpToolSummary[],
): McpToolBinding | null {
  if (tools.length === 0) return null;
  const first = tools[0].binding ?? 'ptc';
  return tools.every((tool) => (tool.binding ?? 'ptc') === first) ? first : null;
}
