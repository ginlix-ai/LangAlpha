import type { TFunction } from 'i18next';
import type { BuiltinMcpServer, CatalogServer, SkillInfo } from '@/pages/ChatAgent/utils/api';
import { isEffectivelyEnabled } from '../../utils/provenance';
import { readChecklist, reachText, type ScopeReach, type ScopeWorkspace } from '../ScopeControl';

/**
 * Where a plugin reaches. A plugin has no scope of its own: each connector and
 * skill it carries is scoped on its own page, so the plugin's reach is theirs
 * when they agree, and a pointer to them when they do not. Read against the
 * real workspaces only, the list a part's own checklist offers.
 */

type Part = CatalogServer | BuiltinMcpServer | SkillInfo;

function newWorkspacesOn(part: Part): boolean | undefined {
  // Builtins and skills have no such setting and always start on.
  return 'enabled_in_new_workspaces' in part ? part.enabled_in_new_workspaces : undefined;
}

/** A part's reach as its own page's control words it; off everywhere when it
 *  or its plugin is switched off. */
export function partReach(part: Part, workspaces: ScopeWorkspace[], home?: ScopeWorkspace): ScopeReach {
  if (!isEffectivelyEnabled(part)) return { kind: 'none' };
  return readChecklist(workspaces, part.disabled_workspace_ids, home, newWorkspacesOn(part)).reach;
}

/** Which workspaces a part is on in, and whether the next one gets it: two
 *  parts with the same signature reach exactly the same places. */
function signature(part: Part, workspaces: ScopeWorkspace[]): string {
  if (!isEffectivelyEnabled(part)) return 'off';
  const { active } = readChecklist(workspaces, part.disabled_workspace_ids);
  const on = active.map((w) => w.id).sort();
  return `${newWorkspacesOn(part) === false ? 'fixed' : 'new'}:${on.join(',')}`;
}

export type PluginReach =
  | { kind: 'same'; reach: ScopeReach }
  | { kind: 'varies'; by: 'connector' | 'skill' | 'both' };

export function pluginReach(
  connectors: readonly Part[],
  skills: readonly Part[],
  workspaces: ScopeWorkspace[],
): PluginReach | null {
  const parts = [...connectors, ...skills];
  if (parts.length === 0) return null;
  const signatures = new Set(parts.map((p) => signature(p, workspaces)));
  if (signatures.size === 1) return { kind: 'same', reach: partReach(parts[0], workspaces) };
  if (skills.length === 0) return { kind: 'varies', by: 'connector' };
  if (connectors.length === 0) return { kind: 'varies', by: 'skill' };
  return { kind: 'varies', by: 'both' };
}

export function pluginReachText(t: TFunction, reach: PluginReach): string {
  if (reach.kind === 'same') return reachText(t, reach.reach);
  if (reach.by === 'connector') return t('plugins.pluginsPage.reachVariesConnector');
  if (reach.by === 'skill') return t('plugins.pluginsPage.reachVariesSkill');
  return t('plugins.pluginsPage.reachVariesBoth');
}

/**
 * A cell's reach, said only when it is not everywhere, in the badge's words
 * ("All except 2", "Only in Research"): the exception is the only thing worth
 * reading on a cell, and a part that is off already says so in its state.
 * `home` is Home, when the part's own checklist offers it.
 */
export function partialReachText(
  t: TFunction,
  part: Part,
  workspaces: ScopeWorkspace[],
  home?: ScopeWorkspace,
): string | null {
  if (!isEffectivelyEnabled(part)) return null;
  const reach = partReach(part, workspaces, home);
  return reach.kind === 'all' ? null : reachText(t, reach);
}
