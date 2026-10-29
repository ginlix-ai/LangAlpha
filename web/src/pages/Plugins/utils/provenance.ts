/**
 * Package provenance for the Plugins page lists — which package a row came
 * from, and whether that package is holding it down.
 *
 * Typed against the concrete rows that carry BOTH provenance fields rather
 * than a structural shape. `EffectiveServer` has `plugin_name` but no
 * `plugin_enabled`, so a structural parameter accepted a workspace row and
 * answered "not suppressed" for it forever, silently; naming the real types
 * turns that gap into a compile error at the call site.
 */
import type {
  BuiltinMcpServer,
  CatalogServer,
  PluginInfo,
  SkillInfo,
} from '@/pages/ChatAgent/utils/api';
import { isBundled } from './pluginSurface';

/**
 * The row tiers a package can own: the user MCP catalog, the skill list, and
 * the built-ins a shipped bundle declares. The last one is owned by a package
 * nobody installed, which is exactly why it belongs here — the row's identity
 * is the same question whether the owner arrived in a zip or in the image.
 */
export type PluginProvenancedRow = CatalogServer | SkillInfo | BuiltinMcpServer;

/**
 * The row came from a package rather than from the user. Its config is the
 * package's to define, so the surfaces offer state rather than shape: enable
 * or disable it, bind its secrets, and, on an installed plugin's row, edit it
 * and thereby detach it.
 */
export function isPluginOwned(row: PluginProvenancedRow): boolean {
  return !!row.plugin_name;
}

/**
 * Suppressed by its package being switched off, so the row's own `enabled` is
 * not the truth the agent sees. Undefined package state means "not
 * suppressed": a row we can't prove is held down is shown as it is.
 */
export function isPluginSuppressed(row: PluginProvenancedRow): boolean {
  return isPluginOwned(row) && row.plugin_enabled === false;
}

/**
 * The state the agent actually sees. A row the user switched on is still off
 * everywhere if its package is off, so filters and counts have to ask this
 * rather than `enabled` — a deck that reads the row's own flag files a
 * suppressed server under Enabled and contradicts its own badge.
 */
export function isEffectivelyEnabled(row: PluginProvenancedRow): boolean {
  return !!row.enabled && !isPluginSuppressed(row);
}

/** The workspace a skill row belongs to: its own for a workspace's skill,
 *  null for the account's and the platform's. */
export function skillScopeId(skill: SkillInfo): string | null {
  return skill.origin === 'workspace' ? (skill.workspace_id ?? null) : null;
}

/** One plugin's skills, under the plugin they came with. */
export interface PluginSkillGroup {
  name: string;
  rows: SkillInfo[];
  /** Ships with the app rather than installed by the user. */
  builtIn: boolean;
  /** Null when the plugin list has not answered, or does not hold it. */
  plugin: PluginInfo | null;
}

export interface SkillSections {
  /** Added by the user, for every workspace. */
  own: SkillInfo[];
  /** Added by the user to one workspace. */
  workspace: SkillInfo[];
  /** Installed plugins before the ones that ship with the app, the biggest
   *  first within each, as the rail lists packages. */
  plugins: PluginSkillGroup[];
  /** Served by the platform with no package claiming it: an operator's own
   *  skills root. Nothing in a stock build. */
  builtIn: SkillInfo[];
}

const byName = (a: SkillInfo, b: SkillInfo) => a.name.localeCompare(b.name);

/**
 * Skills grouped by who put each one here. Every skill list groups the same
 * way, so a skill sits under the same heading on the Plugins page and in a
 * workspace's settings.
 */
export function skillSections(
  skills: readonly SkillInfo[],
  plugins: readonly PluginInfo[] = [],
): SkillSections {
  const own: SkillInfo[] = [];
  const workspace: SkillInfo[] = [];
  const builtIn: SkillInfo[] = [];
  const byPlugin = new Map<string, SkillInfo[]>();
  for (const skill of skills) {
    const owner = skill.plugin_name;
    if (skill.origin === 'workspace') workspace.push(skill);
    else if (owner) byPlugin.set(owner, [...(byPlugin.get(owner) ?? []), skill]);
    else if (skill.origin === 'user') own.push(skill);
    else builtIn.push(skill);
  }
  const groups = [...byPlugin].map(([name, rows]): PluginSkillGroup => {
    const plugin = plugins.find((p) => p.name === name) ?? null;
    return {
      name,
      rows: rows.sort(byName),
      builtIn: plugin ? isBundled(plugin) : rows.every((s) => s.origin === 'platform'),
      plugin,
    };
  });
  groups.sort(
    (a, b) =>
      Number(a.builtIn) - Number(b.builtIn) ||
      b.rows.length - a.rows.length ||
      a.name.localeCompare(b.name),
  );
  return {
    own: own.sort(byName),
    workspace: workspace.sort(
      (a, b) => byName(a, b) || (a.workspace_id ?? '').localeCompare(b.workspace_id ?? ''),
    ),
    plugins: groups,
    builtIn: builtIn.sort(byName),
  };
}
