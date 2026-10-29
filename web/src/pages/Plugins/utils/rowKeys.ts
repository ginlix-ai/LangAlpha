import type {
  BuiltinMcpServer,
  CatalogServer,
  PluginInfo,
  SkillInfo,
} from '@/pages/ChatAgent/utils/api';

/**
 * Every Plugins list's row keys, in one place. A key is what select mode
 * remembers and what a bulk action resolves back to its row, so the list that
 * draws a checkbox and the hook that acts on it have to spell it the same way.
 */

export const pluginRowKey = (plugin: Pick<PluginInfo, 'name'>) => `plugin:${plugin.name}`;

/**
 * Scope-qualified: a workspace row may share its name with the inherited
 * user-tier row it shadows, and only the mutated row should read busy.
 */
export const skillRowKey = (skill: Pick<SkillInfo, 'name' | 'workspace_id'>) =>
  `${skill.workspace_id ?? ''}:${skill.name}`;

/**
 * A connector row of either tier. One selection spans the user's rows and the
 * ones the app ships, which go to different endpoints, so the rows travel
 * tagged rather than as parallel arrays and the key carries the tier.
 */
export type ConnectorRow =
  | { tier: 'builtin'; server: BuiltinMcpServer }
  | { tier: 'catalog'; server: CatalogServer };

export const connectorRowKey = (row: ConnectorRow) => `${row.tier}:${row.server.name}`;
