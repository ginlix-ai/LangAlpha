import { useTranslation } from 'react-i18next';
import { useSubagents } from '@/hooks/useSubagents';
import { deletePlugin, setPluginEnabled, type PluginInfo } from '@/pages/ChatAgent/utils/api';
import { bulkUninstallConfirm, changedSubagents, uninstallLines } from '../components/pluginsPage/pluginStatus';
import type { BulkRunOptions } from '../components/useBulkSelection';
import { buildBulkActions, type BulkActions } from '../utils/bulkActions';
import { hasLifecycle } from '../utils/pluginSurface';
import { pluginRowKey } from '../utils/rowKeys';
import type { PluginListSurface } from './usePluginListSurface';

/**
 * The select-mode actions for Plugins: switch on, switch off, uninstall. No
 * workspaces entry: a plugin is on or off for the account, and its connectors
 * and skills each carry their own reach. Built-in plugins switch but never
 * uninstall, which the bar says rather than leaving the count to explain.
 * An uninstall says what goes as the single one does, subagents and the
 * user's changes to them included.
 */
export function usePluginBulkActions(
  /** The rows on screen, not the whole account: see useBulkSelection. */
  visible: readonly PluginInfo[],
  surface: PluginListSurface,
  /** Starts the sign-in a switch-on was refused for, until a connector is
   *  signed in to. */
  signIn?: BulkRunOptions['signIn'],
): BulkActions {
  const { t } = useTranslation();
  // The page's shell has this list already; the changes are counted from
  // what the confirm showed, since each delete takes its plugin's rows.
  const { data: subagentList } = useSubagents();
  const views = subagentList?.subagents;
  const selected = visible.filter((p) => surface.selection.selected.has(pluginRowKey(p)));
  return buildBulkActions(t, surface, selected, {
    noun: 'plugin',
    key: pluginRowKey,
    name: (p) => p.name,
    isOn: (p) => p.enabled,
    setEnabled: (p, on) => setPluginEnabled(p.name, on),
    remove: {
      verb: 'uninstall',
      labelKey: 'plugins.bulk.uninstall',
      confirm: (plugins) => bulkUninstallConfirm(t, plugins, views),
      eligible: hasLifecycle,
      run: async (p) => ({ deleted: (await deletePlugin(p.name)).deleted, changed: changedSubagents(p, views) }),
      outcome: (results) =>
        uninstallLines(
          t,
          {
            connectors: results.reduce((n, r) => n + r.deleted.servers.length, 0),
            skills: results.reduce((n, r) => n + r.deleted.skills.length, 0),
            subagents: results.reduce((n, r) => n + (r.deleted.subagents?.length ?? 0), 0),
            changedSubagents: results.reduce((n, r) => n + r.changed, 0),
            plugins: results.length,
          },
          [],
          'after',
        ),
    },
    keptKey: 'plugins.bulk.keptPlugins',
    signIn,
  });
}
