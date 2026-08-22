import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { AnimatePresence } from 'framer-motion';
import { Blocks, FolderGit2, Package, Plus } from 'lucide-react';
import {
  HeaderButton,
  ListEmpty,
  ListError,
  ListHeader,
  ListSkeleton,
} from '@/pages/ChatAgent/components/mcp/McpPrimitives';
import { usePlugins } from '@/hooks/usePlugins';
import {
  deletePlugin,
  setPluginEnabled,
  type PluginInfo,
} from '@/pages/ChatAgent/utils/api';
import {
  matchesFilter,
  pluginSourceOrigin,
  UPLOADED_ORIGIN,
} from '../utils/groupOrigins';
import { BulkActionBar, type BulkAction } from './BulkActionBar';
import { GroupDeck } from './GroupDeck';
import { ListControls } from './ListControls';
import { PluginCard } from './PluginCard';
import { PluginInstallWizard } from './PluginInstallWizard';
import { rowSelection, useBulkRunner, useBulkSelection } from './useBulkSelection';

/**
 * The Plugins tab body: installed Agent Plugins packages, grouped by install
 * origin (the source repo, or "Uploaded" for zips) so several picks out of
 * one marketplace stack together. Each install fans components into the MCP
 * and Skills tabs, where they appear badged with the plugin's name; this
 * list owns identity and lifecycle only.
 */

const pluginKey = (p: PluginInfo) => `plugin:${p.name}`;

export function PluginsList() {
  const { t } = useTranslation();
  const { data, isLoading, error } = usePlugins();
  const [wizardOpen, setWizardOpen] = useState(false);
  const [filter, setFilter] = useState('');
  const selection = useBulkSelection();
  const { progress, run } = useBulkRunner(selection);

  const plugins = data?.plugins ?? [];
  const maxPlugins = data?.max_plugins ?? 0;
  const atCap = maxPlugins > 0 && plugins.length >= maxPlugins;
  const forceExpanded = selection.selecting || !!filter.trim();

  const visible = plugins.filter((p) =>
    matchesFilter(filter, p.name, p.description, pluginSourceOrigin(p)),
  );
  const byOrigin = new Map<string, PluginInfo[]>();
  for (const p of visible) {
    const origin = pluginSourceOrigin(p);
    byOrigin.set(origin, [...(byOrigin.get(origin) ?? []), p]);
  }
  const groups = [...byOrigin.entries()].sort(([a], [b]) => {
    if (a === UPLOADED_ORIGIN) return 1;
    if (b === UPLOADED_ORIGIN) return -1;
    return a.localeCompare(b);
  });

  const selectedPlugins = plugins.filter((p) => selection.selected.has(pluginKey(p)));
  const enableTargets = selectedPlugins.filter((p) => !p.enabled);
  const disableTargets = selectedPlugins.filter((p) => p.enabled);
  const actions: BulkAction[] = [
    {
      id: 'enable',
      label: t('plugins.bulk.enable', { count: enableTargets.length }),
      disabled: enableTargets.length === 0,
      run: () =>
        run(
          enableTargets.map((p) => ({
            key: p.name,
            run: () => setPluginEnabled(p.name, true),
          })),
        ),
    },
    {
      id: 'disable',
      label: t('plugins.bulk.disable', { count: disableTargets.length }),
      disabled: disableTargets.length === 0,
      run: () =>
        run(
          disableTargets.map((p) => ({
            key: p.name,
            run: () => setPluginEnabled(p.name, false),
          })),
        ),
    },
    {
      id: 'uninstall',
      label: t('plugins.bulk.uninstall', { count: selectedPlugins.length }),
      destructive: true,
      disabled: selectedPlugins.length === 0,
      confirmMessage: t('plugins.bulk.confirmUninstall', { count: selectedPlugins.length }),
      run: () =>
        run(
          selectedPlugins.map((p) => ({
            key: p.name,
            run: () => deletePlugin(p.name),
          })),
        ),
    },
  ];

  return (
    <div className="flex flex-col gap-3">
      <ListHeader
        icon={Blocks}
        title={t('plugins.list.title')}
        count={plugins.length}
        max={maxPlugins}
      >
        <ListControls
          filter={filter}
          onFilterChange={setFilter}
          selecting={selection.selecting}
          onStartSelect={selection.start}
          selectDisabled={plugins.length === 0}
        />
        <HeaderButton
          variant="primary"
          icon={Plus}
          onClick={() => setWizardOpen(true)}
          disabled={atCap}
          title={atCap ? t('plugins.list.atCap', { max: maxPlugins }) : undefined}
        >
          {t('plugins.list.install')}
        </HeaderButton>
      </ListHeader>

      <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
        {t('plugins.list.hint')}
      </p>

      {error ? (
        <ListError>
          {(error as { message?: string })?.message || t('mcp.list.loadFailed')}
        </ListError>
      ) : isLoading ? (
        <ListSkeleton />
      ) : plugins.length === 0 ? (
        <ListEmpty>{t('plugins.list.empty')}</ListEmpty>
      ) : visible.length === 0 ? (
        <ListEmpty>{t('plugins.filter.noMatches')}</ListEmpty>
      ) : (
        groups.map(([origin, groupPlugins]) => {
          const keys = groupPlugins.map(pluginKey);
          const cards = (
            <div className="flex flex-col gap-1.5">
              <AnimatePresence initial={false}>
                {groupPlugins.map((plugin) => (
                  <PluginCard
                    key={plugin.name}
                    plugin={plugin}
                    selection={rowSelection(selection, pluginKey(plugin))}
                  />
                ))}
              </AnimatePresence>
            </div>
          );
          // A lone zip-only group would just re-label the whole list; skip
          // the header and keep today's flat look.
          if (groups.length === 1 && origin === UPLOADED_ORIGIN && !selection.selecting) {
            return <div key={origin}>{cards}</div>;
          }
          return (
            <GroupDeck
              key={origin}
              id={`plugins:${origin}`}
              title={origin === UPLOADED_ORIGIN ? t('plugins.groups.uploaded') : origin}
              icon={origin === UPLOADED_ORIGIN ? Package : FolderGit2}
              count={groupPlugins.length}
              enabledCount={groupPlugins.filter((p) => p.enabled).length}
              forceExpanded={forceExpanded}
              selection={selection}
              selectionKeys={keys}
            >
              {cards}
            </GroupDeck>
          );
        })
      )}

      {selection.selecting && (
        <BulkActionBar
          count={selection.selected.size}
          actions={actions}
          progress={progress}
          onExit={selection.exit}
        />
      )}

      {wizardOpen && <PluginInstallWizard onClose={() => setWizardOpen(false)} />}
    </div>
  );
}
