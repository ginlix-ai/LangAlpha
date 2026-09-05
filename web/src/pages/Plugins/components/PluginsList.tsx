import { useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { AnimatePresence } from 'framer-motion';
import { Blocks, Boxes, FolderGit2, Package, Plus } from 'lucide-react';
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
  BUNDLED_ORIGIN,
  groupBy,
  matchesFilter,
  pluginSourceOrigin,
  UPLOADED_ORIGIN,
} from '../utils/groupOrigins';
import { hasLifecycle } from '../utils/pluginSurface';
import { DETAIL_KIND_TAB, withDetail } from '../utils/detailParam';
import { useAddIntent } from '../hooks/useAddIntent';
import { useDetailParam } from '../hooks/useDetailParam';
import { usePluginListSurface } from '../hooks/usePluginListSurface';
import { BulkActionBar, type BulkAction } from './BulkActionBar';
import { EmptyState } from './EmptyState';
import { GroupDeck } from './GroupDeck';
import { ListControls } from './ListControls';
import { PluginCard } from './PluginCard';
import { PluginDetail } from './PluginDetail';
import { PluginInstallWizard } from './PluginInstallWizard';
import { rowSelection } from './useBulkSelection';

/**
 * The Plugins tab body: Agent Plugins packages, grouped by install origin (the
 * source repo, or "Uploaded" for zips) so several picks out of one marketplace
 * stack together. Each install fans components into the MCP and Skills tabs,
 * where they appear badged with the plugin's name; this list owns identity and
 * lifecycle only.
 *
 * The bundles that ship inside the app are packages of the same shape and read
 * as ordinary rows here, but they were never installed: nothing uninstalls
 * them and they hold no slot against the cap. Switching one off is the one
 * verb they do answer, so they select and bulk-toggle like the rest.
 */

const pluginKey = (p: PluginInfo) => `plugin:${p.name}`;

/** Sort weight for a group heading: unranked repo origins lead, then the two
 *  catch-alls, so what the user went and got sits above what came with the app. */
const ORIGIN_RANK: Record<string, number> = {
  [UPLOADED_ORIGIN]: 1,
  [BUNDLED_ORIGIN]: 2,
};

const GROUP_ICON: Record<string, typeof Package> = {
  [UPLOADED_ORIGIN]: Package,
  [BUNDLED_ORIGIN]: Boxes,
};

export function PluginsList() {
  const { t } = useTranslation();
  const [searchParams, setSearchParams] = useSearchParams();
  const { data, isLoading, error } = usePlugins();
  const [wizardOpen, setWizardOpen] = useState(false);
  // The plugin list is the one surface whose bulk actions really are
  // plugin-wide (uninstall), so it keeps the full fan-out.
  const surface = usePluginListSurface();
  const { selection } = surface;

  useAddIntent({ plugin: () => setWizardOpen(true) });

  const plugins = data?.plugins ?? [];
  const maxPlugins = data?.max_plugins ?? 0;
  // The cap counts what the user installed. Bundles arrive with the app and
  // cannot be uninstalled, so counting them would spend an allowance nobody
  // asked for, and read as "6 / 20" on an account holding no plugins at all.
  const installed = plugins.filter(hasLifecycle);
  const atCap = maxPlugins > 0 && installed.length >= maxPlugins;

  const visible = plugins.filter(
    (p) =>
      matchesFilter(surface.filter, p.name, p.description, pluginSourceOrigin(p)) &&
      surface.matchesState(p.enabled),
  );

  // --- Detail overlay (?detail=plugin:NAME) ---
  const detail = useDetailParam<PluginInfo>(
    'plugin',
    (ref) => plugins.find((p) => p.name === ref.name) ?? null,
    !isLoading && data !== undefined,
  );
  const detailPlugin = detail.target;

  const groups = [...groupBy(visible, pluginSourceOrigin).entries()].sort(([a], [b]) => {
    return (ORIGIN_RANK[a] ?? 0) - (ORIGIN_RANK[b] ?? 0) || a.localeCompare(b);
  });

  function groupTitle(origin: string): string {
    if (origin === BUNDLED_ORIGIN) return t('plugins.groups.bundled');
    if (origin === UPLOADED_ORIGIN) return t('plugins.groups.uploaded');
    return origin;
  }

  // Visible rows only: a bulk action reaches exactly what is on screen.
  const selectedPlugins = visible.filter((p) => selection.selected.has(pluginKey(p)));
  const enableTargets = selectedPlugins.filter((p) => !p.enabled);
  const disableTargets = selectedPlugins.filter((p) => p.enabled);
  // Every action counts only the rows it can actually reach — a bundle has no
  // install to undo, the same way an already-enabled row is not an enable.
  const uninstallTargets = selectedPlugins.filter(hasLifecycle);
  const actions: BulkAction[] = [
    {
      id: 'enable',
      label: t('plugins.bulk.enable', { count: enableTargets.length }),
      disabled: enableTargets.length === 0,
      run: () =>
        surface.run(
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
        surface.run(
          disableTargets.map((p) => ({
            key: p.name,
            run: () => setPluginEnabled(p.name, false),
          })),
        ),
    },
    {
      id: 'uninstall',
      label: t('plugins.bulk.uninstall', { count: uninstallTargets.length }),
      destructive: true,
      disabled: uninstallTargets.length === 0,
      confirmMessage: t('plugins.bulk.confirmUninstall', {
        count: uninstallTargets.length,
      }),
      run: () =>
        surface.run(
          uninstallTargets.map((p) => ({
            key: p.name,
            run: () => deletePlugin(p.name),
          })),
        ),
    },
  ];

  function openComponent(kind: 'mcp' | 'skill', name: string) {
    const detailKind = kind === 'mcp' ? 'server' : 'skill';
    const next = withDetail(searchParams, { kind: detailKind, name });
    next.set('tab', DETAIL_KIND_TAB[detailKind]);
    // Pushed, like every other overlay open: Back returns to the card the
    // chip was on rather than leaving the page.
    setSearchParams(next);
  }

  return (
    <div className="flex flex-col gap-3">
      <ListControls
        filter={surface.filter}
        onFilterChange={surface.setFilter}
        stateFilter={surface.stateFilter}
        onStateFilterChange={surface.setStateFilter}
        selecting={selection.selecting}
        onStartSelect={selection.start}
        // Enable and disable reach a bundle the same as any other row; only
        // uninstall needs one that was installed. The mode is empty when there
        // is no row at all, not when nothing was installed.
        selectDisabled={plugins.length === 0}
      />

      <ListHeader
        icon={Blocks}
        title={t('plugins.list.title')}
        count={installed.length}
        max={maxPlugins}
      />

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
        <EmptyState
          message={t('plugins.list.empty')}
          action={
            <HeaderButton
              variant="primary"
              icon={Plus}
              onClick={() => setWizardOpen(true)}
              disabled={atCap}
              title={atCap ? t('plugins.list.atCap', { max: maxPlugins }) : undefined}
            >
              {t('plugins.list.install')}
            </HeaderButton>
          }
        />
      ) : surface.noMatches(visible.length) ? (
        <ListEmpty>{t('plugins.filter.noMatches')}</ListEmpty>
      ) : (
        groups.map(([origin, groupPlugins]) => {
          const keys = groupPlugins.map(pluginKey);
          // Bare rows (no wrapper div): a deck must see the first card as its
          // first child to clip the collapsed cover, and it provides its own
          // spaced container. The flat path wraps them itself.
          const cards = (
            <AnimatePresence initial={false}>
              {groupPlugins.map((plugin) => (
                <PluginCard
                  key={plugin.name}
                  plugin={plugin}
                  onOpen={() => detail.open(plugin.name)}
                  selection={rowSelection(selection, pluginKey(plugin))}
                />
              ))}
            </AnimatePresence>
          );
          // A lone zip-only group would just re-label the whole list; skip
          // the header and keep today's flat look.
          if (groups.length === 1 && origin === UPLOADED_ORIGIN && !selection.selecting) {
            return (
              <div key={origin} className="flex flex-col [&>*+*]:mt-1.5">
                {cards}
              </div>
            );
          }
          return (
            <GroupDeck
              key={origin}
              id={`plugins:${origin}`}
              title={groupTitle(origin)}
              icon={GROUP_ICON[origin] ?? FolderGit2}
              count={groupPlugins.length}
              enabledCount={groupPlugins.filter((p) => p.enabled).length}
              forceExpanded={surface.forceExpanded}
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
          count={selectedPlugins.length}
          actions={actions}
          progress={surface.progress}
          onExit={selection.exit}
        />
      )}

      {wizardOpen && <PluginInstallWizard onClose={() => setWizardOpen(false)} />}

      <AnimatePresence>
        {detailPlugin && (
          <PluginDetail
            key={detailPlugin.name}
            plugin={detailPlugin}
            onClose={detail.close}
            onOpenComponent={openComponent}
          />
        )}
      </AnimatePresence>
    </div>
  );
}
