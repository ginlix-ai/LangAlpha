import { useState, type ReactNode } from 'react';
import { useNavigate, useSearchParams } from 'react-router';
import { useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { Copy, FilePlus, Power, RotateCcw, Trash2 } from 'lucide-react';
import { ConfirmStrip, HeaderButton } from '@/components/mcp/McpPrimitives';
import { DropdownMenuItem, DropdownMenuSeparator } from '@/components/ui/dropdown-menu';
import { toast } from '@/components/ui/use-toast';
import { useAllModels } from '@/hooks/useAllModels';
import { resolveChatWorkspace } from '@/hooks/useHomeWorkspace';
import { usePlugins } from '@/hooks/usePlugins';
import {
  invalidateSubagents,
  sendSubagentEdit,
  useDeleteSubagent,
  usePatchSubagent,
  useSubagents,
} from '@/hooks/useSubagents';
import { useLocale } from '@/hooks/useLocale';
import { formatList } from '@/lib/format';
import { BrandMark } from '@/pages/ChatAgent/components/mcp/BrandMark';
import {
  deleteSubagent,
  type PluginInfo,
  type SubagentPlugin,
  type SubagentView,
} from '@/pages/ChatAgent/utils/api';
import { buildBulkActions } from '../../utils/bulkActions';
import { matchesFilter } from '../../utils/groupOrigins';
import { pluginMark } from '../../utils/pluginSurface';
import { NEW_SUBAGENT_PARAM, withDetail } from '../../utils/detailParam';
import { useAddIntent } from '../../hooks/useAddIntent';
import { useCatalogPlugins } from '../../hooks/useCatalogPlugins';
import { useDetailParam } from '../../hooks/useDetailParam';
import { markJustAdded } from '../../hooks/useJustAdded';
import { usePluginListSurface } from '../../hooks/usePluginListSurface';
import { useSectionNav } from '../../hooks/useSectionNav';
import { EmptyState } from '../EmptyState';
import { PluginListFrame } from '../PluginListFrame';
import {
  CellMenu,
  CellModeProvider,
  CellState,
  SectionLink,
  StatusDot,
  SurfaceCell,
  SurfaceGrid,
  SurfaceSection,
} from '../surface/Surface';
import { sourceLabel } from '../pluginsPage/pluginStatus';
import { SubagentDetail } from './SubagentDetail';
import { SubagentNew } from './SubagentNew';
import {
  capitalize,
  fieldsPhrase,
  isCustom,
  isOrphan,
  isResettable,
  modelMeta,
  problemText,
  removalCopy,
  resetLabel,
  untouchedNote,
} from './subagentText';
import '../pluginsPage/pluginsPage.css';
import './subagents.css';

/**
 * Subagents → Yours, in the Skills list's grammar: what the user wrote (and
 * any override whose base is gone, which only removing clears), one group per
 * plugin that ships subagents, then the built-ins. Those stay unfolded: there
 * are only a handful, and each is a role the user may want to change.
 *
 * On/off lives in the cell's menu, the bulk bar and the page header, never on
 * the cell. A base is never removed, only reset: its menu offers that once the
 * user has changed it, and says why it can't go before then.
 */

export function SubagentsList() {
  const { t } = useTranslation();
  const locale = useLocale();
  const qc = useQueryClient();
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const catalogPlugins = useCatalogPlugins();
  const { openDetail } = useSectionNav();
  const subagentsQuery = useSubagents();
  const { data, isLoading } = subagentsQuery;
  const { data: pluginData } = usePlugins();
  const models = useAllModels();
  const patch = usePatchSubagent();
  const remove = useDeleteSubagent();
  // The row whose removal is being confirmed.
  const [pending, setPending] = useState<string | null>(null);

  const rows = data?.subagents ?? [];
  const plugins = pluginData?.plugins ?? [];
  const newFrom = searchParams.get(NEW_SUBAGENT_PARAM);

  // Pushed, like a detail: Back closes the form. Opening it closes any page
  // it was asked from, so Duplicate on a page lands on the copy, not under it.
  function openNew(from: string | null) {
    const next = withDetail(searchParams, null);
    next.delete('add');
    next.set(NEW_SUBAGENT_PARAM, from ?? '');
    setSearchParams(next);
  }
  function closeNew() {
    const next = new URLSearchParams(searchParams);
    next.delete(NEW_SUBAGENT_PARAM);
    setSearchParams(next, { replace: true });
  }
  function created(name: string) {
    const next = withDetail(searchParams, { kind: 'subagent', name });
    next.delete(NEW_SUBAGENT_PARAM);
    setSearchParams(next, { replace: true });
    markJustAdded('subagent', name);
  }

  // A new Home thread with the request half written: the Chief of Staff can
  // write the file, which a workspace's analyst could too but would leave in
  // that workspace's thread list rather than the user's own.
  async function startChat() {
    try {
      const workspaceId = await resolveChatWorkspace(qc, 'all');
      navigate('/chat/t/__default__', {
        state: { workspaceId, draftMessage: t('plugins.subagents.chatPrefill') },
      });
    } catch {
      toast({ variant: 'destructive', title: t('plugins.subagents.chatFailed') });
    }
  }

  useAddIntent({
    subagent: () => openNew(null),
    'subagent-chat': () => void startChat(),
  });

  const list = usePluginListSurface({
    rows,
    matches: (s, query) =>
      matchesFilter(query, s.name, s.description, s.plugin?.display_name, s.plugin?.name),
    key: (s) => s.name,
    name: (s) => s.name,
    isOn: (s) => s.enabled && !isOrphan(s),
    needsUser: (s) => s.problems.length > 0,
    justAdded: 'subagent',
    startWhen: false,
    invalidate: invalidateSubagents,
  });
  const { visible } = list;

  const detail = useDetailParam<SubagentView>(
    'subagent',
    (ref) => rows.find((s) => s.name === ref.name) ?? null,
    !isLoading && data !== undefined,
  );
  const detailView = detail.target;

  const own = visible.filter((s) => isCustom(s) || isOrphan(s));
  const pluginGroups = groupByPlugin(visible.filter((s) => !isCustom(s) && !isOrphan(s)));
  const builtIn = visible.filter((s) => !isCustom(s) && !isOrphan(s) && s.source !== 'plugin');

  const selected = visible.filter((s) => list.selection.selected.has(s.name));
  const bulk = buildBulkActions(t, list, selected, {
    noun: 'subagent',
    key: (s) => s.name,
    name: (s) => s.name,
    isOn: (s) => s.enabled && !isOrphan(s),
    // An orphan runs nowhere, so switching it on would change nothing.
    enableLocked: isOrphan,
    setEnabled: (s, on) => sendSubagentEdit(qc, s.name, { enabled: on }),
    remove: {
      verb: 'remove',
      labelKey: 'plugins.bulk.remove',
      confirmKey: 'plugins.bulk.confirmRemoveSubagents',
      eligible: (s) => isCustom(s) || isOrphan(s),
      run: (s) => deleteSubagent(s.name, s.version),
    },
    keptKey: 'plugins.bulk.keptBuiltInSubagents',
  });

  const providerName = (name: string) => models.searchProviders?.[name]?.display_name ?? name;

  function confirmPending() {
    if (!pending) return;
    remove.mutate(pending, { onSettled: () => setPending(null) });
  }

  function renderCell(view: SubagentView) {
    return (
      <CellModeProvider key={view.name} value={list.cellMode(view)}>
        <SubagentListCell
          view={view}
          metadata={models.metadata}
          providerName={providerName}
          asking={pending === view.name}
          busy={remove.isPending}
          onOpen={() => detail.open(view.name)}
          onToggle={() => patch.mutate({ name: view.name, edit: { enabled: !view.enabled } })}
          onDuplicate={() => openNew(view.name)}
          onAsk={() => setPending(view.name)}
          onConfirm={confirmPending}
          onCancel={() => setPending(null)}
          resetFields={fieldsPhrase(t, view.user_fields, locale, formatList)}
        />
      </CellModeProvider>
    );
  }

  function pluginSection(group: PluginGroup): ReactNode {
    const info: PluginInfo | undefined = plugins.find((p) => p.name === group.plugin.name);
    const note = info ? sourceLabel(t, info, catalogPlugins) : t('plugins.skillsPage.fromCatalog');
    return (
      <SurfaceSection
        key={group.plugin.name}
        title={
          <span className="pp-group-title">
            <BrandMark name={group.plugin.name} size="xs" {...(info ? pluginMark(info) : { kind: 'plugin' as const })} />
            {group.plugin.display_name}
          </span>
        }
        count={group.rows.length}
        note={note}
        action={
          info ? (
            <SectionLink onClick={() => openDetail({ kind: 'plugin', name: info.name })}>
              {t('plugins.skillsPage.openPlugin')}
            </SectionLink>
          ) : undefined
        }
      >
        <SurfaceGrid>{group.rows.map(renderCell)}</SurfaceGrid>
      </SurfaceSection>
    );
  }

  const formOpen = newFrom !== null;
  return (
    <>
      <PluginListFrame
        list={list}
        query={subagentsQuery}
        loadFailed={t('plugins.loadFailed.subagents')}
        noun={t('plugins.filter.nounSubagents')}
        bulk={bulk}
        hidden={!!detailView || formOpen}
        className="pp-list"
      >
        {(own.length > 0 || !list.narrowed) && (
          <SurfaceSection title={t('plugins.subagents.addedByYou')} count={own.length || undefined}>
            {own.length > 0 ? (
              <SurfaceGrid>{own.map(renderCell)}</SurfaceGrid>
            ) : (
              <EmptyState
                compact
                message={t('plugins.subagents.emptyOwn')}
                action={
                  <HeaderButton variant="secondary" icon={FilePlus} onClick={() => openNew(null)}>
                    {t('plugins.addMenu.newSubagent')}
                  </HeaderButton>
                }
              />
            )}
          </SurfaceSection>
        )}
        {pluginGroups.map(pluginSection)}
        {builtIn.length > 0 && (
          <SurfaceSection title={t('plugins.subagents.builtIn')} count={builtIn.length}>
            <SurfaceGrid>{builtIn.map(renderCell)}</SurfaceGrid>
          </SurfaceSection>
        )}
      </PluginListFrame>

      {data && detailView && !formOpen && (
        <SubagentDetail
          key={detailView.name}
          view={detailView}
          data={data}
          models={models}
          onClose={detail.close}
          onDuplicate={() => openNew(detailView.name)}
          onOpenPlugin={(name) => openDetail({ kind: 'plugin', name })}
        />
      )}
      {data && formOpen && (
        <SubagentNew
          key={newFrom}
          data={data}
          models={models}
          startFrom={newFrom || null}
          onClose={closeNew}
          onCreated={created}
        />
      )}
    </>
  );
}

interface PluginGroup {
  plugin: SubagentPlugin;
  rows: SubagentView[];
}

/** The plugins' rows, one group per plugin; every other row is left out. */
function groupByPlugin(rows: SubagentView[]): PluginGroup[] {
  const groups = new Map<string, PluginGroup>();
  for (const row of rows) {
    if (row.source !== 'plugin') continue;
    const group = groups.get(row.plugin.name);
    if (group) group.rows.push(row);
    else groups.set(row.plugin.name, { plugin: row.plugin, rows: [row] });
  }
  return [...groups.values()].sort((a, b) => a.plugin.display_name.localeCompare(b.plugin.display_name));
}

/**
 * One subagent as a cell. The meta line is the model; a problem takes its
 * place, since a model the subagent cannot run on is itself one of them.
 */
function SubagentListCell({
  view,
  metadata,
  providerName,
  asking,
  busy,
  onOpen,
  onToggle,
  onDuplicate,
  onAsk,
  onConfirm,
  onCancel,
  resetFields,
}: {
  view: SubagentView;
  metadata: ReturnType<typeof useAllModels>['metadata'];
  providerName: (name: string) => string;
  /** Its removal is being confirmed. */
  asking: boolean;
  busy: boolean;
  onOpen: () => void;
  onToggle: () => void;
  onDuplicate: () => void;
  onAsk: () => void;
  onConfirm: () => void;
  onCancel: () => void;
  resetFields: string | null;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const orphan = isOrphan(view);
  const custom = isCustom(view);
  const on = view.enabled && !orphan;

  if (asking) {
    return (
      <div className="pp-confirm">
        <ConfirmStrip
          {...removalCopy(t, view, resetFields, busy)}
          cancelLabel={t('plugins.skills.deleteConfirmNo')}
          pending={busy}
          onConfirm={onConfirm}
          onCancel={onCancel}
        />
      </div>
    );
  }

  const problem = view.problems[0];
  const modelParts = modelMeta(t, view, metadata);
  if (view.edited) modelParts.push(t('plugins.subagents.edited'));
  const meta = (
    <span className="sa-meta">
      {problem ? (
        <>
          <StatusDot tone="attn" />
          <span className="sa-attn">{capitalize(problemText(t, problem, providerName), locale)}</span>
        </>
      ) : (
        <span className="pp-cell-meta">{modelParts.join(' · ')}</span>
      )}
    </span>
  );

  return (
    <SurfaceCell
      mark={<BrandMark name={view.name} kind="subagent" size="md" />}
      name={view.name}
      meta={meta}
      line={view.description || null}
      off={!on}
      trailing={<CellState state={on ? 'on' : 'off'} />}
      onOpen={onOpen}
      menu={
        <CellMenu label={t('mcp.row.actionsAria', { name: view.name })}>
          {!orphan && (
            <>
              <DropdownMenuItem onSelect={onToggle}>
                <Power className="h-3.5 w-3.5" />
                {view.enabled ? t('plugins.pluginsPage.switchOff') : t('plugins.pluginsPage.switchOn')}
              </DropdownMenuItem>
              <DropdownMenuItem onSelect={onDuplicate}>
                <Copy className="h-3.5 w-3.5" />
                {t('plugins.subagents.duplicate')}
              </DropdownMenuItem>
              <DropdownMenuSeparator />
            </>
          )}
          {custom || orphan ? (
            <DropdownMenuItem variant="destructive" onSelect={onAsk}>
              <Trash2 className="h-3.5 w-3.5" />
              {t('plugins.subagents.remove')}
            </DropdownMenuItem>
          ) : isResettable(view) ? (
            <DropdownMenuItem onSelect={onAsk}>
              <RotateCcw className="h-3.5 w-3.5" />
              {resetLabel(t, view)}
            </DropdownMenuItem>
          ) : (
            <p className="pp-menu-note">{untouchedNote(t, view)}</p>
          )}
        </CellMenu>
      }
    />
  );
}
