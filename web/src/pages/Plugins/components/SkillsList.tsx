import { useState, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { AnimatePresence } from '@/lib/framer';
import { Upload } from 'lucide-react';
import { HeaderButton } from '@/components/mcp/McpPrimitives';
import { usePlugins } from '@/hooks/usePlugins';
import { useSkills, useUploadSkill } from '@/hooks/useSkills';
import { BrandMark } from '@/pages/ChatAgent/components/mcp/BrandMark';
import type { SkillInfo } from '@/pages/ChatAgent/utils/api';
import { matchesFilter } from '../utils/groupOrigins';
import { isBundled, pluginMark } from '../utils/pluginSurface';
import {
  isEffectivelyEnabled,
  skillScopeId,
  skillSections,
  type PluginSkillGroup,
} from '../utils/provenance';
import { skillRowKey } from '../utils/rowKeys';
import { useAddIntent } from '../hooks/useAddIntent';
import { useDetailParam } from '../hooks/useDetailParam';
import { useSectionNav } from '../hooks/useSectionNav';
import { usePluginListSurface } from '../hooks/usePluginListSurface';
import { useSkillActions } from '../hooks/useSkillActions';
import { useSkillBulkActions } from '../hooks/useSkillBulkActions';
import { useHomeOption, useWorkspaceOptions } from '../hooks/useWorkspaceOptions';
import { CatalogStart } from './CatalogStart';
import { EmptyState } from './EmptyState';
import { PluginListFrame } from './PluginListFrame';
import { SkillDetail } from './SkillDetail';
import { SkillUploadModal } from './SkillUploadModal';
import { CellModeProvider, SectionLink, SurfaceGrid, SurfaceSection } from './surface/Surface';
import { SkillListCell } from './pluginsPage/SkillListCell';
import { partialReachText } from './pluginsPage/pluginReach';
import { sourceLabel } from './pluginsPage/pluginStatus';
import { useCatalogPlugins } from '../hooks/useCatalogPlugins';
import './pluginsPage/pluginsPage.css';

/**
 * Skills → Yours, grouped by who put each skill here: what the user added
 * (to every workspace, or to one), then one group per plugin that brought
 * skills, installed packages before the ones that ship with the app. Shipped
 * groups fold after three rows; their skills are switched off, never removed.
 *
 * The list asks for switched-off rows too. The slash menu elsewhere reads the
 * enabled-only default, so a skill switched off here leaves the menu but stays
 * listed here.
 */

export function SkillsList() {
  const { t } = useTranslation();
  const catalogPlugins = useCatalogPlugins();
  const { openDetail } = useSectionNav();
  const skillsQuery = useSkills({
    includeDisabled: true,
    allScopes: true,
  });
  const { data: skills, isLoading } = skillsQuery;
  const { data: pluginData } = usePlugins();
  const uploadMutation = useUploadSkill();
  const [uploadOpen, setUploadOpen] = useState(false);
  // "Edit command" from a cell's menu opens the skill's page with its command
  // box already open.
  const [editCommandOf, setEditCommandOf] = useState<string | null>(null);
  const { workspaces: wsOptions, loading: wsLoading, nameById: wsNameById } = useWorkspaceOptions();
  const home = useHomeOption();
  const actions = useSkillActions();

  useAddIntent({ skill: () => setUploadOpen(true) });

  const allSkills = skills ?? [];
  const plugins = pluginData?.plugins ?? [];

  // Nothing of the user's yet, uploaded or from an installed plugin: the
  // catalog comes first.
  const fromInstalled = (name: string) =>
    plugins.some((p) => p.name === name && !isBundled(p));
  const ownNone = !allSkills.some((s) =>
    s.plugin_name ? fromInstalled(s.plugin_name) : s.origin === 'user' || s.origin === 'workspace',
  );
  // A skill has no sign-in or key to go wrong, so it never needs attention
  // and that pill stays hidden here.
  const list = usePluginListSurface({
    rows: allSkills,
    matches: (s, query) => matchesFilter(query, s.name, s.description, s.plugin_name, s.command),
    key: skillRowKey,
    name: (s) => s.name,
    isOn: isEffectivelyEnabled,
    justAdded: 'skill',
    startWhen: !!skills && !!pluginData && ownNone,
  });
  const { visible } = list;

  // Names are unique within a scope; `dws` picks the workspace-tier row when
  // one shadows a same-named skill of the account.
  const detail = useDetailParam<SkillInfo>(
    'skill',
    (ref) =>
      allSkills.find((s) => s.name === ref.name && (s.workspace_id ?? null) === ref.workspaceId) ??
      null,
    !isLoading && skills !== undefined,
  );
  const detailSkill = detail.target;

  const { own, workspace: oneWorkspace, plugins: pluginGroups, builtIn: operator } = skillSections(
    visible,
    plugins,
  );

  const openPlugin = (name: string) => openDetail({ kind: 'plugin', name });

  const bulk = useSkillBulkActions(visible, list, wsOptions);
  const fold = (rows: SkillInfo[]) => list.fold(3, rows.map((s) => s.name));

  function renderCell(skill: SkillInfo) {
    const key = skillRowKey(skill);
    const wsId = skillScopeId(skill);
    const confirming =
      !!actions.deleting &&
      actions.deleting.name === skill.name &&
      actions.deleting.workspaceId === wsId;
    // A workspace's own skill names its workspace; an account skill names
    // its reach when that is not everywhere.
    const meta =
      skill.origin === 'workspace'
        ? (wsNameById.get(skill.workspace_id ?? '') ?? t('plugins.scope.unknownWorkspace'))
        : wsLoading
          ? null
          : partialReachText(t, skill, wsOptions, home);
    return (
      <CellModeProvider key={key} value={list.cellMode(skill)}>
        <SkillListCell
          skill={skill}
          meta={meta}
          toggling={actions.togglingName === key}
          confirmingRemove={confirming}
          removing={confirming && actions.deletePending}
          onOpen={() => {
            setEditCommandOf(null);
            detail.open(skill.name, skill.workspace_id ?? null);
          }}
          onToggle={(enabled) => void actions.toggle(skill, enabled)}
          onEditCommand={() => {
            setEditCommandOf(key);
            detail.open(skill.name, skill.workspace_id ?? null);
          }}
          onRemove={skill.deletable ? () => actions.requestDelete(skill.name, wsId) : undefined}
          onConfirmRemove={() => void actions.confirmDelete()}
          onCancelRemove={actions.cancelDelete}
        />
      </CellModeProvider>
    );
  }

  function pluginSection(group: PluginSkillGroup): ReactNode {
    const note = group.plugin
      ? sourceLabel(t, group.plugin, catalogPlugins)
      : group.builtIn
        ? t('plugins.skillsPage.builtIn')
        : t('plugins.skillsPage.fromCatalog');
    const off = group.plugin ? !group.plugin.enabled : group.rows[0]?.plugin_enabled === false;
    return (
      <SurfaceSection
        key={group.name}
        title={
          group.plugin ? (
            <span className="pp-group-title">
              <BrandMark name={group.name} size="xs" {...pluginMark(group.plugin)} />
              {group.name}
            </span>
          ) : (
            group.name
          )
        }
        count={group.rows.length}
        note={off ? `${note} · ${t('plugins.skillsPage.pluginOff')}` : note}
        action={
          group.plugin ? (
            <SectionLink onClick={() => openPlugin(group.name)}>{t('plugins.skillsPage.openPlugin')}</SectionLink>
          ) : undefined
        }
      >
        <SurfaceGrid {...(group.builtIn ? fold(group.rows) : {})}>
          {group.rows.map(renderCell)}
        </SurfaceGrid>
      </SurfaceSection>
    );
  }

  return (
    <>
      <PluginListFrame
        list={list}
        query={skillsQuery}
        loadFailed={t('plugins.loadFailed.skills')}
        noun={t('plugins.filter.nounSkills')}
        catalogScope="skills"
        bulk={bulk}
        hidden={!!detailSkill}
        className="pp-list"
      >
        {list.showStart && <CatalogStart scope="skills" onAdd={() => setUploadOpen(true)} />}
        {/* Always there unnarrowed, as the place an upload lands; the first-run
            block above already carries the upload, so it says nothing twice. */}
        {(own.length > 0 || (!list.narrowed && !list.showStart)) && (
          <SurfaceSection title={t('plugins.skillsPage.addedByYou')} count={own.length || undefined}>
            {own.length > 0 ? (
              <SurfaceGrid>{own.map(renderCell)}</SurfaceGrid>
            ) : (
              <EmptyState
                compact
                message={t('plugins.skillsPage.emptyOwn')}
                action={
                  <HeaderButton variant="secondary" icon={Upload} onClick={() => setUploadOpen(true)}>
                    {t('plugins.addMenu.uploadSkill')}
                  </HeaderButton>
                }
              />
            )}
          </SurfaceSection>
        )}
        {oneWorkspace.length > 0 && (
          <SurfaceSection title={t('plugins.skillsPage.oneWorkspace')} count={oneWorkspace.length}>
            <SurfaceGrid>{oneWorkspace.map(renderCell)}</SurfaceGrid>
          </SurfaceSection>
        )}
        {pluginGroups.map(pluginSection)}
        {operator.length > 0 && (
          <SurfaceSection title={t('plugins.skillsPage.builtIn')} count={operator.length}>
            <SurfaceGrid {...fold(operator)}>
              {operator.map(renderCell)}
            </SurfaceGrid>
          </SurfaceSection>
        )}
      </PluginListFrame>

      {detailSkill && (
        <SkillDetail
          key={skillRowKey(detailSkill)}
          skill={detailSkill}
          onClose={detail.close}
          actions={actions}
          workspaces={wsOptions}
          workspacesLoading={wsLoading}
          workspaceName={wsNameById.get(detailSkill.workspace_id ?? '') ?? null}
          plugin={plugins.find((p) => p.name === detailSkill.plugin_name) ?? null}
          startEditingCommand={editCommandOf === skillRowKey(detailSkill)}
          onOpenPlugin={openPlugin}
        />
      )}

      <AnimatePresence>
        {uploadOpen && (
          <SkillUploadModal
            onClose={() => setUploadOpen(false)}
            onUpload={(file, onProgress) => uploadMutation.mutateAsync({ file, onProgress })}
          />
        )}
      </AnimatePresence>
    </>
  );
}
