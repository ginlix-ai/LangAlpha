import { useTranslation } from 'react-i18next';
import i18n from '@/i18n';
import { LayoutGrid, Zap, ArrowUpRight, Plus } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { useWorkspaces } from '@/hooks/useWorkspaces';
import { clearChatSession } from '@/pages/ChatAgent/hooks/utils/chatSessionRestore';
import type { Workspace } from '@/types/api';
import { registerWidget } from '../framework/WidgetRegistry';
import { useWidgetContextExport } from '../framework/contextSnapshot';
import { serializeRowsToMarkdown, wrapWidgetContext } from '../framework/snapshotSerializers';
import { WorkspacePickerConfigSchema } from '../framework/configSchemas';
import type { WidgetRenderProps } from '../types';

type WorkspacePickerConfig = { limit?: number };

interface WorkspaceRecord extends Workspace {
  status?: string;
  is_pinned?: boolean;
}

// Outside React render — module-level helper. Components that consume this
// MUST call useTranslation() so the surrounding tree re-renders on locale
// switch and picks up the freshly-resolved string.
function formatRelative(ts?: string): string {
  if (!ts) return '';
  const then = new Date(ts).getTime();
  if (Number.isNaN(then)) return '';
  const mins = Math.max(0, Math.floor((Date.now() - then) / 60000));
  if (mins < 1) return i18n.t('dashboard.widgets.common.relativeNow');
  let when: string;
  if (mins < 60) when = `${mins}m`;
  else {
    const hrs = Math.floor(mins / 60);
    if (hrs < 24) when = `${hrs}h`;
    else {
      const days = Math.floor(hrs / 24);
      if (days < 7) when = `${days}d`;
      else {
        const weeks = Math.floor(days / 7);
        if (weeks < 5) when = `${weeks}w`;
        else when = `${Math.floor(days / 30)}mo`;
      }
    }
  }
  return i18n.t('dashboard.widgets.common.relativePast', { when });
}

function WorkspaceTile({
  workspace,
  index,
  onOpen,
}: {
  workspace: WorkspaceRecord;
  index: number;
  onOpen: () => void;
}) {
  const { t } = useTranslation();
  const isFlash = workspace.status === 'flash';
  const relative = formatRelative(workspace.updated_at as string | undefined);
  const idx = String(index + 1).padStart(2, '0');

  return (
    <button
      type="button"
      onClick={onOpen}
      className="group relative flex flex-col text-left overflow-hidden rounded-[10px] border transition-colors duration-200"
      style={{
        background: isFlash
          ? 'linear-gradient(135deg, var(--color-accent-soft) 0%, var(--color-bg-card) 75%)'
          : 'var(--color-bg-card)',
        borderColor: isFlash ? 'var(--color-accent-overlay)' : 'var(--color-border-muted)',
        minHeight: 86,
      }}
      onMouseEnter={(e) => {
        if (!isFlash) e.currentTarget.style.borderColor = 'var(--color-border-default)';
      }}
      onMouseLeave={(e) => {
        if (!isFlash) e.currentTarget.style.borderColor = 'var(--color-border-muted)';
      }}
    >
      <div className="flex items-start justify-between gap-2 px-3.5 pt-3 pb-1.5">
        <div className="flex items-center gap-2 min-w-0">
          {isFlash ? (
            <Zap
              className="h-3.5 w-3.5 flex-shrink-0"
              style={{ color: 'var(--color-accent-primary)' }}
              fill="currentColor"
            />
          ) : (
            <span
              className="text-[10px] dashboard-mono leading-none select-none"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              {idx}
            </span>
          )}
          <span
            className="text-[13px] font-semibold truncate"
            style={{
              color: 'var(--color-text-primary)',
              letterSpacing: '-0.005em',
            }}
          >
            {workspace.name || t('dashboard.widgets.workspacePicker.untitled')}
          </span>
        </div>
        <ArrowUpRight
          className="h-3.5 w-3.5 flex-shrink-0 opacity-0 -translate-x-0.5 translate-y-0.5 transition-all duration-200 group-hover:opacity-100 group-hover:translate-x-0 group-hover:translate-y-0"
          style={{ color: 'var(--color-text-secondary)' }}
        />
      </div>
      <div className="px-3.5 pb-2.5 flex-1 flex flex-col justify-between gap-2">
        {workspace.description ? (
          <div
            className="text-[11.5px] line-clamp-2 leading-snug"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {workspace.description}
          </div>
        ) : (
          <div
            className="text-[11.5px] italic"
            style={{ color: 'var(--color-text-tertiary)', opacity: 0.7 }}
          >
            {isFlash ? t('dashboard.widgets.workspacePicker.flashDesc') : t('dashboard.widgets.workspacePicker.noDescription')}
          </div>
        )}
        {relative ? (
          <div className="flex items-center justify-between text-[10px] dashboard-mono uppercase tracking-wider">
            <span style={{ color: 'var(--color-text-tertiary)', opacity: 0.75 }}>
              {isFlash ? t('dashboard.widgets.workspacePicker.tagFlash') : t('dashboard.widgets.workspacePicker.tagWorkspace')}
            </span>
            <span style={{ color: 'var(--color-text-tertiary)' }}>{relative}</span>
          </div>
        ) : null}
      </div>
    </button>
  );
}

function WorkspacePickerWidget({ instance }: WidgetRenderProps<WorkspacePickerConfig>) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  // Fetch 100 to share the cache key with the other dashboard workspace consumers;
  // slice locally to honor the widget's configured display limit.
  const { data, isLoading } = useWorkspaces({ limit: 100 });
  const displayLimit = instance.config.limit ?? 12;
  const workspaces: WorkspaceRecord[] = ((data?.workspaces ?? []) as WorkspaceRecord[]).slice(0, displayLimit);

  useWidgetContextExport(instance.id, {
    full: () => {
      const tableRows = workspaces.map((ws) => ({
        name: ws.name || t('dashboard.widgets.workspacePicker.untitled'),
        status: ws.status ?? 'workspace',
        // Newlines in descriptions would corrupt the single-row markdown table;
        // collapse them to spaces here. Pipe-escaping is handled by serializeRowsToMarkdown.
        description: (ws.description ?? '').replace(/\n/g, ' '),
        updated_at: ws.updated_at ?? '',
        workspace_id: ws.workspace_id,
      }));
      const body = workspaces.length
        ? serializeRowsToMarkdown(tableRows, [
            { key: 'name', label: 'name' },
            { key: 'status', label: 'status' },
            { key: 'description', label: 'description' },
            { key: 'updated_at', label: 'updated_at' },
            { key: 'workspace_id', label: 'workspace_id' },
          ])
        : '_no workspaces_';
      const text = wrapWidgetContext('workspace.picker', { count: workspaces.length }, body);
      return {
        widget_type: 'workspace.picker',
        widget_id: instance.id,
        label: `${t('dashboard.widgets.workspacePicker.title')} · ${workspaces.length}`,
        description: workspaces.length
          ? `${workspaces.length} workspace${workspaces.length === 1 ? '' : 's'}`
          : 'empty',
        captured_at: new Date().toISOString(),
        text,
        data: {
          workspaces: workspaces.map((ws) => ({
            workspace_id: ws.workspace_id,
            name: ws.name,
            description: ws.description,
            status: ws.status,
            updated_at: ws.updated_at,
          })),
        },
      };
    },
  });

  return (
    <div className="dashboard-glass-card p-5 flex flex-col h-full">
      <div className="flex items-baseline justify-between mb-4 pb-3 border-b" style={{ borderColor: 'var(--color-border-muted)' }}>
        <div className="flex items-baseline gap-2.5 min-w-0">
          <LayoutGrid className="h-3.5 w-3.5 flex-shrink-0 self-center" style={{ color: 'var(--color-text-tertiary)' }} />
          <span
            className="text-[10px] font-semibold uppercase tracking-[0.14em]"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {t('dashboard.widgets.workspacePicker.header')}
          </span>
          <span
            className="title-font text-lg leading-none dashboard-mono"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {workspaces.length}
          </span>
        </div>
        <button
          type="button"
          onClick={() => {
            clearChatSession();
            navigate('/chat');
          }}
          className="group flex items-center gap-1 text-[11px] uppercase tracking-wider transition-colors"
          style={{ color: 'var(--color-text-tertiary)' }}
          onMouseEnter={(e) => {
            e.currentTarget.style.color = 'var(--color-text-primary)';
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.color = 'var(--color-text-tertiary)';
          }}
        >
          <span>{t('dashboard.widgets.workspacePicker.viewAll')}</span>
          <ArrowUpRight className="h-3 w-3 transition-transform group-hover:translate-x-0.5 group-hover:-translate-y-0.5" />
        </button>
      </div>

      <div className="flex-1 min-h-0 overflow-y-auto -mx-1 px-1">
        {isLoading && workspaces.length === 0 ? (
          <div className="grid grid-cols-2 gap-2">
            {Array.from({ length: 4 }).map((_, i) => (
              <div
                key={i}
                className="rounded-[10px] animate-pulse"
                style={{ backgroundColor: 'var(--color-bg-subtle)', height: 86 }}
              />
            ))}
          </div>
        ) : workspaces.length === 0 ? (
          <div className="h-full flex flex-col items-center justify-center gap-3 py-6">
            <div
              className="h-9 w-9 rounded-full flex items-center justify-center"
              style={{ backgroundColor: 'var(--color-bg-subtle)' }}
            >
              <Plus className="h-4 w-4" style={{ color: 'var(--color-text-tertiary)' }} />
            </div>
            <div className="text-center">
              <div
                className="dashboard-mono text-sm mb-0.5"
                style={{ color: 'var(--color-text-primary)' }}
              >
                {t('dashboard.widgets.workspacePicker.empty')}
              </div>
              <button
                type="button"
                onClick={() => {
                  clearChatSession();
                  navigate('/chat');
                }}
                className="text-[11px] uppercase tracking-wider underline-offset-4 hover:underline"
                style={{ color: 'var(--color-text-secondary)' }}
              >
                {t('dashboard.widgets.workspacePicker.emptyCta')}
              </button>
            </div>
          </div>
        ) : (
          <div className="grid grid-cols-2 gap-2">
            {workspaces.map((ws, i) => (
              <WorkspaceTile
                key={ws.workspace_id}
                workspace={ws}
                index={i}
                onOpen={() => {
                  clearChatSession();
                  navigate(`/chat/${ws.workspace_id}`);
                }}
              />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

registerWidget<WorkspacePickerConfig>({
  type: 'workspace.picker',
  titleKey: 'dashboard.widgets.workspacePicker.title',
  descriptionKey: 'dashboard.widgets.workspacePicker.description',
  category: 'workspace',
  icon: LayoutGrid,
  component: WorkspacePickerWidget,
  defaultConfig: { limit: 12 },
  configSchema: WorkspacePickerConfigSchema,
  defaultSize: { w: 6, h: 22 },
  minSize: { w: 3, h: 15 },
});

export default WorkspacePickerWidget;
