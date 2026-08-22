import { useId, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { ChevronRight, Download, RefreshCw, Trash2 } from 'lucide-react';
import { toast } from '@/components/ui/use-toast';
import { IdentityTile } from '@/pages/ChatAgent/components/mcp/IdentityTile';
import {
  ConfirmStrip,
  EnabledToggle,
  HeaderButton,
} from '@/pages/ChatAgent/components/mcp/McpPrimitives';
import {
  useDeletePlugin,
  useTogglePlugin,
  useUpdatePlugin,
  useUpdatePluginFromZip,
} from '@/hooks/usePlugins';
import { createDateFormatter } from '@/lib/format';
import {
  exportPluginBlobUrl,
  formatApiErrorDetail,
  type PluginInfo,
  type PluginInstallReport,
} from '@/pages/ChatAgent/utils/api';
import {
  DetailField,
  DetailHeader,
  DetailOverlay,
  DetailSection,
} from './DetailOverlay';

/**
 * A plugin's detail overlay. The lifecycle verbs (Update, Export, Uninstall)
 * live here rather than on the card: the card names the plugin, this view
 * manages it. Component rows link through to their own detail views on the
 * MCP and Skills tabs — the plugin never re-renders what those tabs own.
 */

const formatDate = createDateFormatter({ dateStyle: 'medium' });

function summarizeUpdate(report: PluginInstallReport): string {
  const counts = new Map<string, number>();
  for (const c of report.components) {
    counts.set(c.status, (counts.get(c.status) ?? 0) + 1);
  }
  if (counts.size === 0) return '';
  return [...counts.entries()].map(([s, n]) => `${n} ${s}`).join(', ');
}

export function PluginDetail({
  plugin,
  onClose,
  onOpenComponent,
}: {
  plugin: PluginInfo;
  onClose: () => void;
  /** Navigate to a component's own detail view (switches tab). */
  onOpenComponent: (kind: 'mcp' | 'skill', name: string) => void;
}) {
  const { t } = useTranslation();
  const labelId = useId();
  const toggleMutation = useTogglePlugin();
  const deleteMutation = useDeletePlugin();
  const updateMutation = useUpdatePlugin();
  const updateZipMutation = useUpdatePluginFromZip();
  const zipInputRef = useRef<HTMLInputElement>(null);
  const [confirmingDelete, setConfirmingDelete] = useState(false);
  const [exporting, setExporting] = useState(false);

  const busy =
    toggleMutation.isPending ||
    updateMutation.isPending ||
    updateZipMutation.isPending;

  async function handleToggle() {
    try {
      await toggleMutation.mutateAsync({
        name: plugin.name,
        enabled: !plugin.enabled,
      });
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.card.toggleFailed'),
        description: formatApiErrorDetail(err),
      });
    }
  }

  function announceUpdate(report: PluginInstallReport) {
    const summary = summarizeUpdate(report);
    toast({
      title: t('plugins.card.updatedTitle', { plugin: plugin.name }),
      description: summary || t('plugins.card.updatedNoChanges'),
    });
  }

  async function handleUpdate() {
    if (plugin.source_type === 'zip') {
      zipInputRef.current?.click();
      return;
    }
    try {
      const result = await updateMutation.mutateAsync(plugin.name);
      announceUpdate(result.report);
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.card.updateFailed'),
        description: formatApiErrorDetail(err),
      });
    }
  }

  async function handleUpdateZip(file: File | null | undefined) {
    if (!file) return;
    try {
      const result = await updateZipMutation.mutateAsync({
        name: plugin.name,
        file,
      });
      announceUpdate(result.report);
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.card.updateFailed'),
        description: formatApiErrorDetail(err),
      });
    } finally {
      if (zipInputRef.current) zipInputRef.current.value = '';
    }
  }

  async function handleExport() {
    setExporting(true);
    try {
      const url = await exportPluginBlobUrl(plugin.name);
      const anchor = document.createElement('a');
      anchor.href = url;
      anchor.download = `${plugin.name}.zip`;
      anchor.click();
      URL.revokeObjectURL(url);
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.card.exportFailed'),
        description: formatApiErrorDetail(err),
      });
    } finally {
      setExporting(false);
    }
  }

  async function handleDelete() {
    try {
      const result = await deleteMutation.mutateAsync(plugin.name);
      setConfirmingDelete(false);
      toast({
        title: t('plugins.card.uninstalledTitle', { plugin: plugin.name }),
        description: t('plugins.card.uninstalledDesc', {
          servers: t('plugins.card.serverCount', { count: result.deleted.servers.length }),
          skills: t('plugins.card.skillCount', { count: result.deleted.skills.length }),
        }),
      });
      // The plugin row is gone; there is nothing left to detail.
      onClose();
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.card.uninstallFailed'),
        description: formatApiErrorDetail(err),
      });
    }
  }

  const homepage =
    plugin.homepage && /^https?:\/\//i.test(plugin.homepage) ? plugin.homepage : null;

  return (
    <DetailOverlay
      labelId={labelId}
      onClose={onClose}
      footer={
        confirmingDelete ? (
          <ConfirmStrip
            message={t('plugins.card.uninstallConfirm', { plugin: plugin.name })}
            confirmLabel={
              deleteMutation.isPending
                ? t('common.loading')
                : t('plugins.card.uninstallConfirmYes')
            }
            cancelLabel={t('plugins.card.uninstallConfirmNo')}
            pending={deleteMutation.isPending}
            onConfirm={handleDelete}
            onCancel={() => setConfirmingDelete(false)}
          />
        ) : (
          <div className="flex items-center gap-1.5 flex-wrap">
            <HeaderButton
              variant="secondary"
              icon={RefreshCw}
              onClick={handleUpdate}
              disabled={busy}
            >
              {t('plugins.card.update')}
            </HeaderButton>
            <HeaderButton
              variant="secondary"
              icon={Download}
              onClick={handleExport}
              disabled={exporting}
            >
              {t('plugins.card.export')}
            </HeaderButton>
            <HeaderButton
              variant="ghost"
              icon={Trash2}
              onClick={() => setConfirmingDelete(true)}
              disabled={deleteMutation.isPending}
              style={{ color: 'var(--color-loss)' }}
            >
              {t('plugins.card.uninstall')}
            </HeaderButton>
          </div>
        )
      }
      header={
        <DetailHeader
          name={plugin.name}
          labelId={labelId}
          kind={t('plugins.detail.kindPlugin')}
          meta={
            <>
              {plugin.version && <span>v{plugin.version}</span>}
              <span>
                {plugin.source_type === 'zip'
                  ? t('plugins.card.sourceZip')
                  : t('plugins.card.sourceRemote')}
              </span>
              {!plugin.enabled && <span>{t('plugins.card.disabledState')}</span>}
            </>
          }
          controls={
            <EnabledToggle
              enabled={plugin.enabled}
              name={plugin.name}
              disabled={busy}
              onToggle={handleToggle}
            />
          }
        />
      }
    >
      {plugin.description && (
        <p className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
          {plugin.description}
        </p>
      )}

      {plugin.components.length > 0 && (
        <DetailSection
          title={t('plugins.detail.components')}
          count={plugin.components.length}
        >
          <div className="flex flex-col gap-1">
            {plugin.components.map((component) => (
              <button
                key={`${component.kind}:${component.key}`}
                type="button"
                onClick={() => onOpenComponent(component.kind, component.name)}
                aria-label={t('plugins.detail.openComponentAria', {
                  name: component.name,
                })}
                className="group flex items-center gap-2 rounded-md px-2 py-1.5 text-left transition-colors hover:bg-foreground/10"
              >
                <IdentityTile name={component.name} />
                <span
                  className="min-w-0 flex-1 truncate text-xs font-medium"
                  style={{ color: 'var(--color-text-primary)' }}
                >
                  {component.name}
                </span>
                <span
                  className="text-[0.6875rem] flex-shrink-0"
                  style={{ color: 'var(--color-text-tertiary)' }}
                >
                  {component.kind === 'mcp'
                    ? t('plugins.detail.kindServer')
                    : t('plugins.detail.kindSkill')}
                </span>
                <ChevronRight
                  className="h-3.5 w-3.5 flex-shrink-0 transition-transform duration-150 motion-safe:group-hover:translate-x-0.5"
                  style={{ color: 'var(--color-text-quaternary)' }}
                />
              </button>
            ))}
          </div>
        </DetailSection>
      )}

      <DetailSection title={t('plugins.detail.info')}>
        <div className="flex flex-col gap-1.5">
          {plugin.version && (
            <DetailField label={t('plugins.detail.version')}>
              {plugin.version}
            </DetailField>
          )}
          {plugin.author && (
            <DetailField label={t('plugins.detail.author')}>
              {plugin.author}
            </DetailField>
          )}
          {homepage && (
            <DetailField label={t('plugins.detail.homepage')}>
              <a
                href={homepage}
                target="_blank"
                rel="noopener noreferrer"
                className="hover:underline underline-offset-2"
              >
                {homepage}
              </a>
            </DetailField>
          )}
          {plugin.source_ref && (
            <DetailField label={t('plugins.detail.sourceLabel')}>
              {plugin.source_ref}
            </DetailField>
          )}
          {plugin.installed_at && (
            <DetailField label={t('plugins.detail.installed')}>
              {formatDate(new Date(plugin.installed_at))}
            </DetailField>
          )}
          {plugin.updated_at && (
            <DetailField label={t('plugins.detail.updated')}>
              {formatDate(new Date(plugin.updated_at))}
            </DetailField>
          )}
        </div>
      </DetailSection>

      <input
        ref={zipInputRef}
        type="file"
        accept=".zip,application/zip"
        className="hidden"
        tabIndex={-1}
        onChange={(e) => handleUpdateZip(e.target.files?.[0])}
      />
    </DetailOverlay>
  );
}
