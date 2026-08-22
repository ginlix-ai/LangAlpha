import { useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Blocks, Download, RefreshCw, Trash2 } from 'lucide-react';
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuItem,
} from '@/components/ui/dropdown-menu';
import { toast } from '@/components/ui/use-toast';
import {
  ConfirmStrip,
  EnabledToggle,
  KebabTrigger,
  ServerNameLine,
  ServerRowShell,
  TagBadge,
} from '@/pages/ChatAgent/components/mcp/McpPrimitives';
import {
  useDeletePlugin,
  useTogglePlugin,
  useUpdatePlugin,
  useUpdatePluginFromZip,
} from '@/hooks/usePlugins';
import {
  exportPluginBlobUrl,
  formatApiErrorDetail,
  type PluginInfo,
  type PluginInstallReport,
} from '@/pages/ChatAgent/utils/api';

/**
 * One installed plugin. The toggle suppresses every component the plugin
 * still owns without touching their own enabled flags; Update re-fetches a
 * git source or takes a fresh zip; Export regenerates the spec-compliant
 * package. Uninstall deletes owned components — detached ones survive, and
 * the confirm strip says so.
 */

function summarizeUpdate(report: PluginInstallReport): string {
  const counts = new Map<string, number>();
  for (const c of report.components) {
    counts.set(c.status, (counts.get(c.status) ?? 0) + 1);
  }
  if (counts.size === 0) return '';
  return [...counts.entries()].map(([s, n]) => `${n} ${s}`).join(', ');
}

export function PluginCard({ plugin }: { plugin: PluginInfo }) {
  const { t } = useTranslation();
  const toggleMutation = useTogglePlugin();
  const deleteMutation = useDeletePlugin();
  const updateMutation = useUpdatePlugin();
  const updateZipMutation = useUpdatePluginFromZip();
  const zipInputRef = useRef<HTMLInputElement>(null);
  const [confirmingDelete, setConfirmingDelete] = useState(false);
  const [exporting, setExporting] = useState(false);

  const servers = plugin.components.filter((c) => c.kind === 'mcp');
  const skills = plugin.components.filter((c) => c.kind === 'skill');
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
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.card.uninstallFailed'),
        description: formatApiErrorDetail(err),
      });
    }
  }

  return (
    <>
      <ServerRowShell
        testid={`plugin-card-${plugin.name}`}
        main={
          <>
            <ServerNameLine icon={Blocks} name={plugin.name}>
              {plugin.version && <TagBadge>v{plugin.version}</TagBadge>}
              <TagBadge soft>
                {plugin.source_type === 'zip'
                  ? t('plugins.card.sourceZip')
                  : t('plugins.card.sourceRemote')}
              </TagBadge>
            </ServerNameLine>

            <div className="flex items-center gap-2 flex-wrap">
              <span
                className="text-[0.6875rem]"
                style={{ color: 'var(--color-text-tertiary)' }}
              >
                {t('plugins.card.componentCount', {
                  servers: t('plugins.card.serverCount', { count: servers.length }),
                  skills: t('plugins.card.skillCount', { count: skills.length }),
                })}
              </span>
              {!plugin.enabled && (
                <span
                  className="text-[0.6875rem]"
                  style={{ color: 'var(--color-text-tertiary)' }}
                >
                  {t('plugins.card.disabledState')}
                </span>
              )}
            </div>

            {plugin.description && (
              <p
                className="text-[0.6875rem] line-clamp-2"
                style={{ color: 'var(--color-text-tertiary)' }}
              >
                {plugin.description}
              </p>
            )}

            {plugin.components.length > 0 && (
              <div className="flex items-center gap-1 flex-wrap">
                {plugin.components.map((c) => (
                  <TagBadge key={`${c.kind}:${c.key}`} soft>
                    {c.name}
                  </TagBadge>
                ))}
              </div>
            )}
          </>
        }
        actions={
          <>
            <EnabledToggle
              enabled={plugin.enabled}
              name={plugin.name}
              disabled={busy}
              onToggle={handleToggle}
            />
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <KebabTrigger
                  busy={busy || exporting}
                  aria-label={t('mcp.row.actionsAria', { name: plugin.name })}
                />
              </DropdownMenuTrigger>
              <DropdownMenuContent align="end">
                <DropdownMenuItem onSelect={handleUpdate}>
                  <RefreshCw className="h-3.5 w-3.5 mr-2" />
                  {t('plugins.card.update')}
                </DropdownMenuItem>
                <DropdownMenuItem onSelect={handleExport}>
                  <Download className="h-3.5 w-3.5 mr-2" />
                  {t('plugins.card.export')}
                </DropdownMenuItem>
                <DropdownMenuItem
                  onSelect={() => setConfirmingDelete(true)}
                  variant="destructive"
                >
                  <Trash2 className="h-3.5 w-3.5 mr-2" />
                  {t('plugins.card.uninstall')}
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          </>
        }
      />

      {confirmingDelete && (
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
      )}

      <input
        ref={zipInputRef}
        type="file"
        accept=".zip,application/zip"
        className="hidden"
        tabIndex={-1}
        onChange={(e) => handleUpdateZip(e.target.files?.[0])}
      />
    </>
  );
}
