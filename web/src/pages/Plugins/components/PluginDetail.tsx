import { useId, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { BrandMark } from '@/pages/ChatAgent/components/mcp/BrandMark';
import { hasLifecycle, pluginMark, sourceLabelKey } from '../utils/pluginSurface';
import { ChevronRight, Download, RefreshCw, Trash2 } from 'lucide-react';
import { toast } from '@/components/ui/use-toast';
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
  formatApiErrorDetail,
  triggerPluginExportDownload,
  type PluginInfo,
  type PluginInstallResponse,
} from '@/pages/ChatAgent/utils/api';
import {
  DetailField,
  DetailHeader,
  DetailOverlay,
  DetailSection,
} from './DetailOverlay';
import { validatePluginZip } from '../utils/pluginSchemas';
import { webLink } from '../utils/webLink';
import { PluginOutcome } from './PluginOutcome';

const formatDate = createDateFormatter({ dateStyle: 'medium' });

/**
 * A plugin's detail overlay. The lifecycle verbs (Update, Export, Uninstall)
 * live here rather than on the card: the card names the plugin, this view
 * manages it. Component rows link through to their own detail views on the
 * MCP and Skills tabs — the plugin never re-renders what those tabs own.
 */

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
  // An update answers with the same report an install does, and is walked
  // through the same way: consent for held-back sse entries and any newly
  // declared credentials are only reachable from the report itself.
  const [outcome, setOutcome] = useState<PluginInstallResponse | null>(null);

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

  async function handleUpdate() {
    if (plugin.source_type === 'zip') {
      zipInputRef.current?.click();
      return;
    }
    try {
      setOutcome(await updateMutation.mutateAsync(plugin.name));
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
    // The same refusal the install path applies. Without it the two routes
    // disagree about the identical file: install rejects an oversized or
    // non-zip pick instantly and locally, while update uploads the whole
    // thing to be told the same thing by the server.
    const reason = validatePluginZip(file);
    if (reason) {
      toast({
        variant: 'destructive',
        title: t('plugins.card.updateFailed'),
        description: t(`plugins.install.${reason}`),
      });
      if (zipInputRef.current) zipInputRef.current.value = '';
      return;
    }
    try {
      setOutcome(
        await updateZipMutation.mutateAsync({ name: plugin.name, file }),
      );
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
      await triggerPluginExportDownload(plugin.name);
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

  const homepage = webLink(plugin.homepage);
  const repository = webLink(plugin.repository);

  return (
    <>
      <DetailOverlay
        labelId={labelId}
        onClose={onClose}
        footer={
          // No verbs for a package with no lifecycle: Update, Export and
          // Uninstall all answer 404 against a name with no install behind it.
          // The switch above stays — it is the one thing a bundle answers.
          !hasLifecycle(plugin) ? null : confirmingDelete ? (
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
            {...pluginMark(plugin)}
            kindLabel={t('plugins.detail.kindPlugin')}
            meta={
              <>
                {plugin.version && <span>v{plugin.version}</span>}
                <span>{t(sourceLabelKey(plugin))}</span>
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
                  <BrandMark
                    name={component.name}
                    kind={component.kind === 'mcp' ? 'server' : 'skill'}
                  />
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
            {repository && (
              <DetailField label={t('plugins.detail.repository')}>
                <a
                  href={repository}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="hover:underline underline-offset-2"
                >
                  {repository}
                </a>
              </DetailField>
            )}
            {plugin.license && (
              <DetailField label={t('plugins.detail.license')}>
                {plugin.license}
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
      {outcome && (
        <PluginOutcome
          response={outcome}
          title={t('plugins.card.updatedTitle', { plugin: plugin.name })}
          onDone={() => setOutcome(null)}
        />
      )}
    </>
  );
}
