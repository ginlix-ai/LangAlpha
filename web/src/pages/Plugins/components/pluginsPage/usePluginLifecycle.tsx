import { useRef, useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import { queryKeys } from '@/lib/queryKeys';
import {
  useDeletePlugin,
  useTogglePlugin,
  useUpdatePlugin,
  useUpdatePluginFromZip,
} from '@/hooks/usePlugins';
import {
  formatApiErrorDetail,
  getUserVaultSecrets,
  mcpSignInNeeded,
  triggerPluginExportDownload,
  type CatalogServerList,
  type McpSignInNeeded,
  type PluginInfo,
  type SubagentsResponse,
} from '@/pages/ChatAgent/utils/api';
import { catalogUpdateFor, useCatalogPlugins } from '../../hooks/useCatalogPlugins';
import { useKeysAsk } from '../../hooks/useKeysAsk';
import { readInstallError } from '../../utils/installRefusal';
import { validatePluginZip } from '../../utils/pluginSchemas';
import { PluginDialog } from '../PluginDialog';
import { PluginKeysStep } from '../PluginKeysStep';
import { PluginOutcome, type PluginOutcomeOf } from '../PluginOutcome';
import { RefusalView } from '../RefusalView';
import { changedSubagents, pluginStatus, storedKeys, uninstallLines } from './pluginStatus';

/**
 * Every verb a plugin answers (switch, update, export, uninstall), shared by
 * its cell menu and its page so the two never handle a refusal differently.
 *
 * An update answers with the same report an install does and is walked
 * through the same way; keys a new version declares are asked for before it
 * replaces this one, and a zip update keeps its file so the retry sends the
 * same package. When the catalog holds a newer revision than the one
 * installed, Update moves to it: re-fetching the stored source would only
 * fetch the commit the catalog has since moved on from. `dialogs` renders
 * those steps and the hidden file picker, and belongs wherever the hook is
 * used.
 */
export function usePluginLifecycle(
  plugin: PluginInfo,
  {
    onSignInNeeded,
    onUninstalled,
  }: {
    /** The switch was refused until these servers are connected. */
    onSignInNeeded?: (servers: McpSignInNeeded[]) => void;
    /** The package is gone; a page about it has nothing left to show. */
    onUninstalled?: () => void;
  } = {},
) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const toggleMutation = useTogglePlugin();
  const deleteMutation = useDeletePlugin();
  const updateMutation = useUpdatePlugin();
  const updateZipMutation = useUpdatePluginFromZip();
  const catalogUpdate = catalogUpdateFor(useCatalogPlugins(), plugin.name);
  const zipInputRef = useRef<HTMLInputElement>(null);
  const [confirmingUninstall, setConfirmingUninstall] = useState(false);
  const [exporting, setExporting] = useState(false);
  const [outcome, setOutcome] = useState<PluginOutcomeOf | null>(null);
  // The keys a new version asks for, against the file a zip update retries.
  const keys = useKeysAsk<File | null>();

  const busy = toggleMutation.isPending || updateMutation.isPending || updateZipMutation.isPending;

  async function toggle() {
    try {
      await toggleMutation.mutateAsync({ name: plugin.name, enabled: !plugin.enabled });
    } catch (err) {
      const servers = mcpSignInNeeded(err);
      if (servers && onSignInNeeded) {
        onSignInNeeded(servers);
        return;
      }
      toast({
        variant: 'destructive',
        title: t('plugins.card.toggleFailed'),
        description: formatApiErrorDetail(err),
      });
    }
  }

  /** One update attempt, from the source or from `file`. A refusal is said
   *  in the keys form when it is open, since the keys are what to change. An
   *  update never signs in: a new version that would is refused outright. */
  async function runUpdate(file: File | null, secrets?: Record<string, string>) {
    try {
      const response = file
        ? await updateZipMutation.mutateAsync({ name: plugin.name, file, secrets })
        : await updateMutation.mutateAsync({ name: plugin.name, secrets, sourceUrl: catalogUpdate });
      keys.close();
      setOutcome({ response, savedKeys: Object.keys(secrets ?? {}) });
    } catch (err) {
      const error = readInstallError(err);
      if (keys.answer(file, error)) return;
      toast({
        variant: 'destructive',
        title: t('plugins.card.updateFailed'),
        // A toast is narrow, so three of each at most.
        description: <RefusalView refusal={error.refusal} limit={3} lead="plugins.discover.checkLead" />,
      });
    }
  }

  function update() {
    if (plugin.source_type === 'zip') {
      zipInputRef.current?.click();
      return;
    }
    void runUpdate(null);
  }

  function clearPicker() {
    if (zipInputRef.current) zipInputRef.current.value = '';
  }

  async function updateFromZip(file: File | null | undefined) {
    if (!file) return;
    // The same refusal the install path applies, so the two routes agree about
    // an oversized or non-zip pick without uploading it first.
    const reason = validatePluginZip(file);
    if (reason) {
      toast({
        variant: 'destructive',
        title: t('plugins.card.updateFailed'),
        description: t(`plugins.install.${reason}`),
      });
      clearPicker();
      return;
    }
    await runUpdate(file);
    clearPicker();
  }

  async function exportPackage() {
    setExporting(true);
    try {
      await triggerPluginExportDownload(plugin.name);
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.card.exportFailed'),
        description: formatApiErrorDetail(err),
      });
    }
    setExporting(false);
  }

  /** The stored keys this plugin's connectors read. Read before the delete,
   *  which takes the connector rows that name them; a vault that cannot be
   *  read leaves the sentence out rather than guessing. */
  async function keptKeys(): Promise<string[]> {
    const catalog = queryClient.getQueryData<CatalogServerList>(queryKeys.mcp.catalog());
    const keys = pluginStatus(plugin, catalog?.servers ?? []).keys;
    if (keys.length === 0) return [];
    try {
      const vault = await queryClient.ensureQueryData({
        queryKey: queryKeys.userVault.secrets(),
        queryFn: getUserVaultSecrets,
      });
      return storedKeys(keys, new Set(vault.secrets.map((s) => s.name)));
    } catch {
      return [];
    }
  }

  async function uninstall() {
    // Read before the delete, which re-reads the list without them.
    const changed = changedSubagents(
      plugin,
      queryClient.getQueryData<SubagentsResponse>(queryKeys.subagents.list())?.subagents,
    );
    try {
      const [result, kept] = await Promise.all([deleteMutation.mutateAsync(plugin.name), keptKeys()]);
      setConfirmingUninstall(false);
      const lines = uninstallLines(
        t,
        {
          connectors: result.deleted.servers.length,
          skills: result.deleted.skills.length,
          subagents: result.deleted.subagents?.length ?? 0,
          changedSubagents: changed,
        },
        kept,
        'after',
      );
      toast({
        title: t('plugins.card.uninstalledTitle', { plugin: plugin.name }),
        description:
          lines.length > 0 ? (
            <>
              {lines.map((line) => (
                <span key={line} className="block">{line}</span>
              ))}
            </>
          ) : undefined,
      });
      onUninstalled?.();
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.card.uninstallFailed'),
        description: formatApiErrorDetail(err),
      });
    }
  }

  const asked = keys.open;
  const dialogs = (
    <>
      <input
        ref={zipInputRef}
        type="file"
        accept=".zip,application/zip"
        className="hidden"
        tabIndex={-1}
        onChange={(e) => void updateFromZip(e.target.files?.[0])}
      />
      {asked && (
        <PluginDialog
          title={t('plugins.card.updateKeysTitle', { plugin: plugin.name })}
          subtitle={t('plugins.install.stepKeys')}
          dismissable={!busy}
          onClose={keys.close}
        >
          <PluginKeysStep
            ask={asked.ask}
            values={keys.values}
            onChange={keys.setValues}
            onSubmit={(secrets) => void runUpdate(asked.target, secrets)}
            onCancel={keys.close}
            submitting={busy}
            submitLabel={t('plugins.card.updateKeysSubmit')}
          />
        </PluginDialog>
      )}
      {outcome && (
        <PluginOutcome
          outcome={outcome}
          title={t('plugins.card.updatedTitle', { plugin: plugin.name })}
          onDone={() => setOutcome(null)}
        />
      )}
    </>
  );

  return {
    busy,
    toggling: toggleMutation.isPending,
    toggle,
    /** The catalog holds a newer version, which `update` brings in. */
    updateAvailable: catalogUpdate !== null,
    update,
    exporting,
    exportPackage,
    confirmingUninstall,
    requestUninstall: () => setConfirmingUninstall(true),
    cancelUninstall: () => setConfirmingUninstall(false),
    uninstall,
    uninstalling: deleteMutation.isPending,
    dialogs,
  };
}

export type PluginLifecycle = ReturnType<typeof usePluginLifecycle>;
