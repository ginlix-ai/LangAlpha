import { useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { AnimatePresence } from 'framer-motion';
import { toast } from '@/components/ui/use-toast';
import {
  useBrokerages,
  useMcpCatalog,
  useDeleteMcpCatalogServer,
  useSetMcpServerEnabledInWorkspace,
  useToggleBrokerage,
} from '@/hooks/useMcpServers';
import {
  ConfirmStrip,
  ListEmpty,
  ListError,
  ListSkeleton,
} from '@/pages/ChatAgent/components/mcp/McpPrimitives';
import { needsOauthConnect } from '@/pages/ChatAgent/components/mcp/mcpState';
import { formatApiErrorDetail, type CatalogServer } from '@/pages/ChatAgent/utils/api';
import {
  brokerageForUrl,
  connectBlock,
  type Brokerage,
} from '../brokerages';
import { useMcpOauthActions } from '../hooks/useMcpOauthActions';
import { useWorkspaceOptions } from '../hooks/useWorkspaceOptions';
import { useDetailParam } from '../hooks/useDetailParam';
import { withDetail } from '../utils/detailParam';
import { BrokerageConsentDialog } from './BrokerageConsentDialog';
import { BrokerageRow } from './BrokerageRow';
import { ServerDetail, type ServerDetailData } from './ServerDetail';

/**
 * The Plugins → Brokerages tab: every broker this build ships, listed whether
 * or not the user has one, with the whole lifecycle on the row.
 *
 * A tab rather than a section of the MCP list because a brokerage is not one
 * more server — it is an account the agent can trade in, its address is ours
 * rather than the user's, and it is worth finding without knowing what MCP
 * stands for. The rows it creates are still ordinary catalog rows, so they go
 * on appearing under `Your servers` and count toward the same cap; this tab is
 * the front door, not a separate tier.
 *
 * Connecting from here is one click: the row is created and enabled on the way
 * to the vendor's consent screen, because those are only ever the steps
 * between wanting a broker connected and it being connected.
 */
export function Brokerages() {
  const { t } = useTranslation();
  const [searchParams, setSearchParams] = useSearchParams();
  const { data: brokerages, isLoading: loadingOffers, error } = useBrokerages();
  const { data: catalog, isLoading: loadingCatalog, error: catalogError } = useMcpCatalog();
  const { workspaces } = useWorkspaceOptions();
  // Back to this tab, not the MCP one: the vendor round trip should return the
  // user to the page they left.
  const oauth = useMcpOauthActions({ returnTo: '/plugins?tab=brokerages' });
  const toggleMutation = useToggleBrokerage();
  const deleteMutation = useDeleteMcpCatalogServer();
  const wsEnableMutation = useSetMcpServerEnabledInWorkspace();

  // Held here rather than in the row so a click on one broker never greys out
  // the other's controls.
  const [togglingName, setTogglingName] = useState<string | null>(null);
  const [scopeBusyName, setScopeBusyName] = useState<string | null>(null);
  // Removing is this tab's own question; the connect question belongs to the
  // connect lifecycle and is held by the hook, so that every surface that can
  // start one has to answer it. Each entry point closes the other, so only one
  // is ever open -- two questions stacked under the list, each with its own
  // Yes, gave no way to tell which broker either belonged to.
  const [removingName, setRemovingName] = useState<string | null>(null);

  const shipped = brokerages ?? [];
  const rowsByName = new Map((catalog?.servers ?? []).map((s) => [s.name, s]));
  // Resolved once, here, and handed down. The row and the tab paragraph ask the
  // same question of this, and working it out in two places is how they came to
  // disagree: the paragraph asked about the shipped vendor while the row asked
  // about the one its URL actually points at, so a repointed row left the page
  // warning "desktop only" with no row on it saying anything of the kind.
  const rows = shipped.map((b) => {
    const row = rowsByName.get(b.name) ?? null;
    // Resolved against every shipped vendor, not just this row's own: a row is
    // the user's to edit, and one repointed at the OTHER broker's host is that
    // broker now. Matching only against `b` answered null for it, which reads
    // as "no vendor here" and quietly drops the constraints that address
    // carries -- including the one that costs the user a connection elsewhere.
    return { b, row, vendor: row ? brokerageForUrl(row.url, shipped) : b };
  });

  /** Create-and-enable, the step a connect implies when there is no row yet. */
  async function ensureLive(name: string): Promise<boolean> {
    try {
      await toggleMutation.mutateAsync({ name, enabled: true });
      return true;
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.brokerages.toggleFailed'),
        description: formatApiErrorDetail(err),
      });
      return false;
    }
  }

  /**
   * Stand down a row `ensureLive` brought up for a connect that never happened.
   *
   * Switched off rather than deleted, which is the same outcome for both shapes
   * of `wasInert` and the safe one for either: a disabled row is an inert
   * template, in no workspace's effective set and carrying nothing. Deleting
   * would also be right for a row this click created, and destructive for one
   * the user had already made and merely switched off, and by the time this
   * runs the two are no longer distinguishable.
   */
  async function revertLive(name: string) {
    try {
      await toggleMutation.mutateAsync({ name, enabled: false });
    } catch {
      // Silent by choice: the connect failure is already on screen and is the
      // one the user can act on. A second toast about the tidying would bury
      // it, and the row it leaves behind is visible and switchable on the row
      // itself.
    }
  }

  async function handleToggle(name: string, enabled: boolean) {
    setTogglingName(name);
    try {
      await toggleMutation.mutateAsync({ name, enabled });
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.brokerages.toggleFailed'),
        description: formatApiErrorDetail(err),
      });
    } finally {
      setTogglingName(null);
    }
  }

  /**
   * Hand the connect to the lifecycle, with the steps only this tab can do.
   *
   * The gate that used to sit here -- the confirmation a vendor allowing one
   * connected AI platform per account has to raise -- moved into the hook, so
   * the same row reached from the MCP tab asks it too. What stays is the pair
   * of steps that are this tab's alone: a brokerage has no row until someone
   * connects it, and an inert row's grant would be revoked the moment it
   * landed, so bringing it to life is part of connecting rather than something
   * to discover afterwards. It runs after the question, not before, and comes
   * back off if the flow never reached the vendor.
   */
  function requestConnect(
    brokerage: Brokerage,
    row: CatalogServer | null,
    vendor: Brokerage | null,
  ) {
    setRemovingName(null);
    const wasInert = !row?.enabled;
    oauth.connect({
      name: brokerage.name,
      vendor,
      // The row's own address once it has one, and otherwise the address the
      // row `prepare` is about to create will carry -- which is the registry's,
      // the one thing on this tab the user does not choose.
      url: row?.url ?? brokerage.url,
      // What the row already grants, so the dialog opens on the answer the user
      // gave last time rather than on the vendor's default.
      granted: row?.remembered_capabilities ?? null,
      prepare: wasInert ? () => ensureLive(brokerage.name) : undefined,
      rollback: wasInert ? () => revertLive(brokerage.name) : undefined,
    });
  }

  async function handleSetWorkspaceDisabled(
    name: string,
    workspaceId: string,
    disabled: boolean,
  ) {
    setScopeBusyName(name);
    try {
      await wsEnableMutation.mutateAsync({ workspaceId, name, enabled: !disabled });
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.servers.toggleFailed'),
        description: formatApiErrorDetail(err),
      });
    } finally {
      setScopeBusyName(null);
    }
  }

  async function confirmRemove(name: string) {
    try {
      await deleteMutation.mutateAsync(name);
      setRemovingName(null);
    } catch (err) {
      // The strip stays up so the user can retry or back out.
      toast({
        variant: 'destructive',
        title: t('plugins.servers.deleteFailed'),
        description: formatApiErrorDetail(err),
      });
    }
  }

  // --- Detail overlay (?detail=brokerage:NAME) ---
  // Its own kind rather than the Connectors tab's `server:NAME`, because a
  // brokerage has a detail before it has a row: the offer is the thing being
  // described, and the row is one of the facts about it.
  const detail = useDetailParam<ServerDetailData>(
    'brokerage',
    (ref) => {
      const found = rows.find(({ b }) => b.name === ref.name);
      return found
        ? { origin: 'brokerage' as const, brokerage: found.b, server: found.row }
        : null;
    },
    !loadingOffers && !loadingCatalog && brokerages !== undefined && catalog !== undefined,
  );
  const detailData = detail.target;

  /** The row's other home, where its address and headers are editable. */
  function openInMcpTab(name: string) {
    const next = withDetail(searchParams, { kind: 'server', name });
    next.set('tab', 'mcp');
    setSearchParams(next, { replace: true });
  }

  // Re-derived at render rather than captured when the strip opened, so the row
  // it acts on is the one the list holds now.
  const removing = removingName
    ? (rows.find(({ b }) => b.name === removingName) ?? null)
    : null;
  // The same predicate on the same value the row uses, so the paragraph and the
  // row cannot disagree: a revoked connection needs the authorize flow again,
  // and this browser is no better at finishing it the second time. `unknown` is
  // left out on purpose -- a registry that has not answered has nothing to
  // explain yet, and a hint printed on it would be a guess.
  const blockedHere =
    rows
      .filter(({ row }) => needsOauthConnect(row?.oauth_status ?? null))
      .map(({ vendor }) => connectBlock(vendor))
      .find((block) => block === 'shell-outdated' || block === 'native-only') ?? null;
  // Either list failing leaves this tab unable to say anything true. Without the
  // catalog every broker reads "Not added" and takes the filled primary Connect,
  // which for a vendor that allows one AI connection per account silently
  // replaces the one the user may still be relying on elsewhere -- and the note
  // that would warn them is suppressed exactly then, because it only renders on
  // a row believed unconnected. An error is the honest thing to show instead.
  const listError = error ?? catalogError;

  return (
    <div className="flex flex-col gap-3">
      <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
        {t('plugins.brokerages.intro')}
        {blockedHere &&
          ` ${t(
            blockedHere === 'shell-outdated'
              ? 'plugins.oauth.shellOutdatedHint'
              : 'plugins.oauth.nativeHint',
          )}`}
      </p>

      {listError ? (
        <ListError>
          {(listError as { message?: string })?.message || t('mcp.list.loadFailed')}
        </ListError>
      ) : loadingOffers || loadingCatalog ? (
        <ListSkeleton rows={2} />
      ) : shipped.length === 0 ? (
        <ListEmpty>{t('plugins.brokerages.none')}</ListEmpty>
      ) : (
        // `mt` on siblings rather than `gap`, matching the three lists
        // beside it: an exiting row keeps its gap for the whole animation.
        <div className="flex flex-col [&>*+*]:mt-1.5">
          <AnimatePresence initial={false}>
            {rows.map(({ b, row, vendor }) => {
              return (
                <BrokerageRow
                  key={b.name}
                  brokerage={b}
                  row={row}
                  vendor={vendor}
                  workspaces={workspaces}
                  connecting={oauth.connectingName === b.name}
                  refreshing={oauth.refreshingName === b.name}
                  toggling={togglingName === b.name}
                  scopeBusy={scopeBusyName === b.name}
                  onConnect={(vendor) => requestConnect(b, row, vendor)}
                  onDisconnect={() => oauth.disconnect(b.name)}
                  onRefreshSchemas={() => oauth.refreshSchemas(b.name)}
                  onToggle={(enabled) => handleToggle(b.name, enabled)}
                  onRequestRemove={() => {
                    oauth.cancelPending();
                    setRemovingName(b.name);
                  }}
                  onSetWorkspaceDisabled={(wsId, disabled) =>
                    handleSetWorkspaceDisabled(b.name, wsId, disabled)
                  }
                  onOpenInMcpTab={() => openInMcpTab(b.name)}
                  onOpen={() => detail.open(b.name)}
                />
              );
            })}
          </AnimatePresence>
        </div>
      )}

      {removing && (
        <ConfirmStrip
          message={t('plugins.brokerages.removeConfirm', { server: removing.b.label })}
          confirmLabel={
            deleteMutation.isPending
              ? t('common.loading')
              : t('plugins.servers.deleteConfirmYes')
          }
          cancelLabel={t('plugins.servers.deleteConfirmNo')}
          pending={deleteMutation.isPending}
          onConfirm={() => void confirmRemove(removing.b.name)}
          onCancel={() => setRemovingName(null)}
        />
      )}

      {/* What the connection may do, and what making it costs elsewhere. Both
          belong to the lifecycle rather than to this tab, so the same dialog
          opens on the Connectors tab for the same row. */}
      {oauth.pendingConfirm && (
        <BrokerageConsentDialog
          // Keyed by row, because the dialog seeds its toggles once from the
          // grant it opened on. Reused across rows it would show the previous
          // row's answer.
          key={oauth.pendingConfirm.name}
          vendor={oauth.pendingConfirm.vendor}
          name={oauth.pendingConfirm.name}
          granted={oauth.pendingConfirm.granted}
          pending={oauth.connectingName === oauth.pendingConfirm.name}
          // The hook still holds the whole request, prepare and rollback
          // included, so resuming it needs only the answer.
          onConfirm={oauth.confirmPending}
          onCancel={oauth.cancelPending}
        />
      )}

      <AnimatePresence>
        {detailData && (
          <ServerDetail
            key={`brokerage:${detailData.origin === 'brokerage' ? detailData.brokerage.name : ''}`}
            data={detailData}
            onClose={detail.close}
            toggling={
              detailData.origin === 'brokerage' &&
              togglingName === detailData.brokerage.name
            }
            onToggle={(enabled) => {
              if (detailData.origin === 'brokerage') {
                void handleToggle(detailData.brokerage.name, enabled);
              }
            }}
            connecting={
              detailData.origin === 'brokerage' &&
              oauth.connectingName === detailData.brokerage.name
            }
            // The same call the row makes, so the consent dialog and the
            // create-then-connect steps behind it are asked once and answered
            // in one place, wherever the click came from.
            onConnect={() => {
              if (detailData.origin !== 'brokerage') return;
              const found = rows.find(({ b }) => b.name === detailData.brokerage.name);
              if (found) requestConnect(found.b, found.row, found.vendor);
            }}
          />
        )}
      </AnimatePresence>
    </div>
  );
}
