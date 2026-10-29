import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import {
  useBrokerages,
  useMcpCatalog,
  useDeleteMcpCatalogServer,
} from '@/hooks/useMcpServers';
import {
  ConfirmStrip,
  ListEmpty,
  ListSkeleton,
} from '@/components/mcp/McpPrimitives';
import { formatApiErrorDetail } from '@/pages/ChatAgent/utils/api';
import { resolveBrokerages, type ResolvedBroker } from '../brokerages';
import { useBrokerageConnect } from '../hooks/useBrokerageConnect';
import { useWorkspaceOptions } from '../hooks/useWorkspaceOptions';
import { useDetailParam } from '../hooks/useDetailParam';
import { useSectionNav } from '../hooks/useSectionNav';
import { isPluginOwned } from '../utils/provenance';
import { BrokerageCard } from './BrokerageCard';
import { ServerDetail, type ServerDetailData } from './ServerDetail';
import { TradingPermissionBlock } from './TradingPermissionBlock';
import { LoadFailed } from './LoadFailed';
import { AccountsLayout } from './surface/AccountsLayout';

/**
 * The Plugins → Brokerages section: every broker this build ships, listed
 * whether or not the user has one, as cards, with the whole lifecycle on the
 * card's menu and the broker's page. The cards are ordered by what each needs
 * from the user, a sign-in first, and the trading permission sits beside them,
 * read once rather than on every card.
 *
 * A section rather than a group of the Connectors list because a brokerage is not one
 * more server — it is an account the agent can trade in, its address is ours
 * rather than the user's, and it is worth finding without knowing what MCP
 * stands for. The rows it creates are still ordinary catalog rows, so they go
 * on appearing under `Your servers` and count toward the same cap; this tab is
 * the front door, not a separate tier.
 *
 * Connecting from here is one click. A broker with no row yet is created by
 * its sign-in, switched on, once the vendor has answered; nothing is written
 * before then, so a sign-in that does not finish leaves nothing behind.
 */
export function Brokerages() {
  const { t } = useTranslation();
  const { openDetail } = useSectionNav();
  const { data: brokerages, isLoading: loadingOffers, error, refetch } = useBrokerages();
  const {
    data: catalog,
    isLoading: loadingCatalog,
    error: catalogError,
    refetch: refetchCatalog,
  } = useMcpCatalog();
  const { workspaces, loading: workspacesLoading } = useWorkspaceOptions();
  // Back to this tab, not the MCP one: the vendor round trip should return the
  // user to the page they left.
  const {
    oauth,
    requestConnect: connectBrokerage,
    toggle,
    togglingName,
  } = useBrokerageConnect({
    returnTo: '/plugins?tab=brokerages',
  });
  const deleteMutation = useDeleteMcpCatalogServer();

  // Removing is this tab's own question; the connect question belongs to the
  // connect lifecycle and is held by the hook, so that every surface that can
  // start one has to answer it. Each entry point closes the other, so only one
  // is ever open -- two questions stacked under the list, each with its own
  // Yes, gave no way to tell which broker either belonged to.
  const [removingName, setRemovingName] = useState<string | null>(null);

  const shipped = brokerages ?? [];
  const resolved = resolveBrokerages(shipped, catalog?.servers ?? []);

  /**
   * Hand the connect to the shared brokerage connect, closing this tab's own
   * question first: only one question is ever open under the list.
   */
  function requestConnect({ b, row }: ResolvedBroker) {
    setRemovingName(null);
    connectBrokerage(b, row);
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
      const found = resolved.get(ref.name);
      return found
        ? { origin: 'brokerage' as const, brokerage: found.b, server: found.row }
        : null;
    },
    !loadingOffers && !loadingCatalog && brokerages !== undefined && catalog !== undefined,
  );
  const detailData = detail.target;

  // Re-derived at render rather than captured when the strip opened, so the row
  // it acts on is the one the list holds now.
  const removing = removingName ? (resolved.get(removingName) ?? null) : null;
  // Either list failing leaves this tab unable to say anything true. Without the
  // catalog every broker reads "Not added" and offers Connect,
  // which for a vendor that allows one AI connection per account silently
  // replaces the one the user may still be relying on elsewhere -- and the note
  // that would warn them is suppressed exactly then, because it only renders on
  // a row believed unconnected. An error is the honest thing to show instead.
  const listError = error ?? catalogError;

  const removeStrip = removing && (
    <ConfirmStrip
      message={t('plugins.brokerages.removeConfirm', { server: removing.b.label })}
      confirmLabel={
        deleteMutation.isPending ? t('common.loading') : t('plugins.servers.deleteConfirmYes')
      }
      cancelLabel={t('plugins.servers.deleteConfirmNo')}
      pending={deleteMutation.isPending}
      onConfirm={() => void confirmRemove(removing.b.name)}
      onCancel={() => setRemovingName(null)}
    />
  );
  // One grid, in the order the user has to act on them: a broker waiting on a
  // sign-in, then the ones they have, then the rest. Each card says which it is
  // in its corner; with four brokers, a heading per state left a lone card
  // under each one. An offer has no row and sorts with the other offers,
  // whatever it would need once added.
  const rank = ({ row, needsSignIn }: ResolvedBroker) => (!row ? 2 : needsSignIn ? 0 : 1);
  const ordered = [...resolved.values()].sort((x, y) => rank(x) - rank(y));

  const detailBroker =
    detailData?.origin === 'brokerage' ? resolved.get(detailData.brokerage.name) : undefined;

  return (
    <>
      {/* Hidden rather than unmounted while a broker's page is open, so Back
          lands on the list as it was left. */}
      <AccountsLayout
        className="brokerages"
        hidden={!!detailData}
        // Why a broker cannot connect from here is said on its card, beside
        // the button it stops; said again up here it was the same sentence
        // twice on one screen.
        lead={t('plugins.brokerages.intro')}
        aside={<TradingPermissionBlock />}
      >
        {/* Where the column stacks, the aside falls below every card, and
            what a brokerage connection is would be read after the choice it
            explains. The same lead sits above the cards there instead. */}
        <p className="accounts-lead-stacked">{t('plugins.brokerages.intro')}</p>
        {listError ? (
          <LoadFailed
            what={t('plugins.loadFailed.brokerages')}
            error={listError}
            onRetry={() => {
              if (error) void refetch();
              if (catalogError) void refetchCatalog();
            }}
          />
        ) : loadingOffers || loadingCatalog ? (
          <ListSkeleton rows={2} />
        ) : shipped.length === 0 ? (
          <ListEmpty>{t('plugins.brokerages.none')}</ListEmpty>
        ) : (
          <>
            <div className="broker-cards">
              {ordered.map((broker) => (
                <BrokerageCard
                  key={broker.b.name}
                  broker={broker}
                  connecting={oauth.connectingName === broker.b.name}
                  toggling={togglingName === broker.b.name}
                  onConnect={() => requestConnect(broker)}
                  onToggle={(enabled) => void toggle(broker.b, broker.row, enabled)}
                  manage={{
                    workspaces,
                    workspacesLoading,
                    refreshing: oauth.refreshingName === broker.b.name,
                    onDisconnect: () => oauth.disconnect(broker.b.name),
                    onRefreshSchemas: () => oauth.refreshSchemas(broker.b.name),
                    onRequestRemove: () => {
                      oauth.cancelPending();
                      setRemovingName(broker.b.name);
                    },
                    // The row's other home, where its address and headers are
                    // editable. Pushed like any detail, so Back returns here.
                    onOpenInMcpTab: () => openDetail({ kind: 'server', name: broker.b.name }),
                    onOpen: () => detail.open(broker.b.name),
                  }}
                />
              ))}
            </div>
            {!detailData && removeStrip}
          </>
        )}
      </AccountsLayout>

      {/* A page in the content column, not a dialog: no exit animation to
          wait for, so it renders in the list's place and goes with it. */}
      {detailData && detailBroker && (
        <ServerDetail
          key={`brokerage:${detailBroker.b.name}`}
          data={detailData}
          onClose={detail.close}
          toggling={togglingName === detailBroker.b.name}
          onToggle={(enabled) => void toggle(detailBroker.b, detailBroker.row, enabled)}
          connecting={oauth.connectingName === detailBroker.b.name}
          // The same call the card makes, so the consent dialog and the
          // sign-in behind it are asked once and answered in one place,
          // wherever the click came from.
          onConnect={() => requestConnect(detailBroker)}
          onRemove={
            detailBroker.row && !isPluginOwned(detailBroker.row)
              ? () => {
                  oauth.cancelPending();
                  setRemovingName(detailBroker.b.name);
                }
              : undefined
          }
          removeConfirm={removing?.b.name === detailBroker.b.name ? removeStrip : undefined}
          onRefreshTools={() => oauth.refreshSchemas(detailBroker.b.name)}
          refreshingTools={oauth.refreshingName === detailBroker.b.name}
        />
      )}
    </>
  );
}
