import { useEffect, useEffectEvent, useId, useRef, useState } from 'react';
import { useLocation, useNavigate, useSearchParams } from 'react-router';
import { useTranslation } from 'react-i18next';
import { useQueryClient } from '@tanstack/react-query';
import { AlertTriangle } from 'lucide-react';
import { AnimatePresence } from '@/lib/framer';
import { ModalShell } from '@/components/ui/ModalShell';
import { Loader } from '@/components/ui/loader';
import { toast } from '@/components/ui/use-toast';
import {
  ListError,
  ListSkeleton,
  ServerNameLine,
  ServerRowShell,
} from '@/components/mcp/McpPrimitives';
import {
  useBrokerages,
  useMcpCatalog,
  useToggleBrokerage,
  waitForServerTools,
} from '@/hooks/useMcpServers';
import { FLASH_ROUTE_STATE, flashWorkspaceQuery } from '@/hooks/useFlashWorkspace';
import { useLocale } from '@/hooks/useLocale';
import { brokerageArt } from '@/lib/brandArt';
import { formatList } from '@/lib/format';
import { BrandMark } from '@/pages/ChatAgent/components/mcp/BrandMark';
import { McpOauthPill } from '@/pages/ChatAgent/components/mcp/McpStatusPill';
import { warmWorkspace } from '@/pages/ChatAgent/utils/warmWorkspace';
import { brokerageForUrl, connectBlock } from '@/pages/Plugins/brokerages';
import { BrokerageConsentDialog } from '@/pages/Plugins/components/BrokerageConsentDialog';
import { ConnectButton, VendorNotes } from '@/pages/Plugins/components/OauthRowParts';
import { RowNote } from '@/pages/Plugins/components/RowNote';
import { readConnectOutcome } from '@/pages/Plugins/connectOutcome';
import { connectStartedHere, useConnectReturn } from '@/pages/Plugins/connectReturn';
import { useBrokerageConnect } from '@/pages/Plugins/hooks/useBrokerageConnect';
import { isEffectivelyEnabled, isPluginSuppressed } from '@/pages/Plugins/utils/provenance';
import { ONBOARDING_CONNECT, ONBOARDING_PARAM } from './useStartOnboarding';

/** How long a connected brokerage may take to show its tools before the
 *  interview starts without them. */
const TOOLS_WAIT_MS = 20_000;
/** What the OAuth callback adds to the URL it lands on. */
const CALLBACK_PARAMS = ['mcp_connected', 'mcp_error', 'server'] as const;

interface Chosen {
  name: string;
  label: string;
}

/**
 * The lazy root the shell mounts. Kept mounted across a close so the sheet can
 * animate out, which needs `AnimatePresence`, which lives off the entry chunk.
 */
export default function OnboardingConnectLayer({
  open,
  onClose,
}: {
  open: boolean;
  onClose: () => void;
}) {
  return <AnimatePresence>{open && <OnboardingConnectSheet onClose={onClose} />}</AnimatePresence>;
}

/**
 * Step one of onboarding: connect a brokerage, or say there is none, and then
 * open Home with the turn that starts the Chief of Staff's interview.
 *
 * Connecting leaves the app for the vendor and comes back to this same URL,
 * so the sheet is also where the round trip lands: a connect that worked goes
 * straight on to the chat, one that failed or was backed out of says so here.
 */
function OnboardingConnectSheet({ onClose }: { onClose: () => void }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const titleId = useId();
  const navigate = useNavigate();
  const location = useLocation();
  const [, setSearchParams] = useSearchParams();
  const queryClient = useQueryClient();
  const { data: brokerages, isLoading: loadingOffers, error: offersError } = useBrokerages();
  const { data: catalog, isLoading: loadingCatalog, error: catalogError } = useMcpCatalog();
  const standDownMutation = useToggleBrokerage();

  // The callback's verdict, read during the first render: the effect below
  // strips it from the URL so a refresh does not act on it twice.
  const [landing] = useState(() => readConnectOutcome(new URLSearchParams(window.location.search)));
  // Only a connect this tab started goes straight on to the chat. A link that
  // carries the callback's params would otherwise open it and send its first
  // turn without the user doing anything.
  const [startedHere] = useState(
    () => landing?.kind === 'connected' && connectStartedHere(landing.server),
  );
  const [failure, setFailure] = useState<{ server: string; reasonKey: string | null } | null>(() =>
    landing?.kind === 'failed' && landing.server
      ? { server: landing.server, reasonKey: landing.reasonKey }
      : null,
  );
  // The brokerages whose tools the chat is waiting on, or null while the user
  // is still choosing.
  const [waitingFor, setWaitingFor] = useState<string[] | null>(null);
  const waitRef = useRef<AbortController | null>(null);

  // Back to this page with the sheet open, which is what makes the vendor's
  // round trip land where it started.
  const returnParams = new URLSearchParams(location.search);
  for (const key of CALLBACK_PARAMS) returnParams.delete(key);
  returnParams.set(ONBOARDING_PARAM, ONBOARDING_CONNECT);
  const [turningOn, setTurningOn] = useState<string | null>(null);
  const { oauth, requestConnect, turnOn } = useBrokerageConnect({
    returnTo: `${location.pathname}?${returnParams}`,
  });

  useConnectReturn({
    onAbandoned: (server) => setFailure({ server, reasonKey: null }),
    // Silent, as on the Plugins page: the failure is already on screen.
    onStandDown: (server) => {
      void standDownMutation.mutateAsync({ name: server, enabled: false }).catch(() => {});
    },
  });

  // Once, at mount. `setSearchParams` changes with every navigation, and a
  // rerun on its account while the sheet animates out would replace the chat
  // route it just opened, router state and all.
  const stripCallbackParams = useEffectEvent(() => {
    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        for (const key of CALLBACK_PARAMS) next.delete(key);
        return next;
      },
      { replace: true },
    );
  });
  useEffect(() => {
    if (landing) stripCallbackParams();
  }, [landing]);

  // The interview runs on Home's computer, and starting one can take a minute.
  useEffect(() => {
    let live = true;
    void queryClient.ensureQueryData(flashWorkspaceQuery(queryClient)).then(
      (home) => (live ? warmWorkspace(home.workspace_id, queryClient, { home: true }) : undefined),
      () => {},
    );
    return () => {
      live = false;
    };
  }, [queryClient]);

  // Closing mid-wait abandons it rather than opening the chat behind the user.
  // At the close itself, since the sheet stays mounted while it animates out
  // and a wait that ends then would still navigate; unmount covers the rest.
  const closedRef = useRef(false);
  function close() {
    closedRef.current = true;
    waitRef.current?.abort();
    onClose();
  }
  useEffect(() => () => waitRef.current?.abort(), []);

  const shipped = brokerages ?? [];
  const rowsByName = new Map((catalog?.servers ?? []).map((s) => [s.name, s]));
  const rows = shipped.flatMap((b) => {
    const row = rowsByName.get(b.name) ?? null;
    // Resolved against every shipped vendor, as the Brokerages tab does: a row
    // repointed at another broker's host is that broker's now.
    const vendor = row ? brokerageForUrl(row.url, shipped) : b;
    const connected = !!row && row.oauth_status === 'connected' && isEffectivelyEnabled(row);
    // Left out rather than explained: a broker this app cannot connect here
    // (Robinhood outside the desktop app), or a row whose plugin is switched
    // off and so stays out of the agent's tools whatever this sheet does, is a
    // detour from onboarding, and the Brokerages tab is where its reason is
    // spelled out.
    if (
      !connected &&
      (connectBlock(vendor) !== null || (row && (row.transport !== 'http' || isPluginSuppressed(row))))
    ) {
      return [];
    }
    // Named as the Brokerages tab names it: a row repointed elsewhere is no
    // longer that broker, so neither the sheet nor the kickoff calls it one.
    const label = row && vendor?.name !== b.name ? row.name : b.label;
    // Connected, then switched off. Picking it here is the ask to use it, and
    // its tokens are intact, so it comes back on without a second consent.
    const switchedOff = !connected && !!row && row.oauth_status === 'connected' && !row.enabled;
    return [{ b, row, vendor, connected, label, switchedOff }];
  });
  const connected: Chosen[] = rows
    .filter((r) => r.connected)
    .map(({ b, label }) => ({ name: b.name, label }));
  const listError = offersError ?? catalogError;
  const loading = loadingOffers || loadingCatalog;
  const busy = waitingFor !== null || oauth.connectingName !== null || turningOn !== null;
  const labelFor = (name: string) =>
    rows.find((r) => r.b.name === name)?.label ??
    shipped.find((b) => b.name === name)?.label ??
    name;
  const failedLabel = failure ? labelFor(failure.server) : null;

  async function proceed(chosen: Chosen[]) {
    // An aborted wait does not hold the slot: a remount (StrictMode's probe
    // included) starts the continue afresh.
    if (closedRef.current || (waitRef.current && !waitRef.current.signal.aborted)) return;
    const controller = new AbortController();
    waitRef.current = controller;
    setWaitingFor(chosen.map((c) => c.label));
    const home = queryClient.ensureQueryData(flashWorkspaceQuery(queryClient)).catch(() => null);
    // A thread's tools are fixed when it starts, so a turn sent before a fresh
    // connect's discovery lands would run without the brokerage. One already
    // showing its tools has nothing to wait for.
    const pending = chosen.filter((c) => !((rowsByName.get(c.name)?.tool_count ?? 0) > 0));
    await Promise.all(
      pending.map((c) =>
        waitForServerTools(queryClient, c.name, {
          timeoutMs: TOOLS_WAIT_MS,
          signal: controller.signal,
        }),
      ),
    );
    const workspace = await home;
    if (controller.signal.aborted) return;
    if (!workspace) {
      waitRef.current = null;
      setWaitingFor(null);
      toast({
        variant: 'destructive',
        title: t('common.error'),
        description: t('dashboard.failedOnboarding'),
      });
      return;
    }
    // Replacing the sheet's entry, so Back from the chat returns to the page
    // the user started from rather than to the sheet.
    navigate('/chat/t/__default__', {
      replace: true,
      state: {
        workspaceId: workspace.workspace_id,
        isOnboarding: true,
        onboardingBrokerages: chosen.map((c) => c.label),
        ...FLASH_ROUTE_STATE,
      },
    });
  }

  // A connect that just came back connected goes straight on: connecting was
  // the user's answer to this sheet. Every connected brokerage is named, the
  // new one first, and the new one even before the catalog shows it.
  const continueAfterConnect = useEffectEvent(() => {
    if (landing?.kind !== 'connected' || !startedHere) return;
    const fresh = shipped.find((b) => b.name === landing.server);
    const chosen = fresh
      ? [{ name: fresh.name, label: labelFor(fresh.name) }, ...connected.filter((c) => c.name !== fresh.name)]
      : connected;
    void proceed(chosen);
  });
  const ready = brokerages !== undefined && catalog !== undefined;
  useEffect(() => {
    if (ready) continueAfterConnect();
  }, [ready]);

  // The footnote promises a consent screen, which turning a row back on skips.
  const anyConnectable = rows.some((r) => !r.connected && !r.switchedOff);

  async function handleTurnOn(name: string, label: string) {
    setFailure(null);
    setTurningOn(name);
    const on = await turnOn(name);
    setTurningOn(null);
    if (on) void proceed([{ name, label }, ...connected.filter((c) => c.name !== name)]);
  }

  const footer = (
    <div className="flex items-center justify-end gap-2">
      {connected.length > 0 ? (
        <button
          type="button"
          onClick={() => void proceed(connected)}
          disabled={busy || loading}
          className="px-3 py-1.5 text-xs rounded-md transition-opacity enabled:hover:opacity-90 disabled:opacity-50"
          style={{
            color: 'var(--color-btn-primary-text)',
            backgroundColor: 'var(--color-btn-primary-bg)',
          }}
          data-testid="onboarding-connect-continue"
        >
          {t('onboarding.connect.continue')}
        </button>
      ) : (
        <button
          type="button"
          onClick={() => void proceed([])}
          disabled={busy}
          className="px-3 py-1.5 text-xs rounded-md transition-colors enabled:hover:bg-foreground/10 disabled:opacity-50"
          style={{ color: 'var(--color-text-secondary)' }}
          data-testid="onboarding-connect-skip"
        >
          {t('onboarding.connect.skip')}
        </button>
      )}
    </div>
  );

  return (
    <>
      <ModalShell
        labelId={titleId}
        title={t('onboarding.connect.title')}
        subtitle={t('onboarding.connect.description')}
        onClose={close}
        // The page is about to leave for the vendor; closing now would strand
        // a connect this sheet can no longer stop.
        dismissable={oauth.connectingName === null}
        width="narrow"
        footer={footer}
      >
        <div className="flex flex-col gap-3">
          {listError ? (
            <ListError>
              {(listError as { message?: string })?.message || t('mcp.list.loadFailed')}
            </ListError>
          ) : loading ? (
            <ListSkeleton rows={3} />
          ) : (
            <div className="flex flex-col">
              <AnimatePresence initial={false}>
                {rows.map(({ b, row, vendor, connected: isConnected, label, switchedOff }) => {
                  const rowKey = `onboarding-brokerage-${b.name}`;
                  return (
                    <ServerRowShell
                      key={b.name}
                      testid={rowKey}
                      tile={<BrandMark name={b.name} kind="server" art={brokerageArt(vendor)} />}
                      main={
                        <>
                          <ServerNameLine name={label} />
                          <VendorNotes vendor={vendor} unconnected={!isConnected} rowKey={rowKey} />
                        </>
                      }
                      actions={
                        isConnected ? (
                          <McpOauthPill status="connected" />
                        ) : (
                          <ConnectButton
                            status={row?.oauth_status ?? null}
                            connecting={oauth.connectingName === b.name || turningOn === b.name}
                            // A second connect mid-wait would leave for the
                            // vendor while this one opens the chat.
                            disabled={busy}
                            vendor={vendor}
                            rowKey={rowKey}
                            emphasis={connected.length > 0 ? 'quiet' : 'loud'}
                            testid={`onboarding-connect-${b.name}`}
                            label={switchedOff ? t('onboarding.connect.turnOn') : undefined}
                            onClick={() => {
                              if (switchedOff) {
                                void handleTurnOn(b.name, label);
                                return;
                              }
                              setFailure(null);
                              requestConnect(b, row, vendor);
                            }}
                          />
                        )
                      }
                    />
                  );
                })}
              </AnimatePresence>
            </div>
          )}

          {failedLabel && !waitingFor && (
            <div className="flex flex-col gap-0.5" role="alert">
              <RowNote icon={AlertTriangle} tone="warning">
                {t('onboarding.connect.failed', { brokerage: failedLabel })}
              </RowNote>
              {failure?.reasonKey && (
                <p className="text-[0.6875rem] pl-4" style={{ color: 'var(--color-text-tertiary)' }}>
                  {t(failure.reasonKey)}
                </p>
              )}
            </div>
          )}

          {waitingFor && waitingFor.length > 0 ? (
            <p
              className="flex items-center gap-1.5 text-[0.6875rem]"
              style={{ color: 'var(--color-text-secondary)' }}
              aria-live="polite"
            >
              <Loader size={12} className="text-current" label={t('common.loading')} />
              {t('onboarding.connect.waiting', { brokerage: formatList(waitingFor, locale) })}
            </p>
          ) : (
            !loading &&
            !listError &&
            anyConnectable && (
              <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
                {t('onboarding.connect.footnote')}
              </p>
            )
          )}
        </div>
      </ModalShell>

      {/* Stacked above the sheet (PluginDialog sits one layer higher), and
          keyed by row for the reason the Brokerages tab keys it: the toggles
          seed once from the grant the dialog opened on. */}
      <AnimatePresence>
        {oauth.pendingConfirm && (
          <BrokerageConsentDialog
            key={oauth.pendingConfirm.name}
            vendor={oauth.pendingConfirm.vendor}
            name={oauth.pendingConfirm.name}
            granted={oauth.pendingConfirm.granted}
            pending={oauth.connectingName === oauth.pendingConfirm.name}
            onConfirm={oauth.confirmPending}
            onCancel={oauth.cancelPending}
          />
        )}
      </AnimatePresence>
    </>
  );
}
