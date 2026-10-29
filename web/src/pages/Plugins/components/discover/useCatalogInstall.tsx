import { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useQueryClient } from '@tanstack/react-query';
import { LogIn, RotateCcw } from 'lucide-react';
import { AnimatePresence } from '@/lib/framer';
import { useBfcacheRestore } from '@/hooks/useBfcacheRestore';
import {
  markMarketplaceInstalled,
  useInstallPluginFromUrl,
  useMarketplace,
  usePlugins,
} from '@/hooks/usePlugins';
import type { McpSignInNeeded, MarketplaceEntry } from '@/pages/ChatAgent/utils/api';
import type { Refusal } from '@/pages/ChatAgent/utils/api/refusal';
import { spendConnectLanding, useConnectLanding } from '../../hooks/useConnectLanding';
import { markJustAdded } from '../../hooks/useJustAdded';
import { useKeysAsk, type KeysAskState } from '../../hooks/useKeysAsk';
import { useSignInOffer } from '../../hooks/useSignInOffer';
import { withDetail } from '../../utils/detailParam';
import { readInstallError } from '../../utils/installRefusal';
import { pluginSignInRequest } from '../../utils/pluginSignIn';
import { DetailCallout } from '../DetailPage';
import { PluginDialog } from '../PluginDialog';
import { PluginKeysStep } from '../PluginKeysStep';
import { PluginOutcome, type PluginOutcomeOf } from '../PluginOutcome';
import { SignInFailure } from '../SignInFailure';
import { RefusalView } from '../RefusalView';
import { PageButton } from '../pluginsPage/parts';

/**
 * Installing a catalog entry, for every place Discover offers it (a card's
 * "+", the featured row, the entry's own page). An install runs the same
 * request a pasted git URL does and lands on the same report, so nothing here
 * is a second install flow; it only picks the source.
 *
 * Nothing installs until every server the plugin carries has passed its check.
 * A plugin that declares keys asks for them first, and its servers are checked
 * with them; a refused key says why and installs nothing. A vendor that signs
 * in with OAuth goes to the vendor's page with the install, which the sign-in
 * finishes when it lands, so there is never an installed plugin waiting on a
 * sign-in to work.
 *
 * One install at a time: two finishing together would stack two dialogs.
 */

/** Why an entry is still not installed, said on its page until the next try. */
type InstallFailure =
  | { entry: string; kind: 'refused'; refusal: Refusal }
  | {
      entry: string;
      kind: 'signIn';
      outcome: 'failed' | 'denied';
      label: string;
      reasonKey?: string;
      detail?: string | null;
    };

export interface CatalogInstall {
  start: (entry: MarketplaceEntry, secrets?: Record<string, string>) => void;
  /** This entry's install is in flight, or waiting on its sign-in. */
  installing: (entry: MarketplaceEntry) => boolean;
  /** This entry's install cannot start now: another holds the catalog, the
   *  account is at its plugin limit, or its name is taken. */
  held: (entry: MarketplaceEntry) => boolean;
  /** Why it is held, for the control's title; nothing while it only waits
   *  for another install to finish. */
  holdReason: (entry: MarketplaceEntry) => string | undefined;
  /** A plugin by this entry's name is installed from another source. The
   *  server refuses an install on the name alone, so Install would only fail. */
  nameTaken: (entry: MarketplaceEntry) => boolean;
  /** Offer the sign-in an installed plugin's switch was refused for. */
  offerSignIn: (servers: McpSignInNeeded[]) => void;
  /** Why an entry's last install did not happen, until its next try. */
  failure: InstallFailure | null;
  keys: KeysAskState<MarketplaceEntry>;
  /** A request or a sign-in of this catalog's is in flight. */
  busy: boolean;
  outcome: (PluginOutcomeOf & { entry: MarketplaceEntry }) | null;
  closeOutcome: () => void;
  /** The installed plugin's page, the report's way forward. */
  openPlugin: ((name: string) => void) | undefined;
}

/** The entry's own page in the view `returnTo` names, where its sign-in lands. */
function entryPage(returnTo: string, entry: string): string {
  const [path, query = ''] = returnTo.split('?');
  return `${path}?${withDetail(new URLSearchParams(query), { kind: 'catalog', name: entry })}`;
}

/** Who the user signs in with: the vendor publishing an official entry. */
function vendorOf(entry: MarketplaceEntry): string {
  const publisher = entry.publisher?.official ? entry.publisher.name : null;
  // "Massive (formerly Polygon.io)" signs in as Massive.
  return publisher?.replace(/\s*\(.*\)\s*$/, '') || entry.name;
}

export function useCatalogInstall(
  returnTo: string,
  {
    onFailed,
    onOpenPlugin,
  }: {
    /** An install did not happen: open the entry's page, where it says why. */
    onFailed?: (entry: string) => void;
    onOpenPlugin?: (name: string) => void;
  } = {},
): CatalogInstall {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const { data: pluginList } = usePlugins();
  const { data: marketplace } = useMarketplace();
  const install = useInstallPluginFromUrl();
  const { oauth, offer: offerSignIn } = useSignInOffer(returnTo);
  // Said where the user can act on it, on the entry's page, and kept there
  // until they try again: a toast is gone before the reason is read.
  const [failure, setFailure] = useState<InstallFailure | null>(null);
  const landing = useConnectLanding(['catalog']);
  // The entry whose install is waiting on its sign-in, by source URL. Held
  // here rather than read off the lifecycle, which leaves its row connecting
  // across the navigation on purpose. `handedOff` is a sign-in gone on in the
  // system browser (the desktop app): its card keeps saying so until the
  // window comes back, and the rest of the catalog is free meanwhile, as the
  // other rows are during a reconnect.
  const [signingIn, setSigningIn] = useState<{ url: string; handedOff: boolean } | null>(
    null,
  );
  const keys = useKeysAsk<MarketplaceEntry>();
  const [outcome, setOutcome] = useState<CatalogInstall['outcome']>(null);

  // Back from the vendor's page as a bfcache restore: this page never
  // remounted, and the sign-in it was waiting on is over either way.
  useBfcacheRestore(() => setSigningIn(null));

  // A sign-in for an entry that came back without finishing, or the user
  // asking to start one again. Waits for the catalog, which names the entry.
  useEffect(() => {
    if (!landing || !marketplace || !spendConnectLanding(landing.id)) return;
    const { draft } = landing;
    const { retry } = draft;
    if (retry.kind !== 'catalog') return;
    const entry = marketplace.entries.find((e) => e.name === retry.entry);
    if (!entry) return;
    if (landing.outcome === 'retry') {
      void handleInstall(entry);
      return;
    }
    setFailure({
      entry: entry.name,
      kind: 'signIn',
      outcome: landing.outcome,
      label: draft.label,
      reasonKey: landing.reasonKey,
      detail: landing.detail,
    });
    onFailed?.(entry.name);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [landing, marketplace]);

  const maxPlugins = pluginList?.max_plugins ?? 0;
  const atCap = !!pluginList && maxPlugins > 0 && pluginList.remaining_slots <= 0;
  const installedNames = new Set((pluginList?.plugins ?? []).map((p) => p.name));
  const nameTaken = (entry: MarketplaceEntry) => !entry.plugin && installedNames.has(entry.name);
  const busy = install.isPending || (signingIn !== null && !signingIn.handedOff);
  // The question before the vendor's page is still open: nothing has started,
  // so the card does not say it is installing.
  const asking = !!oauth.pendingConfirm && oauth.connectingName !== oauth.pendingConfirm.name;

  /** A refusal the keys form does not take, said on the entry's page. */
  function refused(entry: MarketplaceEntry, refusal: Refusal) {
    setFailure({ entry: entry.name, kind: 'refused', refusal });
    onFailed?.(entry.name);
  }

  async function handleInstall(entry: MarketplaceEntry, secrets?: Record<string, string>) {
    // A failure already said stays up through the retry and is replaced by
    // its outcome, so the page does not close and reopen around the attempt.
    try {
      const response = await install.mutateAsync({ sourceUrl: entry.source_url, secrets });
      markMarketplaceInstalled(queryClient, entry.source_url, response.plugin.name);
      markJustAdded('plugin', response.plugin.name);
      setFailure((f) => (f?.entry === entry.name ? null : f));
      keys.close();
      setOutcome({ entry, response, savedKeys: Object.keys(secrets ?? {}) });
    } catch (err) {
      const error = readInstallError(err);
      if (keys.answer(entry, error)) return;
      if (error.kind === 'signIn') signInToInstall(entry, error.servers, secrets);
      else refused(entry, error.refusal);
    }
  }

  /**
   * Hand the install to the sign-in its server asks for. The install travels
   * with it, keys included, and lands when the user is back from the vendor;
   * a sign-in that does not finish leaves nothing installed.
   */
  function signInToInstall(
    entry: MarketplaceEntry,
    servers: McpSignInNeeded[],
    secrets?: Record<string, string>,
  ) {
    const request = pluginSignInRequest(
      servers,
      {
        source: { url: entry.source_url },
        secrets,
        label: vendorOf(entry),
        // Back to the entry's own page however it was started, so a sign-in
        // that fails or is cancelled is said where Install is.
        retry: { kind: 'catalog', entry: entry.name },
        returnTo: entryPage(returnTo, entry.name),
      },
      {
        onNotStarted: () => setSigningIn(null),
        onRefused: (startErr) => {
          const error = readInstallError(startErr);
          if (!keys.answer(entry, error)) refused(entry, error.refusal);
        },
        onHandedOff: () => {
          keys.close();
          setSigningIn({ url: entry.source_url, handedOff: true });
        },
      },
    );
    if (!request) return;
    setSigningIn({ url: entry.source_url, handedOff: false });
    oauth.connect(request);
  }

  return {
    start: (entry, secrets) => void handleInstall(entry, secrets),
    installing: (entry) =>
      (signingIn?.url === entry.source_url && !asking) ||
      (install.isPending && install.variables?.sourceUrl === entry.source_url),
    held: (entry) => busy || atCap || nameTaken(entry),
    holdReason: (entry) => {
      if (nameTaken(entry)) return t('plugins.discover.nameTaken', { name: entry.name });
      return atCap ? t('plugins.list.atCap', { max: maxPlugins }) : undefined;
    },
    nameTaken,
    offerSignIn,
    failure,
    keys,
    busy,
    outcome,
    closeOutcome: () => setOutcome(null),
    openPlugin:
      onOpenPlugin &&
      ((name) => {
        setOutcome(null);
        onOpenPlugin(name);
      }),
  };
}

/** The keys form and the report an install opens over the page that started
 *  it, wherever that page renders its catalog. */
export function CatalogInstallDialogs({ install }: { install: CatalogInstall }) {
  const { t } = useTranslation();
  const { outcome, keys } = install;
  const asked = keys.open;
  return (
    <>
      <AnimatePresence>
        {outcome && (
          <PluginOutcome
            key={outcome.entry.source_url}
            outcome={outcome}
            title={t('plugins.discover.installedTitle', { plugin: outcome.entry.name })}
            onDone={install.closeOutcome}
            onOpenPlugin={install.openPlugin}
          />
        )}
      </AnimatePresence>
      <AnimatePresence>
        {asked && (
          <PluginDialog
            key={asked.target.source_url}
            title={t('plugins.discover.keysTitle', { plugin: asked.target.name })}
            subtitle={t('plugins.install.stepKeys')}
            dismissable={!install.busy}
            onClose={keys.close}
          >
            <PluginKeysStep
              ask={asked.ask}
              values={keys.values}
              onChange={keys.setValues}
              onSubmit={(secrets) => install.start(asked.target, secrets)}
              onCancel={keys.close}
              submitting={install.busy}
              submitLabel={t('plugins.install.keysInstall')}
            />
          </PluginDialog>
        )}
      </AnimatePresence>
    </>
  );
}

/** Why this entry cannot install, or why its last install did not happen,
 *  with the way on; nothing once it installed or before it was tried. */
export function InstallNotice({ install, entry }: { install: CatalogInstall; entry: MarketplaceEntry }) {
  const { t } = useTranslation();
  const { failure, openPlugin } = install;
  if (entry.installed) return null;
  if (install.nameTaken(entry)) {
    return (
      <DetailCallout
        title={t('plugins.discover.nameTaken', { name: entry.name })}
        action={
          openPlugin && (
            <PageButton onClick={() => openPlugin(entry.name)}>
              {t('plugins.discoverPage.openPlugin')}
            </PageButton>
          )
        }
      >
        {t('plugins.discover.nameTakenFix')}
      </DetailCallout>
    );
  }
  if (failure?.entry !== entry.name) return null;
  const retry = (
    // Outline: the header's install button stays the page's one primary.
    <PageButton icon={<RotateCcw />} onClick={() => install.start(entry)} disabled={install.held(entry)}>
      {t('plugins.signin.tryAgain')}
    </PageButton>
  );
  if (failure.kind === 'refused') {
    return (
      <DetailCallout
        title={t('plugins.signin.failed.install', { name: entry.name })}
        action={retry}
        align="start"
      >
        <RefusalView refusal={failure.refusal} lead="plugins.discover.checkLead" />
      </DetailCallout>
    );
  }
  if (failure.outcome === 'denied') {
    // The user's own choice at the vendor, not a fault: said plainly, with
    // the way back in reach rather than pressed on them.
    return (
      <DetailCallout
        tone="quiet"
        icon={<LogIn className="h-4 w-4" aria-hidden />}
        title={t('plugins.signin.denied', { label: failure.label })}
        action={retry}
      >
        {t('plugins.signin.nothing.install')}
      </DetailCallout>
    );
  }
  return (
    <DetailCallout
      title={t('plugins.signin.failed.install', { name: entry.name })}
      action={retry}
      align="start"
    >
      <SignInFailure reasonKey={failure.reasonKey} detail={failure.detail} />
    </DetailCallout>
  );
}
