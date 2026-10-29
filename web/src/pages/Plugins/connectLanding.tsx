import type { TFunction } from 'i18next';
import type { NavigateFunction } from 'react-router';
import { Trans } from 'react-i18next';
import { ToastAction } from '@/components/ui/toast';
import { toast } from '@/components/ui/use-toast';
import { canBeginMcpOAuth } from '@/lib/desktop';
import { pendingIntent } from '@/pages/ChatAgent/utils/api/mcpOauth';
import { WELCOME_PATH, isLegacyOnboardingSearch, legacyOnboardingTarget } from '@/pages/Onboarding/welcome/paths';
import { DesktopDownloadLink } from './components/DesktopDownloadLink';
import { SignInFailure } from './components/SignInFailure';
import { CALLBACK_PARAMS } from './connectOutcome';
import { settleConnectReturn, type ConnectRecord, type ConnectRetry } from './connectReturn';
import { setConnectLanding } from './hooks/useConnectLanding';
import { markJustAdded } from './hooks/useJustAdded';
import { VIEW_PARAM, withDetail } from './utils/detailParam';

const PLUGINS_PATH = '/plugins';
const DENIED = 'plugins.oauth.callbackError.denied';

/**
 * Where a sign-in that did not finish puts the user back, as the Plugins
 * page's params: the entry's page for a Discover install, the section whose
 * form reopens otherwise. Null for one with no form to reopen (a reconnect, a
 * brokerage, a workspace's), which only gets its sentence.
 */
export function landingPlace(retry: ConnectRetry, params: URLSearchParams): URLSearchParams | null {
  switch (retry.kind) {
    case 'catalog': {
      const next = withDetail(params, { kind: 'catalog', name: retry.entry });
      if (!['plugins', 'mcp', 'skills'].includes(next.get('tab') ?? '')) next.set('tab', 'plugins');
      next.set(VIEW_PARAM, 'discover');
      return next;
    }
    case 'plugin':
    case 'plugin_upload':
    case 'server':
    case 'server_edit': {
      const next = withDetail(params, null);
      next.set('tab', retry.kind === 'server' || retry.kind === 'server_edit' ? 'mcp' : 'plugins');
      next.delete(VIEW_PARAM);
      return next;
    }
    default:
      return null;
  }
}

type Place = Pick<Location, 'pathname' | 'search' | 'hash'>;

/** The Plugins page's params to build on: the page's own where the user is on
 *  it, so the section they are in is kept where the step allows it. */
function pluginsParams(at: Place = window.location): URLSearchParams {
  const params =
    at.pathname === PLUGINS_PATH ? new URLSearchParams(at.search) : new URLSearchParams();
  for (const key of CALLBACK_PARAMS) params.delete(key);
  return params;
}

/**
 * Where an answered connect is read: where it asked its callback to land. The
 * callback lands there itself, except when the server could no longer tie it
 * to its flow (a sign-in slower than the flow's state lives), which leaves it
 * only the Plugins page to fall back on; the record still knows the way.
 *
 * A connect an older build's onboarding sheet sent out names the sheet's URL,
 * which is the welcome flow's brokerage screen now. The shell moves a URL it
 * is opened on there by itself, before this reads the landing; a recorded one
 * is moved here.
 */
function askedToLand(record: ConnectRecord | null): Place {
  if (record?.returnTo) {
    try {
      const url = new URL(record.returnTo, window.location.origin);
      if (url.origin !== window.location.origin) return window.location;
      return isLegacyOnboardingSearch(url.search) ? new URL(legacyOnboardingTarget(url.search), url.origin) : url;
    } catch {
      // Not a path any build wrote; where the callback landed will do.
    }
  }
  return window.location;
}

function goToPlugins(navigate: NavigateFunction, params: URLSearchParams, replace: boolean) {
  const search = `${params}`;
  navigate(`${PLUGINS_PATH}${search ? `?${search}` : ''}`, {
    // Already there: a second entry would make Back a no-op.
    replace:
      replace ||
      (window.location.pathname === PLUGINS_PATH &&
        search === new URLSearchParams(window.location.search).toString()),
  });
}

/**
 * Say so when a sign-in never came back from the vendor.
 *
 * A provider that refuses our redirect_uri renders its own page and never
 * redirects, so Back is the only way home, and this toast is the only thing
 * that explains why nothing happened. Neutral rather than an error: backing
 * out of the vendor's page is the common case and a choice, not a fault, and
 * nothing here can tell it from a vendor that refused this app silently. What
 * it can say for certain is that nothing happened, and offer the same step
 * again, reopened on what the user had typed. Only a vendor known to accept
 * the desktop app alone is said to need it: in a browser every vendor would
 * otherwise read that way.
 */
function toastAbandoned(record: ConnectRecord, t: TFunction, navigate: NavigateFunction) {
  const { label } = record;
  const needsDesktop = record.desktopOnly && !canBeginMcpOAuth();
  const place = needsDesktop ? null : landingPlace(record.retry, pluginsParams());
  toast({
    title: t('plugins.signin.abandoned', { label }),
    description: needsDesktop ? (
      <Trans
        i18nKey="plugins.signin.needsDesktop"
        values={{ label }}
        // Not `link`: the Trans parser reads that as the void HTML element
        // and drops the words inside it.
        components={{ download: <DesktopDownloadLink /> }}
      />
    ) : (
      t(`plugins.signin.nothing.${pendingIntent(record.retry.kind)}`)
    ),
    action: place ? (
      <ToastAction
        altText={t('plugins.signin.tryAgain')}
        onClick={() => {
          setConnectLanding({ owner: 'page', draft: record, outcome: 'retry' });
          goToPlugins(navigate, landingPlace(record.retry, pluginsParams()) ?? place, false);
        }}
      >
        {t('plugins.signin.tryAgain')}
      </ToastAction>
    ) : undefined,
  });
}

/** The welcome flow, which a brokerage connect it sent out returns to. */
function onWelcome(at: Pick<Place, 'pathname'>): boolean {
  return at.pathname === WELCOME_PATH;
}

/**
 * Read what this landing says about the connects this tab sent out, once, and
 * hand it to whoever acts on it: the welcome flow when it reopens with it, the
 * page otherwise.
 *
 * Every surface's connect comes back through here: the callback returns to the
 * page it was given, and Back returns to the page the connect left from. The
 * callback's params are stripped once read, so a refresh does not act on them
 * twice.
 */
export function landConnectReturn({ t, navigate }: { t: TFunction; navigate: NavigateFunction }) {
  const landed = settleConnectReturn(window.location.search);
  if (!landed) return;

  if (landed.kind === 'abandoned') {
    if (onWelcome(window.location)) {
      setConnectLanding({ owner: 'welcome', server: landed.record.server, outcome: 'failed', reasonKey: null });
    } else {
      toastAbandoned(landed.record, t, navigate);
    }
    return;
  }

  const { outcome, record } = landed;
  const back = askedToLand(record);
  const welcome = onWelcome(back);
  const here = new URLSearchParams(back.search);
  for (const key of CALLBACK_PARAMS) here.delete(key);
  const rest = `${here}`;
  const strip = () => navigate(`${back.pathname}${rest ? `?${rest}` : ''}${back.hash}`, { replace: true });
  const server = outcome.server ?? record?.server ?? null;

  if (welcome) {
    // The flow moves on by itself on a connect, so only one this tab sent
    // counts: a link carrying the callback's params is not the user's answer.
    if (outcome.kind === 'failed' && server) {
      setConnectLanding({ owner: 'welcome', server, outcome: 'failed', reasonKey: outcome.reasonKey });
    } else if (outcome.kind === 'connected' && record) {
      setConnectLanding({ owner: 'welcome', server: record.server, outcome: 'connected', reasonKey: null });
    }
    strip();
    return;
  }

  if (outcome.kind === 'connected') {
    toast({
      title: t('plugins.oauth.connectedTitle'),
      description: outcome.plugin
        ? t('plugins.oauth.pluginInstalledDesc', { plugin: outcome.plugin })
        : server
          ? t('plugins.oauth.connectedDesc', { server })
          : t('plugins.oauth.connectedDescAnon'),
    });
    // A sign-in that installed a plugin lands on its page, on the section it
    // started from: Plugins and Discover both open a plugin's page.
    if (outcome.plugin) {
      markJustAdded('plugin', outcome.plugin);
      goToPlugins(navigate, withDetail(pluginsParams(back), { kind: 'plugin', name: outcome.plugin }), true);
      return;
    }
    if (server && record?.retry.kind === 'server') markJustAdded('server', server);
    strip();
    return;
  }

  // What the sign-in was in the middle of. Only one this tab started has a
  // record, so params a link carries cannot reopen a form of their choosing.
  const denied = outcome.reasonKey === DENIED;
  const place = record && landingPlace(record.retry, pluginsParams(back));
  if (record && place) {
    // Said where the user can act on it: the entry's page, or the form the
    // sign-in was saving, open again on what they had typed.
    setConnectLanding({
      owner: 'page',
      draft: record,
      outcome: denied ? 'denied' : 'failed',
      reasonKey: outcome.reasonKey,
      detail: outcome.detail,
    });
    goToPlugins(navigate, place, true);
    return;
  }
  if (denied) {
    // The user's own choice at the vendor, not a fault.
    toast({
      title: t('plugins.signin.denied', {
        label: record?.label ?? outcome.plugin ?? server ?? t('plugins.signin.theVendor'),
      }),
      description: t('plugins.signin.nothing.other'),
    });
  } else {
    // A reconnect has no form to reopen: the row it started from is the way
    // to try again, and the toast stays until it is read.
    toast({
      variant: 'destructive',
      title: t('plugins.oauth.callbackErrorTitle'),
      description: <SignInFailure reasonKey={outcome.reasonKey} detail={outcome.detail} />,
    });
  }
  strip();
}
