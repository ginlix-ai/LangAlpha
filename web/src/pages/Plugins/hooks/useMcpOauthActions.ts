import { createElement, useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useQueryClient } from '@tanstack/react-query';
import { ToastAction } from '@/components/ui/toast';
import { toast } from '@/components/ui/use-toast';
import { useBfcacheRestore } from '@/hooks/useBfcacheRestore';
import {
  brokeragesQuery,
  useDisconnectMcpOauth,
  useRefreshMcpOauthSchemas,
} from '@/hooks/useMcpServers';
import { isDesktopShell } from '@/lib/desktop';
import {
  LoopbackRequiredError,
  apiErrorStatus,
  formatApiErrorDetail,
  mcpSignInNeeded,
  startMcpOauth,
  startPendingMcpOauth,
  type McpOauthStart,
  type McpPendingStart,
  type PendingSignIn,
} from '@/pages/ChatAgent/utils/api';
import {
  markConnectHandedOff,
  markConnectLeft,
  rememberConnect,
  type ConnectRetry,
} from '../connectReturn';
import { brokerageForUrl, connectAsks, type Brokerage } from '../brokerages';
import {
  preloadConnectConsent,
  readConnectQuestion,
  setConnectQuestion,
  useConnectQuestion,
} from '../connectQuestion';
import { useVendorOf } from './useVendorOf';

/** One row's request to connect, and the steps that belong to that row alone. */
export interface ConnectRequest {
  name: string;
  /**
   * The address this row had when the surface drew it, or null for a brokerage
   * that has no row yet. Carried to the backend, which refuses the connect if
   * the row has moved off it -- the row is editable from any tab, so without
   * this the questions asked above could belong to a different server than the
   * one about to be connected.
   */
  url: string | null;
  /**
   * What the user last chose for this connection, or null when there is no
   * connection to read one from.
   *
   * `remembered_capabilities`, not `granted_capabilities`: the consent dialog
   * is also how an existing grant is narrowed and the only way to change one,
   * so it has to open on the user's own answer rather than the vendor's
   * default. The two fields part company exactly where it matters -- a
   * `needs_reauth` row withholds its grant, so seeding from that one re-ticked
   * every group the user had declined on a flow they entered to repair an
   * expiry, and the only sign of it was the user noticing.
   */
  granted?: string[] | null;
  /**
   * A save that waits for this sign-in, for a server (or brokerage, or
   * plugin) with no row yet. Absent, the connect is a reconnect of the saved
   * row called `name`. Nothing is written before the callback either way.
   */
  pending?: PendingSignIn;
  /**
   * What the user knows this sign-in as when it is not the row's name: the
   * catalog entry being installed, or the vendor. Said in the question asked
   * before leaving, and again if the sign-in does not finish.
   */
  label?: string;
  /** Where this connect's callback lands, when not the surface's own page:
   *  a Discover install comes back to its entry's page. */
  returnTo?: string;
  /**
   * How to put the user back if the sign-in does not finish, when `pending`
   * alone does not say: a Discover install is retried from its entry, not
   * from a source URL the user never typed.
   */
  retry?: ConnectRetry;
  /**
   * Told when the connect ends without leaving for the vendor, whatever the
   * reason: the user took it back, the start was refused, or this build
   * cannot carry the sign-in.
   */
  onNotStarted?: () => void;
  /**
   * Shows a refused start in the form that asked, in place of the toast. Only
   * a refusal from the server (or the registry the vendor is read from)
   * arrives here: a connect taken back has nothing to say, and a desktop shell
   * that cannot carry the sign-in has a remedy only the lifecycle's own toast
   * puts into words.
   */
  onRefused?: (err: unknown) => void;
  /**
   * Told when the vendor's page opened somewhere this page cannot follow: the
   * desktop app hands it to the system browser and stays where it is. The
   * surface that held the user while the sign-in started lets go here. The
   * callback's landing reports the outcome, and a row, where there is one,
   * shows connecting until then.
   */
  onHandedOff?: () => void;
}

/** A connect once its address has been read against the brokerage registry. */
export interface ResolvedConnect extends ConnectRequest {
  /**
   * Whichever shipped brokerage `url` actually points at, or null for
   * everything else. It answers two questions here: does this vendor's
   * authorization server refuse the hosted callback, and does connecting cost
   * the user a connection they already have somewhere else. Whether the flow
   * ALSO has to come home through the shell for where it is running is not
   * this row's business and is settled inside `startMcpOauth`.
   *
   * Resolved by `connect`, which waits for an unanswered registry rather than
   * reading it as "no vendor": a brokerage started that early skipped its
   * consent and was refused for want of a capability selection.
   */
  vendor: Brokerage | null;
}

/**
 * The connect a row's own Connect starts: a reconnect of the saved row, at the
 * address it was drawn with, opening on what the user last chose. A save that
 * waits on a sign-in spreads it and adds its `pending`.
 */
export function connectRequestFor(server: {
  name: string;
  url?: string | null;
  remembered_capabilities?: string[] | null;
}): ConnectRequest {
  return {
    name: server.name,
    url: server.url ?? null,
    granted: server.remembered_capabilities ?? null,
  };
}

/**
 * How long a browser tab may take to leave for the vendor before the
 * navigation counts as not having happened.
 *
 * `location.assign` only asks. A "leave this page?" prompt answered Stay, or a
 * blocked navigation, leaves the page exactly where it was with nothing
 * reported, and a surface held for a page that is never going to leave is held
 * for good. Long enough for a slow vendor to answer; one slower than this has
 * the surface let go just before the page leaves anyway, which costs nothing.
 */
const LEAVE_GRACE_MS = 5_000;

/**
 * The user-tier OAuth connect lifecycle, for every surface that starts a
 * sign-in: the Plugins page's connectors, and the workspace MCP tab, where an
 * add or a switch that needs one offers it.
 *
 * The vendor bearer never leaves the host — a sandbox reaches the server
 * through the egress relay — so connecting here is all a workspace needs.
 *
 * The lifecycle includes the questions a connect has to ask, which is why they
 * live here and not in a caller. A vendor that allows one connected AI platform
 * per account drops the previous one the moment a new grant lands, and the one
 * place that used to ask about it was the brokerage tab -- the same row reached
 * from the MCP tab went straight to the consent screen. A gate a caller has to
 * remember is a gate the next caller does not have, so `connect` raises the
 * question itself and the one host in the app shell renders it.
 *
 * A brokerage's capability consent is the same kind of gate and is raised the
 * same way, and it is the one with teeth: what the user does not tick is
 * refused at the relay for the life of the connection. A surface that never
 * asks sends no selection, and the backend grants such a connection nothing --
 * so forgetting the question costs the user a working broker, never a broker
 * that can do more than they agreed to.
 */
/**
 * Which run currently owns each row's connect.
 *
 * A connect can fail long after it started -- the shell's answer comes back
 * over IPC, the mint is a round trip -- and by then a second connect for the
 * same row may have taken it over. Clearing the spinner and telling the caller
 * are only this run's to do while the row is still its own.
 *
 * At module scope rather than in a ref, because what is owned is the row, and
 * the row is not any one tab's. Only the open Plugins tab is mounted, so a
 * connect started from the brokerage tab and taken over from the MCP tab was
 * two maps that could not see each other -- and unmounting a tab does not stop
 * the run it started. That run came back to a map where it was still the owner
 * and undid what the newer connect had just set up.
 *
 * Nothing is ever released here, and releasing on the way out is what an
 * earlier attempt got wrong: absence is how the user taking the connect back is
 * told apart from a later connect taking it over, so a finished run deleting
 * its own claim made the next failure read as a cancel and undo work that was
 * no longer its own. What makes the leftovers harmless is that every run writes
 * its own claim before reading one, so a claim from a run that has ended is
 * overwritten before anything can consult it -- including across a sign-out.
 */
const owner = new Map<string, symbol>();

/**
 * The phase-1 request in flight for each row, so a row only ever has one.
 *
 * Two connects for one row are already told apart by `owner`, which settles who
 * the row belongs to. What it cannot settle is the order the backend sees,
 * because that is decided by which request lands first: the backend retires an
 * in-flight connect on arrival, so an older start landing last retires the
 * newer one, and the consent screen the user is actually looking at is dead
 * before they touch it. Letting the second start only after the first has
 * settled is what makes arrival order and click order the same order.
 *
 * The one ahead is always about to lose the row and stop, so the wait is short
 * by construction -- but it is a network round trip, so it is bounded rather
 * than trusted, and past the bound the two race exactly as they used to.
 */
const starting = new Map<string, Promise<unknown>>();
const START_QUEUE_TIMEOUT_MS = 20_000;

/** What a sign-in's waiting save puts back if the sign-in does not finish. */
function retryFor(pending: PendingSignIn | undefined): ConnectRetry {
  switch (pending?.kind) {
    case 'plugin':
      return { kind: 'plugin', sourceUrl: pending.sourceUrl, subdir: pending.subdir ?? null };
    case 'plugin_upload':
      return { kind: 'plugin_upload' };
    case 'server':
      return { kind: 'server', server: pending.server };
    case 'server_edit':
      // A workspace's form lives in that workspace's panel, which this page
      // cannot reopen; it gets the sentence without the form.
      return pending.workspaceId
        ? { kind: 'other' }
        : { kind: 'server_edit', name: pending.name, server: pending.server };
    default:
      return { kind: 'other' };
  }
}

function onlyStartAfter<T>(name: string, start: () => Promise<T>): Promise<T> {
  const ahead = starting.get(name);
  const mine: Promise<T> = ahead ? ahead.then(start, start) : start();
  const settled = Promise.race([
    mine.catch(() => {}),
    new Promise((resolve) => setTimeout(resolve, START_QUEUE_TIMEOUT_MS)),
  ]).then(() => {
    // Let the queue empty behind the last one, so a lone connect never waits.
    if (starting.get(name) === settled) starting.delete(name);
  });
  starting.set(name, settled);
  return mine;
}

/**
 * Take a question back.
 *
 * Before the answer nothing has started, so the caller hears it here. After
 * it, the connect is already running -- the dialog stays up while the flow is
 * minted -- so the ownership that run holds over the row is released, and
 * `run` reads that as the user having taken the answer back: it stops before
 * the consent screen and says so on its way out. Released rather than flagged
 * because the row already answers who owns it, and an unowned row is the one
 * state that can only mean this.
 */
function cancelQuestion(request: ConnectRequest) {
  const open = readConnectQuestion();
  if (open?.request !== request) return;
  setConnectQuestion(null);
  if (!open.starting) request.onNotStarted?.();
  owner.delete(request.name);
}

/** Close the question a run has settled, unless a newer one replaced it. */
function settleQuestion(request: ConnectRequest) {
  if (readConnectQuestion()?.request === request) setConnectQuestion(null);
}

export function useMcpOauthActions({
  returnTo = '/plugins?tab=mcp',
}: {
  /** Where the backend callback should land the user. Each surface that can
   *  start a connect names its own, so the vendor's round trip returns to the
   *  tab the user actually left. */
  returnTo?: string;
} = {}) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const disconnectMutation = useDisconnectMcpOauth();
  const refreshMutation = useRefreshMcpOauthSchemas();
  const vendorOf = useVendorOf();
  const [connectingName, setConnectingName] = useState<string | null>(null);
  const [refreshingName, setRefreshingName] = useState<string | null>(null);
  // Which surface this is, so the one open question can be read as its own.
  const [me] = useState(() => Symbol('connect surface'));
  const question = useConnectQuestion();
  // The connect this surface asked about, held whole until it is answered.
  const pendingConfirm = question?.owner === me ? question.request : null;

  // The dialog that asks is loaded apart from the shell that mounts it, and a
  // surface that can ask warms it, so the first question opens on the click.
  useEffect(preloadConnectConsent, []);
  // A surface that goes takes its question with it. A run already answered
  // goes on, as one always has: it owns the row, not the surface.
  useEffect(
    () => () => {
      if (readConnectQuestion()?.owner === me) setConnectQuestion(null);
    },
    [me],
  );

  // A connect leaves its row connecting across the navigation on purpose, and
  // a bfcache restore brings that state back with the page. The flow it shows
  // is over either way, so the row, and a sheet that locks on it, comes back
  // to idle.
  useBfcacheRestore(() => setConnectingName(null));

  /**
   * Start a connect, or raise the question that has to be answered first.
   *
   * Returns having done one or the other; `pendingConfirm` is how the caller
   * tells which. Nothing is prepared and nothing is created before the answer,
   * which is the reason the question is asked here rather than after the row
   * has already been brought to life for a connect the user then declines.
   */
  function connect(request: ConnectRequest) {
    const vendor = vendorOf(request.url);
    if (vendor === undefined) {
      void connectOnceResolved(request);
      return;
    }
    proceed({ ...request, vendor });
  }

  /**
   * Which vendor the address is decides whether there is a question to ask, so
   * an unanswered registry is waited for rather than read as none. Usually
   * already answered, since every surface that starts a connect renders from
   * it.
   */
  async function connectOnceResolved(request: ConnectRequest) {
    let vendor: Brokerage | null;
    try {
      vendor = brokerageForUrl(request.url, await queryClient.fetchQuery(brokeragesQuery));
    } catch (err) {
      reportNotStarted(request, err, formatApiErrorDetail(err));
      return;
    }
    proceed({ ...request, vendor });
  }

  function proceed(request: ResolvedConnect) {
    // A question still open is the user moving on from it, whichever surface
    // asked. Left standing, its Yes would start that connect instead of this
    // one, and the surface waiting on it would wait for good.
    const open = readConnectQuestion();
    if (open) cancelQuestion(open.request);
    // A save or an install that goes by way of a sign-in is asked about too,
    // with no terms to choose: the user pressed Install or Add, and nothing
    // in that said "leave this app for the vendor's page". A reconnect is not
    // asked, because the button the user pressed already said Sign in.
    if (connectAsks(request.vendor) || request.pending) {
      setConnectQuestion({
        owner: me,
        request,
        starting: false,
        answer: (grantedCapabilities) => answer(request, grantedCapabilities),
        cancel: () => cancelQuestion(request),
      });
      return;
    }
    void run(request);
  }

  /** A connect that ended before the vendor's page, said in the form that
   *  asked when its surface takes the refusal, and otherwise as a toast. */
  function reportNotStarted(request: ConnectRequest, err: unknown, description: string) {
    request.onNotStarted?.();
    if (request.onRefused && !(err instanceof LoopbackRequiredError)) {
      request.onRefused(err);
      return;
    }
    toast({ variant: 'destructive', title: t('plugins.oauth.connectFailed'), description });
  }

  /**
   * Go ahead with the connect that was asked about, on the terms answered.
   *
   * The selection arrives here rather than being held beside the question
   * because it is the dialog's to collect and this hook's only to forward: it
   * is what the user ticked, it exists for exactly as long as the question is
   * on screen, and mirroring it into state here would be a second copy that
   * could disagree with the toggles the user is looking at.
   */
  function answer(request: ResolvedConnect, grantedCapabilities?: string[]) {
    const open = readConnectQuestion();
    if (open?.request !== request || open.starting) return;
    setConnectQuestion({ ...open, starting: true });
    void run(request, grantedCapabilities);
  }

  /** Take this surface's question back, for a surface about to ask one of
   *  its own: only one question is ever open. */
  function cancelPending() {
    if (pendingConfirm) cancelQuestion(pendingConfirm);
  }

  /**
   * Offer a sign-in after a switch was refused for want of one: a toast whose
   * action starts the connect. Not started unasked, because the switch the
   * user pressed did not say "leave this page for the vendor's".
   */
  function offerSignIn(request: ConnectRequest) {
    toast({
      title: t('plugins.oauth.signInNeededTitle'),
      description: t('plugins.oauth.signInToEnable', { server: request.name }),
      action: createElement(
        ToastAction,
        { altText: t('plugins.oauth.signIn'), onClick: () => connect(request) },
        t('plugins.oauth.signIn'),
      ),
    });
  }

  /** `offerSignIn` when `err` is a switch refused for want of a sign-in.
   *  False for any other refusal, which stays the caller's to say. */
  function offerSignInIfRefused(err: unknown, request: ConnectRequest): boolean {
    if (!mcpSignInNeeded(err)) return false;
    offerSignIn(request);
    return true;
  }

  async function run(request: ResolvedConnect, grantedCapabilities?: string[]) {
    const { name, vendor, url, pending, onNotStarted, onHandedOff } = request;
    const token = Symbol(name);
    owner.set(name, token);
    // Three answers to "is this row still ours", not two: ours, taken over by a
    // later connect for the same row, or taken back by the user. The two ways
    // of not being ours want opposite things: a run the user took back tells
    // its caller, and a run that was taken over leaves the spinner and the
    // caller to the newer connect.
    const ours = () => owner.get(name) === token;
    const superseded = () => owner.has(name) && !ours();
    const standDown = () => {
      if (superseded()) return;
      setConnectingName(null);
      onNotStarted?.();
    };
    setConnectingName(name);
    try {
      // The ownership question goes into the start, which is the only place it
      // has teeth: in the shell that call arms a loopback listener before its
      // own round trips, and the flow id never comes back out here. Null is
      // the answer that this run no longer owns the row, with whatever it
      // armed already given back.
      //
      // Ownership and not merely "did somebody cancel". A run that asked the
      // weaker question found an owner, took it for its own, and went on to
      // mark the row and open a consent screen for a connect the user had
      // already replaced -- landing them on the older attempt's vendor page
      // with the newer one's work underneath it.
      const arming = {
        vendorRefusesHostedCallback: !!vendor?.native_callback_only,
        stillWanted: ours,
        grantedCapabilities,
      };
      const landTo = request.returnTo ?? returnTo;
      const started = await onlyStartAfter<McpOauthStart | McpPendingStart | null>(name, () =>
        pending
          ? startPendingMcpOauth(pending, landTo, arming)
          : startMcpOauth(name, landTo, { ...arming, expectedUrl: url }),
      );
      if (!started) {
        standDown();
        return;
      }
      // Named as the callback will report it, which for a plugin is its
      // server's.
      const answered = 'kind' in started ? started : null;
      const server = answered?.server ?? name;
      const plugin = answered?.kind === 'plugin' ? answered.plugin : null;
      const label = request.label ?? vendor?.label ?? plugin ?? server;
      // Kept from the start, not from the page leaving: the desktop app never
      // leaves this page, and its callback still has to find what the user
      // was doing.
      rememberConnect({
        server,
        label,
        retry: request.retry ?? retryFor(pending),
        desktopOnly: !!vendor?.native_callback_only,
        returnTo: landTo,
      });
      // Leaving is what makes a return without an answer read as backed out
      // of. Starting the navigation is not leaving: a browser can refuse it.
      // The desktop app never leaves at all: the vendor's page opens in the
      // system browser, so this page going is a reload with the flow still
      // live, and what ends the flow is the shell's timeout, which brings the
      // window back here.
      const shell = isDesktopShell();
      let left = false;
      const onHide = () => {
        left = true;
        markConnectLeft(server);
      };
      if (shell) markConnectHandedOff(server);
      else window.addEventListener('pagehide', onHide, { once: true });
      // Left in its connecting state on purpose -- the navigation below is what
      // ends this page, and clearing first flashes the row back to idle.
      settleQuestion(request);
      // Full-page navigation into the vendor's consent screen; the backend
      // callback lands back on `landTo` with ?mcp_connected / ?mcp_error.
      window.location.assign(started.authorize_url);
      if (shell) {
        // The shell sends the authorize URL to the system browser, so this page
        // stays, for as long as ten minutes if the user walks away from the
        // vendor's page. Holding the surface that long holds the user, so it
        // lets go now; the row stays connecting, as a reconnect's always has,
        // until the shell brings the window back through the callback or its
        // timeout. Asked of the shell rather than waited out, since every
        // connect that starts in it was armed for exactly this.
        onHandedOff?.();
        if (pending) {
          toast({
            title: t('plugins.oauth.continueInBrowserTitle'),
            description: t('plugins.oauth.continueInBrowserDesc', { server: label }),
          });
        }
        return;
      }
      // In a browser the page is on its way out. One still here after the grace
      // never left, and nothing reached the vendor: the surface goes back to
      // where the user can try again, as a connect taken back does, and a page
      // that leaves later is not this sign-in. The record stays: a slow vendor
      // page can still take the document after the grace, and the landing that
      // follows needs it to reopen the form.
      setTimeout(() => {
        window.removeEventListener('pagehide', onHide);
        if (!left) standDown();
      }, LEAVE_GRACE_MS);
    } catch (err) {
      // A run a later connect took over leaves the spinner, the dialog and the
      // caller to that connect, as `standDown` does: its refusal would land in
      // a form the newer attempt now owns.
      if (superseded()) return;
      setConnectingName(null);
      settleQuestion(request);
      reportNotStarted(
        request,
        err,
        // A loopback refusal never reached the server, so it has no API detail
        // to format -- and the four reasons have four different remedies,
        // which is the whole point of carrying the reason instead of a string.
        err instanceof LoopbackRequiredError
          ? t(`plugins.oauth.loopback.${err.reason}`)
          : // A reconnect's 409 is the row having moved off the address this
            // page drew it at, so what the user was asked about and what would
            // have been connected are two different servers. Said in our own
            // words: the API's are about a row, and what the user needs is
            // what to do. A pending start's 409 is its own sentence (a taken
            // name, a cap), which the API's words already say.
            apiErrorStatus(err) === 409 && !pending
            ? t('plugins.oauth.serverMoved')
            : formatApiErrorDetail(err),
      );
    }
  }

  async function disconnect(name: string) {
    try {
      await disconnectMutation.mutateAsync(name);
      toast({
        title: t('plugins.oauth.disconnectedTitle'),
        description: t('plugins.oauth.disconnectedDesc', { server: name }),
      });
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.oauth.disconnectFailed'),
        description: formatApiErrorDetail(err),
      });
    }
  }

  async function refreshSchemas(name: string) {
    setRefreshingName(name);
    try {
      const result = await refreshMutation.mutateAsync(name);
      if (result.status === 'ok' && !result.error) {
        toast({
          title: t('plugins.oauth.refreshedTitle'),
          description: t('plugins.oauth.refreshedDesc', {
            server: name,
            count: result.tool_count,
          }),
        });
      } else if (result.status === 'ok') {
        // The cache keeps `status`/`tools` from the last good snapshot on a
        // failed re-discovery but always overwrites `error` — so an ok status
        // carrying error text means this attempt failed and the count below is
        // stale. Claiming success here would be a lie. The error string itself
        // stays out of the copy: it can be a raw connection error against a
        // user-chosen address, i.e. an internal-reachability oracle.
        toast({
          title: t('plugins.oauth.refreshFailedStaleTitle'),
          description: t('plugins.oauth.refreshFailedStaleDesc', {
            server: name,
            count: result.tool_count,
          }),
        });
      } else {
        toast({
          variant: 'destructive',
          title: t('plugins.oauth.refreshFailed'),
          description: result.error || result.status,
        });
      }
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.oauth.refreshFailed'),
        description: formatApiErrorDetail(err),
      });
    } finally {
      setRefreshingName(null);
    }
  }

  return {
    connectingName,
    refreshingName,
    pendingConfirm,
    connect,
    cancelPending,
    offerSignIn,
    offerSignInIfRefused,
    disconnect,
    refreshSchemas,
  };
}
