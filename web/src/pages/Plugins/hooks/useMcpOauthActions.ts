import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import { useDisconnectMcpOauth, useRefreshMcpOauthSchemas } from '@/hooks/useMcpServers';
import {
  LoopbackRequiredError,
  apiErrorStatus,
  formatApiErrorDetail,
  startMcpOauth,
} from '@/pages/ChatAgent/utils/api';
import { markConnectStarted } from '../connectReturn';
import { connectAsks, type Brokerage } from '../brokerages';

/** One row's request to connect, and the steps that belong to that row alone. */
export interface ConnectRequest {
  name: string;
  /**
   * Whichever shipped brokerage this row's URL actually points at, or null for
   * everything else, resolved by the caller because only the row knows its own
   * address. It answers two questions here: does this vendor's authorization
   * server refuse the hosted callback, and does connecting cost the user a
   * connection they already have somewhere else. Whether the flow ALSO has to
   * come home through the shell for where it is running is not this row's
   * business and is settled inside `startMcpOauth`.
   */
  vendor: Brokerage | null | undefined;
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
   * Whatever this surface must do before the vendor's consent screen, run only
   * once the user has answered anything this connect had to ask. Answers false
   * to abandon the connect, having reported its own reason.
   */
  prepare?: () => Promise<boolean>;
  /** Undo `prepare` when the flow never reached the vendor. */
  rollback?: () => Promise<void>;
}

/**
 * The user-tier OAuth connect lifecycle: the one thing the Plugins MCP tab
 * genuinely owns, with no counterpart on the workspace MCP tab.
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
 * question itself and every surface renders the same answer.
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
 * same row may have taken it over. Undoing `prepare` is only this run's to do
 * while the row is still its own.
 *
 * At module scope rather than in a ref, because what is owned is the row, and
 * the row is not any one tab's. Only the open Plugins tab is mounted, so a
 * connect started from the brokerage tab and taken over from the MCP tab was
 * two maps that could not see each other -- and unmounting a tab does not stop
 * the run it started. That run came back to a map where it was still the owner
 * and switched off a row the newer connect had just brought live.
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

export function useMcpOauthActions({
  returnTo = '/plugins?tab=mcp',
}: {
  /** Where the backend callback should land the user. Each surface that can
   *  start a connect names its own, so the vendor's round trip returns to the
   *  tab the user actually left. */
  returnTo?: string;
} = {}) {
  const { t } = useTranslation();
  const disconnectMutation = useDisconnectMcpOauth();
  const refreshMutation = useRefreshMcpOauthSchemas();
  const [connectingName, setConnectingName] = useState<string | null>(null);
  const [refreshingName, setRefreshingName] = useState<string | null>(null);
  // The connect waiting on an answer, held whole so the surface that renders
  // the question does not have to reassemble the request to resume it.
  const [pendingConfirm, setPendingConfirm] = useState<ConnectRequest | null>(null);

  /**
   * Start a connect, or raise the question that has to be answered first.
   *
   * Returns having done one or the other; `pendingConfirm` is how the caller
   * tells which. Nothing is prepared and nothing is created before the answer,
   * which is the reason the question is asked here rather than after the row
   * has already been brought to life for a connect the user then declines.
   */
  function connect(request: ConnectRequest) {
    if (connectAsks(request.vendor)) {
      setPendingConfirm(request);
      return;
    }
    // A question still open belongs to some other row, and this click is the
    // user moving on from it. Left standing, its Yes would start that connect
    // instead of the one just asked for.
    setPendingConfirm(null);
    void run(request);
  }

  /**
   * Go ahead with the connect that was asked about, on the terms answered.
   *
   * The selection arrives here rather than being held beside `pendingConfirm`
   * because it is the surface's to collect and this hook's only to forward: it
   * is what the user ticked, it exists for exactly as long as the question is
   * on screen, and mirroring it into state here would be a second copy that
   * could disagree with the toggles the user is looking at.
   */
  function confirmPending(grantedCapabilities?: string[]) {
    if (pendingConfirm) void run(pendingConfirm, grantedCapabilities);
  }

  /**
   * Take the question back.
   *
   * Before the answer this has prepared nothing and there is nothing to undo.
   * After it, the connect is already running -- the strip stays up while the
   * row is enabled and the flow is minted -- so the ownership this run holds
   * over the row is released, and `run` reads that as the user having taken
   * the answer back: it stops before the consent screen and puts the row back.
   * Released rather than flagged because the row already answers who owns it,
   * and an unowned row is the one state that can only mean this.
   */
  function cancelPending() {
    const request = pendingConfirm;
    setPendingConfirm(null);
    if (request) owner.delete(request.name);
  }

  async function run(request: ConnectRequest, grantedCapabilities?: string[]) {
    const { name, vendor, url, prepare, rollback } = request;
    const token = Symbol(name);
    owner.set(name, token);
    // Three answers to "is this row still ours", not two: ours, taken over by a
    // later connect for the same row, or taken back by the user. The two ways
    // of not being ours want opposite things, which is the whole reason they
    // are told apart: a run the user took back has its own state to put back,
    // and a run that was taken over has none -- what it prepared belongs to the
    // newer connect now, and so does the row.
    const ours = () => owner.get(name) === token;
    const superseded = () => owner.has(name) && !ours();
    /**
     * Stop before the vendor, leaving behind only what is still this run's.
     *
     * Which is nothing at all once a newer connect has the row: the state this
     * would put back is the one that connect is using, and the spinner it would
     * clear is that connect's.
     */
    const standDown = async () => {
      if (superseded()) return;
      setConnectingName(null);
      if (rollback) await rollback();
    };
    setConnectingName(name);
    try {
      if (prepare && !(await prepare())) {
        setConnectingName(null);
        setPendingConfirm(null);
        return;
      }
      // Asked before anything is armed, because the strip stays on screen
      // through every await below and the cheapest cancel is the one that never
      // armed a listener at all.
      if (!ours()) {
        await standDown();
        return;
      }
      // The same question goes into the start, which is the only place it has
      // teeth: in the shell that call arms a loopback listener before its own
      // round trips, and the flow id never comes back out here. Null is the
      // answer that this run no longer owns the row, with whatever it armed
      // already given back.
      //
      // Ownership and not merely "did somebody cancel". A run that asked the
      // weaker question found an owner, took it for its own, and went on to
      // mark the row and open a consent screen for a connect the user had
      // already replaced -- landing them on the older attempt's vendor page
      // with the newer one's work underneath it.
      const started = await onlyStartAfter(name, () =>
        startMcpOauth(name, returnTo, {
          vendorRefusesHostedCallback: !!vendor?.native_callback_only,
          expectedUrl: url,
          stillWanted: ours,
          grantedCapabilities,
        }),
      );
      if (!started) {
        await standDown();
        return;
      }
      // Marked only once the URL is in hand: a start that threw never left the
      // page, so there would be nothing to explain on the way back.
      //
      // `prepare` is carried across with it. Everything the vendor can refuse
      // happens after the navigation below, past the reach of the catch that
      // holds `rollback`, so the return is the only place left that can put the
      // row back -- and it can only know to if this run says it brought it up.
      markConnectStarted(name, !!prepare);
      // Left in its connecting state on purpose -- the navigation below is what
      // ends this page, and clearing first flashes the row back to idle.
      setPendingConfirm(null);
      // Full-page navigation into the vendor's consent screen; the backend
      // callback lands back on /plugins with ?mcp_connected / ?mcp_error.
      window.location.assign(started.authorize_url);
    } catch (err) {
      setConnectingName(null);
      setPendingConfirm(null);
      toast({
        variant: 'destructive',
        title: t('plugins.oauth.connectFailed'),
        // A loopback refusal never reached the server, so it has no API detail
        // to format -- and the four reasons have four different remedies,
        // which is the whole point of carrying the reason instead of a string.
        description:
          err instanceof LoopbackRequiredError
            ? t(`plugins.oauth.loopback.${err.reason}`)
            : // The row moved off the address this page drew it at, so what
              // the user was asked about and what would have been connected
              // are two different servers. Said in our own words: the API's
              // are about a row, and what the user needs is what to do.
              apiErrorStatus(err) === 409
              ? t('plugins.oauth.serverMoved')
              : formatApiErrorDetail(err),
      });
      // Whatever `prepare` did was for a consent screen that never opened, so
      // it comes back off -- unless a second connect for this row has started
      // since, in which case the state being torn down is now that one's and
      // the undo belongs to it. A cancelled run still undoes its own work: no
      // later run owns the row, so there is nobody else to leave it to.
      //
      // Asked of the row rather than read off the error. `not-bound` looks like
      // evidence of a live replacement and is not: the shell answers the same
      // way for a flow that timed out, was cancelled, or went down with it, and
      // reading it as ownership left a freshly enabled brokerage switched on
      // and inherited by every workspace with no connection behind it.
      if (rollback && !superseded()) {
        await rollback();
      }
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
    confirmPending,
    cancelPending,
    disconnect,
    refreshSchemas,
  };
}
