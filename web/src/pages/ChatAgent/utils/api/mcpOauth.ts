/**
 * The user-level OAuth connect flow driven from the Plugins page, including the
 * desktop shell's loopback callback. Per-server OAuth *state* lives in mcp.ts.
 */
import { api } from '@/api/client';
import {
  beginMcpOAuth,
  bindMcpOAuth,
  canBeginMcpOAuth,
  cancelMcpOAuth,
  isDesktopShell,
  type McpOAuthFlow,
} from '@/lib/desktop';

const OAUTH_CALLBACK_PATH = '/api/v1/mcp/oauth/callback';

/**
 * Offer the desktop shell's loopback listener as this flow's redirect target.
 *
 * Some authorization servers allowlist only the native-app profile and reject a
 * hosted callback outright, which is a wall a browser cannot get past. The shell
 * can, so it is asked first and its answer travels with the request: the backend
 * binds the redirect target into the flow at this moment and checks it again at
 * the token exchange, which is why this has to be settled here rather than
 * rewritten in the navigation later.
 *
 * Undefined means there is no listener on offer, and the flow proceeds the way
 * it does in a browser.
 */
async function loopbackFlow(): Promise<McpOAuthFlow | undefined> {
  // Built outside the ask, because the two failures are not the same fault and
  // must not answer alike: a base URL this cannot parse is a misconfigured
  // deployment, and reporting it as "no listener" sends whoever is debugging it
  // to the shell, which was fine. `beginMcpOAuth` swallows its own.
  const callback = new URL(
    OAUTH_CALLBACK_PATH,
    api.defaults.baseURL || window.location.origin,
  ).toString();
  return beginMcpOAuth(callback);
}

/** Phase 1's answer. `state` and `redirect_uri` are for the desktop path. */
export interface McpOauthStart {
  authorize_url: string;
  /** This flow's OAuth `state`; absent from a build older than the field. */
  state?: string;
  /** The callback the flow was really minted against; see `startMcpOauth`. */
  redirect_uri?: string;
}

/**
 * A connect that had to come home through the shell could not be set up that way.
 *
 * Carries the reason rather than a sentence because the four are not the same
 * fault and do not have the same remedy, and the words belong to whoever is
 * rendering them: this module has no translator. `shell-outdated` and
 * `no-listener` are the pair most worth keeping apart -- one is answered by
 * updating the app and the other by quitting a second copy of it, and a reader
 * given the wrong one of those goes looking for a window that is not open.
 */
export class LoopbackRequiredError extends Error {
  constructor(
    readonly reason: 'shell-outdated' | 'no-listener' | 'not-minted' | 'not-bound',
  ) {
    super(`loopback callback unavailable: ${reason}`);
    this.name = 'LoopbackRequiredError';
  }
}

/** What a connect needs to know beyond which row it is for. */
export interface StartMcpOauthOptions {
  /**
   * This vendor's authorization server will not accept the hosted callback at
   * all, so the flow can only come home through the desktop shell's listener.
   */
  vendorRefusesHostedCallback?: boolean;
  /**
   * The address the caller drew the row from, checked against the row before
   * anything starts.
   *
   * The row is the user's to edit from any tab, so what the page asked them
   * about and what the connect would do can be two different servers -- and a
   * vendor that allows one connected AI platform per account makes that
   * difference expensive. Naming the address is what turns the question the
   * page asked into a gate. A caller that names none has nothing to be wrong
   * about and is let through as before.
   */
  expectedUrl?: string | null;
  /**
   * Whether the caller still wants this connect, asked once there is a URL to
   * navigate to. Answering false gives back whatever this armed and returns
   * null.
   *
   * The question belongs here rather than to the caller because the thing that
   * has to be released is not the caller's to see: arming happens inside this
   * function, before either round trip below, and the flow id never leaves it.
   * A caller that took the connect back in the meantime would otherwise be
   * holding a listener it has no handle on, and the shell would run the flow's
   * full timeout and then raise the window over a connect the user stopped ten
   * minutes earlier.
   */
  stillWanted?: () => boolean;
  /**
   * The capability groups the user agreed this connection may carry, or
   * undefined for a connect that was never asked.
   *
   * The empty array and undefined are different answers and both travel as they
   * are: `[]` is a brokerage granted nothing, undefined is a server we curate no
   * groups for and hold no policy over. Collapsing them would make "declined
   * everything" and "not a brokerage" the same request, and only one of those
   * may reach a vendor's whole tool list.
   */
  grantedCapabilities?: string[];
}

/**
 * Phase 1 of the connect flow; the caller navigates to `authorize_url`.
 *
 * Everything past `returnTo` is named rather than positional. Three of the four
 * are optional and two of them are a boolean beside a callback, which is the
 * shape an argument gets silently passed in the wrong slot.
 */
export async function startMcpOauth(
  name: string,
  returnTo = '/plugins',
  {
    vendorRefusesHostedCallback = false,
    expectedUrl,
    stillWanted,
    grantedCapabilities,
  }: StartMcpOauthOptions = {},
): Promise<McpOauthStart | null> {
  // Asked unconditionally, because asking is free: `loopbackFlow` answers
  // undefined wherever there is no shell, so this arms a listener exactly where
  // one exists and changes nothing in a browser.
  const flow = await loopbackFlow();
  // Whether its absence is fatal is the real question, and it is fatal in two
  // separate situations.
  //
  // A vendor that refuses the hosted callback has no other way home anywhere.
  //
  // And inside the shell, NOTHING has another way home -- whatever the vendor
  // accepts, and whatever this shell is old enough to offer. The authorize URL
  // is an external navigation, so it leaves for the system browser, and the
  // callback then lands in a browser that never received this flow's nonce
  // cookie -- the cookie was set on the request this window made. The backend
  // refuses it as a state mismatch, in a browser the user may not even be
  // signed into, while this window sits on "connecting" because its navigation
  // was cancelled and it never unloaded. Degrading to the hosted callback there
  // is a guaranteed failure nobody is told about, which is worse than not
  // starting.
  if (!flow) {
    // Being IN the shell is the question, not what this shell can do. A build
    // that predates the loopback channel has no method to find, and reading
    // that as "not the desktop app" is how it ends up on the browser path --
    // the one path that cannot work here.
    if (isDesktopShell() && !canBeginMcpOAuth()) {
      throw new LoopbackRequiredError('shell-outdated');
    }
    if (vendorRefusesHostedCallback || isDesktopShell()) {
      throw new LoopbackRequiredError('no-listener');
    }
  }
  try {
    const { data } = await api.post<McpOauthStart>(
      `/api/v1/mcp/servers/${name}/oauth/start`,
      // One guard, and it is `loopbackFlow` above: every way of not having a
      // URI already arrives here as undefined. A second truthiness check would
      // read as belt and braces and is really a place for the two to disagree.
      {
        return_to: returnTo,
        ...(flow ? { redirect_uri: flow.redirectUri } : {}),
        ...(expectedUrl ? { expected_url: expectedUrl } : {}),
        // Presence, not truthiness, unlike the two above: an empty selection is
        // a decision the user made and has to be sent, and it is the one value
        // here that a shorthand check would drop.
        ...(grantedCapabilities !== undefined
          ? { granted_capabilities: grantedCapabilities }
          : {}),
      },
    );
    if (flow) {
      // Did the flow actually get minted against the listener we armed? A build
      // that predates the field ignores it and mints the hosted callback, and
      // so does this one when the value fails its loopback check -- both answer
      // 200, so without asking, the only symptom is a listener holding for five
      // minutes on a code that was never coming, and then raising the window.
      // For the vendors this path exists for there is no error even then: their
      // consent screen simply never redirects anywhere we can hear.
      if (data.redirect_uri !== flow.redirectUri || !data.state) {
        throw new LoopbackRequiredError('not-minted');
      }
      // Only now can the shell recognise this flow's callback. Before it, an
      // armed listener accepts nothing at all -- and a second connect started
      // in this window while the first was still minting takes the slot, so a
      // refusal here is this flow losing that race, not a broken shell.
      if (!(await bindMcpOAuth(flow.flowId, data.state))) {
        throw new LoopbackRequiredError('not-bound');
      }
    }
    // Asked last, with the listener armed and bound and the URL in hand, since
    // that is the widest the window gets: everything above is a round trip the
    // user could have pressed Cancel through. Not raised as a failure -- there
    // is nothing wrong here and nothing to tell them about a connect they are
    // the one who stopped.
    if (stillWanted && !stillWanted()) {
      if (flow) await cancelMcpOAuth(flow.flowId);
      return null;
    }
    return data;
  } catch (err) {
    // Asking armed a listener, and this is the answer that it was for nothing.
    // The order is forced -- the redirect_uri has to be in hand before the
    // request that binds it -- so the only way to keep arming honest is to say
    // when the flow it was armed for did not happen. By id: this window may
    // have started another connect since, and that one is still live.
    if (flow) await cancelMcpOAuth(flow.flowId);
    throw err;
  }
}

export async function disconnectMcpOauth(name: string) {
  const { data } = await api.delete(`/api/v1/mcp/servers/${name}/oauth`);
  return data as { ok: boolean };
}

export interface McpOauthSchemaRefreshResult {
  server_name: string;
  status: string;
  error: string;
  tool_count: number;
  discovered_at: string | null;
}

/** Host-side re-discovery of an OAuth server's tool schemas. */
export async function refreshMcpOauthSchemas(
  name: string,
): Promise<McpOauthSchemaRefreshResult> {
  const { data } = await api.post<McpOauthSchemaRefreshResult>(
    `/api/v1/mcp/servers/${name}/oauth/refresh-schemas`,
  );
  return data;
}
