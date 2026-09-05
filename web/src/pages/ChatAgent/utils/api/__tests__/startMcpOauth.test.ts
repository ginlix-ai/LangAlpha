/**
 * Phase 1 of the connector OAuth, and the one thing the desktop shell adds to it.
 *
 * Some authorization servers allowlist only the native-app profile and refuse a
 * hosted callback outright. The shell can hold a loopback listener, so its URI
 * has to travel with this request: the backend binds the redirect target into
 * the flow here and checks it again at the token exchange, which is why it
 * cannot be swapped into the navigation afterwards.
 *
 * What is pinned here is this layer's own jobs: who gets a listener at all and
 * for whom its absence is fatal,
 * which URL the shell is asked to drive the window back to, that the flow is
 * bound before the caller navigates, and that a listener armed for a flow that
 * never happened is always given back.
 *
 * The one thing that cannot be delegated down to `lib/desktop` is what happens
 * when the listener is required and does not materialise. There is no browser
 * to fall back to for these vendors, so every way of not having one has to end
 * the attempt here, while the failure is still something the user can be told.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';
import enUS from '@/locales/en-US.json';
import zhCN from '@/locales/zh-CN.json';

const LOOPBACK = 'http://127.0.0.1:8788/mcp/callback';
const flow = { redirectUri: LOOPBACK, flowId: 'flow-1' };

let answer: { authorize_url: string; state?: string; redirect_uri?: string } = {
  authorize_url: 'https://vendor.test/authorize',
  state: 'st-1',
  redirect_uri: LOOPBACK,
};
const post = vi.fn(async (_url: string, _body?: unknown) => ({ data: answer }));

vi.mock('@/api/client', () => ({ api: { post, defaults: { baseURL: '' } } }));

const beginMcpOAuth = vi.fn();
const bindMcpOAuth = vi.fn();
const cancelMcpOAuth = vi.fn();
const canBeginMcpOAuth = vi.fn();
const isDesktopShell = vi.fn();
vi.mock('@/lib/desktop', () => ({
  beginMcpOAuth: (returnUrl: string) => beginMcpOAuth(returnUrl),
  bindMcpOAuth: (id: string, state: string) => bindMcpOAuth(id, state),
  cancelMcpOAuth: (id: string) => cancelMcpOAuth(id),
  canBeginMcpOAuth: () => canBeginMcpOAuth(),
  isDesktopShell: () => isDesktopShell(),
}));

/**
 * The three worlds this runs in, each set as a whole because their parts are
 * not independent: a browser has no bridge to answer, a current shell has one
 * that answers, and an outdated shell has a bridge with no such method on it.
 * Setting the parts separately let a test describe a browser that handed back a
 * loopback flow, which is not a state anything can be in.
 */
const inShell = (armed: typeof flow | undefined) => {
  isDesktopShell.mockReturnValue(true);
  canBeginMcpOAuth.mockReturnValue(true);
  beginMcpOAuth.mockResolvedValue(armed);
};
const inOutdatedShell = () => {
  isDesktopShell.mockReturnValue(true);
  canBeginMcpOAuth.mockReturnValue(false);
  beginMcpOAuth.mockResolvedValue(undefined);
};
const inBrowser = () => {
  isDesktopShell.mockReturnValue(false);
  canBeginMcpOAuth.mockReturnValue(false);
  beginMcpOAuth.mockResolvedValue(undefined);
};

const { startMcpOauth, LoopbackRequiredError } = await import('../mcp');

const body = () => (post.mock.calls.at(-1)?.[1] ?? {}) as Record<string, unknown>;

// Before, not after: defaults set on the way out leave the first test of the
// run holding bare mocks, so a single `it.only` or a -t filter fails on a
// `bindMcpOAuth` that never resolved true.
beforeEach(() => {
  post.mockClear();
  post.mockImplementation(async () => ({ data: answer }));
  beginMcpOAuth.mockReset();
  canBeginMcpOAuth.mockReset();
  isDesktopShell.mockReset();
  inBrowser();
  bindMcpOAuth.mockReset();
  // The shell's normal answer: it is still holding the flow we armed. Only the
  // test about losing that race says otherwise.
  bindMcpOAuth.mockResolvedValue(true);
  cancelMcpOAuth.mockReset();
  answer = {
    authorize_url: 'https://vendor.test/authorize',
    state: 'st-1',
    redirect_uri: LOOPBACK,
  };
});

/**
 * Every `LoopbackRequiredError` reason has a sentence in both catalogs.
 *
 * The caller renders these as `t(`plugins.oauth.loopback.${reason}`)`, a
 * template the tree-wide locale sweep cannot read, so nothing else notices one
 * going missing -- and what the user then sees is the raw key, on the one
 * failure path whose whole purpose is to say which of four things went wrong.
 */
describe('the loopback refusal sentences', () => {
  it.each(['shell-outdated', 'no-listener', 'not-minted', 'not-bound'])(
    '%s reads in both catalogs',
    (reason) => {
    for (const catalog of [enUS, zhCN]) {
      const loopback = (catalog as { plugins: { oauth: { loopback?: Record<string, string> } } })
        .plugins.oauth.loopback;
      expect(typeof loopback?.[reason]).toBe('string');
      }
    },
  );
});

describe('startMcpOauth', () => {
  // Asking is unconditional and costs nothing: with no bridge to answer, the
  // request goes out exactly as it always did, with the key omitted rather than
  // sent empty.
  it('sends no redirect_uri in a browser', async () => {
    inBrowser();
    await startMcpOauth('some-server', '/plugins?tab=mcp');
    expect(body()).toEqual({ return_to: '/plugins?tab=mcp' });
    expect('redirect_uri' in body()).toBe(false);
  });

  // Why every connect in the shell needs the listener, not only the vendors
  // that demand it. An authorize URL is an external navigation, so it opens in
  // the system browser -- which never received the nonce cookie this window was
  // issued, so the hosted callback comes back a state mismatch in a browser the
  // user may not even be signed into, while this window sits on "connecting"
  // because its navigation was cancelled and it never unloaded.
  it('routes an ordinary connector through the listener inside the shell', async () => {
    inShell(flow);

    await startMcpOauth('some-server', '/plugins');

    expect(beginMcpOAuth).toHaveBeenCalled();
    expect(body().redirect_uri).toBe(LOOPBACK);
  });

  // The same reasoning the other way round: in the shell there is no hosted
  // callback to degrade to, so a shell that could not bind a port must stop the
  // attempt rather than start one that can neither complete nor be reported.
  it('refuses an ordinary connector in a shell that has no listener', async () => {
    inShell(undefined);

    await expect(startMcpOauth('some-server', '/plugins')).rejects.toThrow(
      expect.objectContaining({ reason: 'no-listener' }),
    );
    expect(post).not.toHaveBeenCalled();
  });

  // The shell having the method is not the same as a port being free: all three
  // can be occupied by another edition, a preview build, or anything else local.
  // Going on without one starts a hosted flow for a vendor that refuses the
  // hosted callback, and its consent screen then redirects somewhere nothing is
  // listening -- no error, no return, and not even the abandoned-connect notice,
  // because in the shell the authorize URL opens in the system browser and this
  // window is never navigated at all.
  it('refuses the start outright when the listener it needs was not armed', async () => {
    inShell(undefined);

    await expect(startMcpOauth('robinhood', '/plugins?tab=mcp', { vendorRefusesHostedCallback: true })).rejects.toThrow(
      LoopbackRequiredError,
    );
    expect(post).not.toHaveBeenCalled();
  });

  // The shell is the fatal part, not the method. A build that predates the
  // loopback channel has nothing for `canBeginMcpOAuth` to find, and reading
  // that as "so this is a browser" put the flow on the hosted callback -- which
  // in the shell is an external navigation, so the reply lands in a system
  // browser holding no nonce cookie for it. The backend refuses it as a state
  // mismatch, possibly in a browser the user is not even signed into, while
  // this window sits on "connecting" forever because it was never navigated.
  // Every vendor, not only the ones that demand the native profile.
  it('refuses a connect the shell it is running in has no way to finish', async () => {
    inOutdatedShell();

    await expect(startMcpOauth('anything', '/plugins?tab=mcp')).rejects.toMatchObject({
      reason: 'shell-outdated',
    });
    expect(post).not.toHaveBeenCalled();
  });

  // ...and it is a different sentence from the one a shell with no free port
  // gets, because the remedies are different: update the app, versus quit the
  // other copy of it that is already holding the port.
  it('tells an outdated shell apart from one whose ports are all taken', async () => {
    inShell(undefined);

    await expect(startMcpOauth('anything', '/plugins?tab=mcp')).rejects.toMatchObject({
      reason: 'no-listener',
    });
  });

  it('passes the shell its own callback and forwards what it answers', async () => {
    inShell(flow);

    await startMcpOauth('robinhood', '/plugins', { vendorRefusesHostedCallback: true });

    // Absolute, and the backend's own callback: this is where the shell will
    // drive the window once a code lands, and it refuses anything else.
    expect(beginMcpOAuth).toHaveBeenCalledWith(
      `${window.location.origin}/api/v1/mcp/oauth/callback`,
    );
    expect(body().redirect_uri).toBe(LOOPBACK);
  });

  // Until the shell knows the state, the flow it armed accepts nothing, so this
  // is what turns an armed listener into one that can actually complete.
  it('binds the flow to the state the backend minted', async () => {
    inShell(flow);
    await startMcpOauth('robinhood', '/plugins', { vendorRefusesHostedCallback: true });
    expect(bindMcpOAuth).toHaveBeenCalledWith('flow-1', 'st-1');
    expect(cancelMcpOAuth).not.toHaveBeenCalled();
  });

  // Arming happens before the request that mints the flow, because the URI has
  // to travel with it. A start that fails therefore leaves a listener held for
  // a code nobody will send; left alone it runs the full timeout and then
  // raises the window, minutes after the user was told the connect failed.
  it('gives the listener back when the start request fails', async () => {
    inShell(flow);
    post.mockRejectedValueOnce(new Error('nope'));

    await expect(startMcpOauth('robinhood', '/plugins', { vendorRefusesHostedCallback: true })).rejects.toThrow('nope');
    expect(cancelMcpOAuth).toHaveBeenCalledWith('flow-1');
  });

  it('has nothing to give back when it never armed one', async () => {
    inShell(undefined);

    await expect(startMcpOauth('robinhood', '/plugins', { vendorRefusesHostedCallback: true })).rejects.toThrow(
      LoopbackRequiredError,
    );
    expect(cancelMcpOAuth).not.toHaveBeenCalled();
  });

  // The web bundle and the backend ship on their own cadences, so a page that
  // knows this field can reach a build that does not. That build answers 200
  // having quietly minted the hosted callback instead, and for the vendors this
  // path exists for there is then no error anywhere: their consent screen
  // simply never redirects to anything we can hear. Asking is the only way to
  // tell that apart from success.
  it('refuses a start that was minted against a different callback', async () => {
    inShell(flow);
    answer = {
      authorize_url: 'https://vendor.test/authorize',
      state: 'st-1',
      redirect_uri: 'https://app.example.com/api/v1/mcp/oauth/callback',
    };

    await expect(startMcpOauth('robinhood', '/plugins', { vendorRefusesHostedCallback: true })).rejects.toThrow(
      expect.objectContaining({ reason: 'not-minted' }),
    );
    expect(cancelMcpOAuth).toHaveBeenCalledWith('flow-1');
    expect(bindMcpOAuth).not.toHaveBeenCalled();
  });

  it('refuses a start from a build too old to echo the field at all', async () => {
    inShell(flow);
    answer = { authorize_url: 'https://vendor.test/authorize' };

    await expect(startMcpOauth('robinhood', '/plugins', { vendorRefusesHostedCallback: true })).rejects.toThrow(
      expect.objectContaining({ reason: 'not-minted' }),
    );
    expect(cancelMcpOAuth).toHaveBeenCalledWith('flow-1');
  });

  // The tab deliberately lets a second broker be started while the first is
  // still minting, and the shell holds one slot: the loser is told so. Going on
  // would open a consent screen whose reply the listener refuses by design, and
  // the flow that IS live would be the one that looked broken.
  it('stops when the shell is no longer holding the flow it armed', async () => {
    inShell(flow);
    bindMcpOAuth.mockResolvedValue(false);

    await expect(startMcpOauth('robinhood', '/plugins', { vendorRefusesHostedCallback: true })).rejects.toThrow(
      expect.objectContaining({ reason: 'not-bound' }),
    );
    expect(cancelMcpOAuth).toHaveBeenCalledWith('flow-1');
  });

  // Everything above has already run by the time this is asked, which is the
  // point: the caller has been sitting through two round trips with a Cancel
  // button on screen. The listener is armed and bound by then, and its id never
  // left this module, so nobody else could give it back. Left alone it runs the
  // shell's full ten-minute timeout and then raises the window over a connect
  // the user stopped at the start of it.
  it('gives back a bound listener when the caller no longer wants the connect', async () => {
    inShell(flow);

    const started = await startMcpOauth('robinhood', '/plugins', {
      vendorRefusesHostedCallback: true,
      stillWanted: () => false,
    });

    expect(started).toBeNull();
    expect(bindMcpOAuth).toHaveBeenCalledWith('flow-1', 'st-1');
    expect(cancelMcpOAuth).toHaveBeenCalledWith('flow-1');
  });

  // No listener in a browser, so there is nothing to give back -- but the
  // answer is the same one, because what the caller does with it is the same:
  // stop, and put back whatever it turned on for this.
  it('answers a browser caller that changed its mind with the same null', async () => {
    inBrowser();

    const started = await startMcpOauth('some-server', '/plugins', {
      stillWanted: () => false,
    });

    expect(started).toBeNull();
    expect(cancelMcpOAuth).not.toHaveBeenCalled();
  });

  // The consent the user gave, and the difference between the two ways of
  // giving none. A brokerage that named no group is one granted nothing; a
  // server with no groups to name has no policy at all and keeps the whole
  // vendor tool list. The backend reads exactly that difference off the
  // presence of this key, so a shorthand that dropped an empty array would
  // quietly turn "I declined everything" into "grant everything".
  it('sends the selection the caller was given', async () => {
    inBrowser();

    await startMcpOauth('moomoo', '/plugins?tab=brokerages', {
      grantedCapabilities: ['market_data', 'account'],
    });

    expect(body().granted_capabilities).toEqual(['market_data', 'account']);
  });

  it('sends an empty selection as one, not as no selection', async () => {
    inBrowser();

    await startMcpOauth('moomoo', '/plugins?tab=brokerages', {
      grantedCapabilities: [],
    });

    expect('granted_capabilities' in body()).toBe(true);
    expect(body().granted_capabilities).toEqual([]);
  });

  it('omits the key entirely for a connect that was never asked', async () => {
    inBrowser();

    await startMcpOauth('some-server', '/plugins?tab=mcp');

    expect('granted_capabilities' in body()).toBe(false);
  });

  it('goes on to the consent screen when the caller still wants it', async () => {
    inShell(flow);

    const started = await startMcpOauth('robinhood', '/plugins', {
      vendorRefusesHostedCallback: true,
      stillWanted: () => true,
    });

    expect(started?.authorize_url).toBe('https://vendor.test/authorize');
    expect(cancelMcpOAuth).not.toHaveBeenCalled();
  });
});
