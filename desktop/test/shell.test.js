'use strict'

const { test, describe, before, after, beforeEach } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')
const { EventEmitter } = require('node:events')
const http = require('node:http')
const { loadShell, loadEntryWith, cleanup, opened, setOnline, setSaveDialog, tempDir, electronStub } = require('./helpers')

after(cleanup)

const SUPABASE = 'https://ref.supabase.co/auth/v1/authorize'

/** The little of a BrowserWindow the navigation policy actually touches. */
// `at` tracks where the window is, because `loadURL` is how a window moves and
// the OAuth flow now asks. A test drives it with `loadURL` exactly as the app
// would, and `landed` records every move including that one.
const windowStub = (landed = [], at = 'https://app.example.com/login') => ({
  isDestroyed: () => false,
  show() {},
  focus() {},
  // Returns a promise because the real one does, and always: `navigate()` in
  // src/navigate.js attaches the `.catch` that keeps an ordinary failed
  // navigation from being reported as a shell fault, and a stub returning
  // undefined would let that call site break with every test still green.
  loadURL(u) { at = u; landed.push(u); return Promise.resolve() },
  webContents: { getURL: () => at },
})

/**
 * A window whose webContents actually emits, so a test drives the listeners
 * `outage.attach` registers rather than reaching past them to the internals.
 */
const fakeWebContentsWindow = (landed = [], history = null) => {
  const win = {
    isDestroyed: () => false,
    show() {},
    focus() {},
    loadURL(u) { landed.push(u); return Promise.resolve() },
    loadFile() { return Promise.resolve() },
    webContents: Object.assign(new EventEmitter(), { getURL: () => '', send() {} }),
  }
  if (history) {
    // Enough of Electron's navigationHistory to prove which entries a listener
    // removes and which one it must not: removing shifts the active index, and
    // a stub that ignored that would pass a walk that deleted the wrong rows.
    let active = history.active
    const list = history.urls.slice()
    win.webContents.navigationHistory = {
      getActiveIndex: () => active,
      getAllEntries: () => list.map((url) => ({ url })),
      removeEntryAtIndex: (i) => {
        list.splice(i, 1)
        if (i < active) active -= 1
      },
      entries: () => list.slice(),
    }
  }
  return win
}

describe('origin policy', () => {
  // The spike classified the account console by the path /account, a shape that
  // only exists in dev where a proxy stitches both SPAs onto one host. In
  // production they are separate origins, so that test never matched and a
  // packaged build threw its own console at the system browser.
  test('a saas build recognises both of its origins', () => {
    const { origins } = loadShell({ edition: 'saas' })
    assert.ok(origins.isApp('https://app.example.com/dashboard'))
    assert.ok(origins.isPlatform('https://platform.example.com/billing'))
    assert.ok(origins.isOurs('https://platform.example.com/onboarding'))
  })

  test('a path called /account on a foreign host is not ours', () => {
    const { origins } = loadShell({ edition: 'saas' })
    assert.equal(origins.isPlatform('https://evil.example.com/account/billing'), false)
    assert.equal(origins.isOurs('https://evil.example.com/account'), false)
  })

  test('an oss build has no platform at all', () => {
    const { origins } = loadShell({ edition: 'oss' })
    assert.equal(origins.platformOrigin(), null)
    assert.equal(origins.isPlatform('https://platform.example.com/'), false)
  })

  test('the stored server wins over the compiled default in oss', () => {
    const { origins } = loadShell({ edition: 'oss', serverUrl: 'http://192.168.1.9:8080' })
    assert.equal(origins.appOrigin(), 'http://192.168.1.9:8080')
    assert.ok(origins.isOurs('http://192.168.1.9:8080/chat'))
    assert.equal(origins.isOurs('http://localhost:5173/chat'), false)
  })

  test('an unparseable stored server falls back instead of throwing', () => {
    const { store, origins } = loadShell({ edition: 'oss' })
    store.set('serverUrl', 'not a url')
    assert.equal(origins.appOrigin(), 'http://localhost:5173')
  })

  // A blob URL inherits the origin of the page that minted it, so this one
  // really does report `https://app.example.com`. Treating that as ours let a
  // `window.open` of agent-generated widget HTML classify as an app navigation,
  // which the window-open handler carries out by loading it into the main
  // window: the running turn is gone, and the HTML has the preload bridge.
  test('a blob minted by our own page is not a page of ours', () => {
    const { origins, policy } = loadShell({ edition: 'saas' })
    const blob = 'blob:https://app.example.com/6f1e-9c2a'
    assert.equal(new URL(blob).origin, 'https://app.example.com', 'premise of the bug')
    assert.equal(origins.isApp(blob), false)
    assert.equal(origins.isOurs(blob), false)
    assert.equal(policy.classifyNavigation(blob, { isMainWindow: true }), 'external')
  })

  test('nor is a data: or filesystem: URL', () => {
    const { origins } = loadShell({ edition: 'saas' })
    assert.equal(origins.isOurs('data:text/html,<h1>hi'), false)
    assert.equal(origins.isOurs('filesystem:https://app.example.com/temporary/x'), false)
  })
})

// `shell.openExternal` hands the string to the OS, which on macOS launches an
// application bundle for a file: URL and whatever app has registered any other
// scheme. Two callers pass a URL the renderer chose, so the scheme is the
// boundary. The outage and updater paths pick their own URLs and are not here.
describe('what may be handed to the browser', () => {
  test('the schemes a browser is for', () => {
    const { origins } = loadShell({ edition: 'saas' })
    assert.ok(origins.isExternallyOpenable('https://example.com/docs'))
    assert.ok(origins.isExternallyOpenable('http://example.com/docs'))
    assert.ok(origins.isExternallyOpenable('mailto:support@example.com'))
  })

  test('and the ones that are a way to start a program', () => {
    const { origins } = loadShell({ edition: 'saas' })
    for (const url of [
      'file:///Applications/Calculator.app',
      'file:///Users/someone/Downloads/payload.command',
      'smb://attacker.example.com/share',
      'ms-msdt:/id PCWDiagnostic',
      'javascript:fetch("/x")',
      'not a url',
      '',
    ]) {
      assert.equal(origins.isExternallyOpenable(url), false, url)
    }
  })
})

describe('where a launch lands', () => {
  // The bug this locks: onboarding was decided from per-install state, so an
  // returning user installing on a new machine got a first-run screen
  // with "Downgrade" next to the plan they already pay for. Only the platform
  // knows whether an account has been set up, and its post-auth funnel already
  // answers it — so the shell asks by entering at sign-in.
  test('a saas first run enters at platform sign-in, never at onboarding', () => {
    const { policy } = loadShell({ edition: 'saas' })
    assert.equal(policy.entryUrl(), 'https://platform.example.com/login')
  })

  test('an install that has reached the app skips the platform', () => {
    const { policy, store } = loadShell({ edition: 'saas' })
    store.set('reachedApp', true)
    assert.equal(policy.entryUrl(), 'https://app.example.com')
  })

  test('an oss build with no server has nothing to open', () => {
    const { policy } = loadShell({ edition: 'oss' })
    assert.equal(policy.entryUrl(), null)
  })
})

describe('which window a page lands in', () => {
  const CONSOLE = 'https://platform.example.com/billing'
  const APP = 'https://app.example.com/dashboard'

  // "Usage & Plan" is a plain link in the web app, so the console shares the
  // window there and should here. The exception is the one that produced the
  // bug: a main window with no titlebar, showing a page that reserves no strip
  // for the window buttons, puts them on top of that page's own header.
  test('the console shares the main window', () => {
    const { policy } = loadShell({ edition: 'saas' })
    assert.equal(policy.classifyNavigation(CONSOLE, { isMainWindow: true }), 'allow')
  })

  // Both flags default true in a saas build, so the exception has to be asked
  // for explicitly now. It is still reachable: the console is a separate deploy,
  // and the day it stops declaring that it reserves is the day this fires.
  test('unless the main window has no titlebar and the console reserves no strip', () => {
    const { policy, store } = loadShell({ edition: 'saas' })
    store.set('appChrome', true)
    store.set('platformChrome', false)
    assert.equal(policy.classifyNavigation(CONSOLE, { isMainWindow: true }), 'platform-window')
  })

  test('and it shares it again once the console reserves one', () => {
    const { policy, store } = loadShell({ edition: 'saas' })
    store.set('appChrome', true)
    store.set('platformChrome', true)
    assert.equal(policy.classifyNavigation(CONSOLE, { isMainWindow: true }), 'allow')
  })

  // What a fresh install assumes before any page has answered. A saas build
  // knows both origins at package time and both of those apps declare that they
  // reserve, so it opens frameless straight away rather than spending the first
  // launch framed. An oss build points at whatever stack the user types in, so
  // it assumes nothing and opens with a titlebar.
  test('a saas install opens frameless before it has loaded anything', () => {
    const { store } = loadShell({ edition: 'saas' })
    assert.equal(store.get('appChrome'), true)
    assert.equal(store.get('platformChrome'), true)
  })

  test('an oss install opens framed until a page says otherwise', () => {
    const { store } = loadShell({ edition: 'oss' })
    assert.equal(store.get('appChrome'), false)
    assert.equal(store.get('platformChrome'), false)
  })

  // The app is the other direction, and has no such condition: it reserves the
  // strip or the main window keeps its titlebar, so it is always at home there.
  test('the app never renders in the console window', () => {
    const { policy } = loadShell({ edition: 'saas' })
    assert.equal(policy.classifyNavigation(APP, { isMainWindow: false }), 'app-window')
  })

  test('a navigation within either app stays where it is', () => {
    const { policy } = loadShell({ edition: 'saas' })
    assert.equal(
      policy.classifyNavigation('https://platform.example.com/onboarding/plan', { isMainWindow: false }),
      'allow',
    )
    assert.equal(policy.classifyNavigation('https://app.example.com/chat', { isMainWindow: true }), 'allow')
  })

  // Observed, not predicted. Classifying a navigation must not record it: a
  // verdict of 'allow' on a link that then drops would retire onboarding for an
  // install that never once reached the app, and every later launch would open
  // the app origin unauthenticated instead of platform sign-in.
  test('a completed load of the app is what retires onboarding', () => {
    const { main, policy, store } = loadShell({ edition: 'saas' })
    assert.equal(store.get('reachedApp'), false)
    policy.classifyNavigation(APP, { isMainWindow: false })
    assert.equal(store.get('reachedApp'), false)
    main.noteAppReached(APP, 200)
    assert.equal(store.get('reachedApp'), true)
  })

  test('a load of anything else does not', () => {
    const { main, store } = loadShell({ edition: 'saas' })
    main.noteAppReached('https://platform.example.com/login', 200)
    main.noteAppReached('https://evil.example.com/dashboard', 200)
    assert.equal(store.get('reachedApp'), false)
  })

  // An edge answering 502 is a completely successful load of an error document,
  // so 'the load finished' is not the same question as 'the app was there'. One
  // bad gateway on a first run used to retire onboarding permanently: every
  // launch afterwards opened the app origin unauthenticated, for someone who had
  // never signed in, and nothing in the app could undo it.
  test('a load the server refused is not an arrival', () => {
    for (const status of [500, 502, 503, 404, 401, 0]) {
      const { main, store } = loadShell({ edition: 'saas' })
      main.noteAppReached(APP, status)
      assert.equal(store.get('reachedApp'), false, `status ${status} retired onboarding`)
    }
    // The whole 2xx/3xx range still counts, including the redirect the app
    // origin answers a signed-in user with.
    for (const status of [200, 204, 302]) {
      const { main, store } = loadShell({ edition: 'saas' })
      main.noteAppReached(APP, status)
      assert.equal(store.get('reachedApp'), true, `status ${status} was not an arrival`)
    }
  })
})

// The stored answer to "does the app reserve the window-button strip" is learned
// from a declaration the loaded page makes, which makes it an answer about that
// ORIGIN. In the OSS edition the origin is whatever the user typed, so it can
// change under the flag.
// A completed load is the only thing that revokes the window-chrome flag, and it
// reads a status that arrived on a SEPARATE event. Pairing that status with its
// URL is what stops one navigation answering for another.
describe('what a finished load is allowed to conclude', () => {
  const APP = 'http://stack.example:5243'

  // A window that answers like a page which ships no declaration, so the only
  // thing deciding the outcome is the status the load is credited with.
  const loadedWindow = (at) => ({
    isDestroyed: () => false,
    webContents: Object.assign(new EventEmitter(), {
      getURL: () => at,
      executeJavaScript: async () => null,
    }),
  })

  const finish = async (win, navigatedTo, status) => {
    win.webContents.emit('did-navigate', {}, navigatedTo, status)
    win.webContents.emit('did-finish-load')
    // The handler is async: it awaits the declaration before it decides.
    await new Promise((r) => setImmediate(r))
  }

  test('a 502 from the origin does not revoke the flag', async () => {
    const { main, store } = loadShell({ edition: 'oss', serverUrl: APP })
    store.set('appChrome', true)
    const win = loadedWindow(`${APP}/dashboard`)
    main.watchLoads(win)

    await finish(win, `${APP}/dashboard`, 502)

    // A bad gateway is a successful load of an error document, and an error
    // document declares nothing either. Reading that as "does not reserve" would
    // reframe the window on an outage instead of on a real rollback.
    assert.equal(store.get('appChrome'), true)
  })

  test('a status belonging to another navigation does not answer for this one', async () => {
    const { main, store } = loadShell({ edition: 'oss', serverUrl: APP })
    store.set('appChrome', true)
    const win = loadedWindow(`${APP}/dashboard`)
    main.watchLoads(win)

    // The shape the outage screen makes: it aborts the load that failed and
    // navigates the window to its own page, so the last status seen is its own.
    await finish(win, 'file:///tmp/outage.html', 200)

    assert.equal(store.get('appChrome'), true, 'an unmatched pair is not evidence of anything')
  })

  test('a page the origin really served, declaring nothing, does revoke it', async () => {
    const { main, store } = loadShell({ edition: 'oss', serverUrl: APP })
    store.set('appChrome', true)
    const win = loadedWindow(`${APP}/dashboard`)
    main.watchLoads(win)

    await finish(win, `${APP}/dashboard`, 200)

    // Nothing else revokes this: changing origins resets it, but redeploying an
    // older build at the same origin does not.
    assert.equal(store.get('appChrome'), false)
  })
})

describe('adopting a server', () => {
  test('a scheme that is not a browser scheme is refused', () => {
    const { main, store } = loadShell({ edition: 'oss' })
    for (const bad of ['file:///etc/passwd', 'javascript:alert(1)', 'data:text/html,x', 'not a url']) {
      assert.equal(main.adoptServer(bad).ok, false, bad)
    }
    // `new URL('file:///x').origin` is the STRING "null", which is truthy: it
    // stores fine, `entryUrl` then reads the picker as answered, and
    // `origins.appOrigin` throws on it and falls back to the compiled default.
    // The install can never reach its own picker again.
    assert.equal(store.get('serverUrl'), null)
  })

  test('an address carrying credentials is refused, not silently stripped', () => {
    const { main, store } = loadShell({ edition: 'oss' })
    for (const bad of ['http://deploy:hunter2@a.example:8080', 'http://token@a.example:8080']) {
      const result = main.adoptServer(bad)
      assert.equal(result.ok, false, bad)
      // The path guard cannot catch these: the pathname of `https://u:p@host`
      // is '/'. And `parsed.origin` drops the userinfo, so accepting would store
      // a bare host that can no longer authenticate against the stack the user
      // actually named.
      assert.match(result.error, /username or password/)
    }
    assert.equal(store.get('serverUrl'), null)
  })

  test('pointing somewhere new forgets what the last server declared', () => {
    const { main, store } = loadShell({ edition: 'oss' })
    assert.equal(main.adoptServer('http://a.example:8080').ok, true)
    // What a modern server that ships the declaration would have taught it.
    store.set('appChrome', true)
    store.set('platformChrome', true)

    // An older build that ships no declaration answers null, which is
    // deliberately not recorded as a "no" — so without the reset the stale
    // `true` survived and the next launch opened frameless against a page that
    // reserves nothing: window buttons over its header, and nothing draggable.
    assert.equal(main.adoptServer('http://b.example:8080').ok, true)
    assert.equal(store.get('appChrome'), false)
    assert.equal(store.get('platformChrome'), false)
  })

  test('re-adopting the same server keeps what it already taught', () => {
    const { main, store } = loadShell({ edition: 'oss' })
    main.adoptServer('http://a.example:8080')
    store.set('appChrome', true)
    main.adoptServer('http://a.example:8080/')
    assert.equal(store.get('appChrome'), true, 'the same origin re-learned nothing')
  })
})

describe('authorize-URL classification', () => {
  let oauth
  before(() => { ({ oauth } = loadShell({ edition: 'saas' })) })

  test('matches the Supabase authorize endpoint on any host', () => {
    // Host-agnostic on purpose: the shell is never told which Supabase project
    // the web build points at, and a self-hoster brings their own.
    assert.ok(oauth.isAuthorizeUrl(`${SUPABASE}?provider=google&redirect_to=https://app.example.com/callback`))
    assert.ok(oauth.isAuthorizeUrl('https://other.supabase.co/auth/v1/authorize?redirect_to=x'))
  })

  test('ignores anything that is not an authorize navigation', () => {
    assert.equal(oauth.isAuthorizeUrl(`${SUPABASE}?provider=google`), false, 'no redirect_to')
    assert.equal(oauth.isAuthorizeUrl('https://ref.supabase.co/auth/v1/token?redirect_to=x'), false)
    assert.equal(oauth.isAuthorizeUrl('https://app.example.com/dashboard'), false)
    assert.equal(oauth.isAuthorizeUrl('about:blank'), false)
    assert.equal(oauth.isAuthorizeUrl('langalpha://callback?redirect_to=x'), false)
    assert.equal(oauth.isAuthorizeUrl(''), false)
  })

  test('a suffix match cannot be spoofed by a lookalike path', () => {
    assert.equal(
      oauth.isAuthorizeUrl('https://evil.example.com/not-auth/v1/authorizex?redirect_to=x'),
      false,
    )
  })
})

describe('interception, and what it refuses', () => {
  // The dangerous case: taking over a flow whose redirect_to points elsewhere
  // would drive our own window wherever a crafted link said, carrying a code
  // the user had just authorized. Live testing cannot produce this on demand.
  let oauth
  before(() => { ({ oauth } = loadShell({ edition: 'saas' })) })
  const win = windowStub()

  const authorize = (redirectTo) => {
    const u = new URL(SUPABASE)
    u.searchParams.set('provider', 'google')
    u.searchParams.set('code_challenge', 'CHALLENGE')
    if (redirectTo) u.searchParams.set('redirect_to', redirectTo)
    return u.toString()
  }

  // begin() bails on its first line when nothing is listening, so without a live
  // listener every refusal below would pass for the wrong reason.
  let port = null
  before(async () => {
    port = await oauth.startCallbackServer()
    assert.ok(port, 'no free callback port for the test')
  })
  after(() => oauth.stopCallbackServer())

  test('a redirect_to into one of our origins is taken over', () => {
    opened.length = 0
    assert.equal(oauth.begin(authorize('https://app.example.com/callback'), win), true)
    assert.equal(opened.length, 1, 'the authorize URL should go to the system browser')

    const sent = new URL(opened[0])
    assert.equal(sent.origin + sent.pathname, SUPABASE, 'still the same authorize endpoint')
    assert.match(sent.searchParams.get('redirect_to'), /^http:\/\/127\.0\.0\.1:\d+\/callback$/)
    // Rewriting redirect_to must not disturb the rest: the challenge belongs to
    // a verifier sitting in the renderer, and losing it fails the exchange.
    assert.equal(sent.searchParams.get('code_challenge'), 'CHALLENGE')
    assert.equal(sent.searchParams.get('provider'), 'google')
  })

  test('a redirect_to outside our origins is not taken over', () => {
    opened.length = 0
    assert.equal(oauth.begin(authorize('https://evil.example.com/callback'), win), false)
    assert.equal(opened.length, 0, 'nothing should have reached the browser')
  })

  // `isAuthorizeUrl` matches a pathname suffix, so any host at all can present
  // one. Refusing to intercept it is not the same as vouching for it: answering
  // 'allow' let a path decide what the origin policy is there to decide, and
  // loaded the foreign page into the window that has the preload bridge.
  test('an authorize URL the shell refuses is still judged on its origin', () => {
    const { main, policy } = loadShell({ edition: 'saas' })
    const hostile = 'https://evil.example.com/auth/v1/authorize?redirect_to=https://evil.example.com/cb'
    assert.equal(policy.classifyNavigation(hostile, { isMainWindow: true }), 'external')
    assert.equal(main.decide(hostile, windowStub(), { isMainWindow: true }), 'external')
  })

  test('a missing redirect_to is not taken over', () => {
    opened.length = 0
    assert.equal(oauth.begin(authorize(null), win), false)
    assert.equal(opened.length, 0)
  })

  // Anything on this machine can reach a loopback port, and a bare GET used to
  // be enough to consume the flow: a probe, a favicon fetch, or the URL left in
  // a tab would send the window to 'sign-in failed', and the real callback
  // arriving a moment later then found nothing waiting.
  test('a /callback carrying neither code nor error does not consume the flow', async () => {
    const landed = []
    const waiting = windowStub(landed)
    assert.equal(oauth.begin(authorize('https://app.example.com/callback'), waiting), true)

    const bare = await fetch(`http://127.0.0.1:${port}/callback`)
    assert.equal(bare.status, 404)
    assert.deepEqual(landed, [], 'the window should not have been sent anywhere')

    const real = await fetch(`http://127.0.0.1:${port}/callback?code=abc123`)
    assert.equal(real.status, 200)
    assert.equal(landed.length, 1, 'the real callback should still be honoured')
    assert.equal(new URL(landed[0]).searchParams.get('code'), 'abc123')
  })

  // The loopback port is reachable from any page on the open web, not just from
  // this machine: `<img src="http://127.0.0.1:<port>/callback?error=x">` needs no
  // CORS, because the attacker never has to read the reply. The damage is the
  // side effect — the pending flow is consumed and the window signing in is
  // driven to a failure it never had. What separates that from the provider is
  // not a parameter (the attacker supplies those too) but how the request was
  // made, which the browser states and a page inside it cannot forge.
  test('a /callback fetched as a subresource cannot consume the flow', async () => {
    const landed = []
    const waiting = windowStub(landed)
    assert.equal(oauth.begin(authorize('https://app.example.com/callback'), waiting), true)

    for (const dest of ['image', 'empty', 'iframe', 'script', 'style']) {
      const forged = await fetch(`http://127.0.0.1:${port}/callback?error=forged`, {
        headers: { 'sec-fetch-dest': dest },
      })
      assert.equal(forged.status, 404, `${dest} was answered`)
      assert.deepEqual(landed, [], `${dest} moved the window`)
    }

    // The flow is still live, so the provider's own navigation still lands.
    const real = await fetch(`http://127.0.0.1:${port}/callback?code=abc123`, {
      headers: { 'sec-fetch-dest': 'document' },
    })
    assert.equal(real.status, 200)
    assert.equal(landed.length, 1)
  })

  // Everything this shell decides is read with `get()`, which answers the FIRST
  // value, while the forward loop rebuilds the query with `set()`, which keeps
  // the LAST. A repeated parameter therefore splits the two: the flow is
  // approved against one state and the backend is handed another, so neither
  // end can tell it was handed a different callback than the one it checked.
  test('a callback whose query repeats a parameter is not answered', async () => {
    const landed = []
    const waiting = windowStub(landed)
    assert.equal(oauth.begin(authorize('https://app.example.com/callback'), waiting), true)

    const split = await fetch(
      `http://127.0.0.1:${port}/callback?code=abc123&code=zzz`,
      { headers: { 'sec-fetch-dest': 'document' } },
    )
    assert.equal(split.status, 404)
    assert.deepEqual(landed, [], 'the split callback moved the window')

    // Not spent by the refusal: the real navigation still completes.
    const real = await fetch(`http://127.0.0.1:${port}/callback?code=abc123`, {
      headers: { 'sec-fetch-dest': 'document' },
    })
    assert.equal(real.status, 200)
    assert.equal(landed.length, 1)
  })

  test('a client that states nothing is still served', () => {
    // curl, a non-browser client, and any browser too old to send the header.
    // A page inside a browser that DOES send it cannot suppress it, so treating
    // absence as hostile would break real clients and stop no attack.
    assert.equal(oauth.isProviderNavigation({}), true)
    assert.equal(oauth.isProviderNavigation({ 'sec-fetch-dest': 'document' }), true)
    assert.equal(oauth.isProviderNavigation({ 'sec-fetch-dest': 'image' }), false)
  })

  // Five minutes is shorter than a research turn, and 'superseded' needs no
  // network round trip at all — so any page in the shell could force the main
  // window to navigate just by starting two flows. Neither is worth throwing
  // away a page the user went back to work on.
  test('an expired flow does not evict a window that moved on', async () => {
    const landed = []
    const waiting = windowStub(landed)
    assert.equal(oauth.begin(authorize('https://app.example.com/callback'), waiting), true)

    // The user gives up on signing in and goes back to a running turn.
    waiting.loadURL('https://app.example.com/chat/t/abc')
    assert.equal(landed.length, 1)

    // A second flow supersedes the first, which ends it with an error.
    assert.equal(oauth.begin(authorize('https://app.example.com/callback'), windowStub()), true)
    assert.deepEqual(landed, ['https://app.example.com/chat/t/abc'], 'the turn was evicted')
  })

  test('but a code still lands, wherever the user is', async () => {
    const landed = []
    const waiting = windowStub(landed)
    assert.equal(oauth.begin(authorize('https://app.example.com/callback'), waiting), true)
    waiting.loadURL('https://app.example.com/chat/t/abc')

    // Completing a sign-in is the user asking for this, so it lands regardless.
    const real = await fetch(`http://127.0.0.1:${port}/callback?code=xyz`, {
      headers: { 'sec-fetch-dest': 'document' },
    })
    assert.equal(real.status, 200)
    assert.equal(landed.length, 2)
    assert.equal(new URL(landed[1]).searchParams.get('code'), 'xyz')
  })

  // The port is asked for, never chosen (RFC 8252 7.3), and the allowlist entry
  // on the provider's side is a wildcard because of it. A hardcoded number would
  // still pass every test above -- the listener works fine on any port -- and
  // would fail in production only for the users whose machine already had that
  // one taken, which is the hardest possible thing to reproduce. So the absence
  // is asserted at the source, where reintroducing it is visible.
  test('no callback port is hardcoded', () => {
    const source = fs.readFileSync(path.join(__dirname, '..', 'src', 'oauth.js'), 'utf8')
    assert.match(source, /\.listen\(EPHEMERAL_PORT, '127\.0\.0\.1'/)
    assert.equal(/^const EPHEMERAL_PORT = 0$/m.test(source), true)
    for (const line of source.split('\n')) {
      assert.doesNotMatch(line, /\.listen\(\s*\d/, `a literal port reached listen(): ${line.trim()}`)
    }
  })

  // And what it hands out is the port it actually got, which is the only way a
  // caller can learn an OS-assigned one.
  test('the port it reports is the one it bound', () => {
    assert.ok(port >= 1024 && port <= 65535, `port ${port} is outside the usable range`)
  })

  // A code that arrives with nothing waiting is discarded, and the page written
  // for it is the only thing the person in the browser ever sees. Telling them
  // "Signed in" for a sign-in the app was never handed left the two surfaces
  // they are looking at contradicting each other.
  test('a discarded callback does not tell the browser it worked', async () => {
    const orphan = await fetch(`http://127.0.0.1:${port}/callback?code=nothing-is-waiting`, {
      headers: { 'sec-fetch-dest': 'document' },
    })
    assert.equal(orphan.status, 200)
    const page = await orphan.text()
    assert.match(page, /Sign-in failed/)
    assert.doesNotMatch(page, /<h1[^>]*>Signed in/)
  })
})

// ---------------------------------------------------------------------------
// The connector flow. Same listener, nothing else in common with sign-in: this
// code is redeemable only by the backend that minted the flow, so the shell's
// whole job is catching it and driving the window to that backend's callback.
// ---------------------------------------------------------------------------
describe('an MCP connector whose provider allows only a loopback callback', () => {
  let oauth
  before(() => { ({ oauth } = loadShell({ edition: 'saas' })) })

  const RETURN = 'https://app.example.com/api/v1/mcp/oauth/callback'
  const PLUGINS = 'https://app.example.com/plugins?tab=mcp'

  let port = null
  before(async () => {
    port = await oauth.startCallbackServer()
    assert.ok(port, 'no free callback port for the test')
  })
  after(() => oauth.stopCallbackServer())

  // `agent: false` rather than fetch(). Both suites here take whichever loopback
  // port is free, so they routinely land on the same one, and fetch's pool keeps
  // a socket to it from the suite before — dispatched onto a server that has
  // since closed, which reads as ECONNRESET in whichever test happens to be
  // first. A connection per request has no pool to go stale.
  const hit = (path, query, dest = 'document') => new Promise((resolve, reject) => {
    const req = http.request(
      { host: '127.0.0.1', port, path: `${path}?${query}`, agent: false,
        headers: { 'sec-fetch-dest': dest } },
      (res) => {
        let body = ''
        res.setEncoding('utf8')
        res.on('data', (c) => { body += c })
        res.on('end', () => resolve({ status: res.statusCode, text: async () => body }))
      },
    )
    req.on('error', reject)
    req.end()
  })

  // Arm and bind, the way the page does it: the shell learns the flow's `state`
  // only after the backend has minted it, and an unbound flow accepts nothing.
  const armed = async (win, state = 'rh-state') => {
    const flow = await oauth.beginMcp(RETURN, win)
    assert.ok(flow, 'no flow was armed')
    assert.equal(oauth.bindMcp(win, flow.flowId, state), true)
    return flow
  }

  // Every test starts from a listener with nothing armed. Several of these arm a
  // flow and leave it, and the store is module-level, so without this the suite
  // passes in declaration order and nothing else: a shuffle, a `.only`, or a
  // test inserted between two of them changes what the next one starts from.
  //
  // Tearing the listener down is what empties it, and the port is re-read after
  // because a reopen can land on a different one. A leaked flow is not merely
  // untidy here: flows are found by `state`, most of these use the same one, and
  // the older flow is the one a later callback would match.
  beforeEach(async () => {
    oauth.stopCallbackServer()
    port = await oauth.startCallbackServer()
    assert.ok(port, 'no free callback port for the test')
  })

  test('the page is told which URI to mint the flow against', async () => {
    const flow = await oauth.beginMcp(RETURN, windowStub([], PLUGINS))
    assert.equal(flow.redirectUri, `http://127.0.0.1:${port}/mcp/callback`)
    assert.ok(flow.flowId, 'the flow has no id to name it by later')
  })

  // The backend allowlists this value before it will mint a flow against it:
  // http, a loopback IP literal, a port at or above 1024, no userinfo, no query
  // and no fragment. A change on either side that broke the agreement would
  // degrade every desktop connect to the hosted callback silently, which for
  // the vendors this exists for is a dead end with no error to report.
  test('the URI it mints is one the backend will accept', async () => {
    const u = new URL((await oauth.beginMcp(RETURN, windowStub([], PLUGINS))).redirectUri)
    assert.ok(Number(u.port) >= 1024, `port ${u.port} is below the backend's floor`)
    assert.equal(u.protocol, 'http:')
    assert.equal(u.hostname, '127.0.0.1')
    assert.equal(u.pathname, oauth.MCP_CALLBACK_PATH)
    assert.equal(u.search, '')
    assert.equal(u.hash, '')
    assert.equal(u.username, '')
  })

  // Everything the provider sent, not a chosen few. `iss` is the one that was
  // being dropped: the backend checks it against the metadata of the server the
  // request went to, and an authorization server that advertises it makes the
  // whole flow fail closed when it does not arrive.
  test('every parameter the provider returned reaches the app callback', async () => {
    const landed = []
    await armed(windowStub(landed, PLUGINS))

    const back = await hit('/mcp/callback', 'code=rh-code&state=rh-state&iss=https%3A%2F%2Fas.test')
    assert.equal(back.status, 200)
    // Receipt, not success: the code has not reached the backend yet, so the
    // exchange, the issuer check and the write are all still ahead of it.
    const page = await back.text()
    assert.match(page, /Authorization received/)
    assert.doesNotMatch(page, /<h1[^>]*>Connected/)

    assert.equal(landed.length, 1)
    const got = new URL(landed[0])
    assert.equal(got.origin + got.pathname, RETURN)
    assert.equal(got.searchParams.get('code'), 'rh-code')
    assert.equal(got.searchParams.get('state'), 'rh-state')
    assert.equal(got.searchParams.get('iss'), 'https://as.test')
  })

  // The backend classifies on the OAuth error code, and `access_denied` is the
  // user pressing Cancel rather than anything having gone wrong. Folding the
  // description over it reported every cancel as a provider fault.
  test('a denial keeps its code and its description apart', async () => {
    const landed = []
    await armed(windowStub(landed, PLUGINS))

    const back = await hit(
      '/mcp/callback',
      'error=access_denied&error_description=User+declined&state=rh-state',
    )
    assert.equal(back.status, 200)
    assert.match(await back.text(), /User declined/)

    const got = new URL(landed[0])
    assert.equal(got.searchParams.get('error'), 'access_denied')
    assert.equal(got.searchParams.get('error_description'), 'User declined')
  })

  // The tab leaves the other brokers clickable on purpose, so two connector
  // flows a second apart is ordinary. The path says only 'a connector', so a
  // late callback for the first would have been handed to the second and taken
  // the slot with it: the backend completes a connection nobody is waiting on
  // while the one the user is watching is told nothing arrived.
  test("a callback for another flow does not spend this one", async () => {
    const landed = []
    await armed(windowStub(landed, PLUGINS), 'second-flow-state')

    const stale = await hit('/mcp/callback', 'code=for-the-first&state=first-flow-state')
    assert.equal(stale.status, 200)
    assert.deepEqual(landed, [], 'a stale callback moved the window')

    const real = await hit('/mcp/callback', 'code=rh-code&state=second-flow-state')
    assert.equal(real.status, 200)
    assert.equal(landed.length, 1, 'the live flow should still have been there')
    assert.equal(new URL(landed[0]).searchParams.get('code'), 'rh-code')
  })

  // Between arming and binding there is no authorize URL in the world for this
  // flow, so no callback for it can exist. That is also what stops a page in the
  // system browser from spending the slot by navigating this port with an
  // invented error, which `sec-fetch-dest: document` cannot tell from the real
  // thing.
  test('a flow that was never bound accepts nothing', async () => {
    const landed = []
    const win = windowStub(landed, PLUGINS)
    assert.ok(await oauth.beginMcp(RETURN, win))

    const forged = await hit('/mcp/callback', 'error=forged&state=guessed')
    assert.equal(forged.status, 200)
    assert.deepEqual(landed, [], 'an unbound flow was spent')
  })

  test('only the flow that was armed can be bound', async () => {
    const win = windowStub([], PLUGINS)
    const flow = await oauth.beginMcp(RETURN, win)
    assert.equal(oauth.bindMcp(win, 'not-the-flow-id', 'x'), false)
    assert.equal(oauth.bindMcp(windowStub([], PLUGINS), flow.flowId, 'x'), false)
    assert.equal(oauth.bindMcp(win, flow.flowId, ''), false)
    assert.equal(oauth.bindMcp(win, flow.flowId, 'real-state'), true)
  })

  // The window is driven wherever `returnUrl` says, carrying a code the user
  // just authorized. Any page the shell renders can call this, so neither the
  // destination nor the asker may be taken on trust.
  test('a return outside our origins is refused', async () => {
    assert.equal(await oauth.beginMcp('https://evil.example.com/steal', windowStub([], PLUGINS)), null)
    assert.equal(await oauth.beginMcp('', windowStub([], PLUGINS)), null)
  })

  test('an asker outside our origins is refused', async () => {
    assert.equal(await oauth.beginMcp(RETURN, windowStub([], 'https://evil.example.com/page')), null)
  })

  // One slot, two flows. A code for one is redeemable only by the party that
  // holds the other end of that flow, so handing it to whatever happens to be
  // waiting could not work and would consume the slot on the way.
  test('a sign-in callback cannot consume a connector flow', async () => {
    const landed = []
    await armed(windowStub(landed, PLUGINS))

    const wrong = await hit('/callback', 'code=not-for-this-flow')
    assert.equal(wrong.status, 200)
    assert.match(await wrong.text(), /Sign-in failed/)
    assert.deepEqual(landed, [], 'the connector flow was spent on a sign-in callback')

    const real = await hit('/mcp/callback', 'code=rh-code&state=rh-state')
    assert.equal(real.status, 200)
    assert.equal(landed.length, 1, 'the connector flow should still have been live')
  })

  test('and a connector callback cannot consume a sign-in', async () => {
    const landed = []
    const authorize = `${SUPABASE}?provider=google&redirect_to=https://app.example.com/callback`
    assert.equal(oauth.begin(authorize, windowStub(landed, PLUGINS)), true)

    const wrong = await hit('/mcp/callback', 'code=not-for-this-flow&state=x')
    assert.equal(wrong.status, 200)
    assert.match(await wrong.text(), /Connection failed/)
    assert.deepEqual(landed, [], 'the sign-in was spent on a connector callback')

    const real = await hit('/callback', 'code=supabase-code')
    assert.equal(real.status, 200)
    assert.equal(landed.length, 1)
    assert.equal(new URL(landed[0]).searchParams.get('code'), 'supabase-code')
  })

  // A timeout or a supersede is this shell talking, not the provider. Passing it
  // on as an authorization error would have the backend explain a failure that
  // never happened there, and the page would report the provider refused a
  // connection the provider was never asked about. The window goes back where it
  // started instead, which is all it takes to leave the connecting state.
  test('a failure the shell invented is not reported as the provider refusing', async (t) => {
    t.mock.timers.enable({ apis: ['setTimeout'] })
    const landed = []
    await armed(windowStub(landed, PLUGINS))

    // The shell's own clock running out, which is the only way a connector flow
    // ends without the authorization server having said anything.
    t.mock.timers.tick(10 * 60_000)

    assert.deepEqual(landed, [PLUGINS])
  })

  // The shell must not give up before the backend does: its record outlives this
  // by design, so a shorter clock here can only discard a flow the server would
  // still have completed. Brokerage consent behind 2FA routinely runs long.
  test('the shell waits at least as long as the backend keeps the flow', async (t) => {
    t.mock.timers.enable({ apis: ['setTimeout'] })
    const landed = []
    await armed(windowStub(landed, PLUGINS))

    t.mock.timers.tick(5 * 60_000 + 1000)
    assert.deepEqual(landed, [], 'the flow was dropped while the backend still held it')

    t.mock.timers.tick(5 * 60_000)
    assert.deepEqual(landed, [PLUGINS])
  })

  // The backend's own clock starts when it mints the state, and the page has to
  // arm before it can ask for one -- the redirect_uri travels with that request.
  // Discovery, and a client registration if the vendor has not seen us before,
  // happen in between. A clock left running from `beginMcp` spends them out of
  // the user's time at the consent screen and drops a flow the backend would
  // still have redeemed.
  test('the clock starts when the backend state lands, not when the listener arms', async (t) => {
    t.mock.timers.enable({ apis: ['setTimeout'] })
    const landed = []
    const win = windowStub(landed, PLUGINS)
    const flow = await oauth.beginMcp(RETURN, win)
    assert.ok(flow, 'no flow was armed')

    // Phase 1, on a vendor whose metadata has to be fetched twice.
    t.mock.timers.tick(60_000)
    assert.equal(oauth.bindMcp(win, flow.flowId, 'rh-state'), true)

    t.mock.timers.tick(10 * 60_000 - 1000)
    assert.deepEqual(landed, [], 'phase 1 was charged to the user at the consent screen')

    t.mock.timers.tick(1000)
    assert.deepEqual(landed, [PLUGINS])
  })

  // Only the first bind moves it. Any page the shell renders can call this, and
  // one that kept re-binding would otherwise hold a loopback port open for as
  // long as it cared to.
  test('binding a second time does not push the clock back', async (t) => {
    t.mock.timers.enable({ apis: ['setTimeout'] })
    const landed = []
    const win = windowStub(landed, PLUGINS)
    const flow = await oauth.beginMcp(RETURN, win)
    assert.equal(oauth.bindMcp(win, flow.flowId, 'rh-state'), true)

    t.mock.timers.tick(9 * 60_000)
    assert.equal(oauth.bindMcp(win, flow.flowId, 'rh-state-again'), true)

    t.mock.timers.tick(60_000)
    assert.deepEqual(landed, [PLUGINS], 'a second bind bought the flow more time')
  })

  // Same reasoning as the sign-in listener, and the same attack: any page on the
  // open web can reach a loopback port with `<img src>`, needing no CORS because
  // it never has to read the reply.
  test('a connector callback fetched as a subresource cannot consume the flow', async () => {
    const landed = []
    await armed(windowStub(landed, PLUGINS), 's')

    for (const dest of ['image', 'empty', 'iframe', 'script']) {
      const forged = await hit('/mcp/callback', 'error=forged&state=s', dest)
      assert.equal(forged.status, 404, `${dest} was answered`)
      assert.deepEqual(landed, [], `${dest} moved the window`)
    }

    assert.equal((await hit('/mcp/callback', 'code=rh-code&state=s')).status, 200)
    assert.equal(landed.length, 1)
  })

  // One window, two flows, and the window is the delivery. Each callback loads
  // it with the backend's callback URL, and a load that supersedes another is
  // routine for a page and fatal here: the superseded one is an authorization
  // code that never reaches the backend, on a flow whose browser tab was told
  // "Authorization received" a moment earlier.
  test('a second connector callback waits for the first to arrive', async () => {
    const landed = []
    const win = windowStub(landed, PLUGINS)
    const holding = []
    win.loadURL = (u) => {
      landed.push(u)
      return new Promise((resolve) => holding.push(resolve))
    }
    await armed(win, 'first')
    await armed(win, 'second')

    assert.equal((await hit('/mcp/callback', 'code=code-1&state=first')).status, 200)
    assert.equal((await hit('/mcp/callback', 'code=code-2&state=second')).status, 200)

    // The second flow's tab has been answered, but its code has not been handed
    // anywhere yet -- the window is still carrying the first one.
    assert.equal(landed.length, 1, 'the second handoff went out over the first')
    assert.match(landed[0], /code=code-1/)

    holding[0]()
    await new Promise((resolve) => setImmediate(resolve))

    assert.equal(landed.length, 2)
    assert.match(landed[1], /code=code-2/)

    // Both loads settled before this test ends: the queue is module state, and
    // one left mid-delivery is one every test after this waits behind.
    holding[1]()
    await new Promise((resolve) => setImmediate(resolve))
  })

  test('the listener answers nothing else', async () => {
    await armed(windowStub([], PLUGINS))
    assert.equal((await hit('/mcp/callback', 'nothing=here')).status, 404)
    assert.equal((await hit('/mcp', 'code=x')).status, 404)
    assert.equal((await hit('/mcp/callback/extra', 'code=x')).status, 404)
  })

  // The page has to arm before it knows whether its backend will mint a flow at
  // all, because the redirect_uri travels with that request. A start that fails
  // therefore leaves a flow armed for a code nobody is going to send, and left
  // alone it runs the full timeout and then reloads the window.
  describe('standing a flow down that never launched', () => {
    test('the slot is free again, and the window was never touched', async () => {
      const landed = []
      const win = windowStub(landed, PLUGINS)
      const flow = await armed(win)

      assert.equal(oauth.cancelMcp(win, flow.flowId), true)
      assert.deepEqual(landed, [], 'cancelling is not an outcome to report')

      // Nothing is waiting, so a callback now is refused rather than consumed.
      assert.equal((await hit('/mcp/callback', 'code=late&state=rh-state')).status, 200)
      assert.deepEqual(landed, [], 'a stood-down flow still moved the window')
    })

    test('a second cancel finds nothing, and says so', async () => {
      const win = windowStub([], PLUGINS)
      const flow = await armed(win)
      assert.equal(oauth.cancelMcp(win, flow.flowId), true)
      assert.equal(oauth.cancelMcp(win, flow.flowId), false)
    })

    test("another window's flow is not this caller's to cancel", async () => {
      const landed = []
      const flow = await armed(windowStub(landed, PLUGINS))
      // The real flow id, from a window that does not own it. An invented id
      // misses the lookup and is refused before the windows are ever compared,
      // so the scoping this test is named for would go unexercised.
      assert.equal(oauth.cancelMcp(windowStub([], PLUGINS), flow.flowId), false)

      // Still armed, so the code still gets home.
      return hit('/mcp/callback', 'code=rh-code&state=rh-state').then(() => {
        assert.equal(landed.length, 1)
      })
    })

    // A start that is still in flight can fail after a second one has armed. It
    // stands down its own flow and only its own: cancelling by 'the connector
    // flow in this window' tore down a live flow the user was watching, whose
    // real callback then arrived with nothing waiting.
    test("a failed start stands down its own flow and leaves the next one running", async () => {
      const landed = []
      const win = windowStub(landed, PLUGINS)
      const first = await oauth.beginMcp(RETURN, win)
      const second = await armed(win, 'second-state')
      assert.notEqual(first.flowId, second.flowId)

      assert.equal(oauth.cancelMcp(win, first.flowId), true)
      assert.equal(oauth.cancelMcp(win, first.flowId), false, 'it was stood down twice')

      assert.equal((await hit('/mcp/callback', 'code=rh-code&state=second-state')).status, 200)
      assert.equal(landed.length, 1, 'the live flow was torn down by the failed one')
    })

    // Two consent screens open at once is ordinary use on this tab, and the one
    // the user finishes second must still get home. A single slot silently
    // dropped whichever was armed first: its row span forever while its grant
    // was collected by a vendor nothing would ever redeem it against.
    test('two connects in one window each complete on their own callback', async () => {
      const landed = []
      const win = windowStub(landed, PLUGINS)
      const first = await armed(win, 'first-state')
      const second = await armed(win, 'second-state')
      assert.notEqual(first.flowId, second.flowId)

      assert.equal((await hit('/mcp/callback', 'code=b&state=second-state')).status, 200)
      assert.equal((await hit('/mcp/callback', 'code=a&state=first-state')).status, 200)

      assert.equal(landed.length, 2, 'one of the two flows was dropped')
      assert.match(landed[0], /code=b/)
      assert.match(landed[1], /code=a/)
    })
  })
})

// Three ports, and a preview plus a packaged app plus one other local service is
// enough to take them all, so this is a state this machine reaches rather than a
// hypothetical. What it must not become is a flow handed to the system browser:
// the code would come back to a browser profile holding none of the PKCE verifier
// the renderer minted, so it could never be redeemed and the window would never
// be told why.
describe('who is allowed to start a sign-in', () => {
  let oauth
  before(() => { ({ oauth } = loadShell({ edition: 'saas' })) })

  const authorizeUrl = (host) =>
    `https://${host}/auth/v1/authorize?redirect_to=${encodeURIComponent('https://app.example.com/callback')}`

  test('a page that is not ours cannot claim a flow, even pointing back at us', () => {
    // Every window.open goes through this path, so third-party content the shell
    // renders would otherwise be able to supersede a real sign-in, or steer the
    // window to a path of its choosing on our own origin: `isOurs` answers for
    // the origin, not the path.
    const landed = []
    const win = windowStub(landed, 'https://widget.example.net/embed')
    assert.equal(oauth.begin(authorizeUrl('attacker.example'), win), false, 'not ours to take')
    assert.deepEqual(landed, [], 'and the window was not steered anywhere')
  })

  test('but our own page may, on whatever host its project lives', () => {
    // The control that keeps the guard honest. A self-hoster brings their own
    // Supabase project, so the authorize host is deliberately not pinned; what
    // has to be ours is the page asking and the place it comes back to.
    const landed = []
    const win = windowStub(landed, 'https://app.example.com/login')
    assert.equal(oauth.begin(authorizeUrl('someone-elses-project.supabase.co'), win), true, 'claimed')
    oauth.cancel?.()
  })
})

describe('an authorize URL with nowhere to come back to', () => {
  let oauth
  before(() => {
    ({ oauth } = loadShell({ edition: 'saas' }))
    oauth.stopCallbackServer()
  })

  const authorize = (redirectTo) => {
    const u = new URL(SUPABASE)
    u.searchParams.set('provider', 'google')
    if (redirectTo) u.searchParams.set('redirect_to', redirectTo)
    return u.toString()
  }

  test('is claimed and reported rather than externalized', () => {
    opened.length = 0
    const landed = []
    assert.equal(oauth.begin(authorize('https://app.example.com/auth/callback'), windowStub(landed)), true,
      'declining hands this to the system browser, where it cannot finish')
    assert.deepEqual(opened, [], 'nothing should reach the system browser')
    assert.equal(landed.length, 1, 'the window has to be told')
    assert.equal(new URL(landed[0]).origin + new URL(landed[0]).pathname, 'https://app.example.com/auth/callback')
    assert.match(new URL(landed[0]).searchParams.get('error'), /port/)
  })

  // The refusal above is for this attempt, not for the session. Reading the port
  // without ever starting one is what latched a failed boot bind for the life of
  // the process: every sign-in refused for a condition that may have cleared in
  // seconds, while the connector path next door recovered on its first retry.
  test('and starts the listener the click after it will need', async () => {
    opened.length = 0
    // Nothing exposes the listener directly, so wait for the effect instead: a
    // click that reaches the browser is a click that got a port.
    for (let i = 0; i < 200 && opened.length === 0; i += 1) {
      await new Promise((resolve) => setTimeout(resolve, 10))
      oauth.begin(authorize('https://app.example.com/auth/callback'), windowStub([]))
    }
    assert.equal(opened.length > 0, true, 'still refused; the failure is latched for the session')
    assert.match(new URL(opened[0]).searchParams.get('redirect_to'), /^http:\/\/127\.0\.0\.1:\d+\/callback$/)
  })

  // The refusal is for flows that are ours. A crafted redirect_to is still not
  // one, and claiming it would drive our own window wherever it pointed.
  test('and does not start claiming flows that were never ours', () => {
    opened.length = 0
    const landed = []
    assert.equal(oauth.begin(authorize('https://evil.example.com/callback'), windowStub(landed)), false)
    assert.deepEqual(landed, [])
  })
})

describe('startup failure', () => {
  // config validates at require time and throws on a build that is quietly the
  // wrong product. Uncaught, that kills the main process before any window
  // exists: on a packaged macOS build the icon bounces once and the app is gone,
  // with the reason on a stderr nobody launched it from.
  test('a saas build with no platform origin says so instead of vanishing', () => {
    const { errorBoxes, exits } = loadEntryWith({ edition: 'saas', appOrigin: 'https://app.example.com' })
    assert.equal(errorBoxes.length, 1)
    assert.match(errorBoxes[0].content, /platformOrigin/)
    assert.deepEqual(exits, [1])
  })

  // The same failure one guarantee over, and the reason it is asked of build.json
  // rather than of the merged config: appOrigin has a committed default, so a
  // saas build that never named its own inherits the OSS localhost address,
  // validates, onboards correctly, and then opens a dev server that is not there.
  test('a saas build with no app origin does not inherit the localhost default', () => {
    const { errorBoxes, exits } = loadEntryWith({
      edition: 'saas',
      platformOrigin: 'https://platform.example.com',
    })
    assert.equal(errorBoxes.length, 1)
    assert.match(errorBoxes[0].content, /appOrigin/)
    assert.deepEqual(exits, [1])
  })

  test('a config it can validate starts silently', () => {
    const { errorBoxes, exits } = loadEntryWith({
      edition: 'saas',
      appOrigin: 'https://app.example.com',
      platformOrigin: 'https://platform.example.com',
    })
    assert.deepEqual(errorBoxes, [])
    assert.deepEqual(exits, [])
  })
})

describe('stored settings', () => {
  // settings.json is a plain JSON file in a directory the user can open, and
  // every flag in it is read as a boolean. The string "false" is truthy, so one
  // hand-edit otherwise retires SaaS sign-in permanently and hides the titlebar
  // of a build that never reserved a strip, leaving a window nothing can drag.
  test('a value of the wrong shape reads as the default, not as itself', () => {
    const { store } = loadShell({
      edition: 'oss',
      settings: { reachedApp: 'false', appChrome: 'yes', theme: 7, serverUrl: 12 },
    })
    assert.strictEqual(store.get('reachedApp'), false)
    assert.strictEqual(store.get('appChrome'), false)
    assert.strictEqual(store.get('theme'), 'dark')
    assert.strictEqual(store.get('serverUrl'), null)
  })

  test('a value of the right shape is kept', () => {
    const { store } = loadShell({
      edition: 'oss',
      settings: { reachedApp: true, theme: 'light', serverUrl: 'http://localhost:5173' },
    })
    assert.strictEqual(store.get('reachedApp'), true)
    assert.strictEqual(store.get('theme'), 'light')
    assert.strictEqual(store.get('serverUrl'), 'http://localhost:5173')
  })
})

describe('outage classification', () => {
  let outage
  before(() => { ({ outage } = loadShell({ edition: 'saas' })) })
  after(() => setOnline(true))

  test('only a real main-frame failure replaces the window', () => {
    // A subframe that fails is the page's own business, and an aborted main-frame
    // load is what a router push looks like from here.
    assert.equal(outage.shouldShow({ code: -106, isMainFrame: true }), true)
    assert.equal(outage.shouldShow({ code: -106, isMainFrame: false }), false)
    assert.equal(outage.shouldShow({ code: -3, isMainFrame: true }), false, 'ERR_ABORTED')
  })

  test('only 5xx is ours to explain', () => {
    // A 404 is a route and a 401 is a login; taking those over would replace the
    // app's own handling with a network error page.
    assert.equal(outage.isServerError(502), true)
    assert.equal(outage.isServerError(500), true)
    assert.equal(outage.isServerError(404), false)
    assert.equal(outage.isServerError(401), false)
    assert.equal(outage.isServerError(200), false)
    assert.equal(outage.isServerError(undefined), false)
  })

  test('a server that answered is never reported as the user being offline', () => {
    // The failure mode this guards: a 503 while the laptop is on a flaky wifi
    // would otherwise tell the user to check their connection, and they would
    // spend the outage debugging their router.
    setOnline(false)
    assert.equal(outage.reasonFor({ status: 503 }), 'server-error')
    assert.equal(outage.reasonFor({ code: -106 }), 'offline')
    setOnline(true)
    assert.equal(outage.reasonFor({ code: -106 }), 'unreachable')
    assert.equal(outage.reasonFor({ status: 502 }), 'server-error')
  })

  test('a portal is only claimed when the machine is otherwise fine', () => {
    // "You are offline" and "sign in to this network" are contradictory advice,
    // and a 5xx proves we got through, so neither may ever be overruled by the
    // portal probe.
    setOnline(true)
    assert.equal(outage.reasonFor({ code: -105, portal: true }), 'captive-portal')
    assert.equal(outage.reasonFor({ code: -105, portal: false }), 'unreachable')
    assert.equal(outage.reasonFor({ status: 502, portal: true }), 'server-error')
    setOnline(false)
    assert.equal(outage.reasonFor({ code: -106, portal: true }), 'offline')
  })

  // Three paths probe before they act on an outage, and a probe lasts long
  // enough for the user to hit Retry, pick Home, or connect a different server.
  // Two of them had this guard hand-rolled and the third, the menu Reload, was
  // written without it: a stale success there loads the failed URL back over the
  // page they just recovered. One named contract, so the next call site has
  // something correct to copy.
  test('a window that moved on during a probe is not acted on', () => {
    const win = { isDestroyed: () => false }
    const since = outage.generation(win)
    assert.equal(outage.movedOn(win, since), false, 'nothing has happened yet')

    // Home, or connecting elsewhere. Both clear the record.
    outage.clear(win)
    assert.equal(outage.movedOn(win, since), true, 'the probe result is stale now')
    assert.equal(outage.movedOn(win, outage.generation(win)), false, 'a token taken after is current')

    assert.equal(outage.movedOn({ isDestroyed: () => true }, 0), true, 'a closed window has also moved on')
  })

  test('a cleared outage page is dropped from history when the window recovers', () => {
    const { outage: fresh } = loadShell({ edition: 'oss' })
    const page = require('node:url').pathToFileURL(
      path.join(__dirname, '..', 'outage', 'outage.html')).href
    const app = 'https://app.example.com/chat'
    // The shape a recovery leaves behind: app, the outage screen, app again.
    const win = fakeWebContentsWindow([], { active: 2, urls: [app, `${page}?reason=unreachable`, app] })

    fresh.attach(win)
    win.webContents.emit('did-navigate', {}, app)

    // Back would otherwise land on a page that still looks like a recovery
    // screen, whose Retry, Open and Change Server all answer nothing because
    // clearing the record is what made `isShowing` false.
    assert.deepEqual(win.webContents.navigationHistory.entries(), [app, app])
  })

  test('the outage page the window is actually on is left alone', () => {
    const { outage: fresh } = loadShell({ edition: 'oss' })
    const page = require('node:url').pathToFileURL(
      path.join(__dirname, '..', 'outage', 'outage.html')).href
    const showing = `${page}?reason=unreachable`
    const win = fakeWebContentsWindow([], { active: 1, urls: ['https://app.example.com/chat', showing] })

    fresh.attach(win)
    win.webContents.emit('did-navigate', {}, showing)

    assert.deepEqual(win.webContents.navigationHistory.entries(),
      ['https://app.example.com/chat', showing], 'the entry being displayed cannot be removed')
  })

  test('the silent retry stands down when the window moved on', async () => {
    const { outage: fresh } = loadShell({ edition: 'oss' })
    const landed = []
    const win = fakeWebContentsWindow(landed)

    fresh.attach(win)
    // A transient failure schedules the one silent retry.
    win.webContents.emit('did-fail-load', {}, -21, 'ERR_NETWORK_CHANGED', 'https://app.example.com/chat', true)

    // Inside the second it waits, the user picks Home. That clears the record,
    // which is exactly what makes `isShowing` false again, so the generation is
    // the only thing left that can tell the retry it is stale.
    fresh.clear(win)

    await new Promise((r) => setTimeout(r, 1100))
    assert.deepEqual(landed, [], 'the retry did not load the failed URL over the recovered page')
  })

  test('a probe left over from an outage the window left does not block the next', async () => {
    const { outage: fresh, captive } = loadShell({ edition: 'oss' })
    setOnline(true)
    // Hold every probe open so both are in flight at once.
    const gates = []
    captive.behindPortal = () => new Promise((resolve) => gates.push(resolve))

    const win = fakeWebContentsWindow()
    const shown = []
    win.loadFile = (_p, opts) => { shown.push(opts.query.reason); return Promise.resolve() }

    fresh.show(win, { target: 'https://app.example.com/a', code: -106 })
    await new Promise((r) => setTimeout(r, 10))
    assert.equal(gates.length, 1, 'the first probe is out')

    // The user navigates somewhere that works, which clears the record.
    fresh.clear(win)

    // A new failure, belonging to a new outage. The stale probe must not speak
    // for it: with a global latch this second call returns immediately and the
    // window is left on a failed load with no outage page at all.
    fresh.show(win, { target: 'https://app.example.com/b', code: -106 })
    await new Promise((r) => setTimeout(r, 10))
    assert.equal(gates.length, 2, 'the second failure got its own probe')

    gates.forEach((resolve) => resolve(false))
    await new Promise((r) => setTimeout(r, 20))
    assert.equal(shown.length, 1, 'and exactly one outage page rendered, for the live failure')
  })

  test('the silent retry still fires when nothing displaced it', async () => {
    const { outage: fresh } = loadShell({ edition: 'oss' })
    const landed = []
    const win = fakeWebContentsWindow(landed)

    fresh.attach(win)
    win.webContents.emit('did-fail-load', {}, -21, 'ERR_NETWORK_CHANGED', 'https://app.example.com/chat', true)

    await new Promise((r) => setTimeout(r, 1100))
    assert.deepEqual(landed, ['https://app.example.com/chat'], 'the retry is not simply dead')
  })
})

describe('packaging a saas build', () => {
  const { spawnSync } = require('node:child_process')
  const script = path.join(__dirname, '..', 'scripts', 'write-build-config.mjs')

  // Only the refusing case is exercised here: it is the one that writes no
  // file, and config/build.json is real state the rest of the suite stashes.
  const written = path.join(__dirname, '..', 'config', 'build.json')

  // Only the refusing cases: they write no file, and `shell.openExternal` is
  // where this value lands, so the scheme is the bar rather than parseability.
  for (const bad of ['mailto:support@example.com', 'file:///tmp/release']) {
    test(`a ${bad.split(':')[0]}: download page is refused at package time`, () => {
      const before = fs.existsSync(written)
      const run = spawnSync(process.execPath, [script], {
        encoding: 'utf8',
        env: {
          ...process.env,
          DESKTOP_EDITION: 'oss',
          DESKTOP_UPDATE_MODE: 'notify',
          DESKTOP_UPDATE_FEED: 'https://example.com/feed',
          DESKTOP_DOWNLOAD_PAGE: bad,
        },
      })
      assert.equal(run.status, 1)
      assert.match(run.stderr, /must be an http:\/\/ or https:\/\/ page/)
      assert.equal(fs.existsSync(written), before, 'and wrote nothing')
    })
  }

  test('an origin carrying credentials is refused at package time', () => {
    const before = fs.existsSync(written)
    const run = spawnSync(process.execPath, [script], {
      encoding: 'utf8',
      env: {
        ...process.env,
        DESKTOP_EDITION: 'saas',
        // Refused rather than accepted-and-stripped: `new URL().origin` drops
        // the userinfo silently, and the path guard cannot see it because the
        // pathname of `https://u:p@host` is '/'.
        DESKTOP_APP_ORIGIN: 'https://deploy:hunter2@app.example.com',
        DESKTOP_PLATFORM_ORIGIN: 'https://platform.example.com',
      },
    })
    assert.equal(run.status, 1)
    assert.match(run.stderr, /without a username or password/)
    assert.doesNotMatch(run.stderr, /hunter2/, 'and does not echo the password it refused')
    assert.equal(fs.existsSync(written), before, 'and wrote nothing')
  })

  test('two identical origins are refused at package time', () => {
    const before = fs.existsSync(written)
    const run = spawnSync(process.execPath, [script], {
      encoding: 'utf8',
      env: {
        ...process.env,
        DESKTOP_EDITION: 'saas',
        DESKTOP_APP_ORIGIN: 'https://app.example.com',
        // The same origin wearing a trailing slash, because `new URL().origin`
        // normalizes it away and a string compare on the input would not.
        DESKTOP_PLATFORM_ORIGIN: 'https://app.example.com/',
      },
    })
    assert.equal(run.status, 1, 'packaging failed rather than producing a broken build')
    assert.match(run.stderr, /cannot tell the two apps apart/)
    // Exit status alone would pass on a regression that wrote the file first and
    // failed after, leaving a broken config for the next packaging run to pick up.
    assert.equal(fs.existsSync(written), before, 'the refused build left config/build.json as it found it')
  })
})

describe('redacting a URL for a human', () => {
  const { forDisplay } = require('../src/redact')

  // Three call sites depend on this now (the outage screen, the navigation log
  // and the external-opener log), and each of them can be handed a URL whose
  // query is a live credential, so the shapes are pinned here rather than
  // implied by the callers.
  test('a query never survives, whatever it carries', () => {
    for (const [raw, safe] of [
      ['https://app.example.com/callback?code=AUTH_CODE', 'https://app.example.com/callback…'],
      ['https://app.example.com/login?token=MAGIC_LINK', 'https://app.example.com/login…'],
      ['https://app.example.com/x#access_token=FRAGMENT', 'https://app.example.com/x…'],
      ['mailto:someone@example.com?subject=hi', 'mailto:…'],
    ]) {
      const out = forDisplay(raw)
      assert.equal(out, safe)
      for (const secret of ['AUTH_CODE', 'MAGIC_LINK', 'FRAGMENT', 'subject']) {
        assert.ok(!out.includes(secret), `${secret} leaked through ${raw}`)
      }
    }
  })

  test('a plain URL is left readable, and a junk one is not invented', () => {
    assert.equal(forDisplay('https://app.example.com/chat'), 'https://app.example.com/chat')
    // The ellipsis is the tell that something was removed, so it must not appear
    // when nothing was; a log line that always looks truncated teaches nothing.
    assert.ok(!forDisplay('https://app.example.com/chat').endsWith('…'))
    assert.equal(forDisplay('not a url'), 'not a url')
  })
})

describe('navigating a window', () => {
  const { navigate } = require('../src/navigate')

  test('an ordinary failed navigation is not a shell fault', async () => {
    // Electron absorbs this rejection itself today, so the point of the catch is
    // that the code does not depend on it doing so.
    const seen = []
    const watch = (r) => seen.push(r)
    process.on('unhandledRejection', watch)
    try {
      navigate({
        isDestroyed: () => false,
        loadURL: () => Promise.reject(new Error('ERR_CONNECTION_REFUSED (-102)')),
      }, 'https://app.example.com/')
      await new Promise((r) => setTimeout(r, 50))
      assert.deepEqual(seen, [], 'nothing escaped to the process')
    } finally {
      // Left attached, this suppresses Node's own handling for every test after
      // it, so a real unhandled rejection later would land here silently.
      process.off('unhandledRejection', watch)
    }
  })

  test('a failed navigation keeps the OAuth code out of the log', async () => {
    // oauth.js builds the app callback as `?code=<authorization code>` and hands
    // it straight to navigate, so this line is one `console.warn` away from
    // writing a live credential into the main-process log.
    const secret = 'AUTH_CODE_THAT_MUST_NOT_BE_LOGGED'
    const url = `https://app.example.com/callback?code=${secret}`
    const err = Object.assign(
      // Shaped like the real one, measured on Electron 43: the message embeds
      // the whole URL, which is why the code and errno are read instead.
      new Error(`ERR_CONNECTION_REFUSED (-102) loading '${url}'`),
      { code: 'ERR_CONNECTION_REFUSED', errno: -102 },
    )
    const warned = []
    const realWarn = console.warn
    console.warn = (...a) => warned.push(a.join(' '))
    try {
      navigate({ isDestroyed: () => false, loadURL: () => Promise.reject(err) }, url)
      await new Promise((r) => setTimeout(r, 50))
    } finally {
      console.warn = realWarn
    }
    assert.equal(warned.length, 1)
    assert.doesNotMatch(warned[0], new RegExp(secret), 'the code did not reach the log')
    assert.match(warned[0], /ERR_CONNECTION_REFUSED/, 'and the diagnosis survived')
    assert.match(warned[0], /app\.example\.com\/callback/, 'along with where it was going')
  })

  test('a window that is already gone is left alone', () => {
    // `loadURL` on a destroyed window throws synchronously rather than
    // rejecting, so there is no promise to catch this one with.
    let called = false
    assert.doesNotThrow(() => navigate({
      isDestroyed: () => true,
      loadURL: () => { called = true; throw new Error('Object has been destroyed') },
    }, 'https://app.example.com/'))
    assert.equal(called, false)
    assert.doesNotThrow(() => navigate(null, 'https://app.example.com/'))
  })
})

describe('captive portal detection', () => {
  let captive
  before(() => { ({ captive } = loadShell({ edition: 'oss' })) })

  test('never probes out for a target it could not explain', () => {
    // The probe is an external request. A self-hoster pointed at their own
    // machine gets nothing out of it, so they must not make one.
    for (const local of [
      'http://localhost:5173', 'http://127.0.0.1:8000', 'http://[::1]:5173',
      'http://192.168.1.40', 'http://10.0.0.5', 'http://172.20.1.1',
      'http://mac-studio.local:5173', 'http://box.localhost',
      // Names, not addresses. A LAN server is usually reached by one, and these
      // were read as public: an unreachable `http://nas:5173` phoned a third
      // party about a machine down the hall.
      'http://nas:5173', 'http://langalpha.home.arpa', 'http://box.internal',
      'http://pi.lan', 'http://server.intranet',
    ]) {
      assert.equal(captive.isLocalTarget(local), true, local)
    }
  })

  test('a public host is worth asking about', () => {
    for (const remote of [
      'https://app.example.com', 'http://172.32.0.1', 'http://9.9.9.9',
      // The private suffixes are suffixes. Matching them anywhere in the name
      // would hand any host that mentions one a free pass out of the probe.
      'http://evil.internal.attacker.com', 'http://lan.example.com',
      'http://home.arpa.example.com',
    ]) {
      assert.equal(captive.isLocalTarget(remote), false, remote)
    }
  })

  test('the check runs over cleartext', () => {
    // Over HTTPS a portal cannot answer at all, and the failure is
    // indistinguishable from the host being down, which is the exact
    // distinction this probe exists to draw.
    assert.match(captive.CHECK_URL, /^http:\/\//)
  })
})

describe('deep links', () => {
  test('map onto a callback route on the origin currently shown', () => {
    const { deeplink } = loadShell({ edition: 'saas' })
    assert.equal(
      deeplink.toAppUrl('langalpha://callback?code=MAGIC&type=magiclink', 'https://platform.example.com/login'),
      'https://platform.example.com/callback?code=MAGIC&type=magiclink',
    )
    assert.equal(
      deeplink.toAppUrl('langalpha://callback?code=MAGIC', 'https://app.example.com/chat'),
      'https://app.example.com/callback?code=MAGIC',
    )
  })

  // The web app relies on this: a confirmation link opened in the default
  // browser is handed back as `langalpha://`, and only the query survives the
  // trip. Nothing here knows an email token from an OAuth code, which is what
  // lets /callback forward one on without the shell learning a second shape.
  test('carry an email token across, not just an OAuth code', () => {
    const { deeplink } = loadShell({ edition: 'saas' })
    assert.equal(
      deeplink.toAppUrl(
        'langalpha://callback?token_hash=abc123&type=email',
        'https://app.example.com/',
      ),
      'https://app.example.com/callback?token_hash=abc123&type=email',
    )
  })

  test('fall back to the app origin when the current page is not ours', () => {
    const { deeplink } = loadShell({ edition: 'saas' })
    assert.equal(
      deeplink.toAppUrl('langalpha://callback?code=M', 'about:blank'),
      'https://app.example.com/callback?code=M',
    )
    // A link clicked while a foreign page happened to be showing must not send
    // the code to that page.
    assert.equal(
      deeplink.toAppUrl('langalpha://callback?code=M', 'https://evil.example.com/x'),
      'https://app.example.com/callback?code=M',
    )
  })

  test('the path comes from us, never from the link', () => {
    const { deeplink } = loadShell({ edition: 'saas' })
    // The "host" of a custom-scheme URL is not a real host and its path is
    // attacker-controlled, so neither is allowed to steer the destination.
    const mapped = deeplink.toAppUrl('langalpha://evil.example.com/wherever?code=M', 'about:blank')
    assert.equal(mapped, 'https://app.example.com/callback?code=M')
  })

  test('reject anything that is not our scheme', () => {
    const { deeplink } = loadShell({ edition: 'saas' })
    assert.equal(deeplink.toAppUrl('https://evil.example.com/callback?code=X', 'about:blank'), null)
    assert.equal(deeplink.toAppUrl('not a url', 'about:blank'), null)
  })

  test('recover the URL from argv, for a windows/linux cold start', () => {
    const { deeplink } = loadShell({ edition: 'saas' })
    assert.equal(deeplink.fromArgv(['/path/App', '--flag', 'langalpha://callback?code=Z']),
      'langalpha://callback?code=Z')
    assert.equal(deeplink.fromArgv(['/path/App', '--flag']), null)
    assert.equal(deeplink.fromArgv(undefined), null)
  })
})

describe('config', () => {
  test('a saas build without a platform origin refuses to start', () => {
    // Otherwise it would open on the app and silently skip onboarding, which is
    // the one thing that edition exists to guarantee.
    const fs = require('node:fs')
    const path = require('node:path')
    const file = path.join(__dirname, '..', 'config', 'build.json')
    fs.writeFileSync(file, JSON.stringify({ edition: 'saas', appOrigin: 'https://app.example.com' }))
    for (const k of Object.keys(require.cache)) {
      if (k.includes('/desktop/src/')) delete require.cache[k]
    }
    assert.throws(() => require('../src/config.js'), /requires platformOrigin/)
    fs.rmSync(file, { force: true })
  })

  // `entryUrl` resolves loginPath against the platform origin, and `new URL()`
  // drops the base for anything that is really an origin. A build input that is
  // not a path is therefore not a path at all: it is a different entry origin,
  // loaded straight into the privileged window without the policy seeing it.
  // The backslash form is the one a prefix check misses — WHATWG folds `\` into
  // `/`, so `/\host` is `//host` by the time it matters.
  test('a loginPath that is really an origin refuses to start', () => {
    for (const loginPath of [
      'https://elsewhere.example.com/login',
      '//elsewhere.example.com/login',
      '/\\elsewhere.example.com/login',
      'https:/\\elsewhere.example.com',
    ]) {
      assert.throws(
        () => loadShell({ edition: 'saas', loginPath }),
        /loginPath must stay on the platform origin/,
        loginPath,
      )
    }
  })

  test('an ordinary path is kept as one', () => {
    const { config } = loadShell({ edition: 'saas', loginPath: '/sign-in?next=/app' })
    assert.equal(config.loginPath, '/sign-in?next=/app')
  })

  // Derived from tokens.css rather than restated beside a comment naming it.
  // The shell paints this colour behind the page so a live resize does not show
  // a band of the wrong ground, which means the literal here is only correct
  // relative to a file in the OTHER half of the repo — and editing that file is
  // exactly the change that would not think to come looking here.
  test('the window background matches the page ground in both themes', () => {
    const { theme } = loadShell({ edition: 'oss' })
    const tokens = fs.readFileSync(path.join(__dirname, '..', '..', 'web/src/styles/tokens.css'), 'utf8')

    // `:root` is the dark ground; the light one is redefined under the stamp.
    const groundOf = (block) => {
      const at = block === 'dark' ? tokens.indexOf(':root {') : tokens.indexOf('[data-theme="light"] {')
      assert.ok(at >= 0, `no ${block} block in tokens.css`)
      const m = /--background:\s*([\d.]+)\s+([\d.]+)%\s+([\d.]+)%/.exec(tokens.slice(at))
      assert.ok(m, `no --background in the ${block} block`)
      const [, h, sat, l] = m.map(Number)
      // Both grounds are achromatic today, which is the only reason a lightness
      // is the whole colour. If that stops being true this fires rather than
      // quietly comparing against a wrong conversion.
      assert.equal(h, 0, `${block} ground gained a hue`)
      assert.equal(sat, 0, `${block} ground gained saturation`)
      const v = Math.round((l / 100) * 255).toString(16).padStart(2, '0')
      return `#${v}${v}${v}`
    }

    assert.equal(theme.backgroundFor('dark'), groundOf('dark'))
    assert.equal(theme.backgroundFor('light'), groundOf('light'))
    assert.equal(theme.backgroundFor('nonsense'), groundOf('dark'))
    assert.equal(theme.isTheme('auto'), false)
  })
})

// The two editions have to be installable side by side, and on macOS that is
// entirely a matter of three strings per edition. They are spelled in four
// places that never read each other: src/config.js (what the running app
// believes), electron-builder.yml (what the oss package is stamped with),
// scripts/build.mjs (what it is rewritten to for saas), and package.json (what
// `pnpm start` uses in dev). Change one and nothing fails — the build succeeds,
// the app runs, and the damage is a self-hosted install quietly sharing the
// hosted build's settings.json, or a hosted magic link opening the wrong app.
describe('the two editions can sit on one machine', () => {
  const read = (rel) => fs.readFileSync(path.join(__dirname, '..', rel), 'utf8')

  const IDENTITY = {
    saas: { appId: 'ai.langalpha.desktop', appName: 'LangAlpha', scheme: 'langalpha' },
    oss: { appId: 'ai.langalpha.desktop.oss', appName: 'LangAlpha OSS', scheme: 'langalpha-oss' },
  }

  test('nothing about the two identities is shared', () => {
    const { saas, oss } = IDENTITY
    // The whole point. userData is derived from the name, so an equal name is
    // an equal profile no matter how different the appId is.
    assert.notEqual(saas.appId, oss.appId)
    assert.notEqual(saas.appName, oss.appName, 'an equal name means an equal userData directory')
    assert.notEqual(saas.scheme, oss.scheme, 'an equal scheme means the OS picks one for both')
  })

  for (const edition of ['saas', 'oss']) {
    test(`the running ${edition} app agrees with what was packaged`, () => {
      const { config, deeplink } = loadShell({ edition })
      assert.equal(config.appName, IDENTITY[edition].appName)
      assert.equal(config.scheme, IDENTITY[edition].scheme)
      // deeplink registers with the OS and parses incoming links off this, so a
      // config it did not read is a scheme nothing answers on.
      assert.equal(deeplink.SCHEME, IDENTITY[edition].scheme)
      // And the name the PROCESS runs under, which is the one that decides
      // userData. Electron reads it from the packaged package.json, not from
      // the Info.plist electron-builder stamps per edition, so correct bundle
      // metadata is not enough on its own: without main setting it, both
      // editions install cleanly, look separate, and share one settings.json.
      assert.equal(electronStub.app.getName(), IDENTITY[edition].appName)
    })
  }

  test('the committed package is stamped with the self-hosted identity', () => {
    const yml = read('electron-builder.yml')
    const { oss } = IDENTITY
    assert.ok(yml.split('\n').includes(`appId: ${oss.appId}`), 'appId')
    assert.ok(yml.split('\n').includes(`productName: ${oss.appName}`), 'productName')
    assert.ok(yml.split('\n').includes(`      - ${oss.scheme}`), 'protocol scheme')
    // And package.json carries no productName of its own. It would be a third
    // copy, edition-blind, and the one Electron actually answers `getName()`
    // with — which is how a correctly-stamped saas bundle resolved the OSS
    // profile. main sets the name from config instead; this keeps the value
    // that used to win from coming back.
    assert.equal(JSON.parse(read('package.json')).productName, undefined)
  })

  test('the saas build rewrites every one of them', () => {
    const build = read('scripts/build.mjs')
    for (const [edition, id] of Object.entries(IDENTITY)) {
      assert.match(build, new RegExp(`${edition}: \\{ appId: '${id.appId.replace(/\./g, '\\.')}'`),
        `${edition} appId missing from build.mjs`)
      assert.ok(build.includes(`productName: '${id.appName}'`), `${edition} productName`)
      assert.ok(build.includes(`scheme: '${id.scheme}'`), `${edition} scheme`)
    }
  })

  // The fourth string, and the only invisible one. electron-updater names its
  // download cache `<package name>-updater`, so two editions sharing a package
  // name share the directory holding the update each is about to install. It
  // reaches the build through `extraMetadata`, because appInfo reads the name
  // off metadata and PublishManager overwrites `updaterCacheDirName` after
  // spreading the publish config -- so neither package.json nor the feed block
  // is a place this can be said.
  test('the editions do not share an updater cache', () => {
    const yml = read('electron-builder.yml')
    const build = read('scripts/build.mjs')

    assert.match(yml, /^extraMetadata:\n {2}name: langalpha-desktop-oss$/m,
      'electron-builder.yml does not stamp the oss package name')
    assert.ok(build.includes("packageName: 'langalpha-desktop-oss'"), 'oss packageName')
    assert.ok(build.includes("packageName: 'langalpha-desktop'"), 'saas packageName')

    // The saas swap has to match the committed line exactly, the same way the
    // other three markers do, or the hosted build keeps the oss cache name.
    assert.match(build, /name: langalpha-desktop-oss\\r\?\$\/m/,
      'build.mjs has no marker for the committed package name')

    // The hosted edition keeps the name it already shipped with: renaming it
    // would strand the cache of every installed hosted app.
    assert.equal(JSON.parse(read('package.json')).name, 'langalpha-desktop')
  })

  // The artifact name must not follow the display name: productName carries a
  // space in the oss edition, and the edition tag already distinguishes the files.
  test('the download filename does not inherit the display name', () => {
    const yml = read('electron-builder.yml')
    assert.match(yml, /^artifactName: LangAlpha\$\{EDITION_TAG\}-/m)
    assert.ok(!/^artifactName:.*\$\{productName\}/m.test(yml), 'artifactName still interpolates productName')
  })

  // The hosted build is the one the download page serves, so it is named plainly
  // and only the self-hosted edition is marked. A tag that resolved to something
  // for both would put the edition back into every public download URL.
  test('only the self-hosted edition is tagged in the filename', () => {
    assert.match(read('scripts/build.mjs'), /EDITION_TAG: edition === 'oss' \? '-oss' : ''/,
      'the edition tag no longer resolves to nothing for the hosted build')
  })

  // The filename is not enough on its own. The unpacked bundle is named from
  // productName and lands in a shared `mac-arm64`, and the update manifests are
  // named for the channel and never the edition (`latest-mac.yml` is the same
  // filename for both), so one output tree means the second edition to build
  // overwrites the first's metadata.
  test('neither edition builds into the other\'s output tree', () => {
    assert.match(read('electron-builder.yml'), /^ {2}output: dist\/\$\{EDITION\}$/m)
  })

  // Everything build.mjs does after electron-builder returns, the stale-feed
  // sweep and the signing verification and the manifest check, is a search of
  // the output directory. A second, hardcoded copy of that path is how all three
  // quietly become searches of an empty tree, which reads exactly like a clean
  // build. It reads the directive back off the config it just resolved instead.
  test('build.mjs looks for its own output where the config puts it', () => {
    const build = read('scripts/build.mjs')
    // The derivation, not the expression that parses it: how the line is found
    // is this script's own business, but that the path comes from the config
    // rather than from a second copy of it is the property.
    assert.match(build, /output:\[ \\t\]\*/,
      'build.mjs no longer reads directories.output out of the resolved config')
    assert.match(build, /const dist = path\.resolve\(root, outputDir\)/,
      'build.mjs no longer derives its output directory from that line')
    assert.ok(!/path\.(join|resolve)\(root, 'dist'\)/.test(build),
      'build.mjs also hardcodes the output directory somewhere')
  })

  // electron-builder names the manifest for the channel it reads off the
  // version's prerelease tag, so a `latest*` check passes on 0.2.0 and fails on
  // 0.2.0-rc.1 while the build is equally fine either way. That is not a corner:
  // a prerelease through the real pipeline is how the release workflow is meant
  // to be tested, and this check failed all three platforms on the first run
  // that did it, after passing locally on a stable version.
  test('the manifest check follows the version to its channel', () => {
    const build = read('scripts/build.mjs')
    assert.ok(!/\/\^latest\.\*\\\.yml\$\//.test(build),
      'build.mjs is back to demanding the latest channel whatever the version says')
    assert.match(build, /no \$\{channel\}\*\.yml was produced/,
      'the failure no longer names the manifest it wanted, which is the whole diagnosis')

    // Run the derivation the file actually ships, not a copy of it.
    const source = /const channel = \/(.+?)\/\.exec\(version\)\?\.\[1\] \|\| 'latest'/.exec(build)
    assert.ok(source, 'build.mjs no longer derives the channel from the version')
    const channelOf = (v) => new RegExp(source[1]).exec(v)?.[1] || 'latest'

    // The expected column is semver `prerelease()[0]`, which is verbatim what
    // electron-builder's `appInfo.channel` returns and names the file after.
    for (const [version, channel] of [
      ['0.1.3', 'latest'], ['0.2.0', 'latest'], ['10.20.30', 'latest'],
      ['0.2.0-rc.1', 'rc'], ['0.2.0-beta.1', 'beta'], ['0.2.0-beta', 'beta'],
      ['1.0.0-alpha.beta', 'alpha'], ['1.0.0-rc-1', 'rc-1'], ['1.0.0-0', '0'],
    ]) assert.equal(channelOf(version), channel, `${version} resolves to the wrong channel`)
  })
})

// The window-chrome contract is four string literals spread across three files
// and two processes, and every consumer only ever READS its half: main writes
// an argv switch preload parses, and main queries a meta tag index.html ships.
// Rename either on one side and nothing fails — not tsc, not the web suite, not
// the tests above, not the packaged build. The strip simply stops being
// reserved and the macOS window loses its drag region. This branch already
// shipped that exact shape once, in a read-only global nothing consumed, which
// is why the literals are asserted against each other here rather than trusted.
describe('the window-chrome contract holds across the two processes', () => {
  const read = (rel) => fs.readFileSync(path.join(__dirname, '..', rel), 'utf8')

  test('the switch main writes is the one preload parses', () => {
    const main = read('src/main.js')
    const preload = read('src/preload.js')
    assert.match(main, /`--langalpha-window-chrome=\$\{chromeHidden \? 'hidden' : 'native'\}`/)
    assert.match(preload, /const prefix = `--langalpha-\$\{name\}=`/)
    assert.match(preload, /flag\('window-chrome'\) === 'hidden'/)
  })

  test('the meta main queries is the one the web build ships', () => {
    const main = read('src/main.js')
    const indexHtml = read('../web/index.html')
    assert.match(main, /meta\[name="langalpha-window-chrome"\]/)
    assert.match(main, /el\.content === 'reserves'/)
    assert.match(indexHtml, /<meta name="langalpha-window-chrome" content="reserves"/)
  })

  // A drag region swallows the mouse, and an element cannot win those clicks
  // back from underneath it. So a control the no-drag list forgets is not merely
  // selectable-when-it-should-not-be — it is UNCLICKABLE, and only in the
  // desktop shell, which is the one place ordinary QA never looks. The chrome
  // list is the project's own answer to "what is a control", so the no-drag list
  // has to be a superset of it. They drifted apart by eight selectors once.
  test('everything the chrome list calls a control can still be clicked', () => {
    const css = read('../web/src/styles/chrome.css')
    const listIn = (marker) => {
      const at = css.indexOf(marker)
      assert.ok(at >= 0, marker)
      const block = css.slice(at, css.indexOf('{', at))
      return new Set(block.match(/\[role='[a-z]+'\]|\blabel\b|\bsummary\b|\bbutton\b/g) || [])
    }
    const chrome = listIn('[data-chrome],\nbutton,')
    const noDrag = listIn("html.desktop-mac :is(a, button")
    assert.ok(chrome.size >= 12, 'the chrome list was not found intact')
    for (const sel of chrome) {
      assert.ok(noDrag.has(sel), `${sel} is chrome but never opts out of drag`)
    }
  })

  test('the class main causes is the one every chrome rule is gated on', () => {
    const indexHtml = read('../web/index.html')
    assert.match(indexHtml, /classList\.add\('desktop-mac'\)/)
    for (const sheet of ['../web/src/styles/chrome.css', '../web/src/App.css',
                         '../web/src/components/Sidebar/Sidebar.css']) {
      assert.ok(read(sheet).includes('html.desktop-mac'), sheet)
    }
  })

  // The fallback strip rests on two properties from two different mechanisms,
  // and the browser suite can only see one of them. The region walk is settled
  // by tree order; the click is settled by hit testing. `getComputedStyle`
  // reports `no-drag` on a control the strip is swallowing whole, verified
  // against a pre-fix document, so a check that reads the region calls the
  // broken arrangement healthy. The e2e suite now hit-tests as well, and these
  // two literals are what stands between either half and a silent revert.
  test('the fallback drag strip declines the mouse and stays first in tree order', () => {
    const indexHtml = read('../web/index.html')
    // Regions are collected in layout-tree preorder and differenced in that
    // order, so the app's `no-drag` controls only subtract from this strip while
    // it comes first. z-index is not an input to that walk.
    const strip = indexHtml.indexOf('<div id="window-drag"')
    const root = indexHtml.indexOf('<div id="root"')
    assert.ok(strip >= 0 && root >= 0, 'one of the two elements is missing')
    assert.ok(strip < root, '#window-drag must precede #root, or the no-drag rule cannot reach it')
    // And the other half, which is a different mechanism entirely: hit testing,
    // where a fixed box beats every in-flow element that is not itself
    // positioned. Without this the strip swallowed the clicks of every static
    // control that scrolled under the titlebar, while those controls went on
    // computing `no-drag` -- so nothing that reads the region could see it.
    assert.match(indexHtml, /#window-drag \{[^}]*pointer-events: none;/,
      '#window-drag takes pointer events, so it eats the clicks of whatever scrolls under it')
  })

  // What the strip cannot subtract is prose: `no-drag` covers controls, so a
  // route that renders a document with no chrome of its own has its top line
  // inside a drag region, where a press-and-drag moves the window instead of
  // selecting the text. Terms and privacy are the two such documents this app
  // ships, and the answer is that neither opens in the shell at all -- an
  // anchor with a target reaches setWindowOpenHandler in src/main.js, which
  // hands one of our own URLs to the system browser. A `<Link>` is a
  // client-side navigation that never reaches that handler, and a plain `<a>`
  // navigates this window; both put the document back under the titlebar, and
  // both look correct in a browser.
  test('a legal document is never opened inside the app window', () => {
    const src = path.join(__dirname, '..', '..', 'web/src')
    // The documents themselves are exempt, and by the same rule rather than as
    // a hole in it: they only ever render in a browser tab, so a link between
    // them is an ordinary in-tab navigation. The rule is about app surface.
    const exempt = `pages${path.sep}Legal${path.sep}`
    const offenders = []
    for (const rel of fs.readdirSync(src, { recursive: true })) {
      if (!/\.tsx$/.test(rel) || rel.includes('__tests__') || rel.includes(exempt)) continue
      const text = fs.readFileSync(path.join(src, rel), 'utf8')
      // `[^<>]` and not `[^>]`: the sign-in page passes its anchors as props to
      // <Trans>, so a match that may cross a `<` swallows the outer element and
      // reports the wrong tag.
      for (const [tag] of text.matchAll(/<\w+\b[^<>]*?\b(?:to|href)=["'{]+\/(?:legal|privacy)\b[^<>]*>/g)) {
        const external = tag.startsWith('<a') && tag.includes('target="_blank"')
        if (!external) offenders.push(`${rel}: ${tag.replace(/\s+/g, ' ')}`)
      }
    }
    assert.deepEqual(offenders, [],
      'a legal page is reachable without leaving the app window:\n' + offenders.join('\n'))
  })
})

// The one string this feature spans two processes on, spelled in exactly two
// places with nothing that reads both. Drift does not fail anything: the page
// sees a channel that never answers, decides the shell is too old to know it,
// and falls back to browser print. That is precisely the pre-feature behaviour,
// so the whole path silently reverts and the tests stay green — savePdf is
// called directly everywhere below, and the ipcMain stub records nothing.
describe('the PDF channel is the same one on both sides', () => {
  const read = (rel) => fs.readFileSync(path.join(__dirname, '..', rel), 'utf8')

  test('the channel main handles is the one preload invokes', () => {
    assert.match(read('src/pdf.js'), /ipcMain\.handle\('shell:save-pdf'/)
    assert.match(read('src/preload.js'), /ipcRenderer\.invoke\('shell:save-pdf'/)
  })

  test('the bridge key preload exposes is the one the web app feature-detects', () => {
    assert.match(read('src/preload.js'), /^\s*savePdf: \(options\) =>/m)
    assert.match(read('../web/src/lib/desktop.ts'), /^\s*savePdf\?\(options\?: SavePdfOptions\)/m)
  })
})

// The three pure functions below stand between a page loaded over the network
// and Chromium's printer. Every one of them exists because the input is not
// trusted: the options come from the renderer, and the filename is a title an
// agent wrote. A live run only ever exercises the well-formed case, so the
// cases that matter here are the ones that must be REFUSED or repaired.
describe('rendering a page to a PDF', () => {
  let pdf
  before(() => { ({ pdf } = loadShell({ edition: 'saas' })) })
  after(() => setSaveDialog({ canceled: true }))

  describe('what a page is allowed to ask for', () => {
    test('a field Chromium has never heard of does not reach it', () => {
      const options = pdf.normalizeOptions({ headerTemplate: '<img src=x>', displayHeaderFooter: true })
      assert.equal('headerTemplate' in options, false)
      assert.equal('displayHeaderFooter' in options, false)
    })

    test('a page size outside the enum is dropped, not passed through', () => {
      // printToPDF throws on a name outside its own enum, and a throw here is
      // an export the user simply does not get.
      assert.equal('pageSize' in pdf.normalizeOptions({ pageSize: 'A4; DROP' }), false)
      assert.equal('pageSize' in pdf.normalizeOptions({ pageSize: 'Poster' }), false)
      assert.equal(pdf.normalizeOptions({ pageSize: 'Letter' }).pageSize, 'Letter')
    })

    test('an out-of-range scale is clamped rather than refused', () => {
      assert.equal(pdf.normalizeOptions({ scale: 99 }).scale, 2)
      assert.equal(pdf.normalizeOptions({ scale: 0 }).scale, 0.1)
      assert.equal(pdf.normalizeOptions({ scale: 1.5 }).scale, 1.5)
      // NaN and Infinity survive JSON round-trips through the widget layer.
      assert.equal('scale' in pdf.normalizeOptions({ scale: NaN }), false)
      assert.equal('scale' in pdf.normalizeOptions({ scale: Infinity }), false)
    })

    test('pageRanges has to look like a page range', () => {
      assert.equal(pdf.normalizeOptions({ pageRanges: '1-3, 7' }).pageRanges, '1-3, 7')
      assert.equal('pageRanges' in pdf.normalizeOptions({ pageRanges: 'all' }), false)
      assert.equal('pageRanges' in pdf.normalizeOptions({ pageRanges: '1'.repeat(65) }), false)
    })

    test('the two things a print dialog cannot produce are on by default', () => {
      // A tagged reading order and a real outline are much of the reason this
      // path exists at all, so they are opt-out rather than opt-in.
      const defaults = pdf.normalizeOptions({})
      assert.equal(defaults.generateTaggedPDF, true)
      assert.equal(defaults.generateDocumentOutline, true)
      assert.equal(defaults.printBackground, true)
      assert.equal(defaults.preferCSSPageSize, true)
      const declined = pdf.normalizeOptions({ tagged: false, outline: false, printBackground: false })
      assert.equal(declined.generateTaggedPDF, false)
      assert.equal(declined.generateDocumentOutline, false)
      assert.equal(declined.printBackground, false)
    })

    test('a request that is not an object at all still yields defaults', () => {
      assert.equal(pdf.normalizeOptions(null).generateTaggedPDF, true)
      assert.equal(pdf.normalizeOptions('A4').generateTaggedPDF, true)
    })
  })

  describe('the name the save dialog opens on', () => {
    test('a name cannot walk out of the folder it was offered in', () => {
      // The suggestion is joined onto the downloads path, so a traversal here
      // would open the dialog somewhere the user never asked for.
      assert.equal(pdf.safeFileName('../../../etc/passwd'), 'etc-passwd.pdf')
      assert.equal(pdf.safeFileName('/tmp/evil'), 'tmp-evil.pdf')
    })

    test('characters that are legal on one desktop and rejected on another go', () => {
      // Otherwise the same export fails only on Windows, which is the kind of
      // bug that gets found by a user rather than by us.
      assert.equal(pdf.safeFileName('Q3: profit/loss <draft>'), 'Q3- profit-loss -draft-.pdf')
      assert.equal(pdf.safeFileName('tab\there'), 'tabhere.pdf')
    })

    test('a name that survives to nothing still produces a file', () => {
      assert.equal(pdf.safeFileName('...'), 'export.pdf')
      assert.equal(pdf.safeFileName(''), 'export.pdf')
      assert.equal(pdf.safeFileName(undefined), 'export.pdf')
      assert.equal(pdf.safeFileName('.pdf'), 'export.pdf')
    })

    test('an agent-length title is cut to something every filesystem accepts', () => {
      const name = pdf.safeFileName('x'.repeat(400))
      assert.equal(name.length, 124)
      assert.ok(name.endsWith('.pdf'))
    })

    test('the extension is not doubled on a name that already carries it', () => {
      assert.equal(pdf.safeFileName('report.pdf'), 'report.pdf')
      assert.equal(pdf.safeFileName('report.PDF'), 'report.pdf')
    })
  })

  describe('what the render does to the window', () => {
    const windowWithColor = (colors) => {
      let current = '#191919'
      return {
        isDestroyed: () => false,
        getBackgroundColor: () => current,
        setBackgroundColor: (c) => { current = c; colors.push(c) },
      }
    }

    test('the page is printed on white paper, not on the window ground', async () => {
      // printToPDF composites over the window's own background, and nothing
      // paints the margin area — so a dark window produced dark paper with the
      // text block floating in it.
      const colors = []
      const win = windowWithColor(colors)
      let duringRender = null
      await pdf.renderToBuffer(win, {
        printToPDF: async () => { duringRender = win.getBackgroundColor(); return Buffer.from('%PDF') },
      }, {})
      assert.equal(duringRender, '#ffffff')
      assert.deepEqual(colors, ['#ffffff', '#191919'])
    })

    test('the window colour is put back even when the render throws', async () => {
      const colors = []
      await assert.rejects(pdf.renderToBuffer(windowWithColor(colors), {
        printToPDF: async () => { throw new Error('boom') },
      }, {}))
      assert.deepEqual(colors, ['#ffffff', '#191919'])
    })
  })

  // The web side branches on which of the three it got, and treats them
  // differently on purpose: only `error` may fall back to browser print, because
  // reopening a dialog the user just dismissed reads as the app ignoring them.
  describe('the three answers a save can give', () => {
    const senderFor = (printToPDF) => ({
      printToPDF,
      window: {
        isDestroyed: () => false,
        getBackgroundColor: () => '#191919',
        setBackgroundColor: () => {},
      },
    })

    test('a dismissed dialog is canceled, never an error', async () => {
      setSaveDialog({ canceled: true })
      const result = await pdf.savePdf({ sender: senderFor(async () => Buffer.from('%PDF')) }, {})
      assert.deepEqual(result, { canceled: true })
    })

    test('a failed render answers with an error instead of rejecting', async () => {
      // A rejected `invoke` reaches the page as an opaque error it cannot tell
      // from a shell too old to know the channel, and the caller's fallback
      // depends on telling those apart.
      const result = await pdf.savePdf({ sender: senderFor(async () => { throw new Error('no printer') }) }, {})
      assert.ok(result.error.includes('no printer'))
      assert.equal('canceled' in result, false)
    })

    test('a chosen path gets the bytes, and the page is told only that it worked', async () => {
      const target = path.join(tempDir('la-pdf-'), 'out.pdf')
      setSaveDialog({ canceled: false, filePath: target })
      const result = await pdf.savePdf(
        { sender: senderFor(async () => Buffer.from('%PDF-1.7 body')) },
        { fileName: 'Q3 review' },
      )
      assert.deepEqual(result, { saved: true })
      assert.equal(fs.readFileSync(target, 'utf8'), '%PDF-1.7 body')
      // No path in the answer: the page gets no filesystem detail back.
      assert.equal('filePath' in result, false)
    })

    test('a window already closing answers rather than throwing on it', async () => {
      const result = await pdf.savePdf({ sender: { printToPDF: async () => Buffer.from('x') } }, {})
      assert.ok(result.error)
    })

    test('a failed write leaves the file the user was overwriting where it was', async () => {
      // The destination is usually a previous export of the same report, so the
      // in-place write had already truncated something the user owned by the
      // time it failed, and the cleanup then removed what was left of it.
      const target = path.join(tempDir('la-pdf-'), 'out.pdf')
      fs.writeFileSync(target, 'the export from last week')
      setSaveDialog({ canceled: false, filePath: target })
      const fsp = require('node:fs/promises')
      const real = fsp.writeFile
      // Some of the bytes land before it fails, which is what a full volume
      // actually does. Throwing without writing leaves no staged file at all,
      // and the directory assertion below then passes on a cleanup that never
      // ran, so removing it would not fail this test.
      fsp.writeFile = async (to, bytes) => {
        await real(to, bytes.subarray(0, 1))
        const e = new Error('no space left')
        e.code = 'ENOSPC'
        throw e
      }
      let result
      try {
        result = await pdf.savePdf({ sender: senderFor(async () => Buffer.from('%PDF')) }, {})
      } finally {
        fsp.writeFile = real
      }
      assert.ok(result.error.includes('ENOSPC'))
      assert.equal(fs.readFileSync(target, 'utf8'), 'the export from last week')
      // And nothing half-written left beside it under a name nobody will explain.
      assert.deepEqual(fs.readdirSync(path.dirname(target)), ['out.pdf'])
    })
  })
})

// Signing and notarization are two separate switches, and the committed config
// has both off: a contributor with neither credential has to be able to run
// `dist`. scripts/build.mjs turns each on by replacing the exact line it finds,
// and it does exit loudly when that line is gone — but only on a build that had
// credentials, which is never a local one. So the pairing is asserted here,
// where a drifted marker fails on every run rather than in the release that
// first needed it.
describe('the committed package is unsigned and un-notarized', () => {
  const read = (rel) => fs.readFileSync(path.join(__dirname, '..', rel), 'utf8')

  test('both switches are committed in the off position', () => {
    const yml = read('electron-builder.yml')
    assert.match(yml, /^ {2}identity: null$/m, 'signing is not disabled by default')
    // The one that is easy to lose: electron-builder signs whenever a
    // certificate is present but submits to Apple only when told, so a config
    // without this line ships a signed build Gatekeeper still refuses.
    assert.match(yml, /^ {2}notarize: false$/m, 'notarization is not stated at all')
  })

  test('build.mjs targets exactly those two lines', () => {
    const build = read('scripts/build.mjs')
    assert.ok(build.includes('/^ {2}identity: null\\r?$/m'), 'the identity marker moved')
    assert.ok(build.includes('/^ {2}notarize: false\\r?$/m'), 'the notarize marker moved')
  })

  // Not a switch build.mjs flips: unlike the two above, this one is committed on.
  // It costs nothing without a certificate, because electron-builder only signs
  // when it has one, and losing it produces a DMG that Gatekeeper refuses on open
  // no matter how well notarized the app inside it is.
  test('the disk image is signed', () => {
    assert.match(read('electron-builder.yml'), /^ {2}sign: true$/m, 'dmg signing is off')
  })

  // The DMG is a separate submission from the .app, and the failure it prevents
  // is silent: the app staples fine, the build goes green, and the file people
  // actually download is the one that is refused.
  test('the disk image is submitted and stapled', () => {
    const build = read('scripts/build.mjs')
    assert.ok(build.includes("'notarytool', 'submit'"), 'disk images are never submitted')
    assert.ok(build.includes("'stapler', 'staple'"), 'disk images are never stapled')
    assert.ok(build.includes("context:primary-signature"), 'the DMG is assessed with the wrong Gatekeeper context')
  })
})

// The scheme contract spans three processes and two repos' worth of literals:
// main writes an argv switch, preload parses it, and the web app maps the value
// onto the path segment it marks email links with. Every consumer reads only its
// own half, so renaming a scheme in config.js breaks nothing here, nothing in
// tsc, and nothing in the web suite — the marked link simply addresses a scheme
// no build answers on, and the OS opens the wrong app or none. That is the
// failure this pins: a link marked `langalpha://` reached bare Electron on a
// machine where the running app was the OSS edition.
describe('the deep-link scheme contract holds across the two processes', () => {
  const read = (rel) => fs.readFileSync(path.join(__dirname, '..', rel), 'utf8')

  test('the switch main writes is the one preload parses', () => {
    assert.match(read('src/main.js'), /`--langalpha-shell-scheme=\$\{config\.scheme\}`/)
    assert.match(read('src/preload.js'), /scheme: flag\('shell-scheme'\)/)
  })

  test('the web app knows every scheme an edition can register', () => {
    const handoff = read('../web/src/lib/desktopAuthHandoff.ts')
    for (const edition of ['saas', 'oss']) {
      const { scheme } = loadShell({ edition }).config
      assert.match(
        handoff,
        new RegExp(`^\\s*'?${scheme}'?:\\s*'[a-z-]+',$`, 'm'),
        `web handoff table has no entry for the ${edition} scheme '${scheme}'`,
      )
    }
  })

  test('the web app routes the segments it marks links with', () => {
    const handoff = read('../web/src/lib/desktopAuthHandoff.ts')
    const app = read('../web/src/App.tsx')
    const segments = [...handoff.matchAll(/^\s*'?[a-z-]+'?:\s*'([a-z-]+)',$/gm)].map((m) => m[1])
    assert.ok(segments.length >= 2, 'no segments found in the handoff table')
    for (const page of ['/auth/confirm', '/reset-password']) {
      assert.match(app, new RegExp(`path="${page}/:shell"`), `${page} does not serve a marked link`)
    }
  })
})
