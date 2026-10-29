'use strict'

// The registered scheme: where each link the OS hands the shell is sent, and
// which window it lands in. Most links land on `/callback`; the ones addressed
// to the console are settled before that, and are pinned here kind by kind for
// what each maps to and in a table for how every one of them lands.

const { test, describe, after } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')
const {
  loadShell, cleanup, opened, emitApp, windows, focusOrder, signedIn, openUrl, isAccount, navigateFrom, quietly,
} = require('./helpers')

after(cleanup)

const APP = 'https://app.example.com'
const PLATFORM = 'https://platform.example.com'

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

// What `deeplink.toConsole` answers for a link it settles on the console.
const integrationAt = (url) => ({ kind: 'integration', url })
const consoleAt = (url) => ({ kind: 'console', url })

// Every console link this build does not open: a page not on the list, the
// listed one with anything around it, paths spelling another host, and the
// console's own host spelled so it is not exactly the console's.
const UNKNOWN = [
  'langalpha://console/settings?x=1',
  'langalpha://console/desktop/return/extra?to=start',
  'langalpha://console/desktop/return/?to=start',
  'langalpha://console//evil.example/desktop/return?to=start',
  'langalpha://console/%2F%2Fevil.example?to=start',
  'langalpha://console/%2F%2Fevil.example/desktop/return?to=start',
  'langalpha://console/\\evil.example/desktop/return?to=start',
  'langalpha://console/@evil.example/desktop/return?to=start',
  'langalpha://console/https://evil.example/desktop/return?to=start',
  'langalpha://console/desktop%2Freturn?to=start',
  'langalpha://console/desktop/return%00?to=start',
  'langalpha://console/DESKTOP/RETURN?to=start',
  'langalpha://console/desktop?to=start',
  'langalpha://console/__proto__?to=start',
  'langalpha://console/callback?code=C',
  'langalpha://console/integrations/login/slack/callback?code=C&state=S',
  'langalpha://console?to=start',
  'langalpha://console/?to=start',
  'langalpha://CONSOLE/desktop/return?to=start',
  'langalpha://console:443/desktop/return?to=start',
]

// Every kind of link the shell settles on the console rather than at
// /callback. Each maps by rules of its own, pinned in its own suite below, and
// main opens all of them through the one function, so what they share is asked
// of every kind from this table.
const RETURNS = [
  // The console's integration logins finish on the provider's page in the
  // system browser, which hands the answer back through the scheme when the
  // login was started here.
  {
    name: 'an integration login',
    kind: 'integration',
    base: 'langalpha://integrations/login/slack/callback',
    path: '/integrations/login/slack/callback',
    query: 'code=C&state=S',
    // Spellings the parser folds back onto the same path.
    folded: ['langalpha://integrations/login/evil/%2e%2e/slack/callback'],
    // oauth.js refuses the same thing on the loopback port, and for the same
    // reason: whatever reads the query takes one value, so a doubled `state` is
    // a link approved on one value and redeemed on the other.
    repeated: ['code=A&code=B&state=S', 'code=A&state=S&state=T', 'code=A&state=S&x=1&x=2'],
    // Refused for a reason of this kind's own.
    refused: ['langalpha://integrations/login/Slack/callback?code=C&state=S'],
    // A second one, for a window already showing the first.
    next: ['langalpha://integrations/login/discord/callback?code=D&state=T', `${PLATFORM}/integrations/login/discord/callback?code=D&state=T`],
    // Where the console sent the user: the provider's sign-in.
    away: 'https://provider.example.com/authorize?state=S',
    // What another kind carries and this one does not.
    foreign: 'team=T1',
    // A self-hosted build's link, and what of it must never reach the log.
    oss: 'langalpha-oss://integrations/login/slack/callback?code=c_7q&state=s_3k',
    secrets: /c_7q|s_3k|slack/,
  },
  // The console's installs leave for the provider's page the same way its
  // logins do and come back through the scheme the same way, under `install`
  // rather than `login`, carrying which team was added or why it was not.
  {
    name: 'an integration install',
    kind: 'integration',
    base: 'langalpha://integrations/install/slack/callback',
    path: '/integrations/install/slack/callback',
    query: 'team=T1&team_name=Acme',
    folded: ['langalpha://integrations/install/evil/%2e%2e/slack/callback'],
    repeated: ['team=A&team=B', 'team=A&team_name=X&team_name=Y', 'error=failed&error=access_denied', 'team=A&x=1&x=2'],
    refused: ['langalpha://integrations/install/Slack/callback?team=T1'],
    next: ['langalpha://integrations/install/discord/callback?team=T2', `${PLATFORM}/integrations/install/discord/callback?team=T2`],
    away: 'https://provider.example.com/install?state=S',
    foreign: 'code=C',
    oss: 'langalpha-oss://integrations/install/slack/callback?team=T1&team_name=Acme',
    secrets: /T1|Acme|slack/,
  },
  // The console now and then sends the user to a third party's page in the
  // system browser, and that page returns to the console in the browser, which
  // holds no session. The console page it lands on hands its query back through
  // the scheme, under the `console` host, and the shell loads the page the link
  // names in the window showing the console.
  {
    name: 'a console page',
    kind: 'console',
    base: 'langalpha://console/desktop/return',
    path: '/desktop/return',
    query: 'to=start&outcome=done&session_id=s_1',
    // Dot segments are resolved by the parser before the path is matched, so
    // these ARE the listed page, and land only on it.
    folded: ['langalpha://console/evil/%2e%2e/desktop/return', 'langalpha://console/desktop/./return'],
    // For the reason the integration returns give: whatever reads the query
    // takes one value, so a link approved on one would be acted on with the
    // other.
    repeated: ['to=a&to=b', 'to=a&outcome=x&outcome=y', 'session_id=s_1&session_id=s_2', 'to=a&x=1&x=2', 'to=a&t%6F=b'],
    refused: [...UNKNOWN, 'langalpha://console/desktop/return?to=evil.example.com%2Fx'],
    next: ['langalpha://console/desktop/return?to=next&outcome=done', `${PLATFORM}/desktop/return?to=next&outcome=done`],
    away: 'https://third-party.example.net/session/s_1',
    foreign: 'code=C',
    oss: 'langalpha-oss://console/desktop/return?to=go_7q&outcome=ok_3k&session_id=s_1x4',
    secrets: /go_7q|ok_3k|s_1x4|desktop/,
  },
]
for (const ret of RETURNS) {
  ret.link = `${ret.base}?${ret.query}`
  ret.page = `${PLATFORM}${ret.path}?${ret.query}`
}

// These pin what the shell makes of each link: one whose host and path it
// reads, and bound for the console rather than for whichever origin the window
// shows.
describe('a link handed back to the console from the browser', () => {
  for (const ret of RETURNS) {
    describe(ret.name, () => {
      test('lands on its page on the console, with what that page carries', () => {
        const { deeplink } = loadShell({ edition: 'saas' })
        assert.deepEqual(deeplink.toConsole(ret.link), { kind: ret.kind, url: ret.page })
      })

      test('on the configured console, never on the app or anything the link names', () => {
        for (const platformOrigin of ['https://console.example.org', 'https://console.example.org:8443']) {
          const { deeplink } = loadShell({ edition: 'saas', platformOrigin })
          const page = `${platformOrigin}${ret.path}?${ret.query}`
          assert.deepEqual(deeplink.toConsole(ret.link), { kind: ret.kind, url: page }, platformOrigin)
          // Credentials in the link are part of nothing the shell builds.
          const credentialed = ret.link.replace('langalpha://', 'langalpha://evil.example.com@')
          assert.deepEqual(deeplink.toConsole(credentialed), { kind: ret.kind, url: page }, platformOrigin)
        }
      })

      test('a link folded into its path still lands only on that page', () => {
        const { deeplink } = loadShell({ edition: 'saas' })
        for (const base of ret.folded) {
          assert.deepEqual(deeplink.toConsole(`${base}?${ret.query}`), { kind: ret.kind, url: ret.page }, base)
        }
      })

      test('a repeated parameter refuses the whole link', () => {
        const { deeplink } = loadShell({ edition: 'saas' })
        for (const query of ret.repeated) {
          const link = `${ret.base}?${query}`
          assert.deepEqual(deeplink.toConsole(link), { kind: ret.kind, url: null }, `${query} should still be recognised`)
        }
      })

      test('the other edition\'s scheme is not ours to read', () => {
        const { deeplink } = loadShell({ edition: 'saas' })
        assert.equal(deeplink.toConsole(ret.link.replace('langalpha://', 'langalpha-oss://')), null)
      })

      // The self-hosted build has no console, so it has no page to open one
      // on. Recognised all the same, because the alternative is /callback
      // trying to redeem a provider's code as a sign-in.
      test('the self-hosted build recognises one and has nowhere to send it', () => {
        const { deeplink } = loadShell({ edition: 'oss' })
        assert.deepEqual(deeplink.toConsole(ret.oss), { kind: ret.kind, url: null })
      })
    })
  }
})

describe('an integration login or install handed back from the browser', () => {
  // A cancel or a provider fault comes back the same way and must arrive too:
  // the console's page is what tells the user which one it was.
  test('a login that was refused or failed lands too', () => {
    const { deeplink } = loadShell({ edition: 'saas' })
    assert.deepEqual(
      deeplink.toConsole('langalpha://integrations/login/discord/callback?error=access_denied&error_description=The%20user%20said%20no&state=S'),
      integrationAt(`${PLATFORM}/integrations/login/discord/callback?error=access_denied&error_description=The+user+said+no&state=S`),
    )
  })

  test('an install lands with each thing an install hands back', () => {
    const { deeplink } = loadShell({ edition: 'saas' })
    for (const query of ['team=T1', 'team_name=Acme%20Corp', 'error=access_denied', 'team=T1&error=failed']) {
      assert.deepEqual(
        deeplink.toConsole(`langalpha://integrations/install/slack/callback?${query}`),
        integrationAt(`${PLATFORM}/integrations/install/slack/callback?${query.replace('%20', '+')}`),
        query,
      )
    }
    // An install that hands back nothing still lands, so the page can say so.
    assert.deepEqual(
      deeplink.toConsole('langalpha://integrations/install/slack/callback'),
      integrationAt(`${PLATFORM}/integrations/install/slack/callback`),
    )
  })

  test('a login carries the provider answer and nothing else', () => {
    const { deeplink } = loadShell({ edition: 'saas' })
    const mapped = deeplink.toConsole(
      'langalpha://integrations/login/slack/callback?code=C&next=https%3A%2F%2Fevil.example.com&state=S%26T&redirect_to=x&type=magiclink#frag',
    )
    const url = new URL(mapped.url)
    assert.deepEqual([...url.searchParams], [['code', 'C'], ['state', 'S&T']])
    assert.equal(url.hash, '')
  })

  // Each kind carries its own list. A login's answer is not an install's, so a
  // code or state riding an install link is not the provider's, and neither is
  // a team riding a login link.
  test('an install carries what an install hands back and nothing else', () => {
    const { deeplink } = loadShell({ edition: 'saas' })
    const mapped = new URL(deeplink.toConsole(
      'langalpha://integrations/install/slack/callback?code=C&team=T1&state=S&next=https%3A%2F%2Fevil.example.com&team_name=A%26B&error_description=x&type=magiclink&error=failed#frag',
    ).url)
    assert.deepEqual([...mapped.searchParams], [['team', 'T1'], ['team_name', 'A&B'], ['error', 'failed']])
    assert.equal(mapped.hash, '')
    const login = new URL(deeplink.toConsole(
      'langalpha://integrations/login/slack/callback?code=C&team=T1&team_name=Acme&state=S',
    ).url)
    assert.deepEqual([...login.searchParams], [['code', 'C'], ['state', 'S']])
  })

  // A name is the one part of the link that reaches the path, so it is the one
  // part held to a pattern. Refused rather than reinterpreted: a link addressed
  // to a login or install is never handed to /callback as a sign-in.
  test('a name outside the pattern is refused', () => {
    const { deeplink } = loadShell({ edition: 'saas' })
    for (const [kind, query] of [['login', 'code=C'], ['install', 'team=T1']]) {
      for (const name of ['Slack', 'SLACK', '1slack', '-slack', 'sl_ack', 'sl.ack', 'sl%2Fack', 'sl%61ck', 'sl%20ack', '', 'a'.repeat(33)]) {
        const link = `langalpha://integrations/${kind}/${name}/callback?${query}`
        assert.deepEqual(deeplink.toConsole(link), integrationAt(null), `${kind} '${name}' should be recognised and refused`)
      }
      for (const name of ['a', 'a'.repeat(32), 'google-chat', 'x1']) {
        assert.deepEqual(
          deeplink.toConsole(`langalpha://integrations/${kind}/${name}/callback?${query}`),
          integrationAt(`${PLATFORM}/integrations/${kind}/${name}/callback?${query}`),
          `${kind} '${name}'`,
        )
      }
    }
  })

  // Anything that is not the shape is an ordinary link, and keeps exactly the
  // route every link had before this one existed.
  test('any other shape is not one, and keeps going to /callback', () => {
    const { deeplink } = loadShell({ edition: 'saas' })
    const shapes = [
      ['code=C', [
        'langalpha://integrations/login/slack/extra/callback?code=C',
        'langalpha://integrations/login/slack/callback/extra?code=C',
        'langalpha://integrations/login/slack/callback/?code=C',
        'langalpha://integrations//login/slack/callback?code=C',
        // Dot segments are resolved by the parser before the shape is asked, so
        // these are the paths `/callback` and `/admin/callback`.
        'langalpha://integrations/login/../callback?code=C',
        'langalpha://integrations/login/slack/../../../admin/callback?code=C',
        'langalpha://integrations/login%2Fslack%2Fcallback?code=C',
        'langalpha://INTEGRATIONS/login/slack/callback?code=C',
        'langalpha://integrations/LOGIN/slack/callback?code=C',
        'langalpha://integrations/login/slack/CALLBACK?code=C',
        'langalpha://integrations:443/login/slack/callback?code=C',
        'langalpha://integrations.evil.example.com/login/slack/callback?code=C',
        'langalpha://evil.example.com/integrations/login/slack/callback?code=C',
        'langalpha:///integrations/login/slack/callback?code=C',
        'langalpha:integrations/login/slack/callback?code=C',
        'langalpha://integrations/other?code=C',
        'langalpha://callback?code=C',
      ]],
      ['team=T1', [
        'langalpha://integrations/install/slack/extra/callback?team=T1',
        'langalpha://integrations/install/slack/callback/?team=T1',
        'langalpha://integrations/install/slack?team=T1',
        'langalpha://integrations/INSTALL/slack/callback?team=T1',
        'langalpha://integrations/installs/slack/callback?team=T1',
        'langalpha://integrations/inst%61ll/slack/callback?team=T1',
        'langalpha://integrations/other/slack/callback?team=T1',
        'langalpha://integrations/__proto__/slack/callback?team=T1',
        'langalpha://evil.example.com/integrations/install/slack/callback?team=T1',
      ]],
    ]
    for (const [query, links] of shapes) {
      for (const link of links) {
        assert.equal(deeplink.toConsole(link), null, link)
        assert.equal(deeplink.toAppUrl(link, 'about:blank'), `${APP}/callback?${query}`, link)
      }
    }
  })
})

// The link names a path here, so these pin that the path can only be a page on
// the list and the origin only the configured console.
describe('a console page asked to be shown again', () => {
  test('lands with any of what the page carries, or with nothing', () => {
    const { deeplink } = loadShell({ edition: 'saas' })
    for (const query of ['to=start', 'outcome=canceled', 'session_id=s_1', 'outcome=A-z_09&to=x']) {
      assert.deepEqual(
        deeplink.toConsole(`langalpha://console/desktop/return?${query}`),
        consoleAt(`${PLATFORM}/desktop/return?${query}`),
        query,
      )
    }
    // A link with nothing to say still lands, so the page can say so.
    assert.deepEqual(deeplink.toConsole('langalpha://console/desktop/return'), consoleAt(`${PLATFORM}/desktop/return`))
  })

  test('carries what the page reads and drops the rest', () => {
    const { deeplink } = loadShell({ edition: 'saas' })
    const mapped = new URL(deeplink.toConsole(
      'langalpha://console/desktop/return?next=https%3A%2F%2Fevil.example.com&to=start&code=C&outcome=done&redirect_to=..%2F..&session_id=s_1&type=magiclink#frag',
    ).url)
    assert.equal(mapped.pathname, '/desktop/return')
    assert.deepEqual([...mapped.searchParams], [['to', 'start'], ['outcome', 'done'], ['session_id', 's_1']])
    assert.equal(mapped.hash, '')
  })

  // One of the values says where the console goes next, so every carried value
  // is one short token: nothing in it can be read as a path, a host, or a value
  // still encoded. Refused rather than dropped, because the page cannot tell a
  // missing value from one the shell took away.
  test('a carried value that is not one short token refuses the whole link', () => {
    const { deeplink } = loadShell({ edition: 'saas' })
    const refused = [
      'a/b', '%2F', '%2Fevil.example', 'a.b', '..', '%2E%2E', 'a%20b', 'a+b', '%25', '%2541', 'a:b', 'a@b',
      'https://evil.example.com', 'https%3A%2F%2Fevil.example.com', '%2F%2Fevil.example.com', '%5Cevil',
      'caf%C3%A9', '%00', '', 'a'.repeat(256),
    ]
    for (const name of ['to', 'outcome', 'session_id']) {
      for (const value of refused) {
        const link = `langalpha://console/desktop/return?${name}=${value}`
        assert.deepEqual(deeplink.toConsole(link), consoleAt(null), `${name}=${value} should be recognised and refused`)
      }
      for (const value of ['a', 'a'.repeat(255), 'A-Z_0-9', '-', '_']) {
        assert.deepEqual(
          deeplink.toConsole(`langalpha://console/desktop/return?${name}=${value}`),
          consoleAt(`${PLATFORM}/desktop/return?${name}=${value}`),
          `${name}=${value}`,
        )
      }
    }
  })

  // Recognised all the same, so it never reaches /callback: that route would
  // try to redeem it as a sign-in.
  test('a console path this build does not know is refused, and still recognised', () => {
    const { deeplink } = loadShell({ edition: 'saas' })
    for (const link of UNKNOWN) assert.deepEqual(deeplink.toConsole(link), consoleAt(null), link)
  })

  // The whole guarantee in one place: whatever the link spells, anything that
  // opens is a listed page on the configured console.
  test('no link opens anything but a listed page on the console', () => {
    const { deeplink } = loadShell({ edition: 'saas' })
    const tricky = [
      ...UNKNOWN,
      'langalpha://console/%2F%2Fevil.example',
      'langalpha://console/desktop/return@evil.example.com?to=start',
      'langalpha://console/desktop\\return?to=start',
      'langalpha://console@evil.example.com/desktop/return?to=start',
      'langalpha://console/desktop/return?to=%2F%2Fevil.example.com',
      'langalpha://console/desktop/return?to=https://evil.example.com/desktop/return',
      'langalpha://console/desktop/return?to=%5C%5Cevil.example.com',
      'langalpha://console/desktop/return?to=start@evil.example.com',
    ]
    const opens = [
      'langalpha://console/desktop/return?to=start&outcome=done&session_id=s_1',
      'langalpha://console/desktop/return?to=start&next=https://evil.example.com',
      'langalpha://evil.example.com@console/desktop/return?to=start',
      'langalpha://console/evil/%2e%2e/desktop/return?to=start',
      'langalpha://console/desktop/return?to=start#https://evil.example.com',
    ]
    for (const link of tricky) assert.equal(deeplink.toConsole(link)?.url ?? null, null, link)
    for (const link of opens) {
      const mapped = deeplink.toConsole(link)
      assert.equal(mapped?.kind, 'console', link)
      assert.notEqual(mapped.url, null, link)
      const url = new URL(mapped.url)
      assert.equal(url.origin, PLATFORM, link)
      assert.equal(url.pathname, '/desktop/return', link)
      assert.equal(url.hash, '', link)
    }
  })

  // Any other host is an ordinary link, and keeps exactly the route every link
  // had before this one existed.
  test('a host that is not the console is not one, and keeps going to /callback', () => {
    const { deeplink } = loadShell({ edition: 'saas' })
    for (const link of [
      'langalpha://consoles/desktop/return?to=start',
      'langalpha://console.evil.example.com/desktop/return?to=start',
      'langalpha://console@evil.example.com/desktop/return?to=start',
      'langalpha://evil.example.com/console/desktop/return?to=start',
      'langalpha:///console/desktop/return?to=start',
      'langalpha:console/desktop/return?to=start',
    ]) {
      assert.equal(deeplink.toConsole(link), null, link)
      assert.equal(deeplink.toAppUrl(link, 'about:blank'), `${APP}/callback?to=start`, link)
    }
  })
})

// Which window a link lands in depends on what the launch opened, in what
// order, and on which way the OS delivered it, so these drive the launch and the
// OS events themselves against a stubbed BrowserWindow rather than calling the
// mapping. `windows` is every window the shell constructed, and each one's
// `loaded` is every URL it was sent to.
describe('which window a link to the console lands in', () => {
  const SETTINGS = `${PLATFORM}/settings`
  const landedOnCallback = () => windows.some((w) => w.loaded.some((u) => new URL(u).pathname === '/callback'))

  // A signed-in launch: the app in the main window, and nothing showing the
  // console yet.
  const launched = ({ consoleSharesMainWindow }) => {
    const shell = signedIn({ consoleSharesMainWindow })
    shell.main.openLaunchWindows()
    return { ...shell, app: windows[0] }
  }

  // The user went from the app to the console, and from a console page out to
  // the browser: in the main window when the console shares it, in an account
  // window of its own when it cannot.
  const sentOutFromTheConsole = (ret, { consoleSharesMainWindow }) => {
    const shell = launched({ consoleSharesMainWindow })
    if (consoleSharesMainWindow) shell.app.loadURL(SETTINGS)
    else navigateFrom(shell.app, SETTINGS)
    const win = windows.at(-1)
    assert.equal(isAccount(win), !consoleSharesMainWindow)
    assert.equal(win.loaded.at(-1), SETTINGS)
    assert.equal(shell.main.openExternallyFrom({ sender: win.webContents }, ret.away), true)
    return { ...shell, win }
  }

  // The account window outlives the console coming to fit the main window, so
  // both can show it at once. Where a link lands is where the user left from:
  // the window whose page sent them out.
  const bothShowTheConsole = (ret) => {
    const shell = sentOutFromTheConsole(ret, { consoleSharesMainWindow: false })
    shell.store.set('platformChrome', true)
    shell.app.loadURL(SETTINGS)
    const pressIn = (win) => shell.main.openExternallyFrom({ sender: win.webContents }, ret.away)
    return { ...shell, account: shell.win, pressIn }
  }

  // The ways a window comes to show the console before a link arrives: an
  // earlier link put it there, or the user went there and out to the browser.
  const SHOWING = [
    ['after an earlier link', (ret, opts) => {
      const shell = launched(opts)
      openUrl(ret.link)
      return { ...shell, win: windows.at(-1) }
    }],
    ['after a console page sent the user out', sentOutFromTheConsole],
  ]

  for (const ret of RETURNS) {
    describe(ret.name, () => {
      // With nothing showing the console, it goes where a navigation to the
      // console goes. Decided before the load: `navigate` never passes through
      // will-navigate, so loading it straight into the main window would skip
      // the routing that gives the console a window of its own.
      test('with no window showing the console, it shares the main window when the console fits', () => {
        launched({ consoleSharesMainWindow: true })
        openUrl(ret.link)
        assert.equal(windows.length, 1)
        assert.deepEqual(windows[0].loaded, [APP, ret.page])
      })

      test('with no window showing the console, one that cannot share gets its own, and the app is left alone', () => {
        const { app } = launched({ consoleSharesMainWindow: false })
        openUrl(ret.link)
        assert.equal(windows.length, 2)
        const account = windows[1]
        assert.deepEqual(app.loaded, [APP], 'the app window was navigated, and a turn in it would be gone')
        assert.ok(isAccount(account))
        assert.deepEqual(account.loaded, [ret.page])
        assert.equal(focusOrder.at(-1), account)
      })

      test('an open console window is where the next one lands, even once the console could share', () => {
        const { app, store } = launched({ consoleSharesMainWindow: false })
        openUrl(ret.link)
        // The console reserving the strip flips the verdict for the rest of the
        // session, while the page that sent the user out is still in its window.
        store.set('platformChrome', true)
        openUrl(ret.next[0])
        assert.equal(windows.length, 2)
        assert.deepEqual(app.loaded, [APP])
        assert.deepEqual(windows[1].loaded, [ret.page, ret.next[1]])
      })

      test('when both windows show the console, in the one whose page sent the user out', () => {
        for (const from of ['main', 'account']) {
          const { app, account, pressIn } = bothShowTheConsole(ret)
          const [sender, other] = from === 'main' ? [app, account] : [account, app]
          pressIn(sender)
          // Back to the other window while the browser has the user.
          emitApp('browser-window-focus', {}, other)
          const before = other.loaded.slice()
          openUrl(ret.link)
          assert.equal(windows.length, 2, from)
          assert.equal(sender.loaded.at(-1), ret.page, from)
          assert.deepEqual(other.loaded, before, `${from}: the page left waiting in the other window was replaced`)
          assert.equal(focusOrder.at(-1), sender, from)
        }
      })

      // Back to the app while the browser has the user, then out to it again:
      // every switch moves focus and blur, and none of them sent the user out.
      test('switching between the windows and the browser does not move where it lands', () => {
        const { app, account, pressIn } = bothShowTheConsole(ret)
        pressIn(account)
        emitApp('browser-window-blur', {}, account)
        emitApp('browser-window-focus', {}, app)
        emitApp('browser-window-blur', {}, app)
        openUrl(ret.link)
        assert.equal(app.loaded.at(-1), SETTINGS)
        assert.equal(account.loaded.at(-1), ret.page)
      })

      test('a link the shell refused to open does not mark its window as the one that sent the user out', () => {
        const { app, account, pressIn, main } = bothShowTheConsole(ret)
        pressIn(account)
        assert.equal(main.openExternallyFrom({ sender: app.webContents }, 'file:///etc/passwd'), false)
        openUrl(ret.link)
        assert.equal(app.loaded.at(-1), SETTINGS)
        assert.equal(account.loaded.at(-1), ret.page)
      })

      // Windows and Linux: the link rides a second launch, whose handler raises
      // the main window before deeplink's delivers it, and Windows can report
      // that focus synchronously. None of it moves where the user left from.
      test('a second launch raising the main window first does not take it from the account window', () => {
        const { app, account, pressIn } = bothShowTheConsole(ret)
        pressIn(account)
        const raise = app.focus
        app.focus = function () { raise.call(this); emitApp('browser-window-focus', {}, this) }
        emitApp('second-instance', {}, ['/Applications/LangAlpha', ret.link], '/')
        assert.equal(app.loaded.at(-1), SETTINGS)
        assert.equal(account.loaded.at(-1), ret.page)
        assert.equal(focusOrder.at(-1), account)
      })

      test('a window showing something else is passed over, though its page last sent the user out', () => {
        const { main, app } = launched({ consoleSharesMainWindow: false })
        openUrl(ret.link)
        const account = windows[1]
        main.openExternallyFrom({ sender: app.webContents }, 'https://example.com/docs')
        openUrl(ret.next[0])
        assert.deepEqual(app.loaded, [APP], 'the app was navigated, and a turn in it would be gone')
        assert.equal(account.loaded.at(-1), ret.next[1])
      })

      test('in the window showing the console, restored and raised', () => {
        for (const [showing, setup] of SHOWING) {
          for (const consoleSharesMainWindow of [true, false]) {
            const tag = `${showing}, shares=${consoleSharesMainWindow}`
            const { app, win } = setup(ret, { consoleSharesMainWindow })
            let minimized = true
            win.isMinimized = () => minimized
            win.restore = () => { minimized = false }
            const count = windows.length
            openUrl(ret.next[0])
            assert.equal(windows.length, count, `${tag}: another window opened`)
            assert.equal(win.loaded.at(-1), ret.next[1], tag)
            assert.equal(minimized, false, `${tag}: the page loaded in a window still minimized`)
            assert.equal(focusOrder.at(-1), win, tag)
            if (!consoleSharesMainWindow) {
              assert.deepEqual(app.loaded, [APP], `${tag}: the app was navigated, and a turn in it would be gone`)
            }
            assert.equal(landedOnCallback(), false, tag)
          }
        }
      })

      // macOS: `open-url` fires before the app is ready, and the link waits in
      // the queue until the launch attaches to it.
      test('a cold start from the link opens it, and not the entry over it', () => {
        for (const consoleSharesMainWindow of [true, false]) {
          const tag = `shares=${consoleSharesMainWindow}`
          const { main } = signedIn({ consoleSharesMainWindow })
          openUrl(ret.link)
          assert.equal(windows.length, 0, `${tag}: delivered before the launch attached`)
          main.openLaunchWindows()
          assert.equal(windows.length, 1, tag)
          assert.equal(isAccount(windows[0]), !consoleSharesMainWindow, tag)
          assert.deepEqual(windows[0].loaded, [ret.page], tag)
        }
      })

      // The case the launch order used to get wrong: the entry of an install
      // that has not reached the app is sign-in in that same console window,
      // and opening it after the link loaded sign-in over the page. A signed-in
      // install would instead have opened the app in front of it.
      test('a cold start into the console window is not covered by the entry', () => {
        for (const reachedApp of [false, true]) {
          const { main, store } = loadShell({ edition: 'saas' })
          store.set('reachedApp', reachedApp)
          store.set('appChrome', true)
          store.set('platformChrome', false)
          openUrl(ret.link)
          main.openLaunchWindows()
          assert.equal(windows.length, 1, `reachedApp=${reachedApp}: another window opened over the link`)
          assert.ok(isAccount(windows[0]))
          assert.deepEqual(windows[0].loaded, [ret.page], `reachedApp=${reachedApp}`)
        }
      })

      // Windows and Linux: the link is in our own argv.
      test('a cold start from argv lands the same way', () => {
        process.argv.push(ret.link)
        let main
        try {
          ({ main } = signedIn({ consoleSharesMainWindow: false }))
        } finally {
          process.argv.pop()
        }
        main.openLaunchWindows()
        assert.equal(windows.length, 1)
        assert.ok(isAccount(windows[0]))
        assert.deepEqual(windows[0].loaded, [ret.page])
      })

      // Windows and Linux again, with the app already running: the OS launches
      // a second copy, and the single-instance lock turns that into this event.
      // The shell's own handler brings the main window forward first, so the
      // link has to end up in front of it rather than behind.
      test('a second instance hands it to the running one', () => {
        const { app } = launched({ consoleSharesMainWindow: false })
        emitApp('second-instance', {}, ['/Applications/LangAlpha', '--flag', ret.link], '/')
        assert.equal(windows.length, 2)
        const account = windows[1]
        assert.deepEqual(app.loaded, [APP])
        assert.ok(isAccount(account))
        assert.deepEqual(account.loaded, [ret.page])
        assert.equal(focusOrder.at(-1), account)
      })

      test('what the page does not carry never reaches the window', () => {
        launched({ consoleSharesMainWindow: true })
        openUrl(`${ret.link}&next=https%3A%2F%2Fevil.example.com&${ret.foreign}`)
        assert.deepEqual(windows[0].loaded, [APP, ret.page])
      })

      test('a refused one opens nothing, moves nothing, and never reaches /callback', () => {
        const refused = [...ret.repeated.map((query) => `${ret.base}?${query}`), ...ret.refused]
        const setups = [
          ['just launched', (_ret, opts) => launched(opts)],
          ['after a console page sent the user out', sentOutFromTheConsole],
        ]
        for (const [state, setup] of setups) {
          for (const consoleSharesMainWindow of [true, false]) {
            const tag = `${state}, shares=${consoleSharesMainWindow}`
            setup(ret, { consoleSharesMainWindow })
            const before = windows.map((w) => w.loaded.slice())
            const focused = focusOrder.length
            const handedOut = opened.length
            const warned = quietly(() => { for (const link of refused) openUrl(link) })
            assert.deepEqual(windows.map((w) => w.loaded), before, `${tag}: a refused link moved a window`)
            assert.equal(focusOrder.length, focused, `${tag}: a refused link raised a window`)
            assert.equal(opened.length, handedOut, `${tag}: a refused link was handed to the browser`)
            assert.equal(landedOnCallback(), false, tag)
            // One line each, and none of the link in it.
            assert.equal(warned.length, refused.length, tag)
            for (const line of warned) assert.doesNotMatch(line, /evil|settings|start|desktop|callback|slack/i, tag)
          }
        }
      })

      // No platform origin, so `toConsole` refuses it and the shell ignores the
      // link, rather than handing it to the app's sign-in callback.
      test('the self-hosted build ignores one rather than sending it to /callback', () => {
        const { main } = loadShell({ edition: 'oss', serverUrl: 'http://localhost:5173' })
        main.openLaunchWindows()
        const warned = quietly(() => openUrl(ret.oss))
        assert.equal(windows.length, 1)
        assert.deepEqual(windows[0].loaded, ['http://localhost:5173'])
        assert.equal(landedOnCallback(), false)
        assert.equal(warned.length, 1)
        assert.doesNotMatch(warned[0], ret.secrets, 'the link\'s values reached the log')
      })
    })
  }

  test('any other link still lands on /callback in the main window', () => {
    for (const consoleSharesMainWindow of [true, false]) {
      launched({ consoleSharesMainWindow })
      openUrl('langalpha://callback?code=M&type=magiclink')
      openUrl('langalpha://integrations/elsewhere?code=N')
      assert.equal(windows.length, 1, 'no console window for a link that is not addressed to the console')
      assert.deepEqual(windows[0].loaded, [
        APP, `${APP}/callback?code=M&type=magiclink`, `${APP}/callback?code=N`,
      ])
    }
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

  test('the console-returns switch main writes is the one preload parses', () => {
    assert.match(read('src/main.js'), /`--langalpha-console-returns=\$\{origins\.platformOrigin\(\) \? '1' : ''\}`/)
    assert.match(read('src/preload.js'), /flag\('console-returns'\) === '1'/)
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
