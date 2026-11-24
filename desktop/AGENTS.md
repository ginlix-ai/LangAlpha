# langalpha desktop

Electron shell around the web app. The hosted edition is a **remote-URL wrapper**: the
package contains Electron, `src/`, two local pages and an entry URL, and nothing else. No
web bundle ships with it, so a web deploy never needs a desktop release. The two local
pages are the ones that have to work when the network does not: the first-run page and
the outage screen.

The OSS edition on macOS and Linux also **carries and runs its own stack**: Python and
the backend, the web app, Postgres and Valkey, so someone can install it and have it
work with no server to run. That is [its own section](#the-bundled-stack-oss-macos-and-linux).
An OSS install can still point at a server instead.

> Single source of truth for AI coding agents in `desktop/`. Edit here.

## Why Electron, not Tauri

Both were built and measured. On macOS the performance is comparable, so speed is not
what decided it: what is left is a straight trade of package size and idle memory, where
Tauri wins outright, against engine control and shell cost, where Electron does.

**Engine control is worth more here than it would be in a bundled app.** This wrapper
carries no web bundle, so under Tauri the rendering engine becomes whatever the user's
OS ships. WebView2 on Windows is Chromium and fine; macOS pins to that machine's
WKWebView and Linux to WebKitGTK, for a payload that deploys weekly and cannot be
version-matched to any of them. Electron puts one Chromium inside the package and moves
it on our schedule. For an app that shipped its own bundle this argument would be much
weaker, because engine and payload would be tested as a pair.

**The window chrome is the same point in miniature.** `-webkit-app-region` is a Chromium
property and the whole contract is built on it, five call sites in `web/` plus
`titleBarStyle`. Tauri's `data-tauri-drag-region` would put shell-specific markup into
`web/` for every browser user to download, and put a JS handler back in front of a
decision that is now pure CSS.

**The shell is cheap because it is JavaScript.** Roughly 2000 lines of URL classification
and window policy, with a suite in plain `node --test` over an `electron` stub and no
runtime needed. In Rust that is a toolchain in CI and on every self-hoster's machine, to
host logic that mostly decides whether a URL is ours.

This is the right call for this stage, not a permanent one. The thing that would move it
is the desktop app shipping its own bundle: engine and payload recouple, the argument
above loses most of its force, and ~90 MB of package and the idle memory become the
whole story.

The OSS edition now does ship a bundle, on macOS and Linux, and the argument stands for
now because the same source still has to be a remote-URL wrapper for the hosted edition,
and an OSS install can point at a server whose web build is any age. Beside a stack of
over a gigabyte, Electron's ~90 MB is also no longer what decides the package's size.
The day the hosted edition ships a bundle too is the day to run the comparison again.

## Commands

```bash
pnpm start                       # run the shell from source (oss defaults → localhost:5173)
pnpm test                        # node:test; pure logic only, no Electron runtime needed
pnpm run preview                 # the shell against a web build that has not deployed yet
pnpm run build                   # unpacked build into dist/<edition>/, fastest way to check packaging
pnpm run dist                    # real installers; on oss this bundles the stack (see below)
DESKTOP_BUNDLE_BACKEND=0 pnpm run dist   # the thin OSS build, which only talks to a server

DESKTOP_EDITION=saas \
DESKTOP_APP_ORIGIN=https://…  \
DESKTOP_PLATFORM_ORIGIN=https://… \
DESKTOP_PAIRING_ORIGIN=https://… pnpm run dist  # a saas package
```

**`pnpm run preview` is how you see a change before it deploys.** A remote-URL shell
loads whatever is *live*, so every frame decision it makes is a reaction to the deployed
bundle rather than to the source in front of you: that is how the window buttons ended up
on the app's own logo, and it is where the console's second window was caught before it
reached anyone. It builds `web/`, serves it on loopback with the api streamed through to
a running stack, and launches the shell against it. `--web-env <file>` builds the
frontend the way another environment builds it and `--platform <origin>` runs the saas
edition, which together make a hosted preview a matter of pointing at a local pair:

```bash
# the console, in the layout production actually runs: at an origin root, not
# under /account/. Its dev default is the legacy same-host layout, and a console
# reachable only at a path prefix is one this shell cannot address, since it
# classifies by origin and nothing else.
VITE_ACCOUNT_PREFIX= VITE_BASE=/ VITE_APP_URL=http://localhost:5399 \
VITE_PLATFORM_URL=http://localhost:5178 VITE_COOKIE_DOMAIN=localhost \
pnpm dev --port 5178 --strictPort          # in the console's own web/

VITE_APP_ENTRY_PATH=/ VITE_PLATFORM_URL=http://localhost:5178 \
pnpm run preview -- --host localhost --web-env ../../web/.env \
  --platform http://localhost:5178 --backend http://127.0.0.1:8000
```

`--host localhost` rather than the loopback literal is deliberate: cookies ignore
port, so a console on the same hostname shares a session with the preview the way
the two subdomains share one in production.

It keeps a user-data dir of its own, which is not tidiness: sharing the installed app's
would teach the *installed* app that its frontend reserves the window-button strip when
the build it actually loads may not, which is the bug it exists to catch.

**Say `pnpm run`, not `pnpm`, for anything that builds.** `pnpm <name>` falls back
to a script only when pnpm has no command of that name, and the failure when it
does is silent: this used to be `pnpm pack`, which quietly built a **tarball**
instead, left the previous artifact in the output tree, and exited 0. A verification run
against that stale artifact is what caught it.

**The `postinstall` line is load-bearing.** Electron 42 removed the package's own
postinstall, so `require('electron')` now downloads the binary lazily on first use.
Left alone that turns a missing download into a stall in the middle of whatever
first touched it; `install-electron` puts the fetch back at install time where a
failure is a failed install. It is also why `allowBuilds` in `pnpm-workspace.yaml`
lists only `sharp`: neither electron nor electron-builder has a build script to approve.

Minimum supported macOS is **12**, raised by Electron 38.

## Two editions, one source

| | `oss` | `saas` |
|---|---|---|
| Entry on first run | run on this computer, or connect to a server | hosted sign-in, then wherever the platform sends them |
| Stack in the package | macOS and Linux | none |
| Platform console | does not exist | shares the app window (see below) |
| Config | `config/default.json` (committed) | `config/build.json` (gitignored) |
| App name | `LangAlpha OSS` | `LangAlpha` |
| Bundle id | `ai.langalpha.desktop.oss` | `ai.langalpha.desktop` |
| URL scheme | `langalpha-oss://` | `langalpha://` |
| Remote-control helper label | `ai.langalpha.desktop.oss.helper` | `ai.langalpha.desktop.helper` |

`src/config.js` merges the two and refuses to start a `saas` build with no
`platformOrigin`: that build would open on the app and silently skip onboarding, which
is the one thing the edition exists to guarantee.

**The shell never decides whether a customer needs onboarding.** A SaaS first run enters
at the platform's *sign-in* page and the platform's own post-auth funnel routes from
there: to the app for an account that is set up, into the wizard for one that is not.
`reachedApp` is per-install state and only ever a shortcut past that round trip; reading
account state off it gets a returning user the wrong first screen,
being offered a downgrade from the plan they already pay for.
Every later sign-in passes through that same page. Inside the SaaS edition (read off the
shell's capabilities), the app sends a signed-out window there instead of showing its own
sign-in, and a sign-in that still finishes on the app's pages, such as an emailed link opened
while the app was closed, continues there before the dashboard (`web/src/lib/hostedSignIn.ts`).
Without that, signing out and registering a new account on an install that has reached the app
skips the wizard. The OSS edition keeps the app's own sign-in, whatever server it points at.

**The two editions are installable side by side, and all three identity strings
carry that.** The name is the load-bearing one: `app.getName()` is what
`getPath('userData')` is derived from, so a shared name is a shared `settings.json`
and the OSS build inherits whatever origin the hosted one last learned. Separate
bundle ids do not separate profiles. The scheme is the functional half; both editions
registering `langalpha://` leaves the OS to pick one, so a hosted magic link can open a
build pointed at localhost, which cannot redeem it.

`src/config.js` holds the table and is the only thing the *running* app reads, including
`app.setName(config.appName)` in `main.js`. Without that call Electron answers
`getName()` from the packaged `package.json`, not from the `CFBundleName`
electron-builder stamps, and a correctly-stamped bundle resolves the other edition's
profile. `scripts/build.mjs` holds the packaging half and rewrites `electron-builder.yml`
per edition; the committed base is the OSS identity, so an unset `DESKTOP_EDITION` builds
the self-hosted one. The two tables are asserted equal in the tests, because every
consumer only ever reads its own half.

**`config/build.json` is deployment configuration, not source.** Where a build points
is decided per package, so the origins are written from the environment at package time
and deleted again on an `oss` build: a tree that previously built `saas` cannot bake
those origins into an OSS package by accident.

**Nor do the two share an output directory** (`directories.output: dist/${EDITION}`).
The artifact filename carries the edition tag; nothing else written there does. The unpacked
bundle is named from `productName`, so both `.app`s land under one `mac-arm64`, and the
update manifests are named for the update channel and never the edition
(`latest-mac.yml`, `latest.yml`), so whichever edition builds second overwrites the first's. `scripts/build.mjs`
then searches that same directory for what it just produced, which is where one tree
turns into a signing check verifying the other edition's stale bundle and a no-feed build
whose sweep strips the other edition's baked `app-update.yml`.

Separate outputs do **not** make the two editions safe to build at the same time. Both
still write `config/build.json` and `.electron-builder.resolved.yml`, one copy each at
fixed paths, so two concurrent packages race over the origins and the identity and can
produce a correctly named artifact carrying the other edition's configuration. Build them
one after the other. For the same reason `scripts/make-release-index.mjs` takes one
edition's output directory and refuses a parent holding several.

## Layout

| File | Purpose |
|---|---|
| `src/index.js` | the packaged entry: turns a require-time crash into a dialog instead of a silent death |
| `src/main.js` | windows, IPC, and carrying out what `policy` decided; wires the menu bar modules below to the windows it owns |
| `src/backend.js` | OSS: which backend this install uses, a server the user named or the bundled stack, and the app's origin under it. `origins` and `policy` ask it; nothing else reads `serverUrl` |
| `src/setupWindow.js` | OSS: the window for `setup/` and its channels (`server:*`, `stack:*`), answered for that window only, and the stack's start shown in it |
| `src/pageFacts.js` | what a window tells its page about itself (`shell:window-facts`), and what a finished load tells the shell: the window-chrome declaration and reaching the app |
| `src/policy.js` | what the shell decides: entry URL, where a navigation belongs. **Pure** — it may not write the store or open anything |
| `src/origins.js` | what counts as ours: **by origin, never by path** |
| `src/oauth.js` | system-browser OAuth, intercepted (below) |
| `src/deeplink.js` | `langalpha://` scheme, for magic links clicked with the app closed, and integration logins, installs and console pages the browser hands back |
| `src/preload.js` | the renderer bridge: version, platform, `windowRole`, `capabilities`, `setTheme`, `openExternal`, `savePdf`, the connector OAuth trio, `getRemoteControl` / `enableRemoteControl` / `disableRemoteControl`, and the menu bar (`setTrayItems`, `onTrayRefresh`, `openInMain`, `hideMenubar`), notification, menu-command and desktop-settings members (below) |
| `src/remoteControl.js` | remote control of this Mac: the helper's login item, its folder, and the confirmation before a pairing (below) |
| `src/remoteControlState.js` | what remote control reads off the Mac: whether it can run here, the status the app is shown, the menu line. **Pure**, like `policy` |
| `src/sender.js` | the window an IPC call came from, for the channels that answer only some pages; `appSenderWindow` for the ones that answer only the app's own main frame, `gatedIpc` to register a channel behind such a check |
| `src/platformTraits.js` | every platform fact the menu bar, notification, login-item and closing code acts on, as one object (below). **Pure** |
| `src/glance.js` | the menu bar feature's root: composes the menu bar window, its icon, notifications and attention, and answers the menu bar page's IPC |
| `src/menubar.js` | the menu bar window: the panel on macOS and Windows, the Market Glance window on Linux, and its navigation policy |
| `src/tray.js` | the menu bar icon and the lines beside it: layout per platform, rotation and refresh timers |
| `src/trayMenu.js` | the icon's menu. **Pure** |
| `src/panelGeometry.js` | where the panel opens, from the icon's bounds and the display. **Pure** |
| `src/desktopSettings.js` | the desktop settings' shape, validation, merge and what each platform supports. **Pure** |
| `src/desktopSettingsService.js` | the live settings: a patch applied and broadcast, `onChange` for each feature that acts on one, and their IPC |
| `src/commands.js` | menu commands: what the main window's page declared, enabling the menu by it, and delivering one to it |
| `src/attention.js` | the Dock badge and Dock menu, the Windows overlay dot and jump list task |
| `src/shortcut.js` | the global shortcut that toggles the panel |
| `src/lifecycle.js` | quitting and what closing the last of the user's windows means (below) |
| `src/loginItem.js` | open at login, whether this launch was one, and the Linux autostart entry |
| `src/pdf.js` | renders the calling window to a PDF the user picks a home for; allowlists the options the page may set |
| `src/downloads.js` | gives a download a visible ending, since a frameless window has no download shelf |
| `src/notify.js` | the dock bounce that says a file landed, and posting thread notifications: whether to, and where a click goes |
| `src/outage.js` | what replaces a blank window when the network does not answer |
| `src/updater.js` | auto-update, gated on a feed actually existing |
| `src/probe.js` | "is anything answering", shared by the picker and the outage page |
| `src/store.js` | a small JSON file: `serverUrl` (the user's own server, never the stack's origin), `stackMode`, `stackPort`, `reachedApp`, `appChrome`, `platformChrome`, `theme`, `updateNotifiedVersion`, `desktop` (the desktop settings: `notifications`, `menubar`, `panel`, `openAtLogin`, `shortcut`, `keepRunningInTray`), `trayNoticeShown` |
| `src/stack/` | the bundled stack's supervisor: `plan.js` decides, **pure**, like `policy`; `children.js` runs a process with its guard; `logs.js` rotates; `dataFolder.js` keeps the data folder, its snapshot and its restore; `supervisor.js` is the service table, the order and the restarts, with no Electron in it; `index.js` binds it to this install. Left out of the hosted package |
| `setup/` | the OSS first-run page: the choice, the server form, the stack's progress and its failure, in its own window with its own preload |
| `outage/` | the outage page, a local file so it renders when nothing else can |
| `test/` | `node:test` over the pure logic, with an `electron` module stub |

## One frame per app

Hiding the titlebar hands the top strip of the window to the page. That is only safe
for a page that takes it: both SPAs reserve a 38px drag region for the macOS window
buttons, and a page that does not leaves the buttons sitting on its own header **and**
the window with no drag region anywhere, which means it cannot be moved at all.

**Each SPA declares its region in `index.html`, not in its bundle** (`#window-drag`,
shown by a `desktop-mac` class the same document stamps from the bridge). Two reasons,
both learned the hard way. It has to hold on every route: langalpha's used to live in
the sidebar, so a logged-out window reserved nothing, the probe below recorded "this
build does not reserve", and the next launch came up with a plain titlebar. And it has
to hold when the bundle is the thing that failed, since a window showing a dead build
is still a window the user has to be able to move. Where a sidebar exists it also
carries a matching 38px spacer, so the app's own header starts below the buttons
rather than under them.

**Once the bundle is up, the band widens past that corner.** Each SPA's
`styles/chrome.css` turns its frame into a titlebar under `html.desktop-mac`: the
sidebar's spacer carries the drag itself, `[data-chrome~="drag"]` marks the header
rows, and `.chrome-drag-strip` gives a column with no header of its own a 38px band
in place of its top padding. Interactive elements and documents declare `no-drag`,
because a drag region swallows the mouse for everything inside it. **None of this is
testable from Playwright**: its mouse events are CDP synthesized and never reach the
hit test that owns `-webkit-app-region`, and posting real CoreGraphics events needs
macOS Accessibility permission. Changes here are checked by dragging the window.

**So the frame is feature-detected, not assumed.** After each app load the shell reads the
page's own declaration, `<meta name="langalpha-window-chrome" content="reserves">` in
`index.html` (`CHROME_DECLARATION` in `pageFacts.js`), and remembers the answer in `appChrome`;
window creation reads it. A declaration rather than a hit-test, because the strip is only
painted once the shell says the titlebar is hidden, so measuring it would read back the
shell's own last answer. A page with no meta is a "no" only when the server really served
it (a 2xx); an error document declares nothing either and is not recorded. The status is
the document's, so it carries across the SPA's own `replaceState` moves while it boots.
A load that failed has none: Chromium's own error page commits at the URL that failed,
so a failed reload would otherwise be credited with the 200 of the page it replaced.
`titleBarStyle` is fixed at construction, so a changed answer applies on the next launch
rather than recreating a window that may have a turn streaming in it.

This is the same rule as the bridge: **feature-detected, never version-detected**. The
shell releases on a slow cadence and the app deploys continuously, so a shell that only
works against the newest frontend would be a shell that has to ship with every web
deploy. It also means a packaged shell run against an older deployed build is merely
plainer, never broken.

The console is asked the same question separately (`platformChrome`), because it is a
separate app on its own deploy cadence. **It shares the main window by default**, the way
"Usage & Plan" is a plain same-tab link in the browser and the console carries its own
way back; a second window would be the shell inventing a journey the product does not
have. The exception is the one combination that cannot work: a main window with no
titlebar showing a page that reserves no strip. That, and only that, gets a
standard-titlebar window of its own, and it stops the day the console reserves a strip.

`classify()` routes it: a navigation to the app from a console window returns
`app-window`, one to the console from the main window returns `platform-window` only in
that exception, and everything else is a plain `allow`. On `will-navigate` the
handoff also closes the window that was left behind (a top-level navigation out is the
user leaving it), while `setWindowOpenHandler` does not, because the page that opened it
is still using the one it has.

A `window.open` or `target=_blank` of our own origin asks `classifyWindowOpen` instead,
which sends it to the browser: the shell keeps no popup windows, and loading it here
would replace a streaming turn. The exception is the console opening the app while they
share the main window (the thread links on its usage page), which loads in the main
window, as it already does when the console has a window of its own.

## OAuth

Google refuses OAuth inside an embedded webview (`disallowed_useragent`, and a
user-agent spoof does not get around it), so the authorize URL has to open in the real
browser and the code has to come back over a loopback listener.

The shell does this **generically, without either SPA knowing**. It catches the
authorize navigation, swaps `redirect_to` for `http://127.0.0.1:<port>/callback`, and
when the code arrives sends the window that started the flow to the `redirect_to` it
originally asked for, with the code appended.

Three things are load-bearing:

- **The exchange happens in the renderer, never in main.** The PKCE verifier lives in
  that renderer's cookie jar and never leaves it. That is also what makes the loopback
  hop safe: anything else listening on the port gets a code it cannot redeem.
- **The port comes from the OS, not from us.** `listen(0)` on loopback, which is what
  RFC 8252 §7.3 tells an authorization server to expect from a native client, and the
  only way to be sure of getting a port at all. Sign-in's `redirect_to` is still matched
  against the Supabase Redirect URLs allowlist, so that entry has to be a port wildcard
  (`http://127.0.0.1:*/callback`) for any of this to work. **That wildcard is wider than
  a port.** Supabase's glob stops only at `.` and `/`, and `@` is neither, so the same
  entry also matches `http://127.0.0.1:x@capture/callback`, a string whose real host is
  `capture` rather than loopback. Reaching it takes a single-label hostname the user's own
  resolver answers for, which is a narrower opening than the glob's shape suggests but not a
  closed one: a search domain or a `hosts` entry can point `capture` at an address anywhere.
  The allowlist is simply not doing the work it looks like it is doing. Whatever a Supabase
  version happens to special-case about loopback literals is theirs to change, so do not
  lean on it: the binding that actually holds is the PKCE verifier, which lives in the
  renderer that minted it and cannot be redeemed anywhere else. A connector's authorization
  server matches loopback as a pattern and adds no constraint, and this deployment's backend
  accepts any loopback port at or above 1024 (`sanitize_loopback_redirect`).
- **The interception mechanic is indirect.** `setWindowOpenHandler` returns `deny`, so
  `window.open` returns `null` and the SPA takes its existing popup-blocked fallback: a
  same-tab navigation to the authorize URL, which `will-navigate` then catches. If an
  SPA ever drops that fallback, sign-in breaks here with no error on the web side.

`begin()` refuses any flow whose `redirect_to` is not one of our origins. Without that
the shell would drive its own window wherever a crafted authorize URL pointed, carrying
a code the user had just authorized.

## Links through the scheme

The registered scheme carries four kinds of link, told apart before any window is picked:

- **Everything else** lands on `/callback` on whichever of our origins the main window
  shows, with the whole query: a magic link or email confirmation, often clicked with
  the app closed. The route is fixed and nothing is read off the link but its query.
- **`<scheme>://integrations/login/<name>/callback`** is an integration login the console
  started in the app and the system browser finished. It becomes that path on the
  configured console origin, carrying only `code`, `state`, `error` and
  `error_description`, and is refused whole if a parameter repeats or the name is not
  `^[a-z][a-z0-9-]{0,31}$`. It lands back on the page that started it: the window whose
  page sent the user to the browser through `openExternal`, if it still shows the
  console, restored if minimized. Never focus or blur, which every switch between the
  app and the browser moves. With none, it goes where a navigation to the console
  goes, decided before the load, because `navigate` never passes through
  `will-navigate`. An edition with no console ignores it rather than handing a
  provider's code to `/callback`. Pages detect it as `integration-login` in the
  bridge's `capabilities`.
- **`<scheme>://integrations/install/<name>/callback`** is an integration install the
  console started in the app and the system browser finished, handled as a login is:
  that path on the console origin, the same refusals, the same window, and never
  `/callback`. It carries only `team`, `team_name` and `error`. Pages detect it as
  `integration-install`.
- **`<scheme>://console/<page>?…`** is a console page asked to be shown again: the
  console sent the user to a third party's page in the system browser, that page
  returned to a console page in the browser, which has no session, and that page
  hands its query back. Any link whose host reads as `console`, in any case or with a
  port, is settled here whatever its path, so one this build does not open exactly is
  dropped and never reaches `/callback`.
  The path must be exactly a key of `CONSOLE_PAGES` (today only `/desktop/return`,
  carrying `to`, `outcome` and `session_id`), and the URL is built from that key on
  the configured console origin, never from the link's path. Parameters the page does
  not carry are dropped; a repeated parameter, or a carried value outside
  `^[A-Za-z0-9_-]{1,255}$`, refuses the whole link. It lands as an integration return
  does: the window that sent the user out if it still shows the console, else any
  window showing it, else where a console navigation goes, restored and raised. An
  edition with no console ignores it, with a warning that names none of the link.
  Pages detect it as `console-return`.

Only an edition with a console advertises these three, and it advertises all of them
or none (main answers `consoleReturns: true` when there is a console; the OSS edition
has none, so it answers false and the list is empty).

A launch that a link started opens only that link's window. `attach` delivers the queued
link before the entry opens, and opening the entry as well would cover it, or load
sign-in over it when both are the console window.

## When the network does not answer

A remote-URL shell shows whatever the network gives it, and a failed load gives it
a blank window. That is the one failure mode this architecture buys, so it has a
real screen. Two paths reach it, and the second is easy to miss:

- `did-fail-load` on the main frame, for transport failures.
- `did-navigate` with a status of 500 or more. **A 502 from an edge is a
  successful load of an error body**, so it never fires `did-fail-load`, and
  without this the user is left looking at the CDN's page with no way back.

Anything under 500 belongs to the app: a 404 is a route, a 401 is a login.

The page reports four distinct situations, because the useful advice differs:
`offline` (this machine has no connection), `captive-portal` (the wifi wants a
sign-in), `unreachable` (we could not connect), and `server-error` (it answered,
badly). A server that answered is never reported as the user being offline, or
they spend the outage debugging their router.

**`net.isOnline()` alone cannot separate the first three.** Its `false` is
reliable; its `true` is documented as *inconclusive*, and a hotel or airport
portal is precisely the case that returns true. So when the machine looks online
and we still could not get there, `captive.js` probes
`http://cp.cloudflare.com/generate_204` and treats anything but a 204 as a
portal. **The cleartext scheme is the mechanism, not an oversight**: a portal can
only intercept plaintext, and over HTTPS the request fails in a way that is
indistinguishable from the host being down. A portal is only ever claimed on
positive evidence, and the probe is skipped for loopback or LAN targets so a
self-hoster never causes an outbound request. Cost is bounded to that one branch;
offline and 5xx still render instantly.

`ERR_NETWORK_CHANGED (-21)` gets one silent retry before any of this, because it
is what switching access point or raising a VPN looks like from here. The retry is
released only by a `did-finish-load`; anything softer is a reload loop on a dead
network.

It retries on a widening backoff and recovers on its own. Each attempt probes
first, so a failed retry does not flash the app's loading screen and bounce back.
An `online` listener fires an immediate retry the moment the OS reports the link
back, so reconnecting wifi recovers the window with no click.

**This covers load-time failure only.** `did-fail-load` needs a navigation, so a
drop that happens while the app is already running never reaches the shell: the
SPA's own fetches and SSE stream fail and it owns that experience. That is
deliberate — replacing a running app with a full-screen outage page on one failed
request would be worse than the disease — but it means "the network died mid-turn"
is a question about `web/`, not about this directory.

Its controls reach the main process, so they are kept off every remote page:
`preload.js` exposes `langalphaOutage` only when `location.protocol === 'file:'`,
and the main process independently refuses those channels unless the window
really is showing the page.

## Remote control of this Mac (Preview)

A mac bundle can carry `langalpha-helper`, a Rust binary built in `../helper`, which
lets the agent use this Mac as an SSH computer. It runs as an SMAppService agent, so
launchd keeps it up whether or not the app is open, and the app never talks to it
directly. They share one folder, `~/Library/Application Support/<app name>/helper`:
the app writes `pairing` there to ask for a pairing and reads `state` to see what the
helper is doing. All the app does to the process itself is register and unregister
its login item.

- **Packaging.** `scripts/build.mjs` adds the binary at `Contents/MacOS/langalpha-helper`
  and a plist generated per edition (`scripts/launch-agent.mjs`) at
  `Contents/Library/LaunchAgents/<label>.plist`, on mac builds only, through the
  `extraFiles` lines `scripts/helper-bundle.mjs` writes under `mac:`. With no helper
  built the app still packages and reports remote control as unsupported;
  `DESKTOP_REQUIRE_HELPER=1` makes that an error instead. The plist makes the job
  `Interactive`, as macOS does its own sshd: with no type launchd throttles the CPU
  and I/O of every command the agent runs.
- **Signing.** `scripts/sign-mac.mjs` signs the helper itself, then hands the rest
  of the bundle to electron-builder's signer with the helper skipped. First, because
  that signer reaches the app's main executable before the helper beside it, and
  codesign refuses to seal a bundle with unsigned code in `Contents/MacOS`. Itself,
  because electron-builder signs every nested Mach-O with `entitlementsInherit`, the
  JIT pair Electron's helpers need, and the helper takes logins from the network and
  runs no JIT, so it gets an empty set (`resources/entitlements.helper.plist`).
  `build.mjs` sets it as the signer on a build that carries the helper or a bundled
  backend, and renders everything under `mac:` once, from both.
- **Enable** checks the URL's shape (https, or http on loopback, and exactly the
  pairing script path), then asks in a native dialog naming the host and the macOS
  account, Cancel the default. A saas build first refuses any server but its
  `pairingOrigin` (`DESKTOP_PAIRING_ORIGIN`, the deployment's API host), so script
  running in the app cannot pair the Mac elsewhere; an OSS build takes any host,
  since its server may be reached through a tunnel or a Docker host name. A Mac already
  paired is replaced only after that yes, since another LangAlpha account signed in on
  the same macOS account may be using it. A running helper whose `state` says
  `supersedes=1` does it in place: it pairs on new keys, keeps the old pairing until the
  new one lands, then logs in to the gateway once with the old tunnel's key to say it was
  superseded, so the other account's computer says this Mac moved and offers Reconnect.
  A pairing that fails there leaves the old one as it was and answers `not_replaced`.
  Any other helper has the folder torn down first, and one that cannot be taken down
  whole is left running as it was. That branch serves only a helper from before
  `supersedes`, still running until it restarts for the update, or not running at all;
  it can go once every helper a supported app may find has the flag.
- **Never half on.** The login item stays registered only while the Mac is paired or
  waiting for approval in System Settings. A pairing that is refused, never picked up,
  or never ends is undone: the item unregistered and the folder deleted, so no helper
  runs unpaired with nothing to turn it off.
- **The menu** gets a Remote Control submenu with Pause and Resume while the Mac is
  paired or the helper's item is on, refreshed by a light poll.
- **Updates.** launchd keeps the helper it started running when an update swaps the
  bundle, so the helper watches its own executable: once another file is in its
  place, it exits when no command, forward or pairing is under way (an hour at
  most), and `KeepAlive` starts the new build. Nothing on the app's side.
- **IPC** answers only a window showing the app's own origin, like the connector
  OAuth channels.

## Menu bar and notifications

The shell keeps one more window than the user sees: the **menu bar window**, the app's
`/desktop/menubar` page with `windowRole` `menubar`. It is made once the app has been
reached (`reachedApp` on SaaS, a server on OSS), and lives until the app quits; closing it
hides it, and turning the icon off takes the icon away and leaves the window. It is
reloaded while hidden after a sleep or a locked screen, since its feed may have died
quietly and a web deploy since then left it on old code.

**The page has to call in before it is shown.** A server whose web build predates the
menu bar answers `/desktop/menubar` with its whole app, so the panel opens only once the
loaded page has called one of the menu bar's methods (it sends its tray lines as it
mounts, signed in or out); until then the main window stands in. A page that routes
itself away in place, or never calls in within 30s, is unloaded and tried again when the
main window next reaches the app.

It has two jobs. It is the panel the menu bar icon opens, and it is **the one page that
posts notifications**: it holds the user's feed whether or not any other window is open,
so a run that finishes with every window closed still reaches the user. The main window
never notifies. The page asks (`notify`, `clearNotifications`, `setAttention`,
`setTrayItems`); the shell filters by the user's desktop settings, holds back a
notification for a thread the focused main window is showing (`watching`), draws the
badge, and owns the click, because a click has to raise a window the page cannot reach.
Each notification's `id` is its thread's, so a background run's silent "running" note is
replaced in place by its result, and one held back takes down the note it would have
replaced.

Every one of those methods is refused by main unless the sender is the menu bar window
showing the app's own origin, in its main frame (`appSenderWindow` in `sender.js`): the
bridge is the same in every window, so the window decides, never the message. A link out
of the panel is an `open` for the main window, carried out by its page in place rather
than as a reload over a turn that may be streaming; anything else goes to the browser.
The panel never navigates away from its page. It keeps default background throttling and
is made with `paintWhenInitiallyHidden: false`, so the page can read its own visibility,
from its first load, and stop polling quotes while hidden.

**Menu commands** go the other way: File › New Thread, Go › Market and the rest are sent to
the main window's page (`onMenuCommand`), and each is enabled only once that page declared
it (`setMenuCommands`), so a build that predates a command shows it greyed rather than as
a key that does nothing. A new document declares afresh. `open { path }` is the exception:
the shell loads the path itself when the page does not take it. With no main window every
command is enabled and opens one, then waits for its page to declare the command.

**Desktop settings** (`desktop` in the store; shape and validation in `desktopSettings.js`)
are edited from the app's Settings and kept here, because the shell is what acts on each.
Read back field by field like every other key, so one bad value in a hand-edited file
costs that field. A change applies live and is sent to every window showing the app
(`shell:desktop-settings`). The view carries `support`, what this platform can do, so the
page offers only switches that work, and `shortcutError` when the accelerator would not
register.

- `menubar`: `enabled`, `items` (what sits beside the icon, in order: `{ kind: 'portfolio' }`
  or a quote `{ symbol, kind: 'index' | 'stock' }`, an index bare as `GSPC`; deduplicated,
  at most 8), `style` (`rotate` or `side`), `rotateSeconds` (2 to 30), `show` (`change`,
  `price` or `both`), `extendedHours`.
- `panel`: `sections` (all four of `market`, `portfolio`, `watchlist`, `agent` exactly once,
  in display order, each with `visible`), `tiles` (at most 4 quotes), `watchlistId` (null
  for the default list), `watchlistRows` (1 to 10).

Lists are repaired rather than refused: a bad or repeated entry goes, a section left out
is put back where the previous settings had it, a number out of range is clamped. A patch
is one level deep and replaces whole arrays (`items`, `sections`, `tiles`).

**The lines beside the icon.** The menu bar page formats each item as a line
(`NVDA ▼5.60%`, `Port ▲0.42%`) and hands the shell the list (`setTrayItems(items,
{ refreshMs })`); the shell lays them out (`layout` in `tray.js`, pure) by the settings'
`menubar.style` and `menubar.rotateSeconds`, never by anything the page sends. On macOS `rotate` is one slot turning
every `rotateSeconds`, each frame padded to the longest line and drawn in the monospaced
system font so the status item never changes width; `side` is the first three, two spaces
apart; one line is static. Rotation lives in the shell, and so does the clock that asks for
fresh lines (`shell:tray-refresh` every `refreshMs`, clamped to 10 s to 10 min, while there
are lines and the icon is shown), because the page is a hidden window, and a hidden page's
timers are throttled to once a minute: a four-second slot would sit for sixty. A refresh
that brings new lines keeps the slot and its timer running rather than restarting them.
Both timers stop when the icon is hidden and at quit. With nothing chosen in
`menubar.items` the icon stands alone whatever the page sends, and a settings change
re-lays or clears the lines at once rather than when the page next reports.

**The icon's menu** (a right-click on macOS and Windows; the icon's only menu on Linux):
Open LangAlpha; "In the menu bar" and each item, checked, a click removing it; "With more
than one" (Rotate, Side by side); "Panel sections" (a checkbox each, toggling `visible`);
Customize… (`open-settings` in the main window); Quit. Each click edits the settings
through the same path as a page's patch, so every window hears it. macOS and Windows build
the menu per click; Linux's sits on the icon and is rebuilt on every settings change and
whenever the lines change.

**Closing** (`lifecycle.js`). The hidden window means `window-all-closed` no longer fires
once it exists, so every user window's `closed` decides instead (`afterUserWindowClosed`),
by `traits.lastWindowClosed`: the app, the
account window, the server picker, and on Linux the Market Glance window, which is one of
the user's windows there. `activate` and `second-instance` look for the user's windows,
not all of them (counting the hidden one left a Dock click with nothing to open). A quit
marks itself before the windows close, including the Squirrel install path on macOS,
which closes them before `before-quit` fires, and that lets the menu bar window close.
It commits to nothing else: a page's `beforeunload` can still cancel the quit (the app
holds one over unsaved edits), which reaches the shell as `will-prevent-unload` on a user
window, and the shell then drops the flag and makes the menu bar window again. The icon,
the shortcut, the sign-in callback server and the update timer go only in `will-quit`,
once the quit is certain, each registered with `onWillQuit`. What must finish before the
process goes registers with `onDrain`, which holds `will-quit` until it settles and then
quits again; the bundled stack's stop is the one. The menu bar page itself can never hold
a quit up.

**The panel** is 380 wide and as tall as its page (`enablePreferredSizeMode`,
`preferred-size-changed`), from 260 up to the screen's height less a margin, re-anchored
to the icon as it grows; past that the page scrolls inside it, with no scrollbar drawn. The
math is `panelSize` and `panelPosition` in `panelGeometry.js`. On macOS it is a
non-activating panel, so opening it makes it key without activating the app: the app the
user was in stays frontmost, Esc reaches it, and a click anywhere else resigns key, the
`blur` that hides it.

The shell logs one `[notify]` line per notification decision and per click (kind, the
thread id's first 8 characters, shown or the reason; never a title or body) and one
`[menubar]` line per load of the menu bar page, with its status.

The platform facts behind this table are `traits()` in `platformTraits.js`. The modules
take that object rather than reading `process.platform`, so a test hands one another
platform's answers directly. Any platform other than macOS and Windows is treated as Linux.

| | macOS | Windows | Linux |
|---|---|---|---|
| Icon | template image; click opens the panel under it, right-click the menu | the app's colours; click opens the panel beside it, whichever edge the taskbar is on, right-click the menu | a menu only (the lines, Open Market Glance…, Open, the menu's settings, Quit), since an AppIndicator reports no clicks; Market Glance is the same page in an ordinary window |
| Menu bar text | beside the icon: one slot rotating every `rotateSeconds`, padded and monospaced, or the first three side by side | the tooltip: the app's name, then a line each, as many as fit in 127 characters | disabled items heading the icon's menu |
| Notifications | replaced by id, subtitle, Open / Mark as read | replaced by id, subtitle folded into the body, Open / Mark as read; the AppUserModelID is the bundle id, which the installer's shortcut carries | title, body and click; no replace, so a run's start is not shown |
| Badge | Dock badge, and a Dock menu of New Thread, Market Glance and up to five threads | an overlay dot on the taskbar button, and a jump list New Thread (`--command=new-thread`, read from the launch's own argv and in `second-instance`) | `setBadgeCount`, where the desktop shows one |
| Last window closed | keeps running | keeps running in the tray when the icon is on and `keepRunningInTray` (telling the user once), else quits | quits |
| Open at login | the login item; a login launch opens no window | the Run key with `--hidden` | an XDG autostart entry with `--hidden`, pointing at the AppImage when there is one |
| Global shortcut | toggles the panel | toggles the panel | X11 only: Wayland lets no app grab a key outside its windows |

A login launch opens only the menu bar presence, and only where there is one: an app
reached before, and on Windows and Linux the icon on, since without it a hidden launch is
an app the user cannot find. A dev run registers no login item, since it would register
Electron's own binary.

The tray icons are committed under `resources/` (`tray*.png`, `trayTemplate*.png`),
because the shell loads them at runtime; `pnpm run tray-icons` regenerates them from the
logo, and `electron-builder.yml` packs only those from that directory.

## The bundled stack (OSS, macOS and Linux)

**What it is.** `scripts/bundle-backend.mjs` builds a tree that `scripts/build.mjs` adds
as `extraResources`, at `Resources/backend/`:

| | |
|---|---|
| `python/` | python-build-standalone through `uv python install`, relocatable, with the locked dependencies installed into the prefix from `uv export --frozen --no-dev`. Not a venv: those record the absolute path they were made at |
| `app/` | the backend's source and everything it reads from its tree, from `git ls-files`, so an overlay or a `.env` cannot come along |
| `web/` | `web/` built with `VITE_HOST_MODE=oss` and no API base, served by the backend itself through `LANGALPHA_WEB_DIST`, so API and app are one origin |
| `pg/` | Postgres 18 from zonky's embedded-postgres-binaries on Maven Central, pinned by SHA-256. Its macOS archives are EnterpriseDB's own signed builds, thinned here to one architecture |
| `valkey/` | built from the pinned source tag, since the project publishes no macOS binaries |

Bytecode is compiled into the tree with `--invalidation-mode unchecked-hash`. A tree in a
signed bundle or an AppImage mount cannot be written, so a stale timestamped `.pyc` would
be recompiled on every launch by the server and by each MCP subprocess.

**One architecture per build.** The tree is native code for the machine that built it, so
a bundled build rewrites the targets to that one architecture and the workflow runs on a
runner of each. `build.mjs` then checks what actually reached the package
(`verifyPackedTrees`): the file count, and the architecture of every Mach-O and ELF in it,
read from their headers. It also drops the zip on a build with no feed, because only the
updater reads it; the config edits a tree needs are `bundleConfigEdits`.

**Native files are read in one place**, `scripts/native-files.mjs`: `readNative` parses a
file's headers once into `{ format, slices: [{ cpu, filetype, minOS }] }`, and `walkFiles`
lists a tree without following symlinks. The architecture and macOS checks, the thinning of
Postgres and the signing plan are each a filter over that.

**Windows has no bundle**, and neither does `DESKTOP_BUNDLE_BACKEND=0` or an ordinary
`pnpm run build` (`=1` forces one for that). A build with no tree shows the server form
alone, the way it always has.

**The order** is `prepare`, `init` on a first run, `database`, `cache`, `migrate`, `server`,
each named on the first-run page while it runs and in the failure if it fails, with the
tail of its log, **Reveal Logs** and **Try Again**. Logs are in `userData/logs`, rotated at
10 MB, three old files each. Data is in `userData/stack`. A login launch starts the stack
with no window, which a failure still opens, and the menu bar page loads only once the
stack is up.

- **The environment is built, never inherited** (`plan.backendEnv`). A shell's exported
  keys would otherwise reach a backend whose only keys are the ones saved in setup.
  `PYTHON_DOTENV_DISABLED` is set because `load_dotenv` walks up from the source file, and an
  app sitting in a project folder would pick up that project's `.env`.
- **The backend's port stays the same across launches** (`stackPort`). Postgres and Valkey
  take a new free port each launch, loopback only, but this one is the app's origin, and a
  browser keys everything it stores by origin.
- **The encryption key is generated once** into a 0600 file and never replaced, with a
  damaged file reported rather than regenerated: a new key would make every saved provider
  key unreadable.
- **Migrations run on every launch.** Some of them commit outside the run's transaction
  (`autocommit_block`), so a failure partway cannot roll back. When a tree's migrations
  differ from the last set that ran (`migrationsDigest`), the data folder is copied first,
  by clone where the filesystem can, and a failure puts it back and says so. A launch that
  finds that copy still there knows an upgrade was cut off and restores it before anything
  else.
- **Data a newer release migrated is refused** before any migration runs, with nothing
  changed: the database's `alembic_version` names a revision this tree's migrations do not
  have (`REVISION_CHECK` in `stack/plan.js`). Otherwise an older app opened on that data
  fails partway through `alembic upgrade` with "Can't locate revision". Checked only when the
  migrations changed since the last run, or for a database the app does not manage.
- **Another Postgres major's data folder is refused**, off `PG_VERSION`, before anything
  touches it. There is no upgrade path yet: moving the bundle to a new major needs one, and
  until it has one such a build cannot open an existing install's data.
- **`postmaster.pid`**: Postgres cleans up after itself only while the pid in the file is
  dead, and after a reboot that number can belong to anything. A process counts as ours
  only if it is a postgres on this very data folder, and anything else clears the file. A
  Homebrew postgres on the same machine is never signalled.

**No orphans, however the app goes.** Postgres and Valkey read nothing from stdin and macOS
has no parent-death signal, so each child gets a guard (`children.js`), a small shell on a
pipe only the app holds. When the pipe closes the app is gone, and the guard signals the
child, then kills it at its deadline. That covers a `kill -9` and a crash. A quit, an update's
included, closes the windows through their pages' unload guards and then waits at
`will-quit` (`lifecycle.onDrain`) for the server, Valkey, and Postgres with its fast shutdown,
in that order, since the server writes its running turns' resume records on the way down.
Restart Now on Linux stops the stack before it installs (`updater.restartToInstall`), because
electron-updater replaces and starts the AppImage there before the app quits; Squirrel.Mac
replaces the bundle only once the process has gone, so the drain covers it.

Choosing the stack is stored as `stackMode: 'bundled'` and takes effect once it is up, so a
switch from a server leaves the app on that server through the start and after a failure.
Its origin is never written to `serverUrl`; builds that did are cleared of it on launch.

**Only the app is the owner.** The bundled server takes a request with no token for its one
user only when it carries the launch's secret, which `stack/owner.js` adds to every session's
requests to that exact origin through `webRequest.onBeforeSendHeaders`, so a container on the
Mac, which can reach the server and name any Host, is refused. A session holds one such
listener and a second replaces the first, which would leave the app refused: add any other
request header in that listener.

**Staying up.** A child that dies is restarted on a widening delay and on the same port,
since the others hold connections to it, and after five deaths in two minutes the first-run
page says which one it was. On wake, a server that stopped answering is restarted.

**Built-in MCP servers.** They declare `uv run python plugins/...` for a checkout, and this
tree has no uv and no project for it to find. `LANGALPHA_STDIO_OWN_PYTHON` makes the backend
run those on its own interpreter (`_stdio_launch` in `mcp_registry.py`), for this spawn only.
No uv ships, because nothing on this side needs one: third-party `uvx` and `npx` servers run
on the computer, where each provider supplies those tools, including the `local` one.

**Signing** (`scripts/sign-mac.mjs`). electron-builder's signer would sign every file it
calls binary, which in this tree is every `.pyc` and font, so the tree is excluded from its
walk and only Mach-O files, found by their headers (`readNative`), are signed here with hardened runtime and
a timestamp. Python's executables get `resources/entitlements.python.plist`, the JIT pair
wasmtime and ffi closures need. Postgres and Valkey get the empty
`entitlements.native.plist`. Nothing gets `disable-library-validation`: every library is
signed with the app's identity.

**Minimum macOS** is `MACOS_MIN` in `scripts/bundle-backend.mjs`, 13.5: Postgres asks for
13.0 and the Node inside Playwright's wheel for 13.5. Left to defaults, a compiler targets
the macOS it runs on and uv picks the newest wheel that macOS takes, so a tree built on a
current Mac refuses every older one, and nothing about that shows on the machine that built
it. So Valkey is compiled with `MACOSX_DEPLOYMENT_TARGET`, wheels are chosen with
`--python-platform`, the app declares it as `LSMinimumSystemVersion`, and `checkTreeMacOS`
fails the build on any Mach-O in the tree that asks for more.

**The release launch check** (`src/stack/smoke.js`). With `LANGALPHA_DESKTOP_SMOKE=1` the
packaged app opens no window: it starts the bundled stack as a first launch does, asks for
the setup status and the web app, stops, and exits 0 or 1. It is the only test that runs
what the tree carries, so the release workflow runs it on every package it builds, through
`scripts/smoke-packaged.sh [profile-dir]`: that finds the app for the platform, gives it a
`--user-data-dir` of its own, and skips with exit 0 a package that carries no stack
(Windows, a thin build). A build with no stack exits 2 in this mode rather than open the
server picker and wait.

**Linux.** Everything is read-only in an AppImage's mount and its path changes each launch,
so nothing persists a path into the tree and data stays in userData. Valkey links against
the build machine's glibc, which is why the workflow builds on the older `ubuntu-22.04`.
Postgres refuses to run as root, which reaches the user as that step's failure.

**Running one from a checkout:**

```bash
node scripts/bundle-backend.mjs --out /some/dir/backend
LANGALPHA_DESKTOP_BACKEND_TREE=/some/dir/backend pnpm exec electron . --user-data-dir=/some/dir/profile
```

Keep the tree outside a checkout that has a `.env` above it, and use a profile of its own.
Three more overrides are for testing: `LANGALPHA_DESKTOP_PG_URL` uses a Postgres you already
run, `LANGALPHA_DESKTOP_PG_PREFIX` with `LANGALPHA_DESKTOP_PG_MAJOR` takes the binaries from
elsewhere, and `LANGALPHA_DESKTOP_VALKEY_BIN` does the same for Valkey.

## Updates

A build learns where to look in one of two ways, and needs exactly one:

- `app-update.yml`, written into the package by electron-builder when a `publish`
  target existed at build time. This is the normal path.
- `updateFeed` in the desktop config, which overrides it.

`electron-builder.yml` carries `publish: null` because where a build looks for
updates is deployment configuration, not source: the feed arrives as
`DESKTOP_UPDATE_FEED` and `scripts/build.mjs` rewrites the config for that one
build. **Supplying a feed is what makes electron-builder emit an update manifest
at all.** Without it there is no manifest, and a build with no manifest installs
perfectly and then never updates, which is why the build script fails hard when a
feed was set but no manifest came out.

**The manifest is named for the update channel, which electron-builder reads off the
version's prerelease tag** (`appInfo.channel`): `0.2.0` writes `latest-mac.yml`,
`0.2.0-rc.1` writes `rc-mac.yml`. The guard derives that same name instead of globbing
`latest*`, and so does every step of the release pipeline that collects or uploads a
manifest. Getting this wrong is not a corner case: shipping a prerelease through the real
pipeline is how that pipeline is meant to be rehearsed, and the first run to do it failed
all three platforms on a guard demanding a filename the build had no reason to write.

Distribution has two halves: the GitHub release is where a person downloads the
app, and the feed host is the only thing an installed app reads. Publishing one
without the other is a silent no-op.

A bundled OSS build updates like any other, through `DESKTOP_UPDATE_FEED`, with the zip
at about the size of the installer and migrations run by the launch that follows.

The workflow in this repository (`.github/workflows/desktop-release.yml`) builds
and publishes the OSS edition. What it signs, notarizes and publishes to comes from
its `desktop-release` environment, whose secrets and variables the workflow's header
lists, so the environment's branch rule and required review stand between any other
workflow and those credentials. Each part is optional: without a certificate the
builds are unsigned, without a feed they never update themselves, and the notes it
writes say which. That is what keeps it useful to a fork, which sets whichever it has.
The hosted edition is released separately from this same source, so a change here
reaches both, and `scripts/` is shared: nothing under it is dead code merely because
this repository's own workflow does not call it.

**The two editions cannot share a feed.** electron-builder names a manifest for
the channel and nothing else, so two editions publishing to one feed root both
write `latest-mac.yml` and the second overwrites the first. Whichever wins then
hands its own archives to both, and a self-hosted install updates itself into the
hosted app: same version, same filename shape, different product. Each edition
needs a feed root of its own, and the `extraMetadata.name` in
`electron-builder.yml` for the same reason one layer down, since electron-updater
names its download cache from the package name.

Applying a macOS update needs a Developer ID signature, and Squirrel.Mac checks
the incoming build's against the running one's: a Team ID that differs is
refused. So an unsigned install can never be updated into a signed one, and
changing enrollment type later strands every install already out there.
Everything up to the signature is verified: the packaged app fetches its
baked-in feed, recognises a newer version, downloads it and verifies the sha512.

**Signing and notarization are two switches and a release needs both.** The
certificate is what makes `codesign --verify` pass, and it says nothing about
whether Apple has ever seen the bundle; the notarization ticket is what
Gatekeeper asks for on first launch, so a signed build without one is refused
exactly like an unsigned one. `electron-builder.yml` commits `identity: null`
and `notarize: false`, because someone with neither credential still has to be
able to run `dist`, and `scripts/build.mjs` replaces each line when the
credentials for it arrive: a certificate in `CSC_LINK` or `CSC_NAME` for the
first, and a complete notarytool authentication for the second (an Apple ID with
an app-specific password, an App Store Connect API key, or a keychain profile).
A partial set counts as none, since `notarize: true` with nothing to authenticate
with packages for twenty minutes and then fails at the submission.

`CSC_NAME` takes the certificate's name **without** the `Developer ID
Application:` prefix that `security find-identity` prints, which is the whole
string a person naturally copies. electron-builder picks the type itself and
rejects the prefixed form, and it does so after unpacking Electron rather than
at startup, so the mistake costs a few minutes each time:

```bash
CSC_NAME="Your Company (TEAMID)" pnpm run dist
```

The check after the build splits the same way. `codesign --verify` passes on a
bundle Apple has never seen, so a notarized build is also put through
`xcrun stapler validate`, and the missing staple is the failure that otherwise
reaches a user as an app that will not open.

**The disk image needs the same two gates as the app it carries.**
electron-builder notarizes the `.app` and then builds the DMG around it, so the
image itself is never submitted; Gatekeeper assesses a downloaded image on its
own signature, and refuses an unsigned one however well notarized its contents
are. `dmg.sign: true` covers the first gate (it is off by default), and
`scripts/build.mjs` submits and staples each image after electron-builder
returns for the second. Only the first-time install was ever affected: the
updater reads the `.zip`, which carries the stapled app, so auto-update looked
healthy while every fresh download was refused.

An image is assessed with a different question than an app: `-t open --context
context:primary-signature` rather than `-t exec`. Ask the wrong one and a
correctly notarized DMG reports a verdict that does not mean what it looks
like.

## Conventions

- **Classify by origin, not by path.** A path like `/account` only identifies the
  account console in dev, where a proxy stitches both SPAs onto one host; in production
  they are separate origins.
- **The window background must equal `--color-bg-page`.** Electron cannot remove the
  gap between frame and paint during a live resize, only match its colour, which is why
  the page pushes its resolved theme down through `setTheme`. If `tokens.css`
  `--background` moves, move `src/theme.js` with it.
- **The bridge is feature-detected, never version-detected.** The shell updates on a
  slow cadence while the web app deploys continuously, so a new web build must never
  require a new shell. Behaviour with no method to look for is a word in
  `capabilities`.
- **Nothing auth-related is exposed to the page.** The shell drives sign-in itself, so
  neither SPA needs, or gets, a way to.
- **A sandboxed preload cannot require arbitrary files.** Its resolver is limited to a
  few Electron built-ins, and a throw there takes the whole bridge down silently. So
  the version comes from main, with the rest of what a window tells its page.
- **What a window tells its page is asked of main, never put on the command line.**
  The preload's `shell:window-facts` (sync IPC) is answered from the sender's
  WebContents (`recordWindowFacts` in `pageFacts.js`): version, frame, scheme, console
  returns. `additionalArguments` belong to the renderer process, and a page can land
  in a process launched without its window's; shells through 0.3.0 read every switch
  as absent there, so the page was told `native` in a frameless window and drew its
  logo under the window buttons. `index.html` reads version `0.0.0` as that case.
- **Packaging resources live in `resources/`, not `build/`.** The repo-root
  `.gitignore` has a Python `build/` rule that silently swallows it.

## Status

macOS is built and verified end to end, including the outage page and the update
path. Windows and Linux targets are declared in `electron-builder.yml` and run in
CI, but are not signed and have not been exercised by hand. The bundled stack's own
status: its signed and notarized form, Intel Macs and Linux have not yet been run.

The menu bar, notifications and menu commands are written Mac first; their Windows and
Linux halves follow the table above and are covered by the suite's pure parts (panel
placement on every taskbar edge, the notification decision, the autostart entry), not by
a run on either system.

The signing and notarization switches above are wired and exercised on CI: both
`.app` bundles and both disk images come back notarized and stapled, verified
with `stapler validate` and `spctl` rather than read off the build log. Windows
builds are still unsigned, so SmartScreen warns on first run.

Both editions ship a feed and update themselves, each from a root of its own. An
install that predates its edition's feed carries no `app-update.yml` and so never
polls: it takes one manual update to reach the feed, and everything after that is
automatic.
