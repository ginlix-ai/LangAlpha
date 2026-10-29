'use strict'

// Injected into the remote page so the hosted web app can feature-detect a
// desktop shell without a separate build. The surface stays narrow and
// explicitly enumerated: the renderer is loading code from the network, and
// anything exposed here is exposed to whatever that page becomes.
//
// No way to drive OUR sign-in is exposed on purpose: the shell intercepts that
// authorize navigation itself, so neither app needs one. `beginMcpOAuth` is the
// deliberate exception and is a different thing — a third-party connector, and a
// local URL handed out rather than any credential taken in.
const { contextBridge, ipcRenderer } = require('electron')

// A sandboxed preload has a restricted module resolver: it can require a few
// Electron built-ins and nothing else, so reading package.json here throws and
// takes the whole bridge down with it. Main passes the version as a switch
// instead (webPreferences.additionalArguments).
const flag = (name) => {
  const prefix = `--langalpha-${name}=`
  const found = process.argv.find((a) => a.startsWith(prefix))
  return found ? found.slice(prefix.length) : ''
}

contextBridge.exposeInMainWorld('langalphaDesktop', {
  version: flag('shell-version') || '0.0.0',
  platform: process.platform,

  /**
   * Whether this window's titlebar is hidden, and so whether the page owes it a
   * strip for the window buttons to float over.
   *
   * Not derivable from `platform`: the frame is per window and per install, not
   * per OS, and it is decided in the main process before this page exists. The
   * page guessing from macOS is what put a reserved strip under a titlebar that
   * was really there.
   */
  windowChrome: flag('window-chrome') === 'hidden' ? 'hidden' : 'native',

  /**
   * The custom scheme this edition registers, so a link that has to come back
   * through the OS can be addressed to THIS build.
   *
   * The two editions answer on different schemes and install side by side, so a
   * page that guessed would hand a hosted link to the self-hosted build, or the
   * reverse. Empty on a shell too old to pass it, which reads as "no handoff"
   * rather than as a wrong one. Added in shell 0.2.1.
   */
  scheme: flag('shell-scheme'),

  /**
   * What this shell does that a page cannot detect by looking for a method,
   * because it is behaviour rather than a function to call. Ask with
   * `includes`, never with the version; a shell older than the list has none,
   * which reads as "can do none of these".
   *
   * - `integration-login`: a `<scheme>://integrations/login/<name>/callback`
   *   link opens the console's callback page for that login in this app, so a
   *   browser that finished the login can hand the provider's answer back
   *   rather than redeem it itself.
   * - `integration-install`: the same for an install, through
   *   `<scheme>://integrations/install/<name>/callback`, so a browser that
   *   finished the install can hand its result back to the page that started
   *   it.
   * - `console-return`: the shell will load a console page a link from the
   *   browser names, on the console's origin, in the console's window. The
   *   link is `<scheme>://console/<page>?…` and the page one of a fixed few, so
   *   a browser that landed on a console page with no session can hand it back
   *   to the app, which has one.
   *
   *   All three present only in an edition that has a console to open them on;
   *   one with no platform origin drops those links, so it leaves all three out.
   *
   * A list fixed when the window opens, and deliberately not a channel: nothing
   * on it hands the page a way to drive anything.
   */
  capabilities: Object.freeze(
    flag('console-returns') === '1' ? ['integration-login', 'integration-install', 'console-return'] : [],
  ),

  /**
   * Tell the shell which theme the page settled on, so the window background
   * matches the page. A mismatch shows as a coloured band during a live resize,
   * because the frame outruns the paint and the window colour fills the gap.
   */
  setTheme: (value) => ipcRenderer.sendSync('shell:set-theme', value),

  /** Open a URL in the user's real browser. */
  openExternal: (url) => ipcRenderer.invoke('shell:open-external', url),

  /**
   * Save this page as a PDF file, replacing the print dialog a browser has to
   * fall back to. Answers `{ saved }`, `{ canceled }` or `{ error }` rather
   * than rejecting, so a caller can tell a refusal apart from a shell too old
   * to know the channel and keep its browser path for the second case.
   */
  savePdf: (options) => ipcRenderer.invoke('shell:save-pdf', options),

  /**
   * Ask the shell to receive a connector's OAuth code on a loopback listener,
   * and answer with the redirect_uri to mint the flow against. `returnUrl` is
   * the backend callback the shell drives this window to once the code lands.
   *
   * Answers `{ redirectUri, flowId }`, or null when the shell has no listener to
   * offer, which is not an error: the caller starts the flow the way a browser
   * does and is no worse off. The `flowId` names this flow in the two calls
   * below, neither of which will act on a flow the caller did not arm. Added in
   * shell 0.1.3.
   */
  beginMcpOAuth: (returnUrl) => ipcRenderer.invoke('shell:mcp-oauth-begin', returnUrl),

  /**
   * Hand back the `state` the backend minted, so the shell can tell this flow's
   * callback from anything else that reaches the port. Until this is called the
   * armed flow accepts nothing. Added in shell 0.1.3, alongside `beginMcpOAuth`.
   */
  bindMcpOAuth: (flowId, state) => ipcRenderer.invoke('shell:mcp-oauth-bind', flowId, state),

  /**
   * Say that the flow just armed is not happening, because the backend refused
   * to mint one. Frees the listener instead of leaving it to time out. Added in
   * shell 0.1.3, alongside `beginMcpOAuth`.
   */
  cancelMcpOAuth: (flowId) => ipcRenderer.invoke('shell:mcp-oauth-cancel', flowId),

  /**
   * Remote control of this Mac (Preview): a helper the bundle carries that lets
   * the agent use this Mac as an SSH computer, run by macOS whether or not the
   * app is open. Present on every platform, answering `supported: false` with a
   * reason where it cannot work, so a page can tell "not here" from a shell too
   * old to know it. Only the app's own pages are answered; anything else gets
   * null.
   */
  getRemoteControl: () => ipcRenderer.invoke('shell:remote-control-status'),

  /**
   * Pair this Mac with the pairing at `scriptUrl` and turn the helper on.
   * Nothing happens until the user agrees in a native dialog naming the server
   * and the account, which is why a page cannot do this on its own. A Mac
   * already paired, perhaps for another account signed in on it, is disconnected
   * only after that same yes, so a page never turns one off to start afresh.
   * Answers an outcome rather than rejecting, `{ ok, error?, message?, status }`.
   */
  enableRemoteControl: (scriptUrl) => ipcRenderer.invoke('shell:remote-control-enable', scriptUrl),

  /** Turn the helper off and delete its folder, keys and tunnel with it. Safe to repeat. */
  disableRemoteControl: () => ipcRenderer.invoke('shell:remote-control-disable'),
})

// The outage page is a local file loaded into this same window, so it shares
// this preload. Gating on the protocol keeps its controls off every remote page
// instead of handing the loaded web app a way to drive the window's recovery.
// The main process independently refuses these unless the window really is
// showing the outage page.
if (location.protocol === 'file:') {
  contextBridge.exposeInMainWorld('langalphaOutage', {
    retry: () => ipcRenderer.invoke('outage:retry'),
    openExternal: () => ipcRenderer.invoke('outage:open-external'),
    changeServer: () => ipcRenderer.invoke('outage:change-server'),
  })
}
