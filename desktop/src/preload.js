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
