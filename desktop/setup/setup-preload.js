'use strict'

// Separate from the remote-page preload on purpose. `server:use` repoints the
// whole app, so it is reachable only from this local page, never from whatever
// the loaded web app happens to be. The stack's channels start and stop the
// backend and sit here for the same reason.
const { contextBridge, ipcRenderer } = require('electron')

contextBridge.exposeInMainWorld('langalphaSetup', {
  probe: (url) => ipcRenderer.invoke('server:probe', url),
  use: (url) => ipcRenderer.invoke('server:use', url),
  current: () => ipcRenderer.invoke('server:current'),
  stackStatus: () => ipcRenderer.invoke('stack:status'),
  useStack: () => ipcRenderer.invoke('stack:use'),
  revealLogs: () => ipcRenderer.invoke('stack:reveal-logs'),
  copyHelpPrompt: () => ipcRenderer.invoke('stack:copy-help-prompt'),
  onStackChange: (listener) => {
    ipcRenderer.on('stack:changed', (_event, view) => listener(view))
  },
})
