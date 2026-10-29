'use strict'

// Separate from the remote-page preload on purpose. `server:use` repoints the
// whole app, so it is reachable only from this local page, never from whatever
// the loaded web app happens to be.
const { contextBridge, ipcRenderer } = require('electron')

contextBridge.exposeInMainWorld('langalphaSetup', {
  probe: (url) => ipcRenderer.invoke('server:probe', url),
  use: (url) => ipcRenderer.invoke('server:use', url),
  current: () => ipcRenderer.invoke('server:current'),
})
