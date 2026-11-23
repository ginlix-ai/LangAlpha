'use strict'

const fs = require('node:fs')
const path = require('node:path')
const { app } = require('electron')

const store = require('../store')
const { createSupervisor } = require('./supervisor')

/**
 * Where the package's bundled tree is, or null on a build that has none. Windows
 * and a build made with DESKTOP_BUNDLE_BACKEND=0 land there, and those keep the
 * server picker exactly as it was.
 *
 * The override is how a checkout runs the shell against a tree it built itself,
 * and how a packaged build is pointed at a database for a test.
 */
function treeRoot() {
  if (process.platform === 'win32') return null
  const root = process.env.LANGALPHA_DESKTOP_BACKEND_TREE
    || (app.isPackaged ? path.join(process.resourcesPath, 'backend') : null)
  return root && fs.existsSync(path.join(root, 'manifest.json')) ? root : null
}

/** The supervisor, wired to this install's userData and stored port. Null with no bundled tree. */
function createStack() {
  const root = treeRoot()
  if (!root) return null
  return createSupervisor({
    treeRoot: root,
    userData: app.getPath('userData'),
    loadPort: () => store.get('stackPort') || null,
    savePort: (port) => store.set('stackPort', port),
  })
}

module.exports = { createStack }
