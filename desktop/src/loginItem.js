'use strict'

const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { app } = require('electron')

const config = require('./config')

// Open at login. macOS and Windows have an API for it; Linux has the XDG
// autostart directory, which every mainstream desktop reads.
//
// A launch at login comes up as the menu bar icon only: on macOS the OS says it
// was a login launch, elsewhere the entry carries `--hidden`.
//
// A dev run is Electron's own binary, so registering it would open a bare
// Electron at every login; it only logs.

const HIDDEN_FLAG = '--hidden'

/**
 * A value of type string in a desktop entry: a backslash doubled, and the
 * control characters a line cannot hold written as their escapes.
 */
function entryString(value) {
  return String(value)
    .replace(/\\/g, '\\\\')
    .replace(/\n/g, '\\n')
    .replace(/\t/g, '\\t')
    .replace(/\r/g, '\\r')
}

/**
 * One argument of an `Exec` key, per the Desktop Entry spec, in its three
 * layers: a literal `%` doubled so it is not read as a field code; the argument
 * double-quoted with `"`, `` ` ``, `$` and `\` each backslash-escaped inside;
 * then the whole value escaped as a string, which doubles every backslash again.
 * So a backslash in the path is four in the file and a `$` is `\\$`, as the spec
 * spells out. Always quoted, since an AppImage path or a home directory can
 * carry a space or any other reserved character.
 */
function execArgument(arg) {
  const quoted = `"${String(arg).replace(/%/g, '%%').replace(/["`$\\]/g, '\\$&')}"`
  return entryString(quoted)
}

/** The XDG autostart entry; `--hidden` is what tells the launch to come up as the tray icon only. */
function autostartEntry({ appName, execPath }) {
  return [
    '[Desktop Entry]',
    'Type=Application',
    `Name=${entryString(appName)}`,
    `Exec=${execArgument(execPath)} ${HIDDEN_FLAG}`,
    'Terminal=false',
    'X-GNOME-Autostart-enabled=true',
    '',
  ].join('\n')
}

/**
 * Whether an autostart entry still launches. A desktop turns one off in place
 * rather than deleting it: `Hidden=true` is the spec's "treat as deleted", and
 * GNOME writes `X-GNOME-Autostart-enabled=false`.
 */
function autostartEnabled(text) {
  let group = null
  for (const raw of String(text).split(/\r?\n/)) {
    const line = raw.trim()
    if (line.startsWith('[')) {
      group = line
      continue
    }
    if (group !== '[Desktop Entry]') continue
    const eq = line.indexOf('=')
    if (eq < 0) continue
    const key = line.slice(0, eq).trim()
    const value = line.slice(eq + 1).trim()
    if (key === 'Hidden' && value === 'true') return false
    if (key === 'X-GNOME-Autostart-enabled' && value === 'false') return false
  }
  return true
}

/** The autostart file's name: the app name, lowercased and hyphenated. */
function autostartFileName(appName) {
  return `${String(appName).toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '')}.desktop`
}

function autostartFile() {
  const base = process.env.XDG_CONFIG_HOME || path.join(os.homedir(), '.config')
  return path.join(base, 'autostart', autostartFileName(config.appName))
}

/** Where the launcher really lives: inside an AppImage, `execPath` is a mount that is gone by the next login. */
function launcherPath() {
  return process.env.APPIMAGE || process.execPath
}

/** `traits.loginItem` (platformTraits.js) picks the mechanism. */
function createLoginItem(traits) {
  function set(enabled) {
    if (!app.isPackaged) {
      console.log(`[shell] open at login ${enabled ? 'on' : 'off'}: not registered for an unpackaged run`)
      return
    }
    if (traits.loginItem === 'login-items') {
      app.setLoginItemSettings({ openAtLogin: enabled })
    } else if (traits.loginItem === 'run-key') {
      app.setLoginItemSettings({ openAtLogin: enabled, args: [HIDDEN_FLAG] })
    } else {
      const file = autostartFile()
      try {
        if (enabled) {
          fs.mkdirSync(path.dirname(file), { recursive: true })
          fs.writeFileSync(file, autostartEntry({ appName: config.appName, execPath: launcherPath() }))
        } else {
          fs.rmSync(file, { force: true })
        }
      } catch (err) {
        console.warn(`[shell] could not ${enabled ? 'write' : 'remove'} the autostart entry: ${err.code || err.message}`)
      }
    }
  }

  /**
   * What the OS says, which is the truth once the user can change it from System
   * Settings or by deleting the file. Null for a dev run, where nothing was ever
   * registered and the stored choice is all there is.
   */
  function isEnabled() {
    if (!app.isPackaged) return null
    try {
      if (traits.loginItem === 'login-items') return Boolean(app.getLoginItemSettings().openAtLogin)
      if (traits.loginItem === 'run-key') {
        // `openAtLogin` reads only the Run key, which stays when the user turns
        // the app off in Startup Apps; that switch is the approval this reads.
        const s = app.getLoginItemSettings({ args: [HIDDEN_FLAG] })
        return Boolean(s.openAtLogin && s.executableWillLaunchAtLogin)
      }
      const file = autostartFile()
      return fs.existsSync(file) && autostartEnabled(fs.readFileSync(file, 'utf8'))
    } catch {
      return null
    }
  }

  /** Whether this launch was the OS opening the app at login. */
  function launchedAtLogin(argv = process.argv) {
    if (argv.includes(HIDDEN_FLAG)) return true
    if (traits.loginItem !== 'login-items') return false
    try {
      return Boolean(app.getLoginItemSettings().wasOpenedAtLogin)
    } catch {
      return false
    }
  }

  return { set, isEnabled, launchedAtLogin }
}

module.exports = {
  createLoginItem, autostartEntry, autostartEnabled, autostartFileName, execArgument, autostartFile, HIDDEN_FLAG,
}
