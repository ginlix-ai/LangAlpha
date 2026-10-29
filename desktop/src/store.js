'use strict'

const fs = require('node:fs')
const path = require('node:path')
const { app } = require('electron')

const config = require('./config')

// Deliberately a plain JSON file rather than a dependency. The shell persists a
// handful of scalars; anything that can corrupt on write is worse than re-asking.
const DEFAULTS = {
  /** OSS edition only: where the user's own stack lives. */
  serverUrl: null,
  /**
   * SaaS edition: whether this install has ever reached the app. Purely a
   * shortcut, letting a launch skip the trip through platform sign-in that would
   * otherwise land the user in the app anyway. It is deliberately NOT the
   * answer to "does this account need setting up": that is a property of the
   * account, the platform owns it, and this file is per install. The two come
   * apart the moment someone installs on a second machine, so answering the
   * account question from install state gets it wrong for the same person
   * twice over.
   */
  reachedApp: false,
  /**
   * Whether the web build this install loads reserves a strip for the macOS
   * window buttons. Hiding the titlebar is only safe once it does: a build that
   * does not leaves the buttons on top of its own header and, with no drag
   * region anywhere, a window the user cannot move. Read from the page's own
   * declaration on every app load, because the shell releases on a slow cadence
   * and the app deploys continuously.
   *
   * The default is what to assume before any page has answered, and the two
   * editions can honestly assume different things. OSS points at whatever stack
   * the user types in, which may be any build of any age, so it assumes the
   * worst and opens framed. SaaS bakes both origins in at package time and both
   * of those apps ship the declaration, so assuming it would only be wrong if
   * the deploy were rolled back years — and being wrong here costs a fresh
   * install a titlebar for one launch, which is what the false default costs
   * every SaaS install today.
   */
  appChrome: config.isSaas,
  /**
   * The same question asked of the account console, which is a separate app on
   * its own deploy cadence. Answering it separately is what lets the console
   * share the main window the moment it starts reserving the strip, and keeps it
   * in a window of its own until then.
   *
   * It moves with `appChrome` and not on its own: `platformFitsMainWindow` is
   * `!appChrome || platformChrome`, so raising appChrome alone would deny the
   * console the main window and hand a first-run customer a second window
   * titled "Account" instead of the app.
   */
  platformChrome: config.isSaas,
  /** Last resolved theme, so the first frame is not a flash of the wrong ground. */
  theme: 'dark',
  /**
   * Notify mode only: the newest version this install has already been told
   * about. The update check runs on a timer for the life of the process, so
   * without this the same dialog reappears every few hours until the user
   * upgrades, which teaches them to dismiss it without reading.
   */
  updateNotifiedVersion: null,
}

let cache = null

function file() {
  return path.join(app.getPath('userData'), 'settings.json')
}

function load() {
  if (cache) return cache
  let parsed = null
  try {
    parsed = JSON.parse(fs.readFileSync(file(), 'utf8'))
  } catch {
    // Missing or corrupt both mean "no usable settings", and the defaults are
    // always safe: the OSS edition re-asks for a server, SaaS re-runs onboarding.
  }
  cache = { ...DEFAULTS, ...(parsed && typeof parsed === 'object' ? parsed : {}) }

  // Every key the shell reads is checked against the shape of its default,
  // because this is a plain JSON file on disk that a user can open and edit and
  // a spread trusts whatever it finds. The flags are the reason: `reachedApp`
  // as the *string* "false" is truthy, so one hand-edit skips SaaS sign-in
  // permanently, and the same typo on `appChrome` hides the titlebar of a build
  // that never reserved a strip, leaving a window with nowhere to drag it by.
  // Unknown keys are left alone: they have no reader here, and dropping them
  // would make a downgrade quietly discard a newer shell's settings.
  for (const [key, fallback] of Object.entries(DEFAULTS)) {
    const value = cache[key]
    const wanted = fallback === null ? 'string' : typeof fallback
    if (value === null && fallback === null) continue
    if (typeof value !== wanted) cache[key] = fallback
  }
  return cache
}

function get(key) {
  return load()[key]
}

function set(key, value) {
  const next = { ...load(), [key]: value }
  cache = next
  try {
    fs.mkdirSync(path.dirname(file()), { recursive: true })
    // Written beside the target and renamed over it, because `writeFileSync`
    // truncates first: a crash or a power loss between the truncate and the
    // write leaves valid-length garbage that `load` parses, fails, and answers
    // with the defaults. Silently, and the defaults are a factory reset — the
    // OSS install forgets its server and the SaaS one forgets it ever signed
    // in. A rename is atomic on the same filesystem, so a reader sees the old
    // file or the new one and never a half of either.
    const tmp = `${file()}.tmp`
    fs.writeFileSync(tmp, JSON.stringify(next, null, 2))
    fs.renameSync(tmp, file())
  } catch (err) {
    // A settings write that fails must not take the session down; the value is
    // live in memory and the worst case is re-asking on next launch.
    console.error(`[store] could not persist settings: ${err.message}`)
  }
}

/**
 * Put a key back to what a fresh install would have had.
 *
 * Exists for the chrome flags, which describe the app origin rather than the
 * install: they are learned from a declaration the loaded page makes, so when
 * the origin changes the learned answer is about somewhere else. Setting a
 * literal at the call site would fork the edition defaults into two files.
 */
function reset(key) {
  set(key, DEFAULTS[key])
}

module.exports = { get, set, reset }
