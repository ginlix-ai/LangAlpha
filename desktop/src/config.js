'use strict'

const fs = require('node:fs')
const path = require('node:path')

// `default.json` is the OSS edition pointing at localhost, and it is the file
// that ships in this repo. A packaged SaaS build carries `build.json` beside it,
// written by scripts/write-build-config.mjs from the environment at package
// time. Where a build points is deployment configuration, not source, so the
// origins arrive as build inputs the same way a .env does.
const CONFIG_DIR = path.join(__dirname, '..', 'config')

function readJson(file) {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'))
  } catch (err) {
    if (err.code === 'ENOENT') return null
    throw new Error(`desktop config ${path.basename(file)} is unreadable: ${err.message}`)
  }
}

const packaged = readJson(path.join(CONFIG_DIR, 'build.json')) || {}
const merged = {
  ...readJson(path.join(CONFIG_DIR, 'default.json')),
  ...packaged,
}

const EDITIONS = ['saas', 'oss']
if (!EDITIONS.includes(merged.edition)) {
  throw new Error(`desktop config: edition must be one of ${EDITIONS.join('|')}, got ${merged.edition}`)
}

// A SaaS build with no platform origin would start on the app and silently skip
// onboarding, which is the one thing that edition exists to guarantee. Fail at
// launch rather than ship a build that is quietly the wrong product.
if (merged.edition === 'saas' && !merged.platformOrigin) {
  throw new Error('desktop config: the saas edition requires platformOrigin')
}

// The other half of the same guarantee, and asked of the packaged config rather
// than the merged one: `appOrigin` has a committed default, so a saas build that
// never named its own inherits the OSS localhost address instead of failing.
// `platformOrigin` decides where a first run enters, `appOrigin` is where
// `policy.entryUrl` sends every run after `reachedApp`, so that build onboards
// correctly and then opens a dev server that is not there.
if (merged.edition === 'saas' && !packaged.appOrigin) {
  throw new Error('desktop config: the saas edition requires appOrigin')
}

// An unsigned build cannot install its own updates on macOS, but it can still
// read the feed and say so: `checkForUpdates` with autoDownload off is a manifest
// fetch and a version compare, and never hands Squirrel an artifact to validate.
// That is what "notify" is for, and why a preview build is not a dead end.
const UPDATE_MODES = ['auto', 'notify']
if (!UPDATE_MODES.includes(merged.updateMode)) {
  throw new Error(`desktop config: updateMode must be one of ${UPDATE_MODES.join('|')}, got ${merged.updateMode}`)
}

// The entire point of notify mode is handing the user a way to get the new
// build. Without one the dialog is a dead end that tells someone they are out of
// date and then abandons them, which is worse than staying quiet.
if (merged.updateMode === 'notify' && !merged.downloadPage) {
  throw new Error('desktop config: updateMode "notify" requires downloadPage')
}

/**
 * A path, and only a path. `entryUrl` resolves this against the platform origin,
 * and `new URL()` drops the base for anything that is really an origin — so a
 * `loginPath` of `https://elsewhere/` is not a path, it is a different entry
 * origin, loaded directly and never seen by the navigation policy. Everything
 * else in this file is validated; this was the one build input that was not.
 *
 * Asked by resolving rather than by inspecting the string, because the parser is
 * the only authority on what the string means and it is more inventive than a
 * prefix check: `//host` and `/\host` both come back as another origin, the
 * second because WHATWG folds a backslash into a slash. The check is simply
 * whether the base survived.
 */
const PROBE_BASE = 'https://desktop-config.invalid'

function relativePath(value) {
  let resolved
  try {
    resolved = new URL(value, PROBE_BASE)
  } catch {
    throw new Error(`desktop config: loginPath '${value}' is not a path`)
  }
  if (resolved.origin !== PROBE_BASE) {
    throw new Error(`desktop config: loginPath must stay on the platform origin, got '${value}'`)
  }
  return `${resolved.pathname}${resolved.search}${resolved.hash}`
}

function normalizeOrigin(value) {
  if (!value) return null
  let parsed
  try {
    parsed = new URL(value)
  } catch {
    throw new Error(`desktop config: '${value}' is not a valid origin`)
  }
  // Parsing is not enough. `new URL('file:///x').origin` is the *string*
  // "null", and an `ftp:` URL yields an origin too: both pass here and are then
  // refused by `origins` as somewhere a window may go, leaving a build whose
  // every navigation is external.
  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
    throw new Error(`desktop config: '${value}' must be an http:// or https:// origin`)
  }
  return parsed.origin
}

/** Unlike an origin, this one keeps its path: it points at a page, not a host. */
function normalizeUrl(value) {
  if (!value) return null
  try {
    return new URL(value).href
  } catch {
    throw new Error(`desktop config: '${value}' is not a valid URL`)
  }
}

/**
 * What this edition calls itself, and the scheme it answers on.
 *
 * Derived from the edition rather than configured, because the two builds have
 * to be installable side by side and every part of that follows from these two
 * strings. The name is the whole of it on macOS: `app.getName()` is what
 * `getPath('userData')` is built from, so two editions sharing a name share one
 * settings.json, and the OSS build would inherit whatever origin the hosted one
 * last learned. It is also the `.app` filename, and two of those cannot sit in
 * one folder.
 *
 * The scheme is the functional half. Both editions registering `langalpha://`
 * leaves the OS to pick one, so a hosted magic link can open a build pointed at
 * localhost, which cannot redeem it.
 *
 * These are asserted against electron-builder.yml and scripts/build.mjs in the
 * tests: every consumer only reads its own half, so a rename on one side is
 * silent everywhere else.
 */
const IDENTITY = {
  saas: { appName: 'LangAlpha', scheme: 'langalpha' },
  oss: { appName: 'LangAlpha OSS', scheme: 'langalpha-oss' },
}

module.exports = Object.freeze({
  edition: merged.edition,
  isSaas: merged.edition === 'saas',
  /** Display name, `.app` filename, and the directory userData is derived from. */
  appName: IDENTITY[merged.edition].appName,
  /** The custom scheme this edition registers, without the `://`. */
  scheme: IDENTITY[merged.edition].scheme,
  /** Where the langalpha SPA lives. In the OSS edition the stored server URL wins. */
  appOrigin: normalizeOrigin(merged.appOrigin),
  /** The account console. Null in the OSS edition, which ships without one. */
  platformOrigin: normalizeOrigin(merged.platformOrigin),
  /**
   * Where a SaaS first run enters the platform. Sign-in rather than onboarding:
   * the platform's post-auth funnel is what decides whether this account still
   * needs setting up.
   */
  loginPath: relativePath(merged.loginPath || '/login'),
  updateFeed: merged.updateFeed || null,
  /** "auto" downloads and installs; "notify" only points at `downloadPage`. */
  updateMode: merged.updateMode,
  /** Where a notify-mode build sends someone to collect the new version. */
  downloadPage: normalizeUrl(merged.downloadPage),
})
