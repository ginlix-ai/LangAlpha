#!/usr/bin/env node
/**
 * Writes desktop/config/build.json from the environment.
 *
 * Where a build points is deployment configuration, not source: the origins
 * arrive as build inputs (CI variables, or the deploy tooling) and land in a
 * gitignored file inside the packaged app. With no environment set this writes
 * nothing, so a plain `pnpm start` runs the committed OSS defaults against
 * localhost.
 *
 * Update settings are read for both editions. `scripts/build.mjs` bakes an
 * update feed into whatever it is packaging, so an OSS build can have one too,
 * and a mode that applied only to SaaS would be silently ignored on exactly the
 * builds most likely to be unsigned.
 */
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const here = path.dirname(fileURLToPath(import.meta.url))
const target = path.join(here, '..', 'config', 'build.json')

const EDITIONS = ['saas', 'oss']
const edition = (process.env.DESKTOP_EDITION || 'oss').trim()
if (!EDITIONS.includes(edition)) {
  console.error(`[config] DESKTOP_EDITION must be 'saas' or 'oss', got '${edition}'`)
  process.exit(1)
}

const config = { edition }

if (edition === 'saas') {
  const required = { appOrigin: 'DESKTOP_APP_ORIGIN', platformOrigin: 'DESKTOP_PLATFORM_ORIGIN' }
  let missing = false
  for (const [key, envName] of Object.entries(required)) {
    const value = process.env[envName]
    if (!value) {
      console.error(`[config] ${envName} is required for the saas edition`)
      missing = true
      continue
    }
    try {
      const parsed = new URL(value)
      // Parsing is not the bar. `ftp://host` has an origin and `file:///tmp/app`
      // has the *string* origin "null", so both are written here and then
      // refused by src/config.js at launch: packaging succeeds, artifacts are
      // distributed, and the failure surfaces on someone else's machine. The
      // same check belongs at the point the value enters the build.
      if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
        console.error(`[config] ${envName}='${value}' must be an http:// or https:// origin`)
        missing = true
      } else if (parsed.username || parsed.password) {
        // `parsed.origin` drops these silently, so without this the build ships
        // pointed at the bare host and the credentials are gone with no diagnostic.
        console.error(`[config] ${envName} must be an origin only, without a username or password`)
        missing = true
      } else {
        config[key] = parsed.origin
      }
    } catch {
      console.error(`[config] ${envName}='${value}' is not a valid URL`)
      missing = true
    }
  }
  // Equal origins package cleanly and fail on the user's machine. `isApp` is
  // tested before `isPlatform` everywhere, so every platform URL answers as an
  // app URL: loading the platform login page counts as reaching the app and
  // persists `reachedApp`, and someone who quits before finishing sign-in never
  // sees the first-run funnel again.
  if (!missing && config.appOrigin === config.platformOrigin) {
    console.error(`[config] DESKTOP_APP_ORIGIN and DESKTOP_PLATFORM_ORIGIN are both '${config.appOrigin}'; the shell cannot tell the two apps apart`)
    missing = true
  }
  if (missing) process.exit(1)
}

// Tracked apart from `config` so an OSS build with none of them can be told from
// one that has some: the first must leave no file behind at all.
const overrides = {}
if (process.env.DESKTOP_LOGIN_PATH) overrides.loginPath = process.env.DESKTOP_LOGIN_PATH
if (process.env.DESKTOP_UPDATE_FEED) overrides.updateFeed = process.env.DESKTOP_UPDATE_FEED
if (process.env.DESKTOP_DOWNLOAD_PAGE) {
  // The one override that gets handed to `shell.openExternal`, so the scheme is
  // the bar and not just parseability: `mailto:` opens an email composer and
  // `file://` opens a path on whoever's machine, both from a button that says
  // Download. `updateFeed` is deliberately not checked the same way, since a
  // file:// feed is a legitimate way to rehearse an update locally.
  const page = process.env.DESKTOP_DOWNLOAD_PAGE
  let scheme = null
  try { scheme = new URL(page).protocol } catch { scheme = null }
  if (scheme !== 'http:' && scheme !== 'https:') {
    console.error(`[config] DESKTOP_DOWNLOAD_PAGE='${page}' must be an http:// or https:// page`)
    process.exit(1)
  }
  overrides.downloadPage = page
}

const updateMode = (process.env.DESKTOP_UPDATE_MODE || 'auto').trim()
if (!['auto', 'notify'].includes(updateMode)) {
  console.error(`[config] DESKTOP_UPDATE_MODE must be 'auto' or 'notify', got '${updateMode}'`)
  process.exit(1)
}
if (updateMode !== 'auto') overrides.updateMode = updateMode

// Both halves are load-bearing and each fails silently on its own: without a
// feed the build never learns a new version exists, and without a page the
// dialog tells someone they are out of date and offers nowhere to go. A preview
// whose one job is surviving the cutover to GA should not ship half-wired.
if (updateMode === 'notify') {
  const unset = [
    !overrides.updateFeed && 'DESKTOP_UPDATE_FEED',
    !overrides.downloadPage && 'DESKTOP_DOWNLOAD_PAGE',
  ].filter(Boolean)
  if (unset.length > 0) {
    console.error(`[config] update mode 'notify' requires ${unset.join(' and ')}`)
    process.exit(1)
  }
}

Object.assign(config, overrides)

// An OSS build carrying nothing but its defaults must leave no build.json at
// all. That is what makes a plain `pnpm start` run the committed localhost
// config, and what stops a previous SaaS `dist` in the same tree from baking
// production origins into an OSS package.
if (edition === 'oss' && Object.keys(overrides).length === 0) {
  fs.rmSync(target, { force: true })
  console.log(`[config] edition=oss (defaults; removed ${path.relative(process.cwd(), target)} if present)`)
  process.exit(0)
}

fs.mkdirSync(path.dirname(target), { recursive: true })
fs.writeFileSync(target, `${JSON.stringify(config, null, 2)}\n`)

const shown = [
  `edition=${edition}`,
  config.appOrigin && `app=${config.appOrigin}`,
  config.platformOrigin && `platform=${config.platformOrigin}`,
  `updates=${updateMode}`,
].filter(Boolean)
console.log(`[config] ${shown.join(' ')}`)
