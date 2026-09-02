#!/usr/bin/env node
/**
 * electron-builder, with the update feed injected rather than committed.
 *
 * Two things this wrapper exists for:
 *
 *  1. `--publish never`, always. electron-builder's default policy will upload to
 *     a release on its own under some conditions, and a build command that can
 *     publish without being asked is not one to leave lying around.
 *
 *  2. The feed URL is a build input, like the origins are, so
 *     `electron-builder.yml` carries `publish: null` and the real feed arrives
 *     as DESKTOP_UPDATE_FEED. Supplying it is also what makes
 *     electron-builder emit `latest-*.yml` and bake `app-update.yml` into the
 *     package. Without it there is simply no update metadata, which is a quiet
 *     way to ship a build that can never update itself.
 */
import { spawn, spawnSync } from 'node:child_process'
import { existsSync, lstatSync, readdirSync, readFileSync, writeFileSync, rmSync } from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const here = path.dirname(fileURLToPath(import.meta.url))
const root = path.join(here, '..')
const BASE = path.join(root, 'electron-builder.yml')
// Beside the base config, not in a temp dir: electron-builder resolves every
// relative path in a config against the project directory.
const RESOLVED = path.join(root, '.electron-builder.resolved.yml')

const feed = (process.env.DESKTOP_UPDATE_FEED || '').trim()
// The two ways electron-builder is told to sign: a certificate file or base64
// blob (CSC_LINK, what CI uses) or a keychain identity by name (CSC_NAME, what a
// developer has locally). Either one has to take `identity: null` back out.
const signing = !!((process.env.CSC_LINK || '').trim() || (process.env.CSC_NAME || '').trim())
// Notarization is a second switch, not a consequence of the first. A signed,
// un-notarized build is refused by Gatekeeper on first launch the same way an
// unsigned one is, so getting a certificate without turning this on buys
// nothing a user can see.
//
// electron-builder reads the credentials itself; what has to be decided here is
// whether a complete set exists, because asking it to notarize with none is a
// twenty-minute package that fails at the submission. Three ways to authenticate
// notarytool, and a partial set is not one of them.
const env = (name) => (process.env[name] || '').trim()
const notarizeAuth =
  (env('APPLE_ID') && env('APPLE_APP_SPECIFIC_PASSWORD') && env('APPLE_TEAM_ID') && 'APPLE_ID') ||
  (env('APPLE_API_KEY') && env('APPLE_API_KEY_ID') && env('APPLE_API_ISSUER') && 'APPLE_API_KEY') ||
  (env('APPLE_KEYCHAIN_PROFILE') && 'APPLE_KEYCHAIN_PROFILE') ||
  null
// Only on macOS: notarytool ships with Xcode, and the mac block is not read on
// the runners that build the other two platforms anyway.
const notarizing = signing && !!notarizeAuth && process.platform === 'darwin'
// Matches write-build-config.mjs, which treats an unset edition as oss.
const edition = (process.env.DESKTOP_EDITION || 'oss').trim()
// `pnpm run dist -- --dir` puts a literal `--` in argv, and electron-builder's
// parser reads that as end-of-options: every flag after it, including the
// `--publish never` and `--config` this wrapper exists to add, becomes a
// positional and is ignored. The build still succeeds, unsigned and publishable,
// against the committed config. Drop the separator rather than inherit that.
const passthrough = process.argv.slice(2).filter((a) => a !== '--')
const args = [...passthrough, '--publish', 'never']

// Both edits rewrite the committed config rather than passing `-c.…` overrides:
// the CLI form does not override an explicit `null`, and it fails by producing a
// wrong-but-successful build instead of erroring.
let config = readFileSync(BASE, 'utf8')
const replace = (marker, replacement, what) => {
  if (!marker.test(config)) {
    console.error(`[build] electron-builder.yml no longer has the \`${what}\` line to replace`)
    process.exit(1)
  }
  config = config.replace(marker, replacement)
}

if (feed) {
  try {
    new URL(feed)
  } catch {
    console.error(`[build] DESKTOP_UPDATE_FEED='${feed}' is not a valid URL`)
    process.exit(1)
  }
  // A function replacement, and quoted: `$&`, `` $` `` and `$'` in a URL are
  // replacement patterns to `String.replace`, and a `#` in an unquoted YAML
  // scalar starts a comment, which truncates the feed in a build that still
  // succeeds.
  replace(/^publish: null\r?$/m,
    () => `publish:\n  provider: generic\n  url: ${JSON.stringify(feed)}`,
    'publish: null')
  console.log(`[build] update feed: ${feed}`)
} else {
  console.log('[build] no DESKTOP_UPDATE_FEED; this build will not update itself')
}

// `identity: null` disables macOS signing outright, secrets or no secrets. Left
// in place it would swallow a certificate the moment one exists and still exit
// 0, so the presence of CSC_LINK is what takes the line back out.
if (signing) {
  replace(/^ {2}identity: null\r?$/m, '  # identity resolved from CSC_LINK by scripts/build.mjs', 'identity: null')
  console.log(`[build] signing enabled (${process.env.CSC_LINK ? 'CSC_LINK' : 'CSC_NAME'})`)
}

if (notarizing) {
  replace(/^ {2}notarize: false\r?$/m, '  notarize: true', 'notarize: false')
  console.log(`[build] notarization enabled (${notarizeAuth})`)
} else if (signing && process.platform === 'darwin') {
  // Loud, because this is the shape of a release that looks finished in every
  // log line and is still refused on the machine it lands on.
  console.warn('[build] WARNING: signing without notarization credentials. Gatekeeper refuses this build on first launch.')
}

// The two editions must be installable side by side, and on macOS that is
// entirely a matter of these three strings: `productName` is the `.app`
// filename and what `app.getName()` returns, which is what `getPath('userData')`
// is built from, so sharing it means sharing one settings.json; `appId` is the
// Launch Services and signing identity; and the scheme decides which build the
// OS hands a deep link to. Kept in step with `IDENTITY` in src/config.js, which
// the tests assert against this table.
//
// Each marker matches the COMMITTED (oss) value rather than a wildcard, so an
// edit to electron-builder.yml that moves one of these lines fails here instead
// of shipping a hosted build wearing half the self-hosted identity.
const IDENTITY = {
  saas: { appId: 'ai.langalpha.desktop', productName: 'LangAlpha', scheme: 'langalpha' },
  oss: { appId: 'ai.langalpha.desktop.oss', productName: 'LangAlpha OSS', scheme: 'langalpha-oss' },
}
const identity = IDENTITY[edition]
if (edition !== 'oss') {
  replace(/^appId: ai\.langalpha\.desktop\.oss\r?$/m, `appId: ${identity.appId}`, 'appId')
  replace(/^productName: LangAlpha OSS\r?$/m, `productName: ${identity.productName}`, 'productName')
  replace(/^ {2}- name: LangAlpha OSS\r?$/m, `  - name: ${identity.productName}`, 'protocol name')
  replace(/^ {6}- langalpha-oss\r?$/m, `      - ${identity.scheme}`, 'protocol scheme')
}

// Always resolved, unlike the feed and signing edits: the artifact filename and
// the output directory both carry the edition on every build, so there is no
// configuration left that electron-builder can consume as committed.
//
// Every occurrence outside a comment, rather than one anchored to a directive by
// name. The prose in electron-builder.yml spells ${EDITION} too, and an anchored
// replace that took the first match once landed in that prose, left the real
// line alone, and failed at packaging with "macro EDITION is not defined",
// which is why the old form needed a second pass to check its own work. Covering
// exactly the lines electron-builder reads leaves nothing for that pass to find,
// and a new directive carrying the placeholder needs no marker added here.
let substitutions = 0
config = config
  .split('\n')
  .map((line) =>
    line.trimStart().startsWith('#')
      ? line
      // A function replacement, so a `$` in the edition is not read as a
      // capture reference.
      : line.replace(/\$\{EDITION\}/g, () => {
        substitutions++
        return edition
      }))
  .join('\n')
if (substitutions === 0) {
  console.error('[build] electron-builder.yml no longer spells ${EDITION} outside its comments')
  process.exit(1)
}

// Asserted rather than assumed, in both directions: the oss path performs no
// substitution at all, so nothing above would notice if the committed base
// drifted to the hosted values and quietly gave every self-hosted build the
// hosted app's user-data directory and protocol registration.
const identityLines = [
  [`appId: ${identity.appId}`, 'appId'],
  [`productName: ${identity.productName}`, 'productName'],
  [`      - ${identity.scheme}`, 'protocol scheme'],
]
// Split on either ending: git may hand a Windows runner this file with CRLF,
// and a trailing \r would fail every check below on a config that is correct.
for (const [line, what] of identityLines) {
  if (!config.split(/\r?\n/).includes(line)) {
    console.error(`[build] resolved config does not carry the ${edition} ${what}: expected '${line}'`)
    process.exit(1)
  }
}
console.log(`[build] identity: ${identity.productName} (${identity.appId}, ${identity.scheme}://)`)

// Read back off the resolved config rather than rebuilt from `edition` here:
// electron-builder writes wherever this line says, and everything below that
// goes looking for what it produced has to ask the same line or drift from it
// in silence. Drift does not announce itself: a sweep, a signing check and a
// manifest check that all simply find nothing look exactly like a clean build.
// One regex, so the key and its exact indent are stated once rather than as a
// find predicate and a slice offset that have to agree. It also has to survive
// the shapes YAML allows and this script does not write itself: a quoted value,
// and a trailing comment, both of which a plain slice would carry into the path.
const outputMatch = /^ {2}output:[ \t]*(?:"([^"]*)"|'([^']*)'|([^#\r\n]*))/m.exec(config)
const outputDir = outputMatch && (outputMatch[1] ?? outputMatch[2] ?? outputMatch[3] ?? '').trim()
if (!outputDir) {
  console.error('[build] electron-builder.yml no longer has a `directories.output` line')
  process.exit(1)
}
// The whole point of the per-edition tree, asserted where it is knowable. The
// substitution count above is file-wide, so dropping ${EDITION} from this one
// line while artifactName keeps its own leaves that count non-zero and both
// editions building into one directory, which is the state this replaced.
if (!outputDir.includes(edition)) {
  console.error(`[build] directories.output resolved to '${outputDir}', which is not this edition's tree`)
  process.exit(1)
}
const dist = path.resolve(root, outputDir)
console.log(`[build] output: ${path.relative(root, dist)}`)

writeFileSync(RESOLVED, config)
args.push('--config', path.basename(RESOLVED))

// Resolved explicitly, because this script is also run as plain `node
// scripts/build.mjs`, where the package manager has not put .bin on PATH and a
// bare name fails with a bare ENOENT.
const local = path.join(root, 'node_modules', '.bin', process.platform === 'win32' ? 'electron-builder.cmd' : 'electron-builder')
const bin = existsSync(local) ? local : 'electron-builder'

// electron-builder writes app-update.yml when a feed exists, but never removes
// one a previous build left in the same output tree. The updater keys on that
// file existing, so a no-feed build inherits the last feed that was configured
// and points itself at it. Local-only in practice, since CI starts clean, but it
// is silent and it survives into a package.
//
// Swept before the builder runs, not after: the dmg, zip, exe and deb are
// written during the run, so a sweep that follows it tidies the unpacked tree
// while every installer already carries the old feed baked in. Those are the
// artifacts that ship.
if (!feed) {
  for (const stale of findStaleFeeds(dist)) {
    rmSync(stale, { force: true })
    console.log(`[build] removed a previous build's feed: ${path.relative(root, stale)}`)
  }
}

const result = spawnSync(bin, args, { cwd: root, stdio: 'inherit', shell: process.platform === 'win32' })
rmSync(RESOLVED, { force: true })
if (result.error) {
  console.error(`[build] could not run ${bin}: ${result.error.message}`)
  process.exit(1)
}
if (result.status !== 0) process.exit(result.status ?? 1)

// Same shape as the manifest guard: a certificate that produced an unsigned
// artifact is a green build that Squirrel.Mac will refuse to install from, and
// the only place it shows is one line of electron-builder's own log.
if (signing && process.platform === 'darwin') {
  const apps = findApps(dist)
  if (apps.length === 0) {
    console.error('[build] signing was requested but no .app was produced')
    process.exit(1)
  }
  // Every bundle, not just the first: a release builds both architectures and an
  // unsigned one among them is a build that half the users cannot update from.
  for (const app of apps) {
    const rel = path.relative(root, app)
    const check = spawnSync('codesign', ['--verify', '--deep', '--strict', app], { encoding: 'utf8' })
    if (check.status !== 0) {
      console.error(`[build] signing was requested but ${rel} is not validly signed`)
      console.error((check.stderr || '').trim())
      console.error(`[build] if that path is from an earlier build, clear ${path.relative(root, dist)}/ and build again`)
      process.exit(1)
    }
    console.log(`[build] signed and verified: ${rel}`)

    // `codesign --verify` answers a different question: it says the bundle is
    // intact and signed, and it says exactly that about a build Apple has never
    // seen. The staple is the half that decides whether the app opens on a
    // machine that has not been told to trust us, so a notarized build that
    // shipped without one is the failure this whole block exists to catch.
    //
    // Skipped for --dir, like the manifest guard: an unpacked build never
    // reaches the target that submits it, and `pnpm run build` is meant to stay
    // the fast way to check packaging.
    if (notarizing && !passthrough.includes('--dir')) {
      const stapled = spawnSync('xcrun', ['stapler', 'validate', app], { encoding: 'utf8' })
      if (stapled.status !== 0) {
        console.error(`[build] ${rel} carries no notarization ticket`)
        console.error(((stapled.stdout || '') + (stapled.stderr || '')).trim())
        process.exit(1)
      }
      // The verdict a user's machine reaches, recorded rather than gated: it
      // needs Gatekeeper's own assessment and that can be turned off locally,
      // so a machine with it disabled would fail a build that is perfectly fine.
      console.log(`[build] notarized: ${rel} (${spctlSource(app, 'exec')})`)
    }
  }
}

// electron-builder notarizes the .app and then builds the DMG around it, so the
// disk image itself is never submitted. Gatekeeper assesses a downloaded image on
// its own signature and ticket rather than on the app inside, so an unstapled DMG
// is refused on open however well notarized its contents are. That is the file
// the download page serves, which makes it the one that has to pass.
//
// Deliberately after the .app loop: `stapler staple` on the image needs a ticket
// Apple only issues for what was submitted, and submitting a DMG built around an
// unsigned app just fails later and more expensively.
if (notarizing && !passthrough.includes('--dir')) {
  const dmgs = existsSync(dist)
    ? readdirSync(dist).filter((f) => f.endsWith('.dmg')).map((f) => path.join(dist, f))
    : []
  if (dmgs.length === 0) {
    console.log('[build] no disk images to notarize')
  } else {
    // Submitted together rather than one after another. Each is an independent
    // wait on Apple's queue, which ran about half an hour per artifact the first
    // time this was measured, and serialising them adds that to every release
    // for every architecture.
    console.log(`[build] submitting ${dmgs.length} disk image(s) to Apple; this waits on their queue`)
    const submissions = await Promise.all(dmgs.map(submitForNotarization))
    let failed = false
    for (const { artifact, code, output } of submissions) {
      const rel = path.relative(root, artifact)
      if (code !== 0) {
        console.error(`[build] notarization failed for ${rel}`)
        console.error(output.trim())
        failed = true
        continue
      }
      const staple = spawnSync('xcrun', ['stapler', 'staple', artifact], { encoding: 'utf8' })
      if (staple.status !== 0) {
        console.error(`[build] could not staple ${rel}`)
        console.error(((staple.stdout || '') + (staple.stderr || '')).trim())
        failed = true
        continue
      }
      // `-t open` with the primary-signature context, not `-t exec`: that is the
      // assessment Gatekeeper runs against a quarantined disk image, and the one
      // that returned `rejected` on every DMG built before this block existed.
      console.log(`[build] notarized: ${rel} (${spctlSource(artifact, 'open')})`)
    }
    if (failed) process.exit(1)
  }
}

/** Resolves rather than rejects, so one bad artifact still reports the others. */
function submitForNotarization(artifact) {
  return new Promise((resolve) => {
    const child = spawn('xcrun', ['notarytool', 'submit', artifact, ...notarytoolAuth(), '--wait'])
    let output = ''
    child.stdout.on('data', (d) => { output += d })
    child.stderr.on('data', (d) => { output += d })
    child.on('error', (err) => resolve({ artifact, code: 1, output: err.message }))
    child.on('close', (code) => resolve({ artifact, code, output }))
  })
}

/**
 * notarytool takes the same three credential shapes detected above and spells
 * each differently. Derived from `notarizeAuth` so the image is submitted with
 * whatever authenticated the app, rather than a second, independent guess.
 */
function notarytoolAuth() {
  if (notarizeAuth === 'APPLE_API_KEY') {
    return ['--key', env('APPLE_API_KEY'), '--key-id', env('APPLE_API_KEY_ID'), '--issuer', env('APPLE_API_ISSUER')]
  }
  if (notarizeAuth === 'APPLE_ID') {
    // notarytool offers no way to pass this off the command line, so it is
    // visible to `ps` for the length of the submission. The API key path has no
    // such exposure, which is one more reason CI uses it.
    return ['--apple-id', env('APPLE_ID'), '--password', env('APPLE_APP_SPECIFIC_PASSWORD'), '--team-id', env('APPLE_TEAM_ID')]
  }
  return ['--keychain-profile', env('APPLE_KEYCHAIN_PROFILE')]
}

/**
 * `source=`, never the last line. The last line is `origin=`, which names the
 * signing certificate on an un-notarized artifact just the same; `source=` is
 * where "Notarized Developer ID" and "Unnotarized Developer ID" differ.
 */
function spctlSource(target, type) {
  const args = type === 'open'
    ? ['-a', '-vvv', '-t', 'open', '--context', 'context:primary-signature', target]
    : ['-a', '-vvv', '-t', 'exec', target]
  const lines = (spawnSync('spctl', args, { encoding: 'utf8' }).stderr || '').trim().split('\n')
  return lines.find((l) => l.startsWith('source=')) || lines.pop() || 'no spctl verdict'
}

function findApps(dir, depth = 0) {
  if (depth > 2 || !existsSync(dir)) return []
  const found = []
  for (const entry of readdirSync(dir)) {
    const full = path.join(dir, entry)
    if (entry.endsWith('.app')) found.push(full)
    else if (lstatSync(full).isDirectory()) found.push(...findApps(full, depth + 1))
  }
  return found
}

// A feed that produced no manifest is the failure this whole wrapper is about:
// the package looks fine, installs fine, and silently never updates.
if (feed && !passthrough.includes('--dir')) {
  const manifests = existsSync(dist) ? readdirSync(dist).filter((f) => /^latest.*\.yml$/.test(f)) : []
  if (manifests.length === 0) {
    console.error('[build] a feed was configured but no latest*.yml was produced; the build cannot update itself')
    process.exit(1)
  }
  console.log(`[build] update manifests: ${manifests.join(', ')}`)
}

function findStaleFeeds(dir, depth = 0) {
  if (depth > 6 || !existsSync(dir)) return []
  const found = []
  for (const entry of readdirSync(dir)) {
    const full = path.join(dir, entry)
    if (entry === 'app-update.yml') found.push(full)
    else if (lstatSync(full).isDirectory()) found.push(...findStaleFeeds(full, depth + 1))
  }
  return found
}
