/**
 * electron-builder's own macOS signing, with two things signed first and on their
 * own terms: the remote-control helper, and the bundled backend tree where a build
 * carries one.
 *
 * First, because @electron/osx-sign signs the deepest files first and leaves
 * files of one depth in directory order, which reaches the app's main
 * executable before the helper beside it in Contents/MacOS. Signing the main
 * executable seals the bundle, so codesign refuses it while the helper is still
 * unsigned ("code object is not signed at all, In subcomponent").
 *
 * On its own terms, because electron-builder signs every nested Mach-O with
 * `entitlementsInherit`, which here relaxes W^X for the JIT Electron's own
 * helpers need. The remote-control helper takes logins from the network and
 * needs none of it, so it is signed with an empty set, which the repo keeps as
 * a file so the grant stays visible. Identity, keychain, timestamp, hardened
 * runtime and extra arguments are taken as electron-builder resolved them for
 * that file.
 *
 * The backend tree is signed here for a different reason. @electron/osx-sign signs
 * every file `isbinaryfile` calls binary, and in that tree that means every .pyc,
 * font and data file, each getting a signature in its extended attributes. Only
 * Mach-O files are code, so those are found by their headers and signed here,
 * and the walk is told to leave the tree alone.
 *
 * Installed as `mac.sign` by scripts/build.mjs, on a build that carries either.
 */
import { execFile } from 'node:child_process'
import { existsSync } from 'node:fs'
import { createRequire } from 'node:module'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { promisify } from 'node:util'

import { HELPER_BINARY } from './launch-agent.mjs'
import { MH_EXECUTE, MH_OBJECT, nativeFiles } from './native-files.mjs'

const here = path.dirname(fileURLToPath(import.meta.url))

export const HELPER_ENTITLEMENTS = path.join(here, '..', 'resources', 'entitlements.helper.plist')

const HELPER_IN_BUNDLE = path.join('Contents', 'MacOS', HELPER_BINARY)

const helperIn = (opts) => path.resolve(opts.app, HELPER_IN_BUNDLE)

/**
 * Python's and the packages' executables keep the pair V8 and wasmtime need, and
 * nothing is granted `disable-library-validation`: every library in the tree is
 * signed here with the same identity, so validation passes. That is also why
 * wheels downloaded at runtime never load into this interpreter: third-party MCP
 * servers run on the computer, on an environment of their own.
 */
export const PYTHON_ENTITLEMENTS = path.join(here, '..', 'resources', 'entitlements.python.plist')
/** Postgres and Valkey run no JIT and are started by the shell, so they get nothing. */
export const NATIVE_ENTITLEMENTS = path.join(here, '..', 'resources', 'entitlements.native.plist')

const BACKEND_IN_BUNDLE = path.join('Contents', 'Resources', 'backend')

/**
 * The entitlements a Mach-O signs with, null for none, or undefined for a file
 * that is not signed: an object file, which is not code that runs. A universal
 * file in these trees is always a library or an extension module, the
 * interpreter being one uv's build ships thin, so it signs as a library.
 */
function signedAs({ file, native }, python) {
  if (native.format === 'macho-universal') return null
  if (native.format !== 'macho') return undefined
  const { filetype } = native.slices[0]
  if (filetype === null || filetype === MH_OBJECT) return undefined
  if (filetype !== MH_EXECUTE) return null
  return file.startsWith(python) ? PYTHON_ENTITLEMENTS : NATIVE_ENTITLEMENTS
}

/** Every Mach-O under the tree, with the entitlements it signs with, or null for none. */
export function backendSignPlan(appPath) {
  const tree = path.resolve(appPath, BACKEND_IN_BUNDLE)
  if (!existsSync(tree)) return []
  const python = path.join(tree, 'python') + path.sep
  const plan = []
  for (const found of nativeFiles(tree)) {
    const entitlements = signedAs(found, python)
    if (entitlements !== undefined) plan.push({ file: found.file, entitlements })
  }
  return plan
}

/** codesign's arguments for one file in the tree, under the app's identity and hardened runtime. */
export function backendSignArgs(opts, { file, entitlements }) {
  if (!opts.identity) throw new Error('no signing identity for the bundled backend')
  const args = ['--sign', opts.identity, '--force', '--options', 'runtime']
  if (opts.keychain) args.push('--keychain', opts.keychain)
  args.push('--timestamp')
  if (entitlements) args.push('--entitlements', entitlements)
  return [...args, file]
}

const isUnderBackend = (opts, file) => path.resolve(file).startsWith(path.resolve(opts.app, BACKEND_IN_BUNDLE) + path.sep)

/** Two thousand codesigns, each a timestamp round trip to Apple, a few at a time. */
async function signBackendTree(opts, retry) {
  const plan = backendSignPlan(opts.app)
  const run = promisify(execFile)
  let next = 0
  const worker = async () => {
    while (next < plan.length) {
      const item = plan[next++]
      await retry(() => run('codesign', backendSignArgs(opts, item)), SIGN_RETRY)
    }
  }
  await Promise.all(Array.from({ length: 8 }, worker))
  console.log(`[build] signed ${plan.length} native files in the bundled backend`)
}

/** codesign's arguments for the helper, built as @electron/osx-sign builds them for any file. */
export function helperSignArgs(opts) {
  if (!opts.identity) throw new Error('no signing identity for the remote control helper')
  const file = helperIn(opts)
  const options = opts.optionsForFile ? opts.optionsForFile(file) : {}
  const args = ['--sign', opts.identity, '--force']
  if (opts.keychain) args.push('--keychain', opts.keychain)
  args.push(options.timestamp ? `--timestamp=${options.timestamp}` : '--timestamp')
  if (options.hardenedRuntime) args.push('--options', 'runtime')
  args.push(...(options.additionalArguments || []))
  return [...args, '--entitlements', HELPER_ENTITLEMENTS, file]
}

/**
 * The sign options electron-builder built, with the helper left out of the
 * walk. One function wrapping electron-builder's own, which it always passes
 * as a function: @electron/osx-sign drops an `ignore` given as an array.
 */
export function withoutHelper(opts) {
  const helper = helperIn(opts)
  const ignored = opts.ignore
  return {
    ...opts,
    ignore: (file) => path.resolve(file) === helper || isUnderBackend(opts, file) || !!ignored?.(file),
  }
}

/** How electron-builder retries its signer, whose timestamp requests to Apple fail now and then. */
const SIGN_RETRY = { retries: 3, interval: 5000, backoff: 5000 }

/**
 * What electron-builder calls in place of its signer. Signs the helper and the
 * backend tree, whichever this bundle has, then delegates the rest to that signer,
 * loaded through electron-builder's own dependency so the two cannot drift apart in
 * version. Each codesign asks Apple for the same timestamp, so it is retried the
 * same way.
 */
export async function sign(opts) {
  const builder = createRequire(import.meta.url).resolve('electron-builder')
  const signer = createRequire(builder).resolve('app-builder-lib/out/codeSign/macCodeSign')
  const { sign: signWithDefaults } = createRequire(signer)(signer)
  const { retry } = createRequire(signer)('builder-util')
  if (existsSync(helperIn(opts))) await retry(() => promisify(execFile)('codesign', helperSignArgs(opts)), SIGN_RETRY)
  await signBackendTree(opts, retry)
  return signWithDefaults(withoutHelper(opts))
}
