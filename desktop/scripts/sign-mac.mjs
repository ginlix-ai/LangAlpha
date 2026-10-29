/**
 * electron-builder's own macOS signing, with the remote-control helper signed
 * first and on its own terms.
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
 * Installed as `mac.sign` by scripts/build.mjs, only on a build that carries the
 * helper.
 */
import { execFile } from 'node:child_process'
import { createRequire } from 'node:module'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { promisify } from 'node:util'

import { HELPER_BINARY } from './launch-agent.mjs'

const here = path.dirname(fileURLToPath(import.meta.url))

export const HELPER_ENTITLEMENTS = path.join(here, '..', 'resources', 'entitlements.helper.plist')

const HELPER_IN_BUNDLE = path.join('Contents', 'MacOS', HELPER_BINARY)

const helperIn = (opts) => path.resolve(opts.app, HELPER_IN_BUNDLE)

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
  return { ...opts, ignore: (file) => path.resolve(file) === helper || !!ignored?.(file) }
}

/** How electron-builder retries its signer, whose timestamp requests to Apple fail now and then. */
const SIGN_RETRY = { retries: 3, interval: 5000, backoff: 5000 }

/**
 * What electron-builder calls in place of its signer. Signs the helper, then
 * delegates the rest to that signer, loaded through electron-builder's own
 * dependency so the two cannot drift apart in version. The helper's codesign
 * asks Apple for the same timestamp, so it is retried the same way.
 */
export async function sign(opts) {
  const builder = createRequire(import.meta.url).resolve('electron-builder')
  const signer = createRequire(builder).resolve('app-builder-lib/out/codeSign/macCodeSign')
  const { sign: signWithDefaults } = createRequire(signer)(signer)
  const { retry } = createRequire(signer)('builder-util')
  await retry(() => promisify(execFile)('codesign', helperSignArgs(opts)), SIGN_RETRY)
  return signWithDefaults(withoutHelper(opts))
}
