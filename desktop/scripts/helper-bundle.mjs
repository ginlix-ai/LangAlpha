/**
 * Remote control's helper in a mac bundle: whether a build can carry it, and
 * the `mac:` block that puts it there.
 *
 * Into `Contents/` through `mac.extraFiles`, where SMAppService looks for an
 * agent's plist (`Library/LaunchAgents`) and where `BundleProgram` resolves the
 * binary from. electron-builder signs every Mach-O it finds in the bundle with
 * the app's identity, the helper included, which is what lets macOS tie the
 * login item to this app. It would also give the helper the JIT entitlements
 * Electron's own helpers carry, which scripts/sign-mac.mjs, set as the signer
 * in the same block, takes back off.
 */
import { spawnSync } from 'node:child_process'
import { accessSync, constants, existsSync } from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { HELPER_BINARY } from './launch-agent.mjs'

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), '..')

/**
 * Why this binary cannot ship, or null if it can. Both architectures, because
 * the arm64 and x64 bundles each carry this one file, and a helper missing one
 * is remote control that fails on that kind of Mac without saying why.
 */
export function helperProblem(file) {
  const rel = path.relative(root, file)
  if (!existsSync(file)) return `there is no helper at ${rel}`
  try {
    accessSync(file, constants.X_OK)
  } catch {
    return `${rel} is not executable`
  }
  const lipo = spawnSync('lipo', ['-archs', file], { encoding: 'utf8' })
  const archs = new Set((lipo.stdout || '').trim().split(/\s+/))
  const missing = ['arm64', 'x86_64'].filter((arch) => !archs.has(arch))
  if (lipo.status !== 0 || missing.length > 0) return `${rel} is not a universal binary (no ${missing.join(' or ')})`
  return null
}

/**
 * The `mac:` line with the helper's signer and files under it, as lines of
 * electron-builder.yml. Every path is quoted: they are absolute, and unquoted,
 * a ` #` anywhere in the checkout's path would start a YAML comment.
 */
export function macHelperConfigLines({ label, source, plist, signer }) {
  return [
    'mac:',
    `  sign: ${JSON.stringify(signer)}`,
    '  extraFiles:',
    `    - from: ${JSON.stringify(source)}`,
    `      to: ${JSON.stringify(`MacOS/${HELPER_BINARY}`)}`,
    `    - from: ${JSON.stringify(plist)}`,
    `      to: ${JSON.stringify(`Library/LaunchAgents/${label}.plist`)}`,
  ]
}
