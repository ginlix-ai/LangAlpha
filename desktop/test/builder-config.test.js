'use strict'

// The electron-builder config scripts/build.mjs resolves, for every combination
// of what changes it: platform and architecture, edition, a bundled backend, the
// remote-control helper, signing, notarization and a feed. Run end to end, the
// script itself in a copy of desktop/ with a stand-in electron-builder that keeps
// the config it was handed, so nothing here restates how the edits are made.
//
// Each expected config is stored as the edits from the committed
// electron-builder.yml, by line, and rebuilt from it to compare byte for byte.
// After a deliberate change, `UPDATE_BUILDER_CONFIG=1` rewrites them.

const { test } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { spawnSync } = require('node:child_process')

const DESKTOP = path.join(__dirname, '..')
const FIXTURE = path.join(__dirname, 'fixtures', 'builder-config.json')
const ROOT = '/ROOT'
const updating = process.env.UPDATE_BUILDER_CONFIG === '1'

function combos() {
  const out = []
  const add = (target, edition, bundle, helper, sign, feed) => out.push({ target, edition, bundle, helper, sign, feed })
  for (const edition of ['oss', 'saas']) {
    for (const bundle of edition === 'oss' ? [false, true] : [false]) {
      for (const helper of [false, true]) {
        for (const sign of ['none', 'sign', 'notarize']) {
          for (const feed of [false, true]) add('darwin-arm64', edition, bundle, helper, sign, feed)
        }
      }
      for (const sign of ['none', 'sign']) {
        for (const feed of [false, true]) add('linux-x64', edition, bundle, false, sign, feed)
      }
    }
  }
  // The architecture reaches only a bundled build's config.
  for (const helper of [false, true]) {
    for (const feed of [false, true]) add('darwin-x64', 'oss', true, helper, 'none', feed)
  }
  return out
}

const nameOf = (c) => [c.target, c.edition, c.bundle ? 'bundle' : 'thin', c.helper ? 'helper' : 'no-helper', c.sign, c.feed ? 'feed' : 'no-feed'].join('.')

/** A copy of what build.mjs reads, a helper and a tree where it looks for them, and a builder that only records. */
function workspace() {
  const top = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'la-builder-config-')))
  const desktop = path.join(top, 'desktop')
  fs.cpSync(path.join(DESKTOP, 'scripts'), path.join(desktop, 'scripts'), { recursive: true })
  for (const file of ['electron-builder.yml', 'package.json']) fs.copyFileSync(path.join(DESKTOP, file), path.join(desktop, file))

  const bin = path.join(top, 'bin')
  fs.mkdirSync(bin)
  const builder = path.join(bin, 'electron-builder')
  fs.writeFileSync(builder, [
    '#!/usr/bin/env node',
    "const at = process.argv.indexOf('--config')",
    "require('node:fs').copyFileSync(process.argv[at + 1], process.env.CAPTURE)",
  ].join('\n'))
  fs.chmodSync(builder, 0o755)

  const emulate = path.join(top, 'emulate.mjs')
  fs.writeFileSync(emulate, [
    "Object.defineProperty(process, 'platform', { value: process.env.EMULATE_PLATFORM })",
    "Object.defineProperty(process, 'arch', { value: process.env.EMULATE_ARCH })",
  ].join('\n'))

  for (const target of ['darwin-arm64', 'darwin-x64', 'linux-x64']) {
    const tree = path.join(desktop, 'dist', 'backend', target)
    fs.mkdirSync(tree, { recursive: true })
    fs.writeFileSync(path.join(tree, 'manifest.json'), '{}\n')
  }
  return { top, desktop, bin, emulate, helper: path.join(top, 'helper', 'target', 'universal-apple-darwin', 'release', 'langalpha-helper') }
}

/** A universal file with an arm64 and an x86_64 slice, which is all `lipo -archs` reads. */
function writeUniversal(file) {
  const thin = (cpu, sub) => {
    const b = Buffer.alloc(32)
    b.writeUInt32LE(0xfeedfacf, 0)
    b.writeUInt32LE(cpu, 4)
    b.writeUInt32LE(sub, 8)
    b.writeUInt32LE(2, 12)
    return b
  }
  const out = Buffer.alloc(8192 + 32)
  out.writeUInt32BE(0xcafebabe, 0)
  out.writeUInt32BE(2, 4)
  for (const [i, cpu, sub, offset] of [[0, 0x0100000c, 0, 4096], [1, 0x01000007, 3, 8192]]) {
    const at = 8 + 20 * i
    out.writeUInt32BE(cpu, at)
    out.writeUInt32BE(sub, at + 4)
    out.writeUInt32BE(offset, at + 8)
    out.writeUInt32BE(32, at + 12)
    out.writeUInt32BE(12, at + 16)
    thin(cpu, sub).copy(out, offset)
  }
  fs.mkdirSync(path.dirname(file), { recursive: true })
  fs.writeFileSync(file, out)
  fs.chmodSync(file, 0o755)
}

function render(ws, c) {
  if (c.helper) writeUniversal(ws.helper)
  else fs.rmSync(ws.helper, { force: true })
  const [platform, arch] = c.target.split('-')
  const capture = path.join(ws.top, 'captured.yml')
  fs.rmSync(capture, { force: true })
  const env = {
    PATH: `${ws.bin}${path.delimiter}${process.env.PATH}`,
    HOME: process.env.HOME,
    CAPTURE: capture,
    EMULATE_PLATFORM: platform,
    EMULATE_ARCH: arch,
    DESKTOP_EDITION: c.edition,
    DESKTOP_BUNDLE_BACKEND: c.bundle ? '1' : '0',
    DESKTOP_BUNDLE_REUSE: '1',
  }
  if (c.sign !== 'none') env.CSC_NAME = 'Developer ID Application: Example'
  if (c.sign === 'notarize') env.APPLE_KEYCHAIN_PROFILE = 'example'
  if (c.feed) env.DESKTOP_UPDATE_FEED = 'https://updates.example.com/desktop'
  const run = spawnSync(process.execPath, ['--import', ws.emulate, 'scripts/build.mjs'], { cwd: ws.desktop, env, encoding: 'utf8' })
  // The checks after packaging fail on a builder that packed nothing, so the
  // exit code says nothing here; the captured config is the result.
  assert.ok(fs.existsSync(capture), `${nameOf(c)} handed electron-builder no config:\n${run.stdout}\n${run.stderr}`)
  return fs.readFileSync(capture, 'utf8').split(ws.top).join(ROOT)
}

/** The edits from `base` to `text` by line: `[line in base, '-' | '+', text]`, from a longest common subsequence. */
function edits(base, text) {
  const a = base.split('\n')
  const b = text.split('\n')
  const lcs = Array.from({ length: a.length + 1 }, () => new Array(b.length + 1).fill(0))
  for (let i = a.length - 1; i >= 0; i--) {
    for (let j = b.length - 1; j >= 0; j--) lcs[i][j] = a[i] === b[j] ? lcs[i + 1][j + 1] + 1 : Math.max(lcs[i + 1][j], lcs[i][j + 1])
  }
  const out = []
  let i = 0
  let j = 0
  while (i < a.length || j < b.length) {
    if (i < a.length && j < b.length && a[i] === b[j]) { i++; j++ } else if (j < b.length && (i === a.length || lcs[i][j + 1] >= lcs[i + 1][j])) out.push([i, '+', b[j++]])
    else out.push([i++, '-', a[i - 1]])
  }
  return out
}

function applied(base, ops) {
  const a = base.split('\n')
  const out = []
  let i = 0
  for (const [at, op, line] of ops) {
    while (i < at) out.push(a[i++])
    if (op === '+') out.push(line)
    else i++
  }
  while (i < a.length) out.push(a[i++])
  return out.join('\n')
}

test('build.mjs resolves the same electron-builder config for every combination', { skip: process.platform === 'win32' && 'the stand-in builder is a POSIX script' }, () => {
  const base = fs.readFileSync(path.join(DESKTOP, 'electron-builder.yml'), 'utf8')
  // An update keeps the old record of what this host cannot render, so updating
  // without `lipo` does not drop the helper's entries.
  const expected = updating && !fs.existsSync(FIXTURE) ? {} : JSON.parse(fs.readFileSync(FIXTURE, 'utf8'))
  // A helper only reaches a config where `lipo` can read it.
  const lipo = process.platform === 'darwin'
  const ws = workspace()
  const recorded = {}
  try {
    for (const c of combos()) {
      const name = nameOf(c)
      if (c.helper && !lipo) {
        if (updating && expected[name]) recorded[name] = expected[name]
        continue
      }
      const text = render(ws, c)
      if (updating) recorded[name] = edits(base, text)
      else {
        assert.ok(expected[name], `no recorded config for ${name}`)
        assert.equal(text, applied(base, expected[name]), name)
      }
    }
  } finally {
    fs.rmSync(ws.top, { recursive: true, force: true })
  }
  if (updating) {
    fs.mkdirSync(path.dirname(FIXTURE), { recursive: true })
    fs.writeFileSync(FIXTURE, `${JSON.stringify(recorded, null, 1)}\n`)
  }
})

test('the stored edits rebuild what they were taken from', () => {
  const base = 'a\nb\nc\nd\n'
  for (const text of ['a\nb\nc\nd\n', 'x\na\nc\nd\ny\n', 'a\nb\nb\nc\n', '']) assert.equal(applied(base, edits(base, text)), text)
})
