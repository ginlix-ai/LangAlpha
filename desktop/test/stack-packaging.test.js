'use strict'

/** Building, checking and signing the backend tree a bundled build carries. */

const { test, describe, after } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')

const { tempRoot, cleanup } = require('./stackFixture')

after(cleanup)

describe('packaging the backend', () => {
  const load = () => import(path.join(__dirname, '..', 'scripts', 'bundle-backend.mjs'))

  test('a path dependency comes out of the export, and hashed lines stay', async () => {
    const { localRequirementsRemoved } = await load()
    const text = [
      'aiohttp==3.0 \\',
      '    --hash=sha256:abc',
      '-e ./libs/market-protocol',
      './libs/market-protocol',
      'langalpha-market-protocol @ file:///x/libs/market-protocol',
      'file:///x/y',
    ].join('\n')
    assert.equal(localRequirementsRemoved(text), 'aiohttp==3.0 \\\n    --hash=sha256:abc')
  })

  test('native code is told apart by its bytes', async () => {
    const { nativeKind } = await load()
    const dir = tempRoot()
    const write = (name, bytes) => {
      const file = path.join(dir, name)
      fs.writeFileSync(file, Buffer.from(bytes))
      return file
    }
    const zeros = new Array(12).fill(0)
    assert.equal(nativeKind(write('thin', [0xcf, 0xfa, 0xed, 0xfe, ...zeros])), 'macho')
    assert.equal(nativeKind(write('fat', [0xca, 0xfe, 0xba, 0xbe, 0, 0, 0, 2, ...zeros])), 'macho-universal')
    // Same magic, but a class file version where the architecture count would be.
    assert.equal(nativeKind(write('class', [0xca, 0xfe, 0xba, 0xbe, 0, 0, 0, 65, ...zeros])), null)
    assert.equal(nativeKind(write('elf', [0x7f, 0x45, 0x4c, 0x46, ...zeros])), 'elf')
    assert.equal(nativeKind(write('py', [...Buffer.from('import os\nprint(1)\n')])), null)
  })

  test('a wrong-architecture file in the tree is found', async () => {
    const { checkTreeArch } = await load()
    const dir = tempRoot()
    const elf = Buffer.alloc(20)
    elf.writeUInt32BE(0x7f454c46, 0)
    elf.writeUInt16LE(62, 18) // x86-64
    fs.writeFileSync(path.join(dir, 'lib.so'), elf)
    assert.deepEqual(checkTreeArch(dir, 'x64'), [])
    assert.deepEqual(checkTreeArch(dir, 'arm64'), [path.join(dir, 'lib.so')])
  })

  test('the signer leaves the tree to itself and signs only Mach-O files in it', async () => {
    const { backendSignPlan, backendSignArgs, withoutHelper, PYTHON_ENTITLEMENTS, NATIVE_ENTITLEMENTS } = await import(path.join(__dirname, '..', 'scripts', 'sign-mac.mjs'))
    const app = tempRoot()
    const tree = path.join(app, 'Contents', 'Resources', 'backend')
    const write = (rel, type) => {
      const file = path.join(tree, rel)
      fs.mkdirSync(path.dirname(file), { recursive: true })
      const head = Buffer.alloc(16)
      if (type === undefined) head.write('not code at all!')
      else {
        head.writeUInt32LE(0xfeedfacf, 0)
        head.writeUInt32LE(type, 12)
      }
      fs.writeFileSync(file, head)
      return file
    }
    const python = write('python/bin/python3.13', 2)
    const ext = write('python/lib/x.so', 8)
    const postgres = write('pg/bin/postgres', 2)
    write('python/lib/thing.pyc')
    write('python/lib/obj.o', 1)

    const signed = new Map(backendSignPlan(app).map((p) => [p.file, p.entitlements]))
    assert.deepEqual([...signed.keys()].sort(), [python, ext, postgres].sort())
    assert.equal(signed.get(python), PYTHON_ENTITLEMENTS)
    assert.equal(signed.get(postgres), NATIVE_ENTITLEMENTS)
    assert.equal(signed.get(ext), null)

    const args = backendSignArgs({ identity: 'ABC' }, { file: python, entitlements: PYTHON_ENTITLEMENTS })
    assert.deepEqual(args.slice(0, 5), ['--sign', 'ABC', '--force', '--options', 'runtime'])
    assert.ok(args.includes('--timestamp'))

    const { ignore } = withoutHelper({ app, ignore: () => false })
    assert.equal(ignore(path.join(tree, 'python', 'lib', 'thing.pyc')), true)
    assert.equal(ignore(path.join(app, 'Contents', 'MacOS', 'LangAlpha OSS')), false)
  })

  test('no grant of disable-library-validation slips into the backend entitlements', () => {
    for (const name of ['entitlements.python.plist', 'entitlements.native.plist']) {
      const text = fs.readFileSync(path.join(__dirname, '..', 'resources', name), 'utf8')
      assert.ok(!/<key>[^<]*disable-library-validation/.test(text), name)
    }
  })

  test('an app of another architecture around the backend is refused', async () => {
    const { verifyPackedTrees } = await load()
    const elf = (machine) => {
      const bytes = Buffer.alloc(20)
      bytes.writeUInt32BE(0x7f454c46, 0)
      bytes.writeUInt16LE(machine, 18)
      return bytes
    }
    const tree = tempRoot()
    fs.writeFileSync(path.join(tree, 'lib.so'), elf(62))
    const app = path.join(tempRoot(), 'linux-unpacked')
    const packed = path.join(app, 'resources', 'backend')
    fs.mkdirSync(packed, { recursive: true })
    fs.writeFileSync(path.join(packed, 'lib.so'), elf(62))
    fs.writeFileSync(path.join(app, 'langalpha'), elf(62))
    assert.equal(verifyPackedTrees([packed], tree, { platform: 'linux', arch: 'x64' }), 1)
    fs.writeFileSync(path.join(app, 'langalpha'), elf(183)) // aarch64
    assert.throws(() => verifyPackedTrees([packed], tree, { platform: 'linux', arch: 'x64' }), /the app around .* is not x64/)
  })

  test('a universal app around a backend of one architecture is refused', async () => {
    const { verifyPackedTrees } = await load()
    const words = (list) => {
      const bytes = Buffer.alloc(list.length * 4)
      list.forEach((word, i) => bytes.writeUInt32BE(word, i * 4))
      return bytes
    }
    const thinArm = Buffer.alloc(32)
    thinArm.writeUInt32LE(0xfeedfacf, 0)
    thinArm.writeUInt32LE(0x0100000c, 4)
    const universal = words([0xcafebabe, 2, 0x0100000c, 0, 0, 0, 0, 0x01000007, 3, 0, 0, 0])
    const tree = tempRoot()
    fs.writeFileSync(path.join(tree, 'run.py'), 'print(1)\n')
    const contents = path.join(tempRoot(), 'LangAlpha.app', 'Contents')
    const packed = path.join(contents, 'Resources', 'backend')
    fs.mkdirSync(packed, { recursive: true })
    fs.mkdirSync(path.join(contents, 'MacOS'))
    fs.writeFileSync(path.join(packed, 'run.py'), 'print(1)\n')
    fs.writeFileSync(path.join(contents, 'MacOS', 'LangAlpha'), thinArm)
    assert.equal(verifyPackedTrees([packed], tree, { platform: 'darwin', arch: 'arm64' }), 1)
    fs.writeFileSync(path.join(contents, 'MacOS', 'LangAlpha'), universal)
    assert.throws(() => verifyPackedTrees([packed], tree, { platform: 'darwin', arch: 'arm64' }), /the app around .* is not arm64/)
  })

  test('a package missing a file is refused even when another takes its count', async () => {
    const { verifyPackedTrees } = await load()
    const tree = tempRoot()
    fs.mkdirSync(path.join(tree, 'app'))
    fs.writeFileSync(path.join(tree, 'app', 'server.py'), 'print(1)\n')
    const packed = path.join(tempRoot(), 'backend')
    fs.mkdirSync(path.join(packed, 'app'), { recursive: true })
    fs.writeFileSync(path.join(packed, 'app', 'stray.txt'), 'x\n')
    assert.throws(() => verifyPackedTrees([packed], tree, { platform: 'linux', arch: 'x64' }), /app\/server\.py is missing/)
  })

  test('the output folder is never the source, nor a folder a bundle did not fill', async () => {
    const { refuseToReplace } = await load()
    const desktop = path.join(__dirname, '..')
    assert.throws(() => refuseToReplace(desktop), /holds the source/)
    assert.throws(() => refuseToReplace(path.join(desktop, '..')), /holds the source/)
    const foreign = tempRoot()
    fs.writeFileSync(path.join(foreign, 'notes.txt'), 'mine\n')
    assert.throws(() => refuseToReplace(foreign), /did not put there/)
    // A project's folder with the names a bundle uses is still not a bundle's.
    const lookalike = tempRoot()
    for (const entry of ['app', 'python', 'pg']) fs.mkdirSync(path.join(lookalike, entry))
    fs.writeFileSync(path.join(lookalike, 'manifest.json'), '{}')
    assert.throws(() => refuseToReplace(lookalike), /did not put there/)
    // Inside the checkout, only under desktop/dist.
    assert.throws(() => refuseToReplace(path.join(desktop, '..', 'src', 'server')), /in the source/)
    assert.throws(() => refuseToReplace(path.join(desktop, 'src')), /in the source/)
    refuseToReplace(path.join(desktop, 'dist', 'backend', 'test-never-built'))
    const finished = tempRoot()
    fs.writeFileSync(path.join(finished, 'manifest.json'), JSON.stringify({ bundle: 1, migrationsDigest: 'abc' }))
    refuseToReplace(finished)
    const cutShort = tempRoot()
    fs.mkdirSync(path.join(cutShort, 'python'))
    fs.writeFileSync(path.join(cutShort, '.langalpha-bundle-building'), '')
    refuseToReplace(cutShort)
    refuseToReplace(tempRoot())
    refuseToReplace(path.join(tempRoot(), 'new'))
  })
})
