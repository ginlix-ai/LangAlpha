#!/usr/bin/env node
/**
 * The tree the OSS desktop edition runs on the user's machine: Python with the
 * locked dependencies, the backend's source, the web app, Postgres and Valkey.
 *
 *   node scripts/bundle-backend.mjs [--out <dir>]
 *
 * Written to dist/backend/<platform>-<arch>/ and picked up by scripts/build.mjs
 * through `extraResources`. It builds only for the machine it runs on: a Python
 * environment resolved for one architecture and shipped in the other's bundle
 * fails on launch, and nothing about that failure points back here, so the
 * release workflow runs this once per architecture on a runner of that
 * architecture. `checkTreeArch` holds the result to it.
 *
 * Nothing here is run from the tree at build time except the Python interpreter,
 * to compile bytecode. Postgres in particular is only unpacked.
 */
import { createHash, randomUUID } from 'node:crypto'
import { spawnSync } from 'node:child_process'
import {
  chmodSync, copyFileSync, createWriteStream, existsSync, lstatSync, mkdirSync, mkdtempSync,
  readdirSync, readFileSync, readlinkSync, renameSync, rmSync, symlinkSync, writeFileSync,
} from 'node:fs'
import path from 'node:path'
import { pipeline } from 'node:stream/promises'
import { Readable } from 'node:stream'
import { fileURLToPath } from 'node:url'
import { parseEnv } from 'node:util'
import { laterThan, nativeFiles, readNative, walkFiles } from './native-files.mjs'

const here = path.dirname(fileURLToPath(import.meta.url))
const desktop = path.join(here, '..')
const repo = path.join(desktop, '..')

/**
 * Python comes from whichever python-build-standalone release the running uv
 * knows for this minor, so the workflow pins uv and with it the exact build. The
 * manifest records the version that came out.
 */
const PYTHON_MINOR = '3.13'
const UV_VERSION = '0.12.24'

/**
 * The oldest macOS the app opens on: the latest floor among what it carries,
 * Postgres asking for 13.0 and the Node inside Playwright's wheel for 13.5.
 * Left to defaults, a compiler targets the macOS it runs on and uv picks the
 * newest wheel that macOS takes, so a build from a current runner refuses to
 * launch on every older one.
 */
export const MACOS_MIN = '13.5'

/**
 * zonky's embedded-postgres-binaries from Maven Central. Its macOS archives are
 * EnterpriseDB's own signed builds, the ones postgresql.org links for macOS, and
 * its Linux ones are built in that project's public CI. pgcrypto is included,
 * which the first migration needs. The package is Apache-2.0; what it carries is
 * under the PostgreSQL License, and the licenses of the libraries beside it are
 * in THIRD-PARTY.md at the top of the tree.
 *
 * Both macOS archives hold universal binaries, thinned below. Both sums are
 * pinned because a jar's name says nothing about what is in it.
 */
const POSTGRES = {
  version: '18.6.0',
  major: 18,
  jar: {
    'darwin-arm64': { name: 'darwin-arm64v8', sha256: '8f6ed6f38195b50a40712ada8b2a12060b6193c1a6977a6eb5cff053bb5a1b89' },
    'darwin-x64': { name: 'darwin-amd64', sha256: '690941008aac9ee360820fde9bb7870a4f3dcac5e9e2f9e45e42333a436ad0f9' },
    'linux-x64': { name: 'linux-amd64', sha256: '008ee189eaa7ca2b58bd01dd950291e638b998392cf982297acd4779ec898ac1' },
    'linux-arm64': { name: 'linux-arm64v8', sha256: '5f54e016723afa6213925a8acb939bac52944b1d348f65c7a20f0cd2928c31be' },
  },
  // The license asks that its notice go with every copy, and the archives carry none.
  copyright: { tag: 'REL_18_6', sha256: '3d6af92ff8a4c2cdf69afb1cf44edea727922f5cd0cf8b5f72b11cdecac8fdfd' },
}

/** BSD-licensed, and built here because the project publishes no macOS binaries. */
const VALKEY = {
  version: '9.1.3',
  sha256: '184e34ed2d9b51879b682b5b15205e2b9d677c60280e3cb453be8f520d04e293',
}

/** What the backend reads at runtime from its own tree, relative to the repo. */
const APP_PATHS = [
  'src', 'libs/market-protocol', 'plugins', 'mcp_servers', 'workflows', 'migrations',
  'alembic.ini', 'agent_config.yaml', 'config.yaml', 'server.py', 'Dockerfile.sandbox', 'cli/sandbox',
  // Not read, only looked for: each plugin's `_bootstrap.py` finds the root by it.
  'pyproject.toml',
]

/** Stdlib parts nothing in the backend imports, by far the largest being its own test suite. */
const STDLIB_DROP = ['test', 'idlelib', 'turtledemo', 'ensurepip', 'lib2to3', 'tkinter', 'pydoc_data']

function log(line) {
  console.log(`[bundle] ${line}`)
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, { stdio: 'inherit', ...options })
  if (result.error) throw new Error(`could not run ${command}: ${result.error.message}`)
  if (result.status !== 0) throw new Error(`${command} ${args.join(' ')} exited with ${result.status}`)
  return result
}

function capture(command, args, options = {}) {
  const result = run(command, args, { stdio: ['ignore', 'pipe', 'inherit'], encoding: 'utf8', ...options })
  return result.stdout.trim()
}

function sha256(file) {
  return createHash('sha256').update(readFileSync(file)).digest('hex')
}

/** A download verified before it is kept, so a cached file is always a good one. */
async function fetchPinned(url, wanted, cacheDir, name) {
  const target = path.join(cacheDir, name)
  if (existsSync(target) && sha256(target) === wanted) return target
  mkdirSync(cacheDir, { recursive: true })
  log(`downloading ${url}`)
  const response = await fetch(url)
  if (!response.ok) throw new Error(`${url} answered ${response.status}`)
  // A name of this build's own, made exclusively, since builds can share the cache.
  const partial = `${target}.${randomUUID()}.partial`
  try {
    await pipeline(Readable.fromWeb(response.body), createWriteStream(partial, { flags: 'wx' }))
    const got = sha256(partial)
    if (got !== wanted) throw new Error(`${name} has sha256 ${got}, expected ${wanted}`)
    renameSync(partial, target)
  } finally {
    rmSync(partial, { force: true })
  }
  return target
}

function sizeOf(dir) {
  let bytes = 0
  for (const { stat } of walkFiles(dir)) if (stat.isFile()) bytes += stat.size
  return bytes
}

const MB = (bytes) => `${Math.round(bytes / 1e6)} MB`

/** What kind of native code a file is: 'macho', 'macho-universal', 'elf', or null. */
export function nativeKind(file) {
  return readNative(file)?.format ?? null
}

/**
 * Whether every piece of native code in `dir` runs on `arch`, as a list of the
 * ones that do not. Read off the files rather than trusted from how they were
 * made, since shipping the wrong arch is the failure nothing else would catch
 * before a user does.
 */
export function checkTreeArch(dir, arch) {
  const wrong = []
  for (const { file, native } of nativeFiles(dir)) {
    if (!native.slices.some((slice) => slice.cpu === arch)) wrong.push(file)
  }
  return wrong
}

/**
 * The Mach-O files in `dir` whose `arch` slice asks for a later macOS than
 * `min`, as `{ file, needs }`. Read off the load commands, since a binary that
 * asks for too new a system does not fail on the machine that built it.
 */
export function checkTreeMacOS(dir, min, arch) {
  const tooNew = []
  for (const { file, native } of nativeFiles(dir)) {
    if (native.format === 'elf') continue
    // A thin file's floor is its own: whether it is `arch` is checkTreeArch's question.
    const slice = native.format === 'macho' ? native.slices[0] : native.slices.findLast((s) => s.cpu === arch)
    const needs = slice?.minOS
    if (needs && laterThan(needs, min)) tooNew.push({ file, needs })
  }
  return tooNew
}

function stagePython({ tree, platform, arch, work }) {
  const os = platform === 'darwin' ? 'macos' : 'linux'
  const cpu = arch === 'arm64' ? 'aarch64' : 'x86_64'
  const libc = platform === 'darwin' ? 'none' : 'gnu'
  const uvVersion = capture('uv', ['--version']).split(/\s+/)[1]
  if (uvVersion !== UV_VERSION) {
    const message = `uv ${uvVersion} is running, and releases are built with ${UV_VERSION}, which decides the Python patch release`
    if (process.env.CI) throw new Error(message)
    log(`WARNING: ${message}`)
  }
  const installDir = path.join(work, 'python-install')
  const request = `cpython-${PYTHON_MINOR}-${os}-${cpu}-${libc}`
  // No links on the PATH: they would point into the work folder, removed below.
  run('uv', ['python', 'install', '--no-bin', '--install-dir', installDir, request], {
    env: { ...process.env, UV_PYTHON_INSTALL_DIR: installDir },
  })
  const installed = readdirSync(installDir).find((d) => d.startsWith(`cpython-${PYTHON_MINOR}`) && lstatSync(path.join(installDir, d)).isDirectory())
  if (!installed) throw new Error(`uv installed nothing matching ${request} into ${installDir}`)
  const prefix = path.join(tree, 'python')
  // A real copy of a real directory. uv makes symlinks beside it for `minor`
  // names that point back at this one, and those must not come along.
  renameSync(path.join(installDir, installed), prefix)

  const python = path.join(prefix, 'bin', `python${PYTHON_MINOR}`)
  const version = capture(python, ['-c', 'import platform; print(platform.python_version())'])
  log(`python ${version} (${installed})`)

  // The lock, with hashes, and not a venv: a venv's activation and `pyvenv.cfg`
  // carry the absolute path it was made at, while this prefix runs from wherever
  // the app was dropped. The workspace member has no hash to check, so it follows
  // separately from the checkout.
  const requirements = path.join(work, 'requirements.txt')
  run('uv', [
    'export', '--project', repo, '--frozen', '--no-dev', '--no-emit-project', '--no-emit-workspace',
    // Claude Code mode's SDK, whose own CLI `prunePython` takes out again: the
    // backend downloads the pinned release once a user connects Claude Code.
    '--extra', 'claude-code',
    '--format', 'requirements-txt', '--no-header', '--output-file', requirements, '--quiet',
  ])
  // A path dependency stays in the export whatever the flags say, as a line with
  // no hash, which `--require-hashes` then refuses.
  writeFileSync(requirements, localRequirementsRemoved(readFileSync(requirements, 'utf8')))
  const pipEnv = { ...process.env, UV_PYTHON_DOWNLOADS: 'never', VIRTUAL_ENV: '' }
  // `--no-config`: uv reads settings from any pyproject.toml above the working
  // directory, and the project's overrides would come in as unhashed requirements.
  const pip = ['pip', 'install', '--no-config', '--python', python, '--break-system-packages', '--no-deps']
  // Wheels for MACOS_MIN rather than for the macOS doing the build.
  if (platform === 'darwin') {
    pip.push('--python-platform', `${cpu}-apple-darwin`)
    pipEnv.MACOSX_DEPLOYMENT_TARGET = MACOS_MIN
  }
  run('uv', [...pip, '--require-hashes', '-r', requirements], { cwd: work, env: pipEnv })
  run('uv', [...pip, ...pathSources(readFileSync(path.join(repo, 'pyproject.toml'), 'utf8')).map((at) => path.join(repo, at))], { cwd: work, env: pipEnv })

  prunePython(prefix)
  return { python, version }
}

/** The packages `[tool.uv.sources]` takes from a folder of the checkout, which the export leaves out. */
export function pathSources(pyproject) {
  const table = pyproject.match(/^\[tool\.uv\.sources\]\n([\s\S]*?)(?=^\[|(?![\s\S]))/m)
  if (!table) return []
  return [...table[1].matchAll(/^\s*[A-Za-z0-9._-]+\s*=\s*\{[^}]*\bpath\s*=\s*"([^"]+)"/gm)].map((m) => m[1])
}

export function localRequirementsRemoved(text) {
  return text
    .split('\n')
    .filter((line) => !/^(-e\s|\.{1,2}\/|file:|[A-Za-z0-9._-]+\s*@\s*file:)/.test(line))
    .join('\n')
}

function prunePython(prefix) {
  const lib = path.join(prefix, 'lib', `python${PYTHON_MINOR}`)
  const before = sizeOf(prefix)
  for (const name of STDLIB_DROP) rmSync(path.join(lib, name), { recursive: true, force: true })
  for (const entry of readdirSync(lib)) {
    if (entry.startsWith('config-')) rmSync(path.join(lib, entry), { recursive: true, force: true })
  }
  for (const entry of readdirSync(path.join(prefix, 'lib'))) {
    if (/^(tcl|tk|itcl|thread)\d/i.test(entry) || /^lib(tcl|tk)/.test(entry)) {
      rmSync(path.join(prefix, 'lib', entry), { recursive: true, force: true })
    }
  }
  rmSync(path.join(prefix, 'include'), { recursive: true, force: true })
  rmSync(path.join(prefix, 'share'), { recursive: true, force: true })
  // Console scripts carry the absolute path of the interpreter that installed
  // them, so here they would be dangling pointers into the build machine.
  // Everything runs as `python -m` or by file.
  const bin = path.join(prefix, 'bin')
  for (const entry of readdirSync(bin)) {
    if (!/^python(3(\.\d+)?)?$/.test(entry)) rmSync(path.join(bin, entry), { force: true })
  }
  // `tests` by exact name only, and only under site-packages, where no package
  // imports its own suite at runtime. `test`, `testing` and `_testing` stay:
  // those names are public or internal modules in several of them.
  const site = path.join(lib, 'site-packages')
  const dropTests = (dir) => {
    for (const entry of readdirSync(dir)) {
      const full = path.join(dir, entry)
      const stat = lstatSync(full)
      if (!stat.isDirectory() || stat.isSymbolicLink()) continue
      if (entry === 'tests' || entry === '__pycache__') rmSync(full, { recursive: true, force: true })
      else dropTests(full)
    }
  }
  dropTests(site)
  // The CLI the SDK's wheel carries is about 300 MB nobody who leaves Claude
  // Code alone needs. The backend fetches the same pinned version when a user
  // connects it (`cli_install.worker_cli`), and never looks for the machine's own.
  rmSync(path.join(site, 'claude_agent_sdk', '_bundled'), { recursive: true, force: true })
  // Where the workspace member was installed from: a path on the build machine.
  for (const entry of readdirSync(site)) {
    // Installing packages is uv's job at build time and nobody's at runtime.
    if (entry === 'pip' || /^pip-.*\.dist-info$/.test(entry)) rmSync(path.join(site, entry), { recursive: true, force: true })
    else if (entry.endsWith('.dist-info')) rmSync(path.join(site, entry, 'direct_url.json'), { force: true })
  }
  log(`python: pruned ${MB(before)} to ${MB(sizeOf(prefix))}`)
}

/**
 * Bytecode, compiled once here. `unchecked-hash` because the sources' timestamps
 * do not survive a disk image, and a timestamped .pyc that no longer matches is
 * one Python recompiles on every launch and, being in a read-only bundle, can
 * never save. Without this each launch pays for it again, in the server and in
 * each of its MCP subprocesses.
 */
function compileBytecode(python, tree, dirs) {
  // `-s` keeps the build machine's paths out of the files. Python reports the real
  // source path at import either way.
  const result = spawnSync(python, ['-m', 'compileall', '-q', '-j', '0', '-s', tree, '--invalidation-mode', 'unchecked-hash', ...dirs], {
    stdio: ['ignore', 'ignore', 'inherit'],
    env: { PATH: process.env.PATH },
  })
  // A file that is not valid Python for this version (templates, a package's py2
  // fixtures) is only a file that will not be precompiled. Nothing imports those.
  if (result.status !== 0) log('compileall reported files it could not compile; those stay source-only')
}

/**
 * The commit the app was copied from, and whether the copy differed from it.
 * Someone debugging an install reads the source it was built from, and a build
 * from an edited checkout would otherwise send them to the wrong one.
 */
function sourceOf() {
  const commit = capture('git', ['-C', repo, 'rev-parse', 'HEAD'])
  const changes = capture('git', ['-C', repo, 'status', '--porcelain', '--', ...APP_PATHS], {
    maxBuffer: 64 * 1024 * 1024,
  })
  return { commit, modified: changes.length > 0 }
}

/** The tracked files under APP_PATHS, plus new ones, and nothing gitignored: no overlay, .env or stray .pyc. */
function stageApp({ tree }) {
  const app = path.join(tree, 'app')
  // NUL-separated, since git otherwise quotes a name with non-ASCII in it, which
  // then names no file and would be skipped.
  const listed = capture('git', ['-C', repo, 'ls-files', '-z', '-co', '--exclude-standard', '--', ...APP_PATHS], {
    maxBuffer: 64 * 1024 * 1024,
  }).split('\0').filter(Boolean)
  let copied = 0
  for (const rel of listed) {
    const from = path.join(repo, rel)
    if (!existsSync(from)) continue
    const stat = lstatSync(from)
    // Only keeps a folder in git. electron-builder would drop it anyway, and the
    // comparison after packaging would then count it as a file that went missing.
    if (stat.isDirectory() || path.basename(rel) === '.gitkeep') continue
    const to = path.join(app, rel)
    mkdirSync(path.dirname(to), { recursive: true })
    if (stat.isSymbolicLink()) symlinkSync(readlinkSync(from), to)
    else copyFileSync(from, to)
    copied++
  }
  log(`app: ${copied} files`)
  return app
}

function stageCli({ app }) {
  const cli = path.join(repo, 'cli')
  run('pnpm', ['install', '--frozen-lockfile'], { cwd: cli })
  run('pnpm', ['run', 'build'], { cwd: cli })
  mkdirSync(path.join(app, 'cli', 'dist'), { recursive: true })
  copyFileSync(path.join(cli, 'dist', 'la.mjs'), path.join(app, 'cli', 'dist', 'la.mjs'))
}

/**
 * The web app for serving from the backend's own origin: OSS mode, with an empty
 * API base so every request is same-origin. A local `web/.env` is deployment
 * configuration for that checkout's own dev server, so anything it sets is
 * blanked rather than baked into a build for somebody else.
 */
function stageWeb({ tree }) {
  const web = path.join(repo, 'web')
  const out = path.join(tree, 'web')
  run('pnpm', ['install', '--frozen-lockfile'], { cwd: web })
  const blank = {}
  for (const entry of readdirSync(web)) {
    if (!entry.startsWith('.env') || entry === '.env.example') continue
    // Parsed as vite parses it, `export NAME=` included.
    for (const name of Object.keys(parseEnv(readFileSync(path.join(web, entry), 'utf8')))) {
      if (name.startsWith('VITE_')) blank[name] = ''
    }
  }
  // The shell's own VITE_ variables are the same kind of configuration, and vite
  // reads them over any file (VITE_CDN_BASE would become the build's base).
  const shell = Object.fromEntries(Object.entries(process.env).filter(([name]) => !name.startsWith('VITE_')))
  run('pnpm', ['exec', 'vite', 'build', '--outDir', out, '--emptyOutDir'], {
    cwd: web,
    env: { ...shell, ...blank, VITE_HOST_MODE: 'oss', VITE_API_BASE_URL: '', NODE_ENV: 'production' },
  })
  if (!existsSync(path.join(out, 'index.html'))) throw new Error('the web build left no index.html')
  log(`web: ${MB(sizeOf(out))}`)
  // The page chartkit snapshots a chart with, shipped into each computer beside
  // it. The backend finds it through CHARTKIT_RENDER_DIR, since the chartkit the
  // tree installed carries none of its own.
  const chartRender = path.join(tree, 'chart-render')
  run('pnpm', ['run', 'build:chart-render'], {
    cwd: web,
    env: { ...shell, CHART_RENDER_OUT_DIR: chartRender, NODE_ENV: 'production' },
  })
  if (!existsSync(path.join(chartRender, 'chart-render.html'))) throw new Error('the chart render build left no chart-render.html')
}

async function stagePostgres({ tree, platform, arch, cacheDir, work }) {
  const key = `${platform}-${arch}`
  const jar = POSTGRES.jar[key]
  if (!jar) throw new Error(`no Postgres build is pinned for ${key}`)
  const artifact = `embedded-postgres-binaries-${jar.name}`
  const url = `https://repo1.maven.org/maven2/io/zonky/test/postgres/${artifact}/${POSTGRES.version}/${artifact}-${POSTGRES.version}.jar`
  const file = await fetchPinned(url, jar.sha256, cacheDir, `${artifact}-${POSTGRES.version}.jar`)

  const unpacked = path.join(work, 'postgres-jar')
  mkdirSync(unpacked, { recursive: true })
  run('unzip', ['-q', '-o', file, '-d', unpacked])
  const archive = readdirSync(unpacked).find((f) => f.endsWith('.txz'))
  if (!archive) throw new Error(`${artifact} holds no .txz`)
  const prefix = path.join(tree, 'pg')
  mkdirSync(prefix, { recursive: true })
  run('tar', ['-xf', path.join(unpacked, archive), '-C', prefix])
  // Headers and the extension-building kit are for compiling against it.
  rmSync(path.join(prefix, 'include'), { recursive: true, force: true })
  rmSync(path.join(prefix, 'lib', 'postgresql', 'pgxs'), { recursive: true, force: true })

  const { tag, sha256: wanted } = POSTGRES.copyright
  const notice = await fetchPinned(`https://raw.githubusercontent.com/postgres/postgres/${tag}/COPYRIGHT`, wanted, cacheDir, `postgres-${tag}-COPYRIGHT`)
  copyFileSync(notice, path.join(prefix, 'COPYRIGHT'))

  if (platform === 'darwin') {
    const before = sizeOf(prefix)
    thinMachO(prefix, arch)
    collapseCopies(path.join(prefix, 'lib'))
    log(`postgres: ${MB(before)} to ${MB(sizeOf(prefix))}`)
  }
}

/** One architecture out of each universal binary; each slice keeps its own signature. */
function thinMachO(dir, arch) {
  // Listed before any is rewritten, so a `.thin` file is never read as one of them.
  const universal = [...nativeFiles(dir)].filter(({ native }) => native.format === 'macho-universal')
  for (const { file, stat } of universal) {
    const thin = `${file}.thin`
    run('lipo', [file, '-thin', LIPO_ARCH[arch], '-output', thin])
    chmodSync(thin, stat.mode)
    renameSync(thin, file)
  }
}

const LIPO_ARCH = { arm64: 'arm64', x64: 'x86_64' }

/**
 * The archive stores each library's version aliases as full copies, 64 MB of ICU
 * data three times over, so identical files become symlinks to one of them.
 * Beside it in the same directory, which is the only kind of link a sealed
 * bundle and a read-only mount both leave alone.
 */
function collapseCopies(dir) {
  const seen = new Map()
  const files = readdirSync(dir)
    .map((name) => path.join(dir, name))
    .filter((file) => lstatSync(file).isFile())
    // Longest first: `libicudata.77.1.dylib` is the real name the others alias.
    .sort((a, b) => b.length - a.length)
  for (const file of files) {
    const digest = sha256(file)
    const kept = seen.get(digest)
    if (!kept) {
      seen.set(digest, file)
      continue
    }
    rmSync(file)
    symlinkSync(path.basename(kept), file)
  }
}

/**
 * Compiled on every run. Only the tarball is cached, because `fetchPinned` can
 * check that against its pin, and nothing could check a binary a cache handed back.
 */
async function stageValkey({ tree, cacheDir, work }) {
  const url = `https://github.com/valkey-io/valkey/archive/refs/tags/${VALKEY.version}.tar.gz`
  const tarball = await fetchPinned(url, VALKEY.sha256, cacheDir, `valkey-${VALKEY.version}.tar.gz`)
  const source = path.join(work, 'valkey')
  mkdirSync(source, { recursive: true })
  run('tar', ['-xzf', tarball, '-C', source, '--strip-components', '1'])
  // libc's allocator and no TLS: it serves one client over loopback, and
  // leaving out jemalloc and OpenSSL leaves no vendored build to keep working.
  run('make', [`-j${navigatorCores()}`, 'BUILD_TLS=no', 'USE_SYSTEMD=no', 'MALLOC=libc', 'valkey-server'], {
    cwd: path.join(source, 'src'),
    stdio: ['ignore', 'ignore', 'inherit'],
    env: process.platform === 'darwin' ? { ...process.env, MACOSX_DEPLOYMENT_TARGET: MACOS_MIN } : process.env,
  })
  const target = path.join(tree, 'valkey', 'bin', 'valkey-server')
  mkdirSync(path.dirname(target), { recursive: true })
  copyFileSync(path.join(source, 'src', 'valkey-server'), target)
  // Its license asks that a binary carry the notice.
  copyFileSync(path.join(source, 'COPYING'), path.join(tree, 'valkey', 'COPYING'))
  chmodSync(target, 0o755)
}

function navigatorCores() {
  return Math.max(2, (globalThis.navigator?.hardwareConcurrency || 4))
}

/**
 * Names the migrations this tree carries. A launch compares it with the digest
 * of the last set that ran against the data folder: a difference is an upgrade,
 * and an upgrade is what gets a snapshot to fall back on.
 */
function migrationsDigest(app) {
  const versions = path.join(app, 'migrations', 'versions')
  const hash = createHash('sha256')
  for (const name of readdirSync(versions).filter((f) => f.endsWith('.py')).sort()) {
    hash.update(name)
    hash.update(readFileSync(path.join(versions, name)))
  }
  return hash.digest('hex')
}

function writeNotices(tree, pythonVersion) {
  writeFileSync(path.join(tree, 'THIRD-PARTY.md'), `# Bundled software

- CPython ${pythonVersion}, python-build-standalone: PSF-2.0. \`python/\`, with the packages in
  \`python/lib/python${PYTHON_MINOR}/site-packages\` under their own licenses, each in its \`.dist-info\`.
- PostgreSQL ${POSTGRES.version}: PostgreSQL License, its notice in \`pg/COPYRIGHT\`. \`pg/\`, packaged by zonkyio/embedded-postgres-binaries
  (Apache-2.0). The libraries in \`pg/lib\`: OpenSSL (Apache-2.0), ICU (Unicode-3.0), MIT Kerberos (MIT),
  libxml2 (MIT), zlib (Zlib), zstd (BSD-3-Clause), LZ4 (BSD-2-Clause), libedit (BSD-3-Clause),
  libiconv and gettext (LGPL-2.1, dynamically linked).
- Valkey ${VALKEY.version}: BSD-3-Clause, its notice in \`valkey/COPYING\`. \`valkey/\`, built from the project's source tag.
`)
}

/**
 * What electron-builder.yml needs for a build that carries `tree`: `edits`, each
 * `{ marker, replacement, what }` for build.mjs to apply in order, and `mac`, the
 * lines that go under `mac:`. A build that carries a tree is also signed by
 * scripts/sign-mac.mjs, which build.mjs decides with the helper's need for it.
 */
export function bundleConfigEdits({ tree, hostArch, feed, platform = process.platform }) {
  // Quoted, since a ` #` in the path would start a YAML comment.
  const edits = [{
    marker: /^files:\r?$/m,
    replacement: ['extraResources:', `  - from: ${JSON.stringify(tree)}`, '    to: backend', '    filter: ["**/*"]', 'files:'].join('\n'),
    what: 'files',
  }]
  if (platform !== 'darwin') return { edits, mac: [] }
  // The tree is one architecture's, so the app holding it has to be. An
  // architecture flag on the command line wins over the one written here, which
  // is why verifyPackedTrees checks the app as well as the tree. The release workflow builds
  // the other architecture on a runner of that architecture.
  edits.push({ marker: /^ {6}arch: \[arm64, x64\]\r?$/gm, replacement: `      arch: [${hostArch}]`, what: 'arch: [arm64, x64]' })
  // The zip exists for the updater, and with no feed there is no updater to read
  // it: only a second copy of well over a gigabyte to compress and upload.
  if (!feed) edits.push({ marker: /^ {4}- target: zip[^\n]*\n {6}arch: \[[a-z0-9]+\]\r?\n/m, replacement: '', what: 'zip target' })
  // Better than the default on a tree this size, and any supported macOS reads it.
  edits.push({ marker: /^dmg:\r?$/m, replacement: 'dmg:\n  format: ULFO', what: 'dmg:' })
  // What the bundled Postgres is built for, a release later than Electron's own floor.
  return { edits, mac: [`  minimumSystemVersion: "${MACOS_MIN}"`] }
}

/**
 * What reached the package, checked rather than what went in: every packed copy
 * of `tree` holds as many files, all its native code and the app's own
 * executables are `arch`, and on macOS none of it asks for more than MACOS_MIN.
 * A tree that did not arrive whole, or an app of another architecture around it
 * (one asked for on electron-builder's command line, in any of its forms),
 * installs fine and fails on first launch. Throws on the first problem and
 * returns the file count.
 */
/** The native executables of the app that holds a packed tree: `Contents/MacOS`, or an unpacked Linux app's top level. */
function appExecutables(packedTree, platform) {
  const top = platform === 'darwin' ? path.join(packedTree, '..', '..', 'MacOS') : path.join(packedTree, '..', '..')
  if (!existsSync(top)) return []
  return readdirSync(top)
    .map((entry) => path.join(top, entry))
    .filter((file) => lstatSync(file).isFile() && readNative(file))
}

export function verifyPackedTrees(packed, tree, { platform = process.platform, arch = process.arch, name = (dir) => dir } = {}) {
  if (packed.length === 0) throw new Error('the bundled backend was requested but no package holds it')
  const filesIn = (dir) => (existsSync(dir) ? [...walkFiles(dir)].map(({ file }) => path.relative(dir, file)).sort() : [])
  const staged = filesIn(tree)
  const expected = staged.length
  for (const dir of packed) {
    const got = new Set(filesIn(dir))
    const missing = staged.find((file) => !got.has(file))
    if (missing || got.size !== expected) {
      throw new Error(`${name(dir)} holds ${got.size} files, and the tree has ${expected}${missing ? `; ${missing} is missing` : ''}`)
    }
    const wrong = checkTreeArch(dir, arch)
    if (wrong.length > 0) throw new Error(`${name(dir)} holds code that is not ${arch}, starting with ${wrong[0]}`)
    const shell = appExecutables(dir, platform).find((file) => !readNative(file).slices.every((slice) => slice.cpu === arch))
    if (shell) throw new Error(`the app around ${name(dir)} is not ${arch}, starting with ${shell}`)
    const tooNew = platform === 'darwin' ? checkTreeMacOS(dir, MACOS_MIN, arch) : []
    if (tooNew.length > 0) {
      throw new Error(`${name(dir)} holds ${tooNew.length} files that need a macOS later than ${MACOS_MIN}, starting with ${tooNew[0].file} (${tooNew[0].needs})`)
    }
  }
  return expected
}

export function defaultTreeDir(platform = process.platform, arch = process.arch) {
  return path.join(desktop, 'dist', 'backend', `${platform}-${arch}`)
}

// Left in a tree while it is built, so a build cut short is known for its own.
const BUILDING_MARK = '.langalpha-bundle-building'

const inside = (dir, parent) => {
  const rel = path.relative(parent, dir)
  return !rel.startsWith('..') && !path.isAbsolute(rel)
}

/** Whether `tree` holds a finished bundle, by its manifest rather than by folder names any project has. */
function finishedBundle(tree) {
  try {
    const manifest = JSON.parse(readFileSync(path.join(tree, 'manifest.json'), 'utf8'))
    return manifest.bundle === 1 && typeof manifest.migrationsDigest === 'string'
  } catch {
    return false
  }
}

/**
 * The tree is emptied before a build, so its place must be one only a build
 * fills: inside the checkout only under `desktop/dist`, never a folder holding
 * the checkout, and never a folder with files in it that no build left.
 */
export function refuseToReplace(tree) {
  if (inside(here, tree)) throw new Error(`${tree} holds the source, so it cannot be the output`)
  if (inside(tree, repo) && !inside(tree, path.join(desktop, 'dist'))) {
    throw new Error(`${tree} is in the source; an output inside the checkout goes under desktop/dist`)
  }
  if (!existsSync(tree) || readdirSync(tree).length === 0) return
  if (!existsSync(path.join(tree, BUILDING_MARK)) && !finishedBundle(tree)) {
    throw new Error(`${tree} holds files a bundle did not put there; name an empty or new folder`)
  }
}

/** Builds the tree and returns where it went. */
export async function bundleBackend({ out, cacheDir = process.env.DESKTOP_BUNDLE_CACHE || path.join(desktop, 'dist', 'bundle-cache') } = {}) {
  const { platform, arch } = process
  if (platform === 'win32') throw new Error('the desktop backend is not bundled on Windows')
  const tree = out || defaultTreeDir(platform, arch)

  // DESKTOP_BUNDLE_REUSE keeps the last tree, for re-running a package step
  // without rebuilding what it holds. Never a release: it trusts what is there.
  if (process.env.DESKTOP_BUNDLE_REUSE === '1' && existsSync(path.join(tree, 'manifest.json'))) {
    log(`reusing ${tree}`)
    return tree
  }

  refuseToReplace(tree)
  rmSync(tree, { recursive: true, force: true })
  mkdirSync(tree, { recursive: true })
  writeFileSync(path.join(tree, BUILDING_MARK), '')
  const work = mkdtempSync(path.join(path.dirname(tree), '.bundle-work-'))
  try {
    const { python, version } = stagePython({ tree, platform, arch, work })
    const app = stageApp({ tree })
    stageCli({ app })
    stageWeb({ tree })
    await stagePostgres({ tree, platform, arch, cacheDir, work })
    await stageValkey({ tree, cacheDir, work })

    if (process.env.DESKTOP_BUNDLE_NO_BYTECODE !== '1') {
      compileBytecode(python, tree, [path.join(tree, 'python', 'lib', `python${PYTHON_MINOR}`), path.join(app, 'src'), path.join(app, 'plugins'), path.join(app, 'mcp_servers')])
    }
    writeNotices(tree, version)
    writeFileSync(path.join(tree, 'manifest.json'), `${JSON.stringify({
      bundle: 1,
      platform,
      arch,
      python: { version, minor: PYTHON_MINOR },
      postgres: { version: POSTGRES.version, major: POSTGRES.major },
      valkey: { version: VALKEY.version },
      migrationsDigest: migrationsDigest(app),
      source: sourceOf(),
    }, null, 2)}\n`)
    // The manifest says from here on that the tree is a bundle's, and the mark would ship.
    rmSync(path.join(tree, BUILDING_MARK))

    // Writable, so codesign can replace what it signs; nothing depends on the
    // read-only modes the Python install came with.
    run('chmod', ['-R', 'u+w', tree])

    const wrong = checkTreeArch(tree, arch)
    if (wrong.length > 0) {
      throw new Error(`${wrong.length} native files in the tree are not ${arch}, starting with ${path.relative(tree, wrong[0])}`)
    }
    for (const part of ['python', 'app', 'web', 'pg', 'valkey']) log(`${part}: ${MB(sizeOf(path.join(tree, part)))}`)
    log(`tree: ${MB(sizeOf(tree))} at ${tree}`)
    return tree
  } finally {
    rmSync(work, { recursive: true, force: true })
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const at = process.argv.indexOf('--out')
  bundleBackend({ out: at > 0 ? path.resolve(process.argv[at + 1]) : undefined }).catch((err) => {
    console.error(`[bundle] ${err.message}`)
    process.exit(1)
  })
}
