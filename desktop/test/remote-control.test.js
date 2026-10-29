'use strict'

// Remote control hands this Mac to a server, so most of what is worth testing
// here is what must be REFUSED or must NOT happen: a URL that is not a pairing
// script, a page that is not the app, a confirmation the user declined, a state
// file left over from some other pairing, a Mac left half turned off. None of
// those can be produced on demand against a real login item, and this suite
// never touches one: the login item is the stub's, and every folder is a temp dir.

const { test, describe, before, after, beforeEach } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { spawn, spawnSync } = require('node:child_process')
const { pathToFileURL } = require('node:url')
const {
  loadShell, cleanup, opened, tempDir, loginItems, resetLoginItems, messageBoxes, setMessageBox, ipcHandlers,
  electronStub,
} = require('./helpers')
const state = require('../src/remoteControlState.js')

after(cleanup)

const TOKEN = '0123456789abcdef0123456789abcdef'
// Shaped like production: the pairing is served from the API host, which is not
// the host the app is on (https://app.example.com in the saas test config).
const SCRIPT = `https://api.example.com/api/v1/computers/ssh/pairings/${TOKEN}/script`
const FAST = { poll: 5, start: 150, pair: 400, exit: 150 }
const ACCOUNT = os.userInfo().username
// The folder's files as each side writes them, shared with the helper's tests.
const CONTRACT = path.join(__dirname, '..', '..', 'helper', 'contract')
const contract = (name) => fs.readFileSync(path.join(CONTRACT, name), 'utf8')
// This Mac's hardware UUID, as the helper writes it to the folder it made.
const THIS_MAC = contract('machine').trim()
const scripts = (name) => import(pathToFileURL(path.join(__dirname, '..', 'scripts', name)).href)

let rc
before(() => { ({ remoteControl: rc } = loadShell({ edition: 'saas' })) })

beforeEach(() => {
  resetLoginItems()
  setMessageBox(0)
  messageBoxes.length = 0
  opened.length = 0
})

/** A remote control on a Mac that can run it, over a folder this suite owns. */
function onMac(overrides = {}) {
  const root = tempDir('la-remote-')
  const agentPlist = path.join(root, 'agent.plist')
  fs.writeFileSync(agentPlist, '')
  return rc.createRemoteControl({
    dir: path.join(root, 'Application Support', 'LangAlpha', 'helper'),
    platform: 'darwin',
    systemVersion: '14.5.0',
    agentPlist,
    timing: FAST,
    machine: THIS_MAC,
    ...overrides,
  })
}

/** Written the way the helper writes it: whole, by rename. */
function writeState(dir, fields) {
  fs.mkdirSync(dir, { recursive: true })
  const text = Object.entries(fields).map(([key, value]) => `${key}=${value}`).join('\n')
  fs.writeFileSync(path.join(dir, '.state.tmp'), `${text}\n`)
  fs.renameSync(path.join(dir, '.state.tmp'), path.join(dir, 'state'))
}

/**
 * The helper's half of the folder, as the contract has it: claim the pairing by
 * renaming it to `pairing.taken`, say `running` under its id, then say how it
 * ended and let the claim go. Answers with a stop.
 */
function fakeHelper(dir, { outcome = 'done', detail = '' } = {}) {
  let ending
  const write = (fields) => {
    try {
      writeState(dir, fields)
    } catch {
      // The test is over and took the folder with it.
    }
  }
  const poll = setInterval(() => {
    const taken = path.join(dir, 'pairing.taken')
    let id
    try {
      fs.renameSync(path.join(dir, 'pairing'), taken)
      id = /^id=(.*)$/m.exec(fs.readFileSync(taken, 'utf8'))[1]
    } catch {
      return
    }
    write({ state: 'connecting', pairing_id: id, pairing: 'running' })
    if (outcome === 'running') return
    const paired = outcome === 'done'
    ending = setTimeout(() => {
      write({
        state: paired ? 'connected' : 'unpaired',
        pairing_id: id,
        pairing: outcome,
        pairing_detail: detail,
        tunnel_id: paired ? 'tun_0123' : '',
        fingerprint: 'SHA256:abcDEF012+/',
      })
      fs.rmSync(taken, { force: true })
    }, 20)
  }, 10)
  return () => {
    clearInterval(poll)
    clearTimeout(ending)
  }
}

/**
 * What `promise` settles to before the next turn of the event loop. Under mocked
 * timers a sleep ends only when the test ticks it, so a call that waited on
 * anything is still pending here, and this fails it rather than hanging.
 */
async function promptly(promise) {
  const waiting = Symbol('waiting')
  const result = await Promise.race([promise, new Promise((resolve) => setImmediate(resolve, waiting))])
  assert.notEqual(result, waiting, 'it waited')
  return result
}

/** A process that stays up for `ms`, standing in for the helper launchd stops. */
function helperProcess(t, ms = 30_000) {
  const child = spawn(process.execPath, ['-e', `setTimeout(() => {}, ${ms})`], { stdio: 'ignore' })
  t.after(() => child.kill())
  return child
}

const DEAD_PID = '999999'
const assertDead = () => assert.throws(() => process.kill(Number(DEAD_PID), 0), { code: 'ESRCH' }, `pid ${DEAD_PID} exists here`)

describe('which Macs can run it', () => {
  test('macOS 13 and up, from a bundle that carries the agent', () => {
    for (const [mac, reason] of [
      [{ platform: 'linux', systemVersion: '14.5', packaged: true }, 'platform'],
      [{ platform: 'win32', systemVersion: '14.5', packaged: true }, 'platform'],
      [{ platform: 'darwin', systemVersion: '12.7.4', packaged: true }, 'os_version'],
      [{ platform: 'darwin', systemVersion: '', packaged: true }, 'os_version'],
      [{ platform: 'darwin', systemVersion: '13.0', packaged: false }, 'not_packaged'],
      [{ platform: 'darwin', systemVersion: '13.0', packaged: true }, null],
    ]) assert.equal(state.unsupportedReason(mac), reason, JSON.stringify(mac))
  })

  test('the bundle is asked whether it carries the plist', () => {
    assert.equal(onMac({ agentPlist: null }).status().reason, 'not_packaged')
    assert.equal(onMac({ agentPlist: '/nonexistent/agent.plist' }).status().reason, 'not_packaged')
    assert.equal(onMac().status().supported, true)
    // An unpackaged run is Electron's own app, with no plist of ours to find.
    assert.equal(electronStub.app.isPackaged, false)
    assert.equal(rc.createRemoteControl({ dir: tempDir('la-remote-'), platform: 'darwin', systemVersion: '15.1' })
      .status().reason, 'not_packaged')
  })

  // A dev run shares D with the installed app of its edition. Reporting that
  // app's pairing would badge a row this shell cannot pause or turn off.
  test('where it cannot run, every answer says so and nothing is read or touched', async () => {
    const remote = onMac({ platform: 'linux' })
    writeState(remote.dir, { state: 'connected', tunnel_id: 'tun_1', fingerprint: 'SHA256:x' })
    const expected = {
      supported: false, reason: 'platform', service: 'off', helper: null, paired: false, fingerprint: null, account: ACCOUNT,
    }
    assert.deepEqual(remote.status(), expected)
    assert.deepEqual(await remote.enable(SCRIPT), { ok: false, error: 'unsupported', status: expected })
    assert.deepEqual(await remote.disable(), expected)
    assert.deepEqual(remote.pause(), expected)
    assert.deepEqual(remote.resume(), expected)
    assert.equal(remote.menuView(), null)
    remote.watch()
    remote.unwatch()
    assert.equal(messageBoxes.length, 0)
    assert.deepEqual(loginItems.calls, [])
    assert.deepEqual(fs.readdirSync(remote.dir), ['state'])
  })
})

describe('what the status says', () => {
  test('the login item, as on, off or waiting for approval', () => {
    for (const [item, service] of [
      ['enabled', 'on'], ['requires-approval', 'needs_approval'], ['not-registered', 'off'], ['not-found', 'off'],
      [undefined, 'off'],
    ]) assert.equal(state.serviceOf(item), service, item)
  })

  // The helper says `stopped` when launchd stops it. A SIGKILL or a crash says
  // nothing, so its last state stays behind and only its pid gives it away.
  test('the helper, the pairing and the key, from the item and the state file', () => {
    const paired = { tunnel_id: 'tun_1', fingerprint: 'SHA256:abc' }
    const alive = () => true
    const dead = () => false
    const status = (fields) => ({ supported: true, helper: null, paired: true, fingerprint: 'SHA256:abc', ...fields })
    for (const [why, args, expected] of [
      ['nothing yet', ['on', null, alive], { service: 'on', paired: false, fingerprint: null }],
      ['connected', ['on', { ...paired, state: 'connected' }, alive], status({ service: 'on', helper: 'connected' })],
      ['awaiting approval', ['needs_approval', { ...paired, state: 'connecting' }, alive],
        status({ service: 'needs_approval', helper: 'connecting' })],
      ['paused', ['off', { ...paired, state: 'connected' }, alive], status({ service: 'off' })],
      ['stopped', ['on', { ...paired, state: 'stopped', pid: '4242' }, alive], status({ service: 'on' })],
      ['a dead pid', ['on', { ...paired, state: 'connected', pid: '4242' }, dead], status({ service: 'on' })],
      ['a live pid', ['on', { ...paired, state: 'connected', pid: '4242' }, alive], status({ service: 'on', helper: 'connected' })],
      ['no usable pid', ['on', { ...paired, state: 'connected', pid: '1' }, dead], status({ service: 'on', helper: 'connected' })],
      ['a state it does not know', ['on', { state: 'exploded', tunnel_id: '' }, alive],
        { service: 'on', paired: false, fingerprint: null }],
    ]) {
      assert.deepEqual(state.toStatus(...args), { supported: true, helper: null, ...expected }, why)
    }
  })

  test('a status is the login item and the folder as they are now', () => {
    loginItems.status = 'enabled'
    const remote = onMac()
    writeState(remote.dir, { state: 'connected', tunnel_id: 'tun_1', fingerprint: 'SHA256:abc' })
    assert.deepEqual(remote.status(), {
      supported: true, service: 'on', helper: 'connected', paired: true, fingerprint: 'SHA256:abc', account: ACCOUNT,
    })
  })

  test("a folder carried over from another Mac says nothing of that Mac's pairing", () => {
    const remote = onMac()
    writeState(remote.dir, { state: 'unpaired', tunnel_id: 'tun_1', fingerprint: 'SHA256:abc' })
    fs.writeFileSync(path.join(remote.dir, 'machine'), '00000000-0000-4000-8000-00000000000B\n')
    assert.deepEqual(remote.status(), {
      supported: true, service: 'off', helper: null, paired: false, fingerprint: null, account: ACCOUNT,
    })
    // The helper made it here, or before it named a machine at all.
    fs.writeFileSync(path.join(remote.dir, 'machine'), contract('machine'))
    assert.equal(remote.status().fingerprint, 'SHA256:abc')
    fs.rmSync(path.join(remote.dir, 'machine'))
    assert.equal(remote.status().fingerprint, 'SHA256:abc')
  })

  test('a pid worth waiting on is never a group, launchd or this process', () => {
    for (const pid of ['0', '1', '-1', '01', String(process.pid), 'abc', '', undefined]) {
      assert.equal(state.pidOf({ pid }), null, `pid '${pid}'`)
    }
    assert.equal(state.pidOf(null), null)
    assert.equal(state.pidOf({ pid: '4242' }), 4242)
  })
})

describe('reading the state file', () => {
  test('lines of key=value, split on the first equals sign', () => {
    assert.deepEqual(
      state.parseState('state=refused\r\npairing_detail=Use a=b instead.\npairing=\n\nnonsense\n=orphan\nnew_key=kept\n'),
      { state: 'refused', pairing_detail: 'Use a=b instead.', pairing: '', new_key: 'kept' },
    )
  })

  test('the state the helper writes for a pairing that finished', () => {
    const parsed = state.parseState(contract('state'))
    assert.deepEqual(parsed, {
      state: 'connected',
      pairing_id: '0f1e2d3c4b5a69788796a5b4c3d2e1f0',
      pairing: 'done',
      pairing_detail: '',
      tunnel_id: '7c9e6679-7425-40de-944b-e07fc1f90ae7',
      fingerprint: 'SHA256:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU',
      pid: '4242',
    })
    assert.deepEqual(state.toStatus('on', parsed, () => true), {
      supported: true, service: 'on', helper: 'connected', paired: true, fingerprint: parsed.fingerprint,
    })
  })

  test("the state the helper writes for a pairing the deployment refused, in the deployment's words", () => {
    const parsed = state.parseState(contract('state-failed'))
    assert.deepEqual(parsed, {
      state: 'unpaired',
      pairing_id: '0f1e2d3c4b5a69788796a5b4c3d2e1f0',
      pairing: 'failed',
      pairing_detail: 'That pairing command has expired. Start again from LangAlpha.',
      tunnel_id: '',
      fingerprint: 'SHA256:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU',
      pid: '4242',
    })
    assert.equal(state.toStatus('on', parsed, () => true).paired, false)
  })
})

describe('the pairing URL', () => {
  test('a pairing on another host than the app is accepted', () => {
    assert.equal(state.scriptUrlFrom(SCRIPT), SCRIPT)
  })

  test('cleartext only where it never leaves this machine', () => {
    for (const host of ['localhost:8000', '127.0.0.1:8000', '127.8.9.10', '[::1]:8000']) {
      const url = `http://${host}/api/v1/computers/ssh/pairings/${TOKEN}/script`
      assert.equal(state.scriptUrlFrom(url), url, host)
    }
    for (const host of ['api.example.com', '192.168.1.20:8000', '10.0.0.5', 'wt3.localhost', '[::2]']) {
      assert.equal(state.scriptUrlFrom(`http://${host}/api/v1/computers/ssh/pairings/${TOKEN}/script`), null, host)
    }
  })

  test('the form the helper is handed is the one that was checked', () => {
    assert.equal(
      state.scriptUrlFrom(`HTTPS://API.EXAMPLE.COM:443/api/v1/computers/ssh/pairings/${TOKEN.toUpperCase()}/script`),
      `https://api.example.com/api/v1/computers/ssh/pairings/${TOKEN.toUpperCase()}/script`,
    )
  })

  test('a token is hex, 16 to 128 characters', () => {
    const at = (token) => state.scriptUrlFrom(`https://api.example.com/api/v1/computers/ssh/pairings/${token}/script`)
    assert.ok(at('a'.repeat(16)))
    assert.ok(at('a'.repeat(128)))
    assert.equal(at('a'.repeat(15)), null)
    assert.equal(at('a'.repeat(129)), null)
    assert.equal(at(`${TOKEN.slice(1)}g`), null)
    assert.equal(at(`${TOKEN.slice(0, 16)}%2f${TOKEN.slice(16)}`), null)
  })

  test('anything else is refused', () => {
    const base = `https://api.example.com/api/v1/computers/ssh/pairings/${TOKEN}`
    for (const url of [
      `https://user:secret@api.example.com/api/v1/computers/ssh/pairings/${TOKEN}/script`,
      `https://user@api.example.com/api/v1/computers/ssh/pairings/${TOKEN}/script`,
      `${base}/script?next=/`,
      `${base}/script?`,
      `${base}/script#x`,
      `${base}/script#`,
      base,
      `${base}/script/`,
      `${base}/script/more`,
      `${base}/key`,
      `https://api.example.com/prefix/api/v1/computers/ssh/pairings/${TOKEN}/script`,
      `ftp://api.example.com/api/v1/computers/ssh/pairings/${TOKEN}/script`,
      `file:///api/v1/computers/ssh/pairings/${TOKEN}/script`,
      'javascript:alert(1)',
      'not a url',
      '',
    ]) assert.equal(state.scriptUrlFrom(url), null, url)
    for (const value of [undefined, null, 42, {}, [SCRIPT]]) assert.equal(state.scriptUrlFrom(value), null)
  })
})

describe('asking the helper to pair', () => {
  test('exactly the file the helper reads', () => {
    const dir = path.join(tempDir('la-remote-'), 'helper')
    rc.writePairing(
      dir,
      'https://example.com/api/v1/computers/ssh/pairings/0123456789abcdef0123456789abcdef/script',
      '0f1e2d3c4b5a69788796a5b4c3d2e1f0',
    )
    assert.equal(fs.readFileSync(path.join(dir, 'pairing'), 'utf8'), contract('pairing'))
  })

  test('one file, written whole, readable by this account alone', () => {
    const dir = path.join(tempDir('la-remote-'), 'LangAlpha', 'helper')
    rc.writePairing(dir, SCRIPT, 'f'.repeat(32))
    assert.equal(fs.statSync(path.join(dir, 'pairing')).mode & 0o777, 0o600)
    assert.equal(fs.statSync(dir).mode & 0o777, 0o700)
    assert.deepEqual(fs.readdirSync(dir), ['pairing'], 'a staged file was left behind')
  })

  test('a folder that already existed is narrowed, and a second pairing replaces the first', () => {
    const dir = path.join(tempDir('la-remote-'), 'helper')
    fs.mkdirSync(dir, { mode: 0o755 })
    fs.chmodSync(dir, 0o755)
    rc.writePairing(dir, SCRIPT, '1'.repeat(32))
    rc.writePairing(dir, SCRIPT, '2'.repeat(32))
    assert.equal(fs.statSync(dir).mode & 0o777, 0o700)
    assert.match(fs.readFileSync(path.join(dir, 'pairing'), 'utf8'), /^id=2{32}$/m)
    assert.deepEqual(fs.readdirSync(dir), ['pairing'])
  })
})

describe('turning it on', () => {
  test('a pairing that finishes turns the service on and reports the paired Mac', async (t) => {
    const remote = onMac()
    t.after(fakeHelper(remote.dir))
    const result = await remote.enable(SCRIPT)
    assert.equal(result.ok, true, JSON.stringify(result))
    assert.equal(result.status.service, 'on')
    assert.equal(result.status.helper, 'connected')
    assert.equal(result.status.paired, true)
    assert.equal(result.status.fingerprint, 'SHA256:abcDEF012+/')
    assert.deepEqual(loginItems.calls, [
      { openAtLogin: true, type: 'agentService', serviceName: 'ai.langalpha.desktop.helper.plist' },
    ])
  })

  test('the user is asked first, shown the server and the account, with Cancel the default', async (t) => {
    const remote = onMac()
    t.after(fakeHelper(remote.dir))
    await remote.enable(SCRIPT)
    assert.equal(messageBoxes.length, 1)
    const options = messageBoxes[0].at(-1)
    assert.match(options.detail, /This Mac will connect to api\.example\.com\./)
    assert.ok(options.message.includes(ACCOUNT))
    assert.ok(options.detail.includes(ACCOUNT))
    assert.equal(options.buttons[options.defaultId], 'Cancel')
    assert.equal(options.cancelId, options.defaultId)
  })

  test('declining writes no pairing and registers nothing', async () => {
    setMessageBox(1)
    const remote = onMac()
    const result = await remote.enable(SCRIPT)
    assert.equal(result.ok, false)
    assert.equal(result.error, 'cancelled')
    assert.equal(fs.existsSync(remote.dir), false)
    assert.deepEqual(loginItems.calls, [])
  })

  test('a page asking twice at once gets one dialog, not two', async () => {
    setMessageBox(1)
    const remote = onMac()
    const results = await Promise.all([remote.enable(SCRIPT), remote.enable(SCRIPT)])
    assert.deepEqual(results.map((r) => r.error), ['cancelled', 'cancelled'])
    assert.equal(messageBoxes.length, 1)
  })

  test('nothing is asked or written for a URL that is not a pairing', async () => {
    const remote = onMac()
    assert.equal((await remote.enable(`http://api.example.com/api/v1/computers/ssh/pairings/${TOKEN}/script`)).error, 'bad_url')
    assert.equal(messageBoxes.length, 0)
    assert.equal(fs.existsSync(remote.dir), false)
    assert.deepEqual(loginItems.calls, [])
  })

  // Script running in the hosted app, an injected one included, could otherwise
  // point this Mac at a server of its choosing, with one dialog in the way.
  test('a hosted build asks nothing about a server that is not its own', async () => {
    const remote = onMac()
    const elsewhere = `https://elsewhere.example/api/v1/computers/ssh/pairings/${TOKEN}/script`
    assert.equal((await remote.enable(elsewhere)).error, 'bad_url')
    assert.equal(messageBoxes.length, 0)
    assert.equal(fs.existsSync(remote.dir), false)
    assert.deepEqual(loginItems.calls, [])
  })

  test('a self-hosted build asks about whatever server it is sent', async () => {
    const { remoteControl: oss } = loadShell({ edition: 'oss' })
    setMessageBox(1)
    const root = tempDir('la-remote-')
    const agentPlist = path.join(root, 'agent.plist')
    fs.writeFileSync(agentPlist, '')
    const remote = oss.createRemoteControl({
      dir: path.join(root, 'helper'), platform: 'darwin', systemVersion: '14.5.0', agentPlist, timing: FAST,
    })
    const result = await remote.enable(`https://elsewhere.example/api/v1/computers/ssh/pairings/${TOKEN}/script`)
    assert.equal(result.error, 'cancelled')
    assert.match(messageBoxes[0].at(-1).detail, /This Mac will connect to elsewhere\.example\./)
  })

  // After the yes, the login item is registered only while this Mac is paired
  // or waiting for approval. Every other ending takes it away again, with the
  // folder, so no helper is left running unpaired with nothing to stop it.
  const detail = 'This pairing has expired. Start again from LangAlpha.'
  for (const [why, helper, error] of [
    ["refused by the deployment, in the deployment's words", { outcome: 'failed', detail }, 'failed'],
    ['never picked up by the helper', null, 'not_started'],
    ['picked up and never ended', { outcome: 'running' }, 'timeout'],
  ]) {
    test(`a pairing ${why} is undone`, async (t) => {
      const remote = onMac()
      if (helper) t.after(fakeHelper(remote.dir, helper))
      const result = await remote.enable(SCRIPT)
      assert.equal(result.error, error)
      assert.equal(result.message, error === 'failed' ? detail : undefined)
      assert.deepEqual(loginItems.calls.map((c) => c.openAtLogin), [true, false])
      assert.equal(fs.existsSync(remote.dir), false)
      assert.deepEqual(
        { service: result.status.service, paired: result.status.paired },
        { service: 'off', paired: false },
      )
    })
  }

  test('a state left by another pairing does not answer for this one', async () => {
    const remote = onMac()
    fs.mkdirSync(remote.dir, { recursive: true })
    fs.writeFileSync(path.join(remote.dir, 'state'), contract('state-failed'))
    assert.equal((await remote.enable(SCRIPT)).error, 'not_started')
  })

  test('an item macOS holds for approval opens Login Items and keeps the item and the pairing', async () => {
    loginItems.onRegister = 'requires-approval'
    const remote = onMac()
    const result = await remote.enable(SCRIPT)
    assert.equal(result.error, 'needs_approval')
    assert.equal(result.status.service, 'needs_approval')
    assert.deepEqual(opened, [rc.LOGIN_ITEMS_URL])
    assert.deepEqual(loginItems.calls.map((c) => c.openAtLogin), [true])
    assert.match(fs.readFileSync(path.join(remote.dir, 'pairing'), 'utf8'), new RegExp(`^url=${SCRIPT}$`, 'm'))
  })

  test('an item already waiting for approval is not registered again', async () => {
    loginItems.status = 'requires-approval'
    const result = await onMac().enable(SCRIPT)
    assert.equal(result.error, 'needs_approval')
    assert.deepEqual(loginItems.calls, [])
  })

  test('a register that did not take is reported at once, and leaves no pairing behind', async (t) => {
    t.mock.timers.enable({ apis: ['setTimeout'] })
    loginItems.onRegister = 'not-registered'
    const remote = onMac()
    assert.equal((await promptly(remote.enable(SCRIPT))).error, 'not_started')
    assert.equal(fs.existsSync(remote.dir), false)
  })

  test('a helper already running is handed the pairing without registering again', async (t) => {
    loginItems.status = 'enabled'
    const remote = onMac()
    t.after(fakeHelper(remote.dir))
    assert.equal((await remote.enable(SCRIPT)).ok, true)
    assert.deepEqual(loginItems.calls, [])
  })

  test('turning it off while a pairing is awaited stands the wait down', async (t) => {
    t.mock.timers.enable({ apis: ['setTimeout'] })
    const remote = onMac()
    const pending = remote.enable(SCRIPT)
    // Through the dialog and into the wait.
    await new Promise(setImmediate)
    await promptly(remote.disable())
    t.mock.timers.tick(FAST.poll)
    assert.equal((await promptly(pending)).error, 'not_started')
    assert.deepEqual(loginItems.calls.map((c) => c.openAtLogin), [true, false])
  })

  // Two LangAlpha accounts can be signed in on one macOS account, and so share
  // one folder. Turning remote control on for the second must not break the
  // first's computer until the user has been told and said yes.
  const REPLACING = 'This Mac is already connected to LangAlpha, perhaps for another account. '
    + 'Continuing disconnects it, and any computer using it stops working.'

  /** A Mac paired for someone else: its state and a key of its own. */
  function pairedElsewhere(remote) {
    writeState(remote.dir, { state: 'connected', tunnel_id: 'tun_other', fingerprint: 'SHA256:other', pid: DEAD_PID })
    fs.writeFileSync(path.join(remote.dir, 'host_key'), 'the other pairing')
  }

  /** Records, at each register and unregister, what the folder held. */
  function watchLoginItem(t, dir) {
    const real = electronStub.app.setLoginItemSettings
    const seen = []
    electronStub.app.setLoginItemSettings = (settings) => {
      seen.push({
        openAtLogin: settings.openAtLogin,
        pairing: fs.existsSync(path.join(dir, 'pairing')),
        oldKey: fs.existsSync(path.join(dir, 'host_key')),
      })
      return real(settings)
    }
    t.after(() => { electronStub.app.setLoginItemSettings = real })
    return seen
  }

  test('a Mac already paired is said so, and torn down after the yes and before the new pairing', async (t) => {
    assertDead()
    loginItems.status = 'enabled'
    const remote = onMac()
    pairedElsewhere(remote)
    const seen = watchLoginItem(t, remote.dir)
    t.after(fakeHelper(remote.dir))

    const result = await remote.enable(SCRIPT)
    assert.equal(result.ok, true, JSON.stringify(result))
    assert.ok(messageBoxes[0].at(-1).detail.endsWith(`\n\n${REPLACING}`), messageBoxes[0].at(-1).detail)
    // Off with no new pairing written yet; on with the old folder gone and the
    // new pairing in its place.
    assert.deepEqual(seen, [
      { openAtLogin: false, pairing: false, oldKey: true },
      { openAtLogin: true, pairing: true, oldKey: false },
    ])
    assert.equal(result.status.fingerprint, 'SHA256:abcDEF012+/')
  })

  test('a Mac already paired is left exactly as it was when the user cancels', async () => {
    setMessageBox(1)
    loginItems.status = 'enabled'
    const remote = onMac()
    pairedElsewhere(remote)
    const before = fs.readFileSync(path.join(remote.dir, 'state'), 'utf8')

    const result = await remote.enable(SCRIPT)
    assert.equal(result.error, 'cancelled')
    assert.ok(messageBoxes[0].at(-1).detail.endsWith(REPLACING))
    assert.deepEqual(loginItems.calls, [])
    assert.equal(loginItems.status, 'enabled')
    assert.deepEqual(fs.readdirSync(remote.dir).sort(), ['host_key', 'state'])
    assert.equal(fs.readFileSync(path.join(remote.dir, 'state'), 'utf8'), before)
  })

  test('an old pairing whose item will not go keeps working: not replaced, and nothing else touched', async (t) => {
    const real = electronStub.app.setLoginItemSettings
    t.after(() => { electronStub.app.setLoginItemSettings = real })
    for (const [how, unregister] of Object.entries({
      throws: () => { throw new Error('SMAppService said no') },
      'does not take': () => {},
    })) {
      resetLoginItems()
      loginItems.status = 'enabled'
      electronStub.app.setLoginItemSettings = (settings) => (settings.openAtLogin ? real(settings) : unregister())
      const remote = onMac()
      pairedElsewhere(remote)
      const result = await remote.enable(SCRIPT)
      assert.equal(result.error, 'not_replaced', how)
      assert.equal('message' in result, false, how)
      assert.equal(loginItems.status, 'enabled', how)
      assert.deepEqual(loginItems.calls, [], `${how}: registered after all`)
      assert.deepEqual(fs.readdirSync(remote.dir).sort(), ['host_key', 'state'], how)
    }
  })

  // A folder that cannot be renamed or deleted: its parent will not let go of it.
  test('an old pairing whose folder will not go is put back as it was: not replaced, and on again only if it was on', async (t) => {
    for (const [was, calls] of [['enabled', [false, true]], ['not-registered', []]]) {
      resetLoginItems()
      loginItems.status = was
      const remote = onMac()
      pairedElsewhere(remote)
      const parent = path.dirname(remote.dir)
      fs.chmodSync(parent, 0o500)
      t.after(() => fs.chmodSync(parent, 0o755))
      const result = await remote.enable(SCRIPT)
      fs.chmodSync(parent, 0o755)
      assert.equal(result.error, 'not_replaced', was)
      assert.equal('message' in result, false, was)
      assert.deepEqual(loginItems.calls.map((c) => c.openAtLogin), calls, was)
      assert.equal(loginItems.status, was)
      assert.deepEqual(fs.readdirSync(remote.dir).sort(), ['host_key', 'state'], `${was}: the old pairing was cut into`)
    }
  })

  test('a pairing still waiting for approval is not a paired Mac: it is replaced, nothing torn down', async () => {
    loginItems.status = 'requires-approval'
    const remote = onMac()
    writeState(remote.dir, { state: 'unpaired', tunnel_id: '' })
    rc.writePairing(remote.dir, SCRIPT, '1'.repeat(32))
    const result = await remote.enable(SCRIPT)
    assert.equal(result.error, 'needs_approval')
    assert.ok(!messageBoxes[0].at(-1).detail.includes('already connected'))
    assert.deepEqual(loginItems.calls, [])
    assert.ok(fs.existsSync(path.join(remote.dir, 'state')))
    assert.doesNotMatch(fs.readFileSync(path.join(remote.dir, 'pairing'), 'utf8'), /^id=1{32}$/m)
  })
})

test("another Mac's pairing carried over is not a paired Mac: no warning, nothing torn down", async () => {
  loginItems.status = 'requires-approval'
  const remote = onMac()
  writeState(remote.dir, { state: 'connected', tunnel_id: 'tun_1', fingerprint: 'SHA256:abc' })
  fs.writeFileSync(path.join(remote.dir, 'machine'), '00000000-0000-4000-8000-00000000000B\n')
  const result = await remote.enable(SCRIPT)
  assert.equal(result.error, 'needs_approval')
  assert.ok(!messageBoxes[0].at(-1).detail.includes('already connected'))
  assert.deepEqual(loginItems.calls, [])
  assert.ok(fs.existsSync(path.join(remote.dir, 'pairing')))
})

describe('turning it off', () => {
  test('the item goes, the helper is let exit, then the folder goes', async (t) => {
    t.mock.timers.enable({ apis: ['setTimeout'] })
    loginItems.status = 'enabled'
    const remote = onMac()
    const child = helperProcess(t)
    writeState(remote.dir, { state: 'connected', tunnel_id: 'tun_1', pid: String(child.pid) })

    const pending = remote.disable()
    await new Promise(setImmediate)
    assert.deepEqual(loginItems.calls, [
      { openAtLogin: false, type: 'agentService', serviceName: 'ai.langalpha.desktop.helper.plist' },
    ])
    assert.ok(fs.existsSync(path.join(remote.dir, 'state')), 'the folder went while the helper was still running')

    const exited = new Promise((resolve) => child.on('exit', resolve))
    child.kill()
    await exited
    t.mock.timers.tick(FAST.poll)
    const status = await promptly(pending)
    assert.equal(fs.existsSync(remote.dir), false)
    assert.equal(status.service, 'off')
    assert.equal(status.paired, false)
  })

  test('a helper that will not exit holds the delete only so long', async (t) => {
    loginItems.status = 'enabled'
    const remote = onMac()
    const child = helperProcess(t)
    writeState(remote.dir, { state: 'connected', pid: String(child.pid) })
    const began = Date.now()
    await remote.disable()
    assert.ok(Date.now() - began >= FAST.exit)
    assert.equal(fs.existsSync(remote.dir), false)
  })

  test('a helper that says it stopped is not waited on to exit', async (t) => {
    t.mock.timers.enable({ apis: ['setTimeout'] })
    loginItems.status = 'enabled'
    const remote = onMac()
    const child = helperProcess(t)
    writeState(remote.dir, { state: 'connected', tunnel_id: 'tun_1', pid: String(child.pid) })
    const pending = remote.disable()
    await new Promise(setImmediate)
    // Its last words, written after launchd's SIGTERM and before it exits.
    writeState(remote.dir, { state: 'stopped', tunnel_id: 'tun_1', pid: String(child.pid) })
    t.mock.timers.tick(FAST.poll)
    await promptly(pending)
    assert.equal(fs.existsSync(remote.dir), false)
  })

  test('a helper already gone, or a pid that names a group, launchd or this process, is not waited on', async (t) => {
    t.mock.timers.enable({ apis: ['setTimeout'] })
    assertDead()
    for (const pid of [DEAD_PID, '0', '1', String(process.pid)]) {
      loginItems.status = 'enabled'
      const remote = onMac()
      writeState(remote.dir, { state: 'connected', tunnel_id: 'tun_1', pid })
      await promptly(remote.disable())
      assert.equal(fs.existsSync(remote.dir), false, pid)
    }
  })

  test('an item that will not go keeps its folder', async (t) => {
    const real = electronStub.app.setLoginItemSettings
    electronStub.app.setLoginItemSettings = () => {}
    t.after(() => { electronStub.app.setLoginItemSettings = real })
    loginItems.status = 'enabled'
    const remote = onMac()
    writeState(remote.dir, { state: 'connected', tunnel_id: 'tun_1' })
    const status = await remote.disable()
    assert.deepEqual({ service: status.service, paired: status.paired }, { service: 'on', paired: true })
    assert.ok(fs.existsSync(path.join(remote.dir, 'state')))
  })

  test('twice is the same as once', async () => {
    loginItems.status = 'enabled'
    const remote = onMac()
    writeState(remote.dir, { state: 'connected', tunnel_id: 'tun_1' })
    await remote.disable()
    const again = await remote.disable()
    assert.equal(again.paired, false)
    assert.equal(loginItems.calls.length, 1, 'unregistered an item that was already gone')
  })
})

describe('the menu', () => {
  test('pause takes the item away and keeps the pairing; resume puts it back', () => {
    loginItems.status = 'enabled'
    const remote = onMac()
    writeState(remote.dir, { state: 'connected', tunnel_id: 'tun_1' })
    assert.equal(remote.pause().service, 'off')
    assert.ok(fs.existsSync(path.join(remote.dir, 'state')))
    assert.equal(remote.status().paired, true)
    assert.equal(remote.resume().service, 'on')
    assert.deepEqual(loginItems.calls.map((c) => c.openAtLogin), [false, true])
  })

  test('a resume macOS holds for approval opens Login Items', () => {
    loginItems.onRegister = 'requires-approval'
    assert.equal(onMac().resume().service, 'needs_approval')
    assert.deepEqual(opened, [rc.LOGIN_ITEMS_URL])
  })

  // A switch wherever a helper is paired or its item is on, so one a pairing
  // left running (the app quit mid-wait) can still be stopped from here.
  test('it shows the switch wherever there is something to stop, and says what it is doing', () => {
    const on = { supported: true, service: 'on', paired: true, helper: null }
    for (const [why, status, view] of [
      ['no status', null, null],
      ['unsupported', { ...on, supported: false }, null],
      ['nothing paired, nothing on', { ...on, paired: false, service: 'off' }, null],
      ['paused', { ...on, service: 'off' }, { line: 'Paused', paused: true }],
      ['awaiting approval', { ...on, service: 'needs_approval' }, { line: 'Allow in System Settings', paused: false }],
      ['awaiting approval, unpaired', { ...on, paired: false, service: 'needs_approval' },
        { line: 'Allow in System Settings', paused: false }],
      ['on', { ...on, helper: 'connected' }, { line: 'On', paused: false }],
      ['connecting', { ...on, helper: 'connecting' }, { line: 'Connecting', paused: false }],
      ['between helpers', on, { line: 'Connecting', paused: false }],
      ['paired, its helper just started', { ...on, helper: 'unpaired' }, { line: 'Connecting', paused: false }],
      ['refused', { ...on, helper: 'refused' }, { line: 'Not connected', paused: false }],
      ['on and unpaired', { ...on, paired: false, helper: 'unpaired' }, { line: 'Not connected', paused: false }],
    ]) assert.deepEqual(state.menuView(status), view, why)
  })

  test('it is told of a change, and only of a change', async () => {
    loginItems.status = 'enabled'
    let changes = 0
    const remote = onMac({ onMenuChange: () => { changes += 1 } })
    writeState(remote.dir, { state: 'connected', tunnel_id: 'tun_1' })
    assert.deepEqual(remote.menuView(), { line: 'On', paused: false })

    remote.pause()
    assert.equal(changes, 1)
    remote.pause()
    assert.equal(changes, 1, 'a pause that changed nothing')
    remote.resume()
    assert.equal(changes, 2)
    setMessageBox(1)
    await remote.enable(SCRIPT)
    assert.equal(changes, 2, 'a cancelled enable')
    await remote.disable()
    assert.equal(changes, 3)
    assert.equal(remote.menuView(), null)
  })

  test('watching hears what the helper changed on its own, until it stops', (t) => {
    t.mock.timers.enable({ apis: ['setInterval'] })
    loginItems.status = 'enabled'
    let changes = 0
    const remote = onMac({ onMenuChange: () => { changes += 1 } })
    writeState(remote.dir, { state: 'connected', tunnel_id: 'tun_1' })
    remote.menuView()
    remote.watch(1000)
    t.after(remote.unwatch)

    writeState(remote.dir, { state: 'connecting', tunnel_id: 'tun_1' })
    t.mock.timers.tick(1000)
    assert.equal(changes, 1)
    t.mock.timers.tick(1000)
    assert.equal(changes, 1, 'told again of the same view')
    writeState(remote.dir, { state: 'connected', tunnel_id: 'tun_1' })
    remote.unwatch()
    t.mock.timers.tick(1000)
    assert.equal(changes, 1, 'still watching')
  })

  // Approved in System Settings once the enable that wrote the pairing had
  // ended, perhaps after the app quit: the helper starts, the pairing fails.
  const FAILED = { state: 'unpaired', pairing_id: 'p1', pairing: 'failed', pairing_detail: 'This pairing has expired.' }

  test('watching turns off a helper left on with nothing paired, from the start', async (t) => {
    t.mock.timers.enable({ apis: ['setInterval'] })
    // The second has restarted since, which forgets how the pairing ended.
    for (const fields of [FAILED, { state: 'unpaired' }]) {
      resetLoginItems()
      loginItems.status = 'enabled'
      const remote = onMac()
      writeState(remote.dir, fields)
      remote.watch(1000)
      await new Promise(setImmediate)
      remote.unwatch()
      assert.deepEqual(loginItems.calls.map((c) => c.openAtLogin), [false], fields.state)
      assert.equal(fs.existsSync(remote.dir), false, fields.state)
    }
  })

  test('watching leaves a helper paired, one with a pairing to take, and one held for approval', async (t) => {
    t.mock.timers.enable({ apis: ['setInterval'] })
    for (const [why, item, fields, file] of [
      ['paired, its re-pair failed', 'enabled', { ...FAILED, state: 'connected', tunnel_id: 'tun_1' }],
      ['paired, its tunnel not yet held', 'enabled', { state: 'unpaired' }, 'paired'],
      ['a newer pairing waiting', 'enabled', FAILED, 'pairing'],
      ['a pairing claimed', 'enabled', FAILED, 'pairing.taken'],
      ['still held for approval', 'requires-approval', FAILED],
      ['pairing now', 'enabled', { ...FAILED, pairing: 'running' }],
    ]) {
      resetLoginItems()
      loginItems.status = item
      const remote = onMac()
      writeState(remote.dir, fields)
      if (file) fs.writeFileSync(path.join(remote.dir, file), '')
      remote.watch(1000)
      t.mock.timers.tick(1000)
      await new Promise(setImmediate)
      remote.unwatch()
      assert.deepEqual(loginItems.calls, [], why)
      assert.ok(fs.existsSync(path.join(remote.dir, 'state')), why)
    }
  })

  test('the app menu carries it under the app name, and its switch reaches the handler', () => {
    const real = Object.getOwnPropertyDescriptor(process, 'platform')
    Object.defineProperty(process, 'platform', { value: 'darwin' })
    try {
      const { buildMenu } = require('../src/menu.js')
      const calls = []
      const handlers = { onPauseRemoteControl: () => calls.push('pause'), onResumeRemoteControl: () => calls.push('resume') }
      const remoteMenu = (view) => buildMenu({ isSaas: true, remoteControl: view, ...handlers })[0].submenu
        .find((item) => item.label === 'Remote Control')

      assert.equal(remoteMenu(null), undefined)
      const on = remoteMenu({ line: 'On', paused: false }).submenu
      assert.deepEqual(on[0], { label: 'On', enabled: false })
      assert.equal(on.at(-1).label, 'Pause Remote Control')
      on.at(-1).click()
      const paused = remoteMenu({ line: 'Paused', paused: true }).submenu
      assert.equal(paused.at(-1).label, 'Resume Remote Control')
      paused.at(-1).click()
      assert.deepEqual(calls, ['pause', 'resume'])
    } finally {
      Object.defineProperty(process, 'platform', real)
    }
  })
})

describe('who may ask', () => {
  /** What `invoke` from a page at `url`, running as `origin`, reaches main as. */
  function from(url, origin = URL.canParse(url) ? new URL(url).origin : 'null') {
    const win = new electronStub.BrowserWindow()
    win.loadURL(url)
    win.webContents.mainFrame = { origin }
    return { win, event: { sender: win.webContents, senderFrame: win.webContents.mainFrame } }
  }

  test('only the app: not the console, not anything else the shell renders', async () => {
    const remote = onMac()
    writeState(remote.dir, { state: 'connected', tunnel_id: 'tun_1' })
    rc.registerIpc(remote)
    const status = ipcHandlers.get('shell:remote-control-status')
    const enable = ipcHandlers.get('shell:remote-control-enable')
    const disable = ipcHandlers.get('shell:remote-control-disable')

    assert.equal(status(from('https://app.example.com/chat').event).supported, true)
    for (const url of ['https://platform.example.com/account', 'https://evil.example.com/', 'file:///tmp/x.html', '']) {
      const { event } = from(url)
      assert.equal(status(event), null, url)
      assert.equal(await enable(event, SCRIPT), null, url)
      assert.equal(await disable(event), null, url)
    }
    assert.equal(messageBoxes.length, 0)
    assert.ok(fs.existsSync(path.join(remote.dir, 'state')), 'a refused caller deleted the folder')
  })

  test("not a document at the app's address that runs as no origin, nor a frame inside the app", async () => {
    const remote = onMac()
    rc.registerIpc(remote)
    const status = ipcHandlers.get('shell:remote-control-status')
    assert.equal(status(from('https://app.example.com/api/v1/files/report.html', 'null').event), null)
    const { event } = from('https://app.example.com/chat')
    assert.equal(status({ ...event, senderFrame: { origin: 'https://app.example.com' } }), null)
    assert.equal(status({ ...event, senderFrame: null }), null)
  })

  test('the confirmation is attached to the asking window', async () => {
    setMessageBox(1)
    rc.registerIpc(onMac())
    const { win, event } = from('https://app.example.com/chat')
    const result = await ipcHandlers.get('shell:remote-control-enable')(event, SCRIPT)
    assert.equal(result.error, 'cancelled')
    assert.equal(messageBoxes[0][0], win)
  })
})

// The plist, the folder and the service name are spelled in three places that
// never read each other: scripts/launch-agent.mjs writes the plist, the helper
// reads its folder from the plist's arguments, and src/remoteControl.js asks
// macOS about the plist by name and reads the same folder from the home
// directory. scripts/helper-bundle.mjs puts both files in the bundle. A drift
// between any two is a login item that never answers, or a helper and an app
// watching different folders.
describe('the launch agent each edition packages', () => {
  const valuesOf = (plist) => {
    const out = {}
    for (const [, key, body] of plist.matchAll(/<key>([^<]+)<\/key>\s*(<string>[^<]*<\/string>|<true\/>|<array>[\s\S]*?<\/array>)/g)) {
      if (body === '<true/>') out[key] = true
      else if (body.startsWith('<array>')) out[key] = [...body.matchAll(/<string>([^<]*)<\/string>/g)].map((m) => m[1])
      else out[key] = /<string>([^<]*)<\/string>/.exec(body)[1]
    }
    return out
  }

  for (const edition of ['saas', 'oss']) {
    test(`the ${edition} plist is the one its app asks macOS about, naming the folder its app reads`, async () => {
      const { launchAgentPlist, HELPER_BINARY } = await scripts('launch-agent.mjs')
      const { macHelperConfigLines } = await scripts('helper-bundle.mjs')
      const { config, remoteControl } = loadShell({ edition })
      const plist = launchAgentPlist({ label: config.helperLabel, productName: config.appName })
      const values = valuesOf(plist)

      assert.deepEqual(values, {
        Label: config.helperLabel,
        BundleProgram: `Contents/MacOS/${HELPER_BINARY}`,
        ProgramArguments: [
          HELPER_BINARY, 'run',
          '--dir', `~/Library/Application Support/${config.appName}/helper`,
          '--log', `~/Library/Logs/${config.appName}/helper.log`,
        ],
        RunAtLoad: true,
        KeepAlive: true,
        AbandonProcessGroup: true,
        ProcessType: 'Interactive',
      })
      const dirArg = values.ProgramArguments[values.ProgramArguments.indexOf('--dir') + 1]
      assert.equal(path.join(os.homedir(), dirArg.slice(2)), remoteControl.helperDir())

      // Into `Contents/`: the binary where BundleProgram resolves it, the plist
      // where SMAppService looks for the name the app asks about.
      const lines = macHelperConfigLines({ label: config.helperLabel, source: '/s', plist: '/p', signer: '/x' })
      const to = lines.filter((line) => line.trimStart().startsWith('to:')).map((line) => JSON.parse(line.split('to: ')[1]))
      assert.deepEqual(to.map((rel) => `Contents/${rel}`), [
        values.BundleProgram, `Contents/Library/LaunchAgents/${remoteControl.SERVICE_NAME}`,
      ])

      if (process.platform === 'darwin') {
        const file = path.join(tempDir('la-plist-'), remoteControl.SERVICE_NAME)
        fs.writeFileSync(file, plist)
        const lint = spawnSync('plutil', ['-lint', file], { encoding: 'utf8' })
        assert.equal(lint.status, 0, lint.stdout + lint.stderr)
      }
    })
  }

  test('the mac block carries the signer and both files, every path quoted', async () => {
    const { macHelperConfigLines } = await scripts('helper-bundle.mjs')
    assert.deepEqual(macHelperConfigLines({
      label: 'ai.example.helper',
      source: '/build # 2/langalpha-helper',
      plist: '/dist/launch-agent/ai.example.helper.plist',
      signer: '/desktop/scripts/sign-mac.mjs',
    }), [
      'mac:',
      '  sign: "/desktop/scripts/sign-mac.mjs"',
      '  extraFiles:',
      '    - from: "/build # 2/langalpha-helper"',
      '      to: "MacOS/langalpha-helper"',
      '    - from: "/dist/launch-agent/ai.example.helper.plist"',
      '      to: "Library/LaunchAgents/ai.example.helper.plist"',
    ])
  })

  test('a helper that cannot ship says why', async () => {
    const { helperProblem } = await scripts('helper-bundle.mjs')
    const dir = tempDir('la-helper-')
    const file = path.join(dir, 'langalpha-helper')
    assert.match(helperProblem(file), /^there is no helper at .*langalpha-helper$/)
    fs.writeFileSync(file, '#!/bin/sh\n', { mode: 0o644 })
    assert.match(helperProblem(file), /langalpha-helper is not executable$/)
    fs.chmodSync(file, 0o755)
    assert.match(helperProblem(file), /langalpha-helper is not a universal binary \(no arm64 or x86_64\)$/)
  })
})

// electron-builder gives every nested Mach-O `entitlementsInherit`, the JIT pair
// Electron's helpers need. The helper takes logins from the network and must not
// carry it.
describe('how the bundle is signed', () => {
  const APP = '/build/dist/oss/mac-arm64/LangAlpha OSS.app'
  // What electron-builder answers for any file but the app itself, as here.
  const builderOptions = () => ({
    entitlements: '/res/entitlements.mac.plist',
    hardenedRuntime: true,
    timestamp: 'http://timestamp.example.com',
    requirements: undefined,
    additionalArguments: ['--deep-verify-off'],
  })

  test('the helper is signed with no entitlements, and everything else electron-builder resolved', async () => {
    const { helperSignArgs, HELPER_ENTITLEMENTS } = await scripts('sign-mac.mjs')
    const opts = { app: APP, identity: 'ABCDEF', keychain: '/tmp/k.keychain', optionsForFile: builderOptions }

    assert.deepEqual(helperSignArgs(opts), [
      '--sign', 'ABCDEF', '--force', '--keychain', '/tmp/k.keychain',
      '--timestamp=http://timestamp.example.com', '--options', 'runtime', '--deep-verify-off',
      '--entitlements', HELPER_ENTITLEMENTS, `${APP}/Contents/MacOS/langalpha-helper`,
    ])
    assert.throws(() => helperSignArgs({ ...opts, identity: undefined }), /no signing identity/)
  })

  test("electron-builder's signer skips the helper and keeps every other file and skip", async () => {
    const { withoutHelper } = await scripts('sign-mac.mjs')
    const opts = { app: APP, identity: 'ABCDEF', optionsForFile: builderOptions, ignore: (file) => file.endsWith('.DS_Store') }
    const signed = withoutHelper(opts)

    assert.equal(signed.identity, 'ABCDEF')
    assert.equal(signed.optionsForFile, builderOptions)
    assert.equal(typeof signed.ignore, 'function')
    assert.equal(signed.ignore(`${APP}/Contents/MacOS/langalpha-helper`), true)
    assert.equal(signed.ignore(`${APP}/Contents/Resources/.DS_Store`), true)
    for (const file of [
      APP,
      `${APP}/Contents/MacOS/LangAlpha OSS`,
      `${APP}/Contents/Frameworks/Electron Framework.framework/Versions/A/Electron Framework`,
      `${APP}/Contents/Resources/langalpha-helper`,
      `${APP}/Contents/MacOS/langalpha-helper-old`,
    ]) {
      assert.equal(signed.ignore(file), false, file)
    }
    assert.equal(withoutHelper({ app: APP }).ignore(`${APP}/Contents/MacOS/LangAlpha OSS`), false)
  })

  // The real signer walks Contents/MacOS in directory order and reached the main
  // executable first, which codesign refuses while the helper beside it is
  // unsigned. Signed ad hoc, so it needs no certificate.
  test('an app with the helper beside its executable signs and verifies', { skip: process.platform !== 'darwin' }, async () => {
    const { sign } = await scripts('sign-mac.mjs')
    const app = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'la-sign-')), 'LangAlpha OSS.app')
    const macos = path.join(app, 'Contents', 'MacOS')
    fs.mkdirSync(macos, { recursive: true })
    fs.writeFileSync(path.join(app, 'Contents', 'Info.plist'), [
      '<?xml version="1.0" encoding="UTF-8"?>',
      '<plist version="1.0"><dict>',
      '<key>CFBundleExecutable</key><string>LangAlpha OSS</string>',
      '<key>CFBundleIdentifier</key><string>ai.langalpha.desktop.test</string>',
      '</dict></plist>',
    ].join('\n'))
    for (const name of ['LangAlpha OSS', 'langalpha-helper']) {
      fs.copyFileSync('/usr/bin/true', path.join(macos, name))
      assert.equal(spawnSync('codesign', ['--remove-signature', path.join(macos, name)]).status, 0)
    }
    const entitlements = path.join(__dirname, '..', 'resources', 'entitlements.mac.plist')

    await sign({
      app,
      identity: '-',
      identityValidation: false,
      platform: 'darwin',
      type: 'distribution',
      preAutoEntitlements: false,
      preEmbedProvisioningProfile: false,
      ignore: () => false,
      optionsForFile: () => ({ entitlements, hardenedRuntime: true, timestamp: 'none', additionalArguments: [] }),
    })

    const verify = spawnSync('codesign', ['--verify', '--deep', '--strict', app], { encoding: 'utf8' })
    assert.equal(verify.status, 0, verify.stderr)
    const granted = (name) =>
      spawnSync('codesign', ['-d', '--entitlements', '-', '--xml', path.join(macos, name)], { encoding: 'utf8' }).stdout
    assert.match(granted('LangAlpha OSS'), /allow-jit/)
    assert.match(granted('langalpha-helper'), /<dict><\/dict>/)
    const helper = spawnSync('codesign', ['-dv', path.join(macos, 'langalpha-helper')], { encoding: 'utf8' })
    assert.match(helper.stderr, /flags=0x\w+\([^)]*runtime/)
    fs.rmSync(path.dirname(app), { recursive: true, force: true })
  })

  test('no entitlements means an empty set, since the signer falls back to its own for none', async () => {
    const { HELPER_ENTITLEMENTS } = await scripts('sign-mac.mjs')
    const text = fs.readFileSync(HELPER_ENTITLEMENTS, 'utf8')
    assert.match(text, /<dict>\s*(<!--[\s\S]*?-->\s*)?<\/dict>/)
    if (process.platform === 'darwin') {
      const parsed = spawnSync('plutil', ['-convert', 'json', '-o', '-', HELPER_ENTITLEMENTS], { encoding: 'utf8' })
      assert.equal(parsed.status, 0, parsed.stderr)
      assert.deepEqual(JSON.parse(parsed.stdout), {})
    }
  })
})
