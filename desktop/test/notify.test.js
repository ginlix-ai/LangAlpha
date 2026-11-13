'use strict'

// Thread notifications (notify.js) and what needs the user (attention.js): the
// decision to post, what each platform is given, and the badge and Dock menu.
// What is worth pinning is what the OS cannot be asked for on demand: a
// notification for a thread the user is already looking at, a platform that
// cannot replace one in place.

const { test, describe, after, beforeEach } = require('node:test')
const assert = require('node:assert/strict')

const {
  cleanup, logged, notifications, FakeNotification, useNotifications, dropNotifications,
} = require('./helpers')

const settings = require('../src/desktopSettings.js')
const notify = require('../src/notify.js')
const attention = require('../src/attention.js')
const { traits } = require('../src/platformTraits.js')

const MAC = traits('darwin', {})
const WIN = traits('win32', {})
const LINUX = traits('linux', {})

after(cleanup)

describe('whether a notification is posted', () => {
  const n = { id: 't-123', kind: 'turn', title: 'Q3 review', body: 'Done.', path: '/chat/t/t-123' }
  const on = settings.normalize({})

  test('only a well-formed one is considered', () => {
    assert.deepEqual(notify.validate(n), { ...n, subtitle: undefined, silent: false })
    for (const bad of [null, 'x', { ...n, kind: 'nope' }, { ...n, id: '' }, { ...n, title: 3 }, { ...n, path: undefined }]) {
      assert.equal(notify.validate(bad), null, JSON.stringify(bad))
    }
    assert.equal(notify.validate({ ...n, body: 'x'.repeat(5000) }).body.length, 1000)
  })

  test('switched off, then unsupported, then watching', () => {
    const off = settings.merge(on, { notifications: { turn: false } })
    assert.deepEqual(notify.decide({ n, settings: off, traits: MAC, supported: true, watching: true }), { shown: false, reason: 'disabled' })
    assert.deepEqual(notify.decide({ n, settings: on, traits: MAC, supported: false, watching: true }), { shown: false, reason: 'unsupported' })
    assert.deepEqual(notify.decide({ n, settings: on, traits: MAC, supported: true, watching: true }), { shown: false, reason: 'watching' })
    assert.deepEqual(notify.decide({ n, settings: on, traits: MAC, supported: true, watching: false }), { shown: true })
  })

  test('Linux skips the start of a run, since it cannot turn it into the result', () => {
    const started = { ...n, kind: 'started' }
    assert.equal(notify.decide({ n: started, settings: on, traits: LINUX, supported: true, watching: false }).shown, false)
    assert.equal(notify.decide({ n: started, settings: on, traits: MAC, supported: true, watching: false }).shown, true)
    assert.equal(notify.decide({ n, settings: on, traits: LINUX, supported: true, watching: false }).shown, true)
  })

  test('watching means the focused main window\'s address names the thread', () => {
    assert.equal(notify.isWatching({ focused: true, url: 'https://app.example.com/chat/t/t-123', id: 't-123' }), true)
    assert.equal(notify.isWatching({ focused: false, url: 'https://app.example.com/chat/t/t-123', id: 't-123' }), false)
    assert.equal(notify.isWatching({ focused: true, url: 'https://app.example.com/chat/t/t-999', id: 't-123' }), false)
    // The query is not the thread on screen.
    assert.equal(notify.isWatching({ focused: true, url: 'https://app.example.com/chat?from=t-123', id: 't-123' }), false)
    assert.equal(notify.isWatching({ focused: true, url: '', id: 't-123' }), false)
  })

  test('each platform gets the options it can show', () => {
    const full = notify.validate({ ...n, subtitle: 'Research' })
    const mac = notify.notificationOptions(full, MAC)
    assert.equal(mac.id, 't-123')
    assert.equal(mac.subtitle, 'Research')
    assert.equal(mac.body, 'Done.')
    assert.deepEqual(mac.actions.map((a) => a.text), ['Open', 'Mark as read'])
    const win = notify.notificationOptions(full, WIN)
    assert.equal(win.id, 't-123')
    assert.equal(win.subtitle, undefined)
    assert.equal(win.body, 'Research\nDone.')
    const linux = notify.notificationOptions(full, LINUX)
    assert.deepEqual(Object.keys(linux).sort(), ['body', 'silent', 'title'])
    assert.equal(linux.body, 'Research\nDone.')
    const started = notify.notificationOptions(notify.validate({ ...n, kind: 'started', silent: true }), MAC)
    assert.deepEqual(started.actions.map((a) => a.text), ['Open'])
    assert.equal(started.silent, true)
    const question = notify.notificationOptions(notify.validate({ ...n, kind: 'needs_input' }), MAC)
    assert.deepEqual(question.actions.map((a) => a.text), ['Open'], 'a question offered Mark as read')
  })
})

describe('the notifier', () => {
  const on = settings.normalize({})
  const n = { id: 't-1', kind: 'wakeup', title: 'Wakeup', body: 'Checked.', path: '/chat/t/t-1' }

  const posted = notifications
  beforeEach(useNotifications)
  after(dropNotifications)

  test('a click opens the path, Mark as read reports back, Open opens', () => {
    const opened = []
    const marked = []
    const notifier = notify.createNotifier({ traits: MAC, onOpen: (p) => opened.push(p), onMarkRead: (a) => marked.push(a) })
    assert.deepEqual(notifier.notify(n, { settings: on, watching: false }), { shown: true })
    assert.equal(posted[0].shown, true)
    posted[0].emit('click', {})
    assert.deepEqual(opened, ['/chat/t/t-1'])
    posted[0].emit('action', { actionIndex: notify.ACTIONS.markRead }, notify.ACTIONS.markRead)
    assert.deepEqual(marked, [{ id: 't-1', action: 'mark-read', path: '/chat/t/t-1' }])
    posted[0].emit('action', { actionIndex: notify.ACTIONS.open }, notify.ACTIONS.open)
    assert.deepEqual(opened, ['/chat/t/t-1', '/chat/t/t-1'])
  })

  test('a result held back takes down the start it would have replaced', () => {
    const notifier = notify.createNotifier({ traits: MAC, onOpen() {}, onMarkRead() {} })
    notifier.notify({ ...n, kind: 'started', silent: true }, { settings: on, watching: false })
    const start = posted[0]
    assert.deepEqual(notifier.notify(n, { settings: on, watching: true }), { shown: false, reason: 'watching' })
    assert.equal(start.closed, true)
    assert.equal(posted.length, 1)
  })

  test('macOS replaces by id; Linux closes the older one first', () => {
    const mac = notify.createNotifier({ traits: MAC, onOpen() {}, onMarkRead() {} })
    mac.notify(n, { settings: on, watching: false })
    mac.notify(n, { settings: on, watching: false })
    assert.equal(posted[0].closed, false, 'macOS took down what the id would have replaced in place')
    posted.length = 0
    const linux = notify.createNotifier({ traits: LINUX, onOpen() {}, onMarkRead() {} })
    linux.notify(n, { settings: on, watching: false })
    linux.notify(n, { settings: on, watching: false })
    assert.equal(posted[0].closed, true)
    assert.equal(posted[1].closed, false)
  })

  test('clearing closes the live ones and, on macOS, removes delivered ones by id', () => {
    const notifier = notify.createNotifier({ traits: MAC, onOpen() {}, onMarkRead() {} })
    notifier.notify(n, { settings: on, watching: false })
    notifier.clear(['t-1', 't-old', 42, ''])
    assert.equal(posted[0].closed, true)
    assert.deepEqual(FakeNotification.removed, ['t-1', 't-old'])
    notifier.clear('t-1')
    assert.deepEqual(FakeNotification.removed, ['t-1', 't-old'])
  })

  test('a new page takes down the starts the last one left, and only those', () => {
    const notifier = notify.createNotifier({ traits: MAC, onOpen() {}, onMarkRead() {} })
    notifier.notify({ ...n, id: 't-run', kind: 'started', silent: true }, { settings: on, watching: false })
    notifier.notify({ ...n, id: 't-done' }, { settings: on, watching: false })
    notifier.clearStarted()
    assert.equal(posted[0].closed, true)
    assert.equal(posted[1].closed, false)
  })

  test('Windows keeps a toast past its close, so reading the thread can still take it down', () => {
    const notifier = notify.createNotifier({ traits: WIN, onOpen() {}, onMarkRead() {} })
    notifier.notify(n, { settings: on, watching: false })
    posted[0].emit('close', {})
    posted[0].closed = false
    notifier.clear(['t-1'])
    assert.equal(posted[0].closed, true, 'a timed-out toast was left in Action Center')
  })

  test('nothing is posted where the system has no notifications', () => {
    FakeNotification.supported = false
    const notifier = notify.createNotifier({ traits: MAC, onOpen() {}, onMarkRead() {} })
    assert.deepEqual(notifier.notify(n, { settings: on, watching: false }), { shown: false, reason: 'unsupported' })
    assert.equal(posted.length, 0)
  })
})

describe('what the notifier logs', () => {
  const on = settings.normalize({})
  const n = {
    id: '0123456789abcdef', kind: 'turn', title: 'SECRET TITLE', subtitle: 'SECRET SUB', body: 'SECRET BODY', path: '/chat/t/0123456789abcdef',
  }

  beforeEach(useNotifications)
  after(dropNotifications)

  test('one line per decision and per click, naming the kind and an id prefix, never the words', () => {
    const opened = []
    const notifier = notify.createNotifier({ traits: MAC, onOpen: (p) => opened.push(p), onMarkRead() {} })
    const lines = logged(() => {
      notifier.notify(n, { settings: on, watching: false })
      notifier.notify(n, { settings: on, watching: true })
      notifier.notify({ ...n, kind: 'wakeup' }, { settings: settings.merge(on, { notifications: { wakeup: false } }), watching: false })
      notifications[0].emit('click', {})
    })
    assert.deepEqual(lines, [
      '[notify] turn 01234567: shown',
      '[notify] turn 01234567: held back (watching)',
      '[notify] wakeup 01234567: held back (disabled)',
      '[notify] turn 01234567: clicked; opening it in the main window',
    ])
    assert.deepEqual(opened, ['/chat/t/0123456789abcdef'])
    assert.ok(!lines.join('\n').includes('SECRET'), 'a title or body reached the log')
    assert.ok(!lines.join('\n').includes('89abcdef'), 'more of the id than its prefix reached the log')
  })
})

describe('attention', () => {
  test('trimmed to the contract: a count, and at most five threads of known states', () => {
    const v = attention.validate({
      count: 3.5,
      threads: [
        ...Array.from({ length: 7 }, (_, i) => ({ id: `t${i}`, title: `T${i}`, path: `/chat/t/t${i}`, state: 'unread' })),
      ],
    })
    assert.equal(v.count, 0)
    assert.equal(v.threads.length, 5)
    assert.equal(attention.validate({ count: -2 }).count, 0)
    assert.equal(attention.validate({ count: 4, threads: [{ id: 'x', title: 'y', path: '/z', state: 'bogus' }] }).threads.length, 0)
  })

  test('the Dock menu: the two commands, then the threads', () => {
    const opened = []
    const template = attention.dockTemplate(
      attention.validate({ count: 1, threads: [{ id: 'a', title: 'Needs you', path: '/chat/t/a', state: 'needs_input' }] }),
      { onNewThread() {}, onGlance() {}, onOpen: (p) => opened.push(p) },
    )
    assert.deepEqual(template.map((i) => i.label || i.type), ['New Thread', 'Market Glance', 'separator', '◆ Needs you'])
    template[3].click()
    assert.deepEqual(opened, ['/chat/t/a'])
    assert.equal(attention.dockTemplate({ count: 0, threads: [] }, {}).length, 2)
    assert.match(attention.threadLabel({ title: 'x'.repeat(80), state: 'running' }), /^◌ x{47}…$/)
  })

  test('the overlay dot is opaque in the middle and clear in the corner', () => {
    const size = 16
    const bmp = attention.dotBitmap(size)
    assert.equal(bmp.length, size * size * 4)
    const alpha = (x, y) => bmp[(y * size + x) * 4 + 3]
    assert.equal(alpha(8, 8), 255)
    assert.equal(alpha(0, 0), 0)
  })
})
