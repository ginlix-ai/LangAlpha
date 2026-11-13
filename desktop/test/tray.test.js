'use strict'

// The menu bar icon (tray.js): the lines the page formats, laid out per
// platform and kept moving on the shell's clock, and the icon's menu
// (trayMenu.js). Layout and whether any lines are wanted come from the desktop
// settings, never from the page.

const { test, describe, after, beforeEach } = require('node:test')
const assert = require('node:assert/strict')

const { loadShell, cleanup, trays, fakeTimers } = require('./helpers')

const settings = require('../src/desktopSettings.js')
const { traits } = require('../src/platformTraits.js')

const MAC = traits('darwin', {})
const WIN = traits('win32', {})
const LINUX = traits('linux', {})

after(cleanup)

describe('the lines beside the icon', () => {
  // The pure layout takes the app's name as an argument, so it is read once.
  // `createTray` reads the edition's own, so it comes from a saas shell loaded
  // for each test.
  const { cleanLines, titleFrames, frameAt, layout, tooltipText } = require('../src/tray.js')
  let tray
  beforeEach(() => {
    loadShell({ edition: 'saas', settings: { reachedApp: true } })
    tray = require('../src/tray.js')
  })

  /** The `menubar` settings the tray reads, as the test sets them. */
  function chosen(overrides = {}) {
    let bar = { ...settings.DEFAULTS.menubar, items: [{ kind: 'portfolio' }], ...overrides }
    return { read: () => bar, set: (next) => { bar = { ...bar, ...next } } }
  }

  test('the page\'s items become short lines: blanks go, whitespace folds, at most eight', () => {
    assert.deepEqual(cleanLines('NVDA'), [])
    assert.deepEqual(cleanLines([{ text: ' NVDA\n ▼5.60% ' }, { text: '' }, 'Port ▲0.42%', null, { text: 7 }]), [
      'NVDA ▼5.60%', 'Port ▲0.42%',
    ])
    assert.equal(cleanLines([{ text: 'x'.repeat(80) }])[0].length, 32)
    assert.equal(cleanLines(Array.from({ length: 12 }, (_, i) => ({ text: `L${i}` }))).length, 8)
  })

  test('the title: rotate pads every frame to the longest; side shows three, two spaces apart; one is static', () => {
    const lines = ['NVDA ▼5.60%', 'Port ▲0.4%', 'AAPL 230.11 ▲1.2%', 'TSLA ▲3%']
    const frames = titleFrames(lines, 'rotate')
    assert.deepEqual(frames, ['NVDA ▼5.60%      ', 'Port ▲0.4%       ', 'AAPL 230.11 ▲1.2%', 'TSLA ▲3%         '])
    assert.equal(new Set(frames.map((f) => [...f].length)).size, 1, 'the frames differ in width')
    assert.deepEqual(titleFrames(lines, 'side'), ['NVDA ▼5.60%  Port ▲0.4%  AAPL 230.11 ▲1.2%'])
    assert.deepEqual(titleFrames(['NVDA ▼5.60%'], 'rotate'), ['NVDA ▼5.60%'])
    assert.deepEqual(titleFrames(['NVDA ▼5.60%'], 'side'), ['NVDA ▼5.60%'])
    assert.deepEqual(titleFrames([], 'rotate'), [])
    assert.equal(frameAt(frames, 0), frames[0])
    assert.equal(frameAt(frames, 5), frames[1])
    assert.equal(frameAt([], 3), '')
  })

  test('Windows lists them under the name in the tooltip, as many as fit; Linux heads the menu with them', () => {
    const lines = ['NVDA ▼5.60%', 'Port ▲0.42%']
    assert.deepEqual(layout(lines, { traits: WIN, appName: 'LangAlpha' }), {
      frames: [], tooltip: 'LangAlpha\nNVDA ▼5.60%\nPort ▲0.42%', headlines: [],
    })
    assert.equal(layout([], { traits: WIN, appName: 'LangAlpha' }).tooltip, 'LangAlpha')
    const long = Array.from({ length: 8 }, (_, i) => `SYMBOL${i} 1234.56 ▲12.34%`)
    const tip = tooltipText('LangAlpha', long)
    assert.ok(tip.length <= 127, `${tip.length} characters`)
    assert.deepEqual(tip.split('\n').slice(1), long.slice(0, tip.split('\n').length - 1), 'a line was cut part way')
    assert.deepEqual(layout(lines, { traits: LINUX, appName: 'LangAlpha' }), {
      frames: [],
      tooltip: 'LangAlpha',
      headlines: [{ label: 'NVDA ▼5.60%', enabled: false }, { label: 'Port ▲0.42%', enabled: false }, { type: 'separator' }],
    })
    assert.deepEqual(layout([], { traits: LINUX }).headlines, [])
    assert.deepEqual(layout(lines, { traits: MAC, style: 'side', appName: 'LangAlpha' }), {
      frames: ['NVDA ▼5.60%  Port ▲0.42%'], tooltip: 'LangAlpha', headlines: [],
    })
  })

  test('the refresh period is clamped to 10 seconds to 10 minutes', () => {
    const { refreshPeriod } = tray
    assert.equal(refreshPeriod(1000), 10_000)
    assert.equal(refreshPeriod(3_600_000), 600_000)
    assert.equal(refreshPeriod(15_000), 15_000)
    assert.equal(refreshPeriod(Number.NaN), 60_000)
    assert.equal(refreshPeriod(undefined), 60_000)
  })

  test('macOS: one slot turns on the shell\'s clock, in a monospaced font, and a refresh does not restart it', () => {
    const timers = fakeTimers()
    const bar = chosen({ style: 'rotate', rotateSeconds: 4 })
    let refreshes = 0
    const t = tray.createTray({
      traits: MAC, timers, onClick() {}, menuTemplate: () => [], menubar: bar.read, onRefresh: () => { refreshes += 1 },
    })
    t.show()
    const icon = trays.at(-1)
    t.setItems([{ text: 'NVDA ▼5.60%' }, { text: 'Port ▲0.4%' }, { text: 'AAPL ▲1%' }], { refreshMs: 15000 })
    assert.equal(icon.title, ' NVDA ▼5.60%')
    assert.deepEqual(icon.titleOptions, { fontType: 'monospaced' })
    assert.deepEqual(timers.periods(), [4000, 15000])
    timers.fire(4000)
    assert.equal(icon.title, ' Port ▲0.4% ')

    // The page answers a refresh with new lines: the slot carries on, the clock is not reset.
    timers.fire(15000)
    assert.equal(refreshes, 1)
    t.setItems([{ text: 'NVDA ▼5.70%' }, { text: 'Port ▲0.5%' }, { text: 'AAPL ▲1%' }], { refreshMs: 15000 })
    assert.equal(icon.title, ' Port ▲0.5% ')
    timers.fire(4000)
    assert.equal(icon.title, ' AAPL ▲1%   ')
    bar.set({ rotateSeconds: 10 })
    t.setItems([{ text: 'A' }, { text: 'B' }], { refreshMs: 15000 })
    assert.deepEqual(timers.periods(), [10000, 15000], 'a new period kept the old timer')

    // Side by side, or one line, is static: no rotation, still a refresh.
    bar.set({ style: 'side' })
    t.setItems([{ text: 'A' }, { text: 'B' }], { refreshMs: 300000 })
    assert.equal(icon.title, ' A  B')
    assert.deepEqual(timers.periods(), [300000])

    // Hidden, nothing runs; shown again, the refresh resumes, and the lines it
    // kept while off are asked for afresh at once rather than at the next tick.
    t.hide()
    assert.deepEqual(timers.periods(), [])
    const before = refreshes
    t.show()
    assert.deepEqual(timers.periods(), [300000])
    assert.equal(refreshes, before + 1)
    assert.equal(trays.at(-1).title, ' A  B')

    // Nothing to show: the title clears and the refresh stops.
    t.setItems([], { refreshMs: 15000 })
    assert.equal(trays.at(-1).title, '')
    assert.deepEqual(timers.periods(), [])
    t.hide()
  })

  test('a layout change in the settings re-lays the lines at once, and keeps the refresh', () => {
    const timers = fakeTimers()
    const bar = chosen({ style: 'side' })
    const t = tray.createTray({ traits: MAC, timers, onClick() {}, menuTemplate: () => [], menubar: bar.read })
    t.show()
    const icon = trays.at(-1)
    t.setItems(['A', 'BB'], { refreshMs: 20000 })
    assert.equal(icon.title, ' A  BB')
    bar.set({ style: 'rotate', rotateSeconds: 6 })
    t.update()
    assert.equal(icon.title, ' A ')
    assert.deepEqual(timers.periods(), [6000, 20000], 'the layout change dropped the refresh')
    t.hide()
  })

  test('with nothing chosen the icon stands alone, whatever the page sends', () => {
    const timers = fakeTimers()
    const bar = chosen({ items: [] })
    const t = tray.createTray({ traits: MAC, timers, onClick() {}, menuTemplate: () => [], menubar: bar.read })
    t.show()
    const icon = trays.at(-1)
    t.setItems(['Port ▲0.42%'], { refreshMs: 15000 })
    assert.equal(icon.title, '')
    assert.deepEqual(timers.periods(), [])

    bar.set({ items: [{ kind: 'portfolio' }] })
    t.setItems(['Port ▲0.42%'], { refreshMs: 15000 })
    assert.equal(icon.title, ' Port ▲0.42%')
    // Turned off in Settings: the line goes at once, and does not come back
    // stale when something is chosen again; the page's next lines bring it.
    bar.set({ items: [] })
    t.update()
    assert.equal(icon.title, '')
    assert.deepEqual(timers.periods(), [])
    bar.set({ items: [{ kind: 'stock', symbol: 'NVDA' }] })
    t.update()
    assert.equal(icon.title, '')
    t.hide()
  })

  test('Windows puts them in the tooltip; Linux rebuilds its menu when they change', () => {
    const bar = chosen()
    const win = tray.createTray({ traits: WIN, timers: fakeTimers(), onClick() {}, menuTemplate: () => [], menubar: bar.read })
    win.show()
    win.setItems(['NVDA ▼5.60%', 'Port ▲0.42%'], { refreshMs: 15000 })
    assert.equal(trays.at(-1).title, undefined, 'Windows was given a title')
    assert.equal(trays.at(-1).tooltip, 'LangAlpha\nNVDA ▼5.60%\nPort ▲0.42%')
    win.hide()

    const linux = tray.createTray({
      traits: LINUX, timers: fakeTimers(), onClick() {}, menuTemplate: () => [{ label: 'Quit' }], menubar: bar.read,
    })
    linux.show()
    const icon = trays.at(-1)
    assert.deepEqual(icon.menu, [{ label: 'Quit' }])
    linux.setItems(['NVDA ▼5.60%'], { refreshMs: 15000 })
    assert.deepEqual(icon.menu, [{ label: 'NVDA ▼5.60%', enabled: false }, { type: 'separator' }, { label: 'Quit' }])
    const before = icon.menu
    linux.setItems(['NVDA ▼5.60%'], { refreshMs: 15000 })
    assert.equal(icon.menu, before, 'the same lines rebuilt the menu')
    linux.hide()
  })
})

describe('the icon\'s menu', () => {
  const { trayMenuTemplate } = require('../src/trayMenu.js')
  const labels = (menu) => menu.map((i) => (i.type === 'separator' ? '---' : i.label))

  /** Settings the menu's clicks edit, through the same merge a page's patch takes. */
  function editable(initial) {
    let current = settings.normalize(initial)
    const ran = []
    const actions = {
      edit: (patchFor) => { current = settings.merge(current, patchFor(current)) },
      showGlance: () => ran.push('glance'),
      showMain: () => ran.push('main'),
      command: (name) => ran.push(name),
      quit: () => ran.push('quit'),
    }
    const menu = (traits_) => trayMenuTemplate(current, { traits: traits_, appName: 'LangAlpha', actions })
    return { get: () => current, menu, ran }
  }

  test('what is in the menu bar, the layout, the panel\'s sections; each click edits the settings', () => {
    const s = editable({ menubar: { items: [{ kind: 'stock', symbol: 'NVDA' }, { kind: 'portfolio' }], style: 'rotate' } })
    const menu = s.menu(MAC)
    assert.deepEqual(labels(menu), [
      'Open LangAlpha', '---', 'In the menu bar', 'NVDA', 'Portfolio', 'With more than one', 'Panel sections', '---', 'Customize…', 'Quit',
    ])
    assert.equal(menu[2].type, 'header')
    assert.ok(menu.slice(3, 5).every((i) => i.type === 'checkbox' && i.checked))
    const [rotate, side] = menu[5].submenu
    assert.deepEqual([rotate.type, rotate.checked, side.label, side.checked], ['radio', true, 'Side by side', false])
    assert.deepEqual(menu[6].submenu.map((i) => [i.label, i.checked]), [
      ['Market', true], ['Portfolio', true], ['Watchlist', true], ['Agent', true],
    ])

    menu[3].click()
    assert.deepEqual(s.get().menubar.items, [{ kind: 'portfolio' }])
    // A menu built before that click still removes by what it names, not where it was.
    menu[3].click()
    assert.deepEqual(s.get().menubar.items, [{ kind: 'portfolio' }])
    side.click()
    assert.equal(s.get().menubar.style, 'side')
    menu[6].submenu[2].click()
    assert.deepEqual(s.get().panel.sections.map((x) => x.visible), [true, true, false, true])
    menu[6].submenu[2].click()
    assert.equal(s.get().panel.sections[2].visible, true)
    menu.at(-2).click()
    assert.deepEqual(s.ran, ['open-settings'])

    menu[4].click()
    const empty = s.menu(WIN)
    assert.deepEqual(empty[2], { label: 'In the menu bar', enabled: false })
    assert.deepEqual(empty[3], { label: 'None', enabled: false })
  })

  test('an index goes by its long name, with its ampersand kept', () => {
    const s = editable({ menubar: { items: [{ kind: 'index', symbol: 'GSPC' }, { kind: 'index', symbol: 'RUT' }] } })
    assert.deepEqual(labels(s.menu(MAC)).slice(3, 5), ['S&&P 500', 'Russell 2000'])
  })

  test('with no panel, the menu leads with Market Glance', () => {
    const s = editable({})
    const linux = s.menu(LINUX)
    assert.deepEqual(labels(linux).slice(0, 3), ['Open Market Glance…', 'Open LangAlpha', '---'])
    linux[0].click()
    assert.deepEqual(s.ran, ['glance'])
    assert.equal(labels(s.menu(MAC))[0], 'Open LangAlpha')
  })
})
