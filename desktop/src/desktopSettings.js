'use strict'

// The user's desktop settings: which notifications to post, the menu bar icon
// and what sits beside it, the panel's sections, open at login, the global
// shortcut. The page edits them in the app's Settings and the shell keeps them,
// because the shell is what acts on every one, and a notification has to be
// filtered by a window that may not have the app's own preferences loaded at
// all.
//
// Pure: a read and a merge, both validated, and what each platform can do. The
// store calls `normalize` on load, desktopSettingsService.js calls `merge` on a
// patch, and nothing here opens or registers anything.

const NOTIFICATION_KINDS = ['turn', 'needs_input', 'wakeup', 'automation', 'started']
const MENUBAR_STYLES = ['rotate', 'side']
const MENUBAR_SHOW = ['change', 'price', 'both']
// The panel's sections in their default order, with the name the icon's menu
// gives each.
const PANEL_SECTIONS = Object.freeze({ market: 'Market', portfolio: 'Portfolio', watchlist: 'Watchlist', agent: 'Agent' })
const SECTION_IDS = Object.keys(PANEL_SECTIONS)
const QUOTE_KINDS = ['index', 'stock']
// The indexes the quote API knows bare, with their long names. The long names
// are for menus; the page puts short ones beside the icon, where room is tight.
const INDEXES = Object.freeze({ GSPC: 'S&P 500', IXIC: 'Nasdaq', DJI: 'Dow', RUT: 'Russell 2000', VIX: 'VIX' })

const LIMITS = Object.freeze({
  items: 8,
  tiles: 4,
  rotateSeconds: Object.freeze({ min: 2, max: 30 }),
  watchlistRows: Object.freeze({ min: 1, max: 10 }),
  watchlistId: 128,
})

const DEFAULTS = Object.freeze({
  notifications: Object.freeze(Object.fromEntries(NOTIFICATION_KINDS.map((k) => [k, true]))),
  menubar: Object.freeze({
    enabled: true,
    items: Object.freeze([]),
    style: 'rotate',
    rotateSeconds: 4,
    show: 'change',
    extendedHours: true,
  }),
  panel: Object.freeze({
    sections: Object.freeze(SECTION_IDS.map((id) => Object.freeze({ id, visible: true }))),
    tiles: Object.freeze(['GSPC', 'IXIC', 'DJI', 'VIX'].map((symbol) => Object.freeze({ symbol, kind: 'index' }))),
    watchlistId: null,
    watchlistRows: 5,
  }),
  openAtLogin: false,
  shortcut: '',
  keepRunningInTray: true,
})

// Tickers as the data providers spell them: `BRK.B`, `^GSPC`, `BTC-USD`,
// `600519.SS`, `EUR/USD`. Wide enough for those, narrow enough that what reaches
// the menu bar is a symbol and not a sentence.
const SYMBOL = /^[A-Z0-9.^=\-:/_]{1,20}$/

// An Electron accelerator is `+`-joined tokens. Whether a key name is real is
// `globalShortcut.register`'s to say (it reports a bad one as a failure, which is
// what `shortcutError` is for); this only keeps the stored string a plausible one.
const ACCELERATOR = /^[A-Za-z0-9]+(\+[A-Za-z0-9`~!@#$%^&*()\-_=[\]{};:'",.<>/?\\|]+)*$/
// A global shortcut takes its key from every app on the system, so it needs a
// modifier, or a function key nobody types with. The Settings page asks the
// same, but any page on the app's origin can call the setter.
const MODIFIER = /^(CommandOrControl|CmdOrCtrl|Command|Cmd|Control|Ctrl|Alt|Option|AltGr|Shift|Super|Meta)$/i
const FUNCTION_KEY = /^F([1-9]|1\d|2[0-4])$/i
const claimsLittle = (accel) => {
  const parts = accel.split('+')
  return parts.slice(0, -1).some((p) => MODIFIER.test(p)) || (parts.length === 1 && FUNCTION_KEY.test(parts[0]))
}

const bool = (value, fallback) => (typeof value === 'boolean' ? value : fallback)
const oneOf = (list, value, fallback) => (list.includes(value) ? value : fallback)

function symbol(value, fallback) {
  if (typeof value !== 'string') return fallback
  const s = value.trim().toUpperCase()
  return SYMBOL.test(s) ? s : fallback
}

function shortcut(value, fallback) {
  if (typeof value !== 'string') return fallback
  const s = value.trim()
  if (s === '') return ''
  return s.length <= 64 && ACCELERATOR.test(s) && claimsLittle(s) ? s : fallback
}

/** A whole number within `range`, a number outside it clamped to it, anything else the fallback. */
function clampInt(value, { min, max }, fallback) {
  if (typeof value !== 'number' || !Number.isFinite(value)) return fallback
  return Math.min(max, Math.max(min, Math.round(value)))
}

const isObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v)

/** A symbol the quote API can price, or null. An index is asked for bare (`GSPC`, not `^GSPC`). */
function quoteRef(value) {
  if (!isObject(value) || !QUOTE_KINDS.includes(value.kind)) return null
  let s = symbol(value.symbol, null)
  if (s && value.kind === 'index') s = s.replace(/^\^/, '') || null
  return s ? { symbol: s, kind: value.kind } : null
}

/** One thing beside the menu bar icon, or null. */
function menubarItem(value) {
  if (isObject(value) && value.kind === 'portfolio') return { kind: 'portfolio' }
  return quoteRef(value)
}

const itemKey = (item) => (item.kind === 'portfolio' ? 'portfolio' : `${item.kind}:${item.symbol}`)

/**
 * A list read entry by entry: what does not parse goes, a repeat goes, and the
 * rest keeps its order up to `max`. Repaired rather than refused, so a list with
 * one bad entry in it costs that entry and not the user's whole choice. Not an
 * array at all is the fallback.
 */
function list(value, read, max, fallback) {
  if (!Array.isArray(value)) return fallback
  const seen = new Set()
  const out = []
  for (const raw of value) {
    const item = read(raw)
    if (!item || seen.has(itemKey(item))) continue
    seen.add(itemKey(item))
    out.push(item)
    if (out.length === max) break
  }
  return out
}

/**
 * The panel's sections: every one of the four exactly once. Entries in the order
 * given, the first of each id winning; an id left out is added at the end, in
 * the order and with the visibility `base` had. A panel missing a section would
 * have no switch left to bring it back, so this repairs and never drops one.
 */
function sections(value, fallback) {
  if (!Array.isArray(value)) return fallback
  const out = []
  for (const raw of value) {
    if (!isObject(raw) || !SECTION_IDS.includes(raw.id) || out.some((s) => s.id === raw.id)) continue
    const before = fallback.find((s) => s.id === raw.id)
    out.push({ id: raw.id, visible: bool(raw.visible, before ? before.visible : true) })
  }
  // In `base`'s order, so a section a patch left out keeps its place relative
  // to the others it left out.
  for (const id of [...fallback.map((s) => s.id), ...SECTION_IDS]) {
    if (out.some((s) => s.id === id)) continue
    const before = fallback.find((s) => s.id === id)
    out.push({ id, visible: before ? before.visible : true })
  }
  return out
}

/** Null for the default watchlist; otherwise an id, which the page resolves. */
function watchlistId(value, fallback) {
  if (value === null) return null
  if (typeof value !== 'string') return fallback
  const s = value.trim()
  if (s === '') return null
  return s.length <= LIMITS.watchlistId ? s : fallback
}

/**
 * The settings `raw` describes, every field checked against its type and falling
 * back to `base` where it fails. Unknown keys go: the page reads this object back
 * whole, and a key nothing here validates is one a hand-edit or an old shell
 * could put there with any value at all.
 */
function normalize(raw, base = DEFAULTS) {
  const r = isObject(raw) ? raw : {}
  const n = isObject(r.notifications) ? r.notifications : {}
  const m = isObject(r.menubar) ? r.menubar : {}
  const p = isObject(r.panel) ? r.panel : {}
  return {
    notifications: Object.fromEntries(
      NOTIFICATION_KINDS.map((k) => [k, bool(n[k], base.notifications[k])]),
    ),
    menubar: {
      enabled: bool(m.enabled, base.menubar.enabled),
      items: list(m.items, menubarItem, LIMITS.items, base.menubar.items.map((i) => ({ ...i }))),
      style: oneOf(MENUBAR_STYLES, m.style, base.menubar.style),
      rotateSeconds: clampInt(m.rotateSeconds, LIMITS.rotateSeconds, base.menubar.rotateSeconds),
      show: oneOf(MENUBAR_SHOW, m.show, base.menubar.show),
      extendedHours: bool(m.extendedHours, base.menubar.extendedHours),
    },
    panel: {
      sections: sections(p.sections, base.panel.sections.map((s) => ({ ...s }))),
      tiles: list(p.tiles, quoteRef, LIMITS.tiles, base.panel.tiles.map((t) => ({ ...t }))),
      watchlistId: watchlistId(p.watchlistId, base.panel.watchlistId),
      watchlistRows: clampInt(p.watchlistRows, LIMITS.watchlistRows, base.panel.watchlistRows),
    },
    openAtLogin: bool(r.openAtLogin, base.openAtLogin),
    shortcut: shortcut(r.shortcut, base.shortcut),
    keepRunningInTray: bool(r.keepRunningInTray, base.keepRunningInTray),
  }
}

/**
 * `current` with `patch` applied one level deep: a group may be given in part,
 * and an array in it (`menubar.items`, `panel.sections`, `panel.tiles`) replaces
 * the whole array. A value that fails validation keeps what was there, not the
 * default, so a page that sends one bad field does not reset a setting the user
 * chose.
 */
function merge(current, patch) {
  const base = normalize(current)
  if (!isObject(patch)) return base
  const next = { ...base }
  for (const key of Object.keys(DEFAULTS)) {
    if (!(key in patch)) continue
    next[key] = isObject(DEFAULTS[key]) && isObject(patch[key]) ? { ...base[key], ...patch[key] } : patch[key]
  }
  return normalize(next, base)
}

/**
 * What this platform can do with each setting, so the page offers only the
 * switches that work here: `traits` (platformTraits.js) as the page reads it.
 */
function support(traits) {
  return {
    menubarPanel: Boolean(traits.panel),
    trayTitle: traits.trayTitle,
    openAtLogin: Boolean(traits.loginItem),
    shortcut: traits.globalShortcut,
    keepRunningInTray: traits.lastWindowClosed === 'tray',
    notificationActions: traits.notificationActions,
  }
}

/** What the page is shown: the settings, what works here, and a shortcut that would not register. */
function view(settings, { traits, shortcutError } = {}) {
  const out = { ...normalize(settings), support: support(traits) }
  if (shortcutError) out.shortcutError = shortcutError
  return out
}

module.exports = {
  DEFAULTS,
  LIMITS,
  NOTIFICATION_KINDS,
  PANEL_SECTIONS,
  INDEXES,
  normalize,
  merge,
  itemKey,
  clampInt,
  support,
  view,
}
