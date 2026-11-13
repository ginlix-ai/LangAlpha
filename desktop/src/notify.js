'use strict'

const electron = require('electron')

const { NOTIFICATION_KINDS } = require('./desktopSettings')

const { app } = electron

/**
 * Tell the desktop a file just landed.
 *
 * The Downloads stack bounce: the platform's own "a file arrived" signal, and
 * the only one that reports without taking focus from a window that may be
 * mid-turn. Every route that writes a file for the user goes through here, so
 * a saved PDF and a finished download announce themselves the same way.
 *
 * Silent everywhere else. Windows and Linux have no equivalent that does not
 * interrupt, and an error box for a success is worse than saying nothing.
 */
function fileLanded(filePath) {
  if (process.platform === 'darwin' && app.dock) app.dock.downloadFinished(filePath)
}

// ---------------------------------------------------------------------------
// Thread notifications.
//
// The menu bar page is the one source: it hears the user's feed and asks the
// shell to post. The shell decides whether to, and owns the click, because the
// click has to raise a window the page cannot reach.
// ---------------------------------------------------------------------------

// One group for every thread, so Notification Center stacks them under the app
// rather than one stack each; replacing is by `id`, not by group.
const GROUP = 'threads'

const MAX = { id: 128, title: 200, subtitle: 200, body: 1000, path: 2048 }

/** The notification the page asked for, trimmed to the contract, or null if it is not one. */
function validate(n) {
  if (!n || typeof n !== 'object') return null
  const str = (v, max) => (typeof v === 'string' ? v.slice(0, max) : null)
  const id = str(n.id, MAX.id)
  const title = str(n.title, MAX.title)
  const body = str(n.body, MAX.body)
  const path = str(n.path, MAX.path)
  if (!id || title === null || body === null || path === null) return null
  if (!NOTIFICATION_KINDS.includes(n.kind)) return null
  const subtitle = typeof n.subtitle === 'string' && n.subtitle ? n.subtitle.slice(0, MAX.subtitle) : undefined
  return { id, kind: n.kind, title, subtitle, body, path, silent: n.silent === true }
}

/**
 * Whether the user is already looking at this thread: the main window is
 * focused and its address names it. A notification then only repeats what is
 * on the screen.
 */
function isWatching({ focused, url, id }) {
  if (!focused || !url || !id) return false
  try {
    return new URL(url).pathname.includes(id)
  } catch {
    return false
  }
}

/**
 * Whether to post, and the reason when not. Pure; `supported` is
 * `Notification.isSupported()`, `watching` is `isWatching` for the main window.
 *
 * Where a notification cannot be replaced in place (Linux's notification
 * servers mostly), `started` is skipped: the silent "running" note would sit
 * beside the result instead of turning into it.
 */
function decide({ n, settings, traits, supported, watching }) {
  if (settings.notifications[n.kind] === false) return { shown: false, reason: 'disabled' }
  if (!supported) return { shown: false, reason: 'unsupported' }
  if (!traits.replaceNotifications && n.kind === 'started') return { shown: false, reason: 'unsupported' }
  if (watching) return { shown: false, reason: 'watching' }
  return { shown: true }
}

const ACTIONS = { open: 0, markRead: 1 }

/**
 * How a notification is named in the log: its kind and the first 8 characters
 * of its thread id. Never the title or body, which are the thread's title and
 * what the agent said; the id prefix is enough to match a line to a thread.
 */
const logName = (n) => `${n.kind} ${String(n.id).slice(0, 8)}`

/**
 * The Electron options for `n` given `traits`. Where notifications replace by
 * id they carry the id and the group, and the buttons where there are buttons;
 * a `started` note has only Open, since there is nothing to mark read while the
 * run is still going. A subtitle is a line of its own where the system has one;
 * elsewhere it leads the body.
 */
function notificationOptions(n, traits) {
  const folded = n.subtitle ? `${n.subtitle}\n${n.body}` : n.body
  const text = traits.notificationSubtitle ? { subtitle: n.subtitle, body: n.body } : { body: folded }
  if (!traits.replaceNotifications) return { title: n.title, ...text, silent: n.silent }
  const options = { id: n.id, groupId: GROUP, title: n.title, ...text, silent: n.silent }
  if (traits.notificationActions) {
    options.actions = [{ type: 'button', text: 'Open' }]
    // A start has nothing to read yet, and a question is answered, not read.
    if (n.kind !== 'started' && n.kind !== 'needs_input') options.actions.push({ type: 'button', text: 'Mark as read' })
  }
  return options
}

/**
 * The live half: posts, keeps each notification referenced until it is gone
 * (an unreferenced Notification is collected and its click is lost), and routes
 * a click or a button back to main.
 */
function createNotifier({ traits, onOpen, onMarkRead }) {
  const live = new Map()
  const kinds = new WeakMap()

  const Notification = () => electron.Notification
  const supported = () => {
    try {
      return Boolean(Notification() && Notification().isSupported())
    } catch {
      return false
    }
  }

  function forget(id, note) {
    if (live.get(id) === note) live.delete(id)
  }

  function close(id) {
    const note = live.get(id)
    live.delete(id)
    if (note) {
      try {
        note.close()
      } catch {
        // Already gone from the screen; nothing left to take down.
      }
    }
  }

  /**
   * Post `raw` if the settings and the screen allow. A notification held back
   * also takes down the one it would have replaced: a run's silent "running"
   * note whose result is switched off, or arrives while the user is watching,
   * would otherwise stay up for a run that has finished.
   */
  function notify(raw, { settings, watching }) {
    const n = validate(raw)
    if (!n) {
      console.warn('[notify] dropped: not a notification')
      return { shown: false }
    }
    const verdict = decide({ n, settings, traits, supported: supported(), watching })
    if (!verdict.shown) {
      console.log(`[notify] ${logName(n)}: held back (${verdict.reason})`)
      if (n.kind !== 'started') close(n.id)
      return verdict
    }
    console.log(`[notify] ${logName(n)}: shown`)

    // Where the system replaces by id it does so on its own; elsewhere the older
    // one is taken down first rather than left stacked under the new one.
    if (!traits.replaceNotifications) close(n.id)

    const Ctor = Notification()
    const note = new Ctor(notificationOptions(n, traits))
    const open = (how) => {
      forget(n.id, note)
      console.log(`[notify] ${logName(n)}: ${how}; opening it in the main window`)
      onOpen(n.path)
    }
    note.on('click', () => open('clicked'))
    note.on('action', (event, legacyIndex) => {
      const index = typeof event?.actionIndex === 'number' ? event.actionIndex : legacyIndex
      if (index !== ACTIONS.markRead) {
        open('Open pressed')
        return
      }
      forget(n.id, note)
      console.log(`[notify] ${logName(n)}: Mark as read pressed`)
      onMarkRead({ id: n.id, action: 'mark-read', path: n.path })
    })
    // Held on Windows past `close`, so reading the thread can still take the
    // toast out of Action Center; a click or action above lets it go.
    if (!traits.toastOutlivesClose) note.on('close', () => forget(n.id, note))
    note.on('failed', () => forget(n.id, note))
    kinds.set(note, n.kind)
    live.set(n.id, note)
    note.show()
    return { shown: true }
  }

  /**
   * Take down every start notice still up. Only the page that posted one knows
   * when its run ended, so a new page (a reload, a sign-out) would leave it up
   * for good; a run still going is told of again by its result.
   */
  function clearStarted() {
    for (const [id, note] of [...live]) if (kinds.get(note) === 'started') close(id)
  }

  /** Take these threads' notifications down, read somewhere else. */
  function clear(ids) {
    if (!Array.isArray(ids)) return
    const valid = ids.filter((id) => typeof id === 'string' && id && id.length <= MAX.id).slice(0, 500)
    for (const id of valid) close(id)
    // Delivered ones from an earlier launch have no object here to close.
    if (traits.removeDelivered && valid.length && Notification()?.remove) {
      try {
        Notification().remove(valid)
      } catch {
        // An unsigned build has no Notification Center entries to remove.
      }
    }
  }

  /** A one-off note from the shell itself, with no thread behind it. */
  function notice({ title, body, onClick }) {
    if (!supported()) return
    const Ctor = Notification()
    const note = new Ctor({ title, body })
    const key = `notice:${title}`
    if (onClick) note.on('click', () => { forget(key, note); onClick() })
    note.on('close', () => forget(key, note))
    live.set(key, note)
    note.show()
  }

  return { notify, clear, clearStarted, notice }
}

module.exports = { fileLanded, validate, isWatching, decide, notificationOptions, createNotifier, ACTIONS }
