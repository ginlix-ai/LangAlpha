'use strict'

// Every platform fact the menu bar, notifications, login item and closing
// rules act on, read in one place. The modules take this object rather than
// reading `process.platform`, so each asks what it needs ("can a notification
// be replaced in place") instead of re-deriving it from a platform name, and a
// test hands a module another platform's answers without touching `process`.
//
// macOS and Windows are themselves; any other platform is treated as Linux:
// a tray icon that is an AppIndicator, notifications without replace or
// actions, and an XDG autostart entry.

/**
 * - `panel`: where the icon's click opens the panel: under the menu bar
 *   (`'menubar'`), beside a taskbar icon on whichever edge (`'taskbar'`), or no
 *   panel (`null`), where an AppIndicator reports no clicks and Market Glance
 *   is an ordinary window.
 * - `trayTitle`: text can sit beside the icon. `tooltipLines`: the tooltip is
 *   where the lines go instead. `trayMenuOnly`: the icon's menu is all it has,
 *   so the lines head that menu.
 * - `lastWindowClosed`: `'stay'` running, `'tray'` (stay in the notification
 *   area when the icon is on and the user wants it), or `'quit'`.
 * - `globalShortcut`: Wayland gives an app no way to grab a key outside its
 *   own windows.
 * - `loginItem`: which mechanism opens the app at login.
 */
function traits(platform = process.platform, env = process.env) {
  const mac = platform === 'darwin'
  const win = platform === 'win32'
  const linux = !mac && !win
  return Object.freeze({
    panel: mac ? 'menubar' : win ? 'taskbar' : null,
    trayTitle: mac,
    tooltipLines: win,
    trayMenuOnly: linux,
    templateIcon: mac,
    menuHeaders: mac,
    replaceNotifications: mac || win,
    notificationActions: mac || win,
    notificationSubtitle: mac,
    removeDelivered: mac,
    // A toast that times out moves to Action Center and still reports `close`;
    // only `close()` on the same object takes it down from there.
    toastOutlivesClose: win,
    appUserModelId: win,
    badgeCount: mac || linux,
    overlayBadge: win,
    // The Dock: a menu of its own, and a way in that needs no window or icon.
    dock: mac,
    jumpList: win,
    lastWindowClosed: mac ? 'stay' : win ? 'tray' : 'quit',
    globalShortcut: !linux || env.XDG_SESSION_TYPE !== 'wayland',
    loginItem: mac ? 'login-items' : win ? 'run-key' : 'xdg-autostart',
    // electron-updater hands a macOS install to Squirrel, whose quit closes the
    // windows before `before-quit` fires.
    updateClosesWindowsFirst: mac,
  })
}

module.exports = { traits }
