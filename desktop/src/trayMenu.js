'use strict'

const { INDEXES, PANEL_SECTIONS, itemKey } = require('./desktopSettings')

/**
 * The icon's menu: a right-click where the icon reports clicks, its only menu
 * where it does not (where tray.js heads it with the lines). What sits in the
 * menu bar and which panel sections show can be changed here without opening
 * Settings.
 *
 * Pure. A click edits through `actions.edit(fn)`, where `fn` gets the settings
 * as they are at the click, not as they were when the menu was built, so a menu
 * opened before a change elsewhere edits what is there now. `actions` also
 * carries `showGlance`, `showMain`, `command(name)` and `quit`.
 */
function trayMenuTemplate(settings, { traits, appName, actions }) {
  const { menubar: bar, panel } = settings
  const header = (label) => (traits.menuHeaders ? { type: 'header', label } : { label, enabled: false })

  const removeItem = (key) => actions.edit((s) => ({
    menubar: { items: s.menubar.items.filter((i) => itemKey(i) !== key) },
  }))
  const toggleSection = (id) => actions.edit((s) => ({
    panel: { sections: s.panel.sections.map((x) => (x.id === id ? { ...x, visible: !x.visible } : x)) },
  }))
  const setStyle = (style) => actions.edit(() => ({ menubar: { style } }))

  const items = bar.items.length
    ? bar.items.map((item) => {
      const key = itemKey(item)
      return {
        label: item.kind === 'portfolio'
          ? 'Portfolio'
          // Electron reads a lone `&` in a menu label as a mnemonic marker and
          // drops it, on macOS too, so `S&P` would show as `SP`.
          : ((item.kind === 'index' && INDEXES[item.symbol]) || item.symbol).replace(/&/g, '&&'),
        type: 'checkbox',
        checked: true,
        click: () => removeItem(key),
      }
    })
    : [{ label: 'None', enabled: false }]

  return [
    // With no panel, Market Glance is a window, and this menu is the way to it.
    ...(traits.panel ? [] : [{ label: 'Open Market Glance…', click: () => actions.showGlance() }]),
    { label: `Open ${appName}`, click: () => actions.showMain() },
    { type: 'separator' },
    header('In the menu bar'),
    ...items,
    {
      label: 'With more than one',
      submenu: [
        { label: 'Rotate', type: 'radio', checked: bar.style === 'rotate', click: () => setStyle('rotate') },
        { label: 'Side by side', type: 'radio', checked: bar.style === 'side', click: () => setStyle('side') },
      ],
    },
    {
      label: 'Panel sections',
      submenu: panel.sections.map((section) => ({
        label: PANEL_SECTIONS[section.id],
        type: 'checkbox',
        checked: section.visible,
        click: () => toggleSection(section.id),
      })),
    },
    { type: 'separator' },
    { label: 'Customize…', click: () => actions.command('open-settings') },
    { label: 'Quit', click: () => actions.quit() },
  ]
}

module.exports = { trayMenuTemplate }
