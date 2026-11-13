'use strict'

// Open at login on Linux: the XDG autostart entry, whose `Exec` line has three
// layers of escaping that no desktop at hand will exercise for a hostile path.

const { test, describe, after } = require('node:test')
const assert = require('node:assert/strict')
const fs = require('node:fs')

const { cleanup, electronStub, tempDir } = require('./helpers')

const loginItem = require('../src/loginItem.js')

after(cleanup)

describe('the Linux autostart entry', () => {
  test('the Linux autostart entry launches hidden', () => {
    const entry = loginItem.autostartEntry({ appName: 'LangAlpha OSS', execPath: '/home/a b/Apps/LangAlpha.AppImage' })
    assert.match(entry, /^\[Desktop Entry\]\n/)
    assert.match(entry, /\nType=Application\n/)
    assert.match(entry, /\nName=LangAlpha OSS\n/)
    assert.ok(entry.includes('\nExec="/home/a b/Apps/LangAlpha.AppImage" --hidden\n'), entry)
    assert.equal(loginItem.autostartFileName('LangAlpha OSS'), 'langalpha-oss.desktop')
    assert.equal(loginItem.autostartFileName('LangAlpha'), 'langalpha.desktop')
  })

  // The Desktop Entry spec's own spelling, layer by layer: a `%` is `%%`; inside
  // the quotes `"`, `` ` ``, `$` and `\` take a backslash; and the string rule
  // then doubles every backslash, so a backslash is four and a `$` is `\\$`.
  test('the Exec argument is escaped as the Desktop Entry spec spells it', () => {
    assert.equal(loginItem.execArgument('/opt/La.AppImage'), '"/opt/La.AppImage"')
    assert.equal(loginItem.execArgument('/a"b'), '"/a\\\\"b"')
    assert.equal(loginItem.execArgument('/a$b'), '"/a\\\\$b"')
    assert.equal(loginItem.execArgument('/a`b'), '"/a\\\\`b"')
    assert.equal(loginItem.execArgument('/a\\b'), '"/a\\\\\\\\b"')
    assert.equal(loginItem.execArgument('/100%/x'), '"/100%%/x"')
    assert.equal(loginItem.execArgument('/a\nb'), '"/a\\nb"')
  })

  // What a launcher does with the line, in the spec's order: the string escapes
  // undone, then the quoting, then the field codes. Whatever the path, it has
  // to come back as the one argument it was.
  function parseExec(value) {
    const string = value.replace(/\\([\\nstr])/g, (_m, c) => ({ '\\': '\\', n: '\n', s: ' ', t: '\t', r: '\r' })[c])
    const args = []
    let i = 0
    while (i < string.length) {
      if (string[i] === ' ') { i += 1; continue }
      let arg = ''
      if (string[i] === '"') {
        i += 1
        while (string[i] !== '"') {
          if (i >= string.length) throw new Error(`unterminated quote in ${value}`)
          if (string[i] === '\\') {
            if (!'"`$\\'.includes(string[i + 1])) throw new Error(`a stray backslash in ${value}`)
            i += 1
          }
          arg += string[i]
          i += 1
        }
        i += 1
      } else {
        while (i < string.length && string[i] !== ' ') arg += string[i++]
      }
      if (/%[^%]/.test(arg.replace(/%%/g, ''))) throw new Error(`a field code in ${value}`)
      args.push(arg.replace(/%%/g, '%'))
    }
    return args
  }

  test('any path reads back from the entry as itself', () => {
    const paths = [
      '/home/a b/Apps/La"$x.AppImage',
      '/home/x/back\\slash/`tick`/$HOME/100%/%U/La.AppImage',
      "/home/it's/<a>|b&c;d*e?f#g(h)~/La.AppImage",
      '/home/tab\there/new\nline/La.AppImage',
    ]
    for (const execPath of paths) {
      const entry = loginItem.autostartEntry({ appName: 'LangAlpha', execPath })
      const exec = entry.split('\n').find((line) => line.startsWith('Exec=')).slice('Exec='.length)
      assert.ok(!exec.includes('\n'), 'the entry broke across lines')
      assert.deepEqual(parseExec(exec), [execPath, '--hidden'], exec)
    }
  })
})

// What the switch in Settings reads back. The user can turn the app off where
// the OS keeps its own switch, which leaves our entry in place, so its being
// there is not the answer.
describe('whether the OS will open the app at login', () => {
  /** Run `fn` as a packaged app, with `getLoginItemSettings` answering `settings`. */
  function packaged(settings, fn) {
    const { app } = electronStub
    const real = { isPackaged: app.isPackaged, getLoginItemSettings: app.getLoginItemSettings }
    app.isPackaged = true
    app.getLoginItemSettings = () => settings
    try {
      return fn()
    } finally {
      Object.assign(app, real)
    }
  }

  test('Windows: a Run key turned off in Startup Apps reads as off', () => {
    const item = loginItem.createLoginItem({ loginItem: 'run-key' })
    const run = (settings) => packaged(settings, () => item.isEnabled())
    assert.equal(run({ openAtLogin: true, executableWillLaunchAtLogin: true }), true)
    assert.equal(run({ openAtLogin: true, executableWillLaunchAtLogin: false }), false)
    assert.equal(run({ openAtLogin: false, executableWillLaunchAtLogin: false }), false)
  })

  test('Linux: an autostart entry a desktop turned off reads as off', () => {
    const real = process.env.XDG_CONFIG_HOME
    process.env.XDG_CONFIG_HOME = tempDir('la-xdg-')
    try {
      const item = loginItem.createLoginItem({ loginItem: 'xdg-autostart' })
      const file = loginItem.autostartFile()
      const read = () => packaged({}, () => item.isEnabled())
      assert.equal(read(), false)
      packaged({}, () => item.set(true))
      assert.equal(read(), true)
      const entry = fs.readFileSync(file, 'utf8')
      fs.writeFileSync(file, `${entry}Hidden=true\n`)
      assert.equal(read(), false)
      fs.writeFileSync(file, entry.replace('X-GNOME-Autostart-enabled=true', 'X-GNOME-Autostart-enabled=false'))
      assert.equal(read(), false)
      // Only the entry's own group counts.
      fs.writeFileSync(file, `${entry}\n[Desktop Action New]\nHidden=true\n`)
      assert.equal(read(), true)
    } finally {
      if (real === undefined) delete process.env.XDG_CONFIG_HOME
      else process.env.XDG_CONFIG_HOME = real
    }
  })
})
