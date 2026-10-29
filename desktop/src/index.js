'use strict'

const { app, dialog } = require('electron')

// The entry exists to make a startup crash visible.
//
// Everything in the shell runs at require time, `config`'s validation of the
// build it was packaged from included, and that one throws on purpose: a SaaS
// build with no platform origin is quietly the wrong product and must not
// launch. Uncaught, it kills the main process before any window exists — on a
// packaged macOS build the icon bounces once and the app is gone, with the
// reason on a stderr nobody launched it from. `showErrorBox` is documented as
// safe before `ready`, which is exactly what it is for.
//
// Load-time only. Once the shell is running, main's own `unhandledRejection`
// handler owns what goes wrong.
try {
  require('./main')
} catch (err) {
  dialog.showErrorBox('LangAlpha cannot start', String((err && err.stack) || err))
  app.exit(1)
}
