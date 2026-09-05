'use strict'

// Must equal the page's --color-bg-page (tokens.css: --background is 0 0% 9.8%
// dark, 0 0% 100% light). During a live resize the window frame outruns the
// painted content and this colour fills the gap, so a mismatch reads as a
// flashing band at the edge. Electron cannot eliminate that gap, only match it,
// which is why a hardcoded dark value gave light-theme users a dark band.
const BACKGROUNDS = {
  dark: '#191919',
  light: '#ffffff',
}

function backgroundFor(theme) {
  return BACKGROUNDS[theme] || BACKGROUNDS.dark
}

function isTheme(value) {
  return value === 'dark' || value === 'light'
}

module.exports = { backgroundFor, isTheme }
