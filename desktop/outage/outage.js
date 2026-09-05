'use strict'

const params = new URLSearchParams(location.search)
const reason = params.get('reason') || 'unreachable'
const edition = params.get('edition') || 'saas'
const target = params.get('target') || ''
const detail = params.get('detail') || ''
const isOss = edition === 'oss'

document.documentElement.dataset.theme = params.get('theme') === 'light' ? 'light' : 'dark'

// Copy differs by edition because the *action* differs: on the hosted service the
// user can only wait, on their own stack they are the one who can fix it.
const COPY = {
  offline: {
    title: 'You are offline',
    lede: 'This computer has no network connection. LangAlpha will pick up again as soon as it is back.',
  },
  'captive-portal': {
    title: 'This network needs you to sign in',
    lede: 'You are connected to the wifi, but it is holding traffic back until you sign in. Hotels, airports and cafes work this way.',
    browser: 'Open the sign-in page',
  },
  unreachable: isOss
    ? {
        title: 'Cannot reach your server',
        lede: 'Nothing answered at this address. Check that your stack is running and that the address is right.',
        hint: 'Started it with <code>make up</code>? The web app is usually on <code>http://localhost:5173</code>, or <code>http://localhost</code> behind the dev proxy.',
      }
    : {
        title: 'Cannot reach LangAlpha',
        lede: 'Your connection looks fine, so this is either on our side or something between us: a VPN, a proxy or a corporate firewall.',
      },
  'server-error': isOss
    ? {
        title: 'Your server returned an error',
        lede: 'The stack is running but answered with an error, so the problem is inside it rather than on the network.',
        hint: 'Check the backend logs with <code>docker compose logs -f backend</code>.',
      }
    : {
        title: 'LangAlpha is having trouble',
        lede: 'The server answered with an error. This is on our side and usually clears on its own.',
      },
}

const copy = COPY[reason] || COPY.unreachable
const $ = (id) => document.getElementById(id)

$('title').textContent = copy.title
$('lede').textContent = copy.lede
$('target').textContent = target
document.title = copy.title

if (copy.hint) {
  $('hint').innerHTML = copy.hint
  $('hint').hidden = false
}
// Sending someone to a browser is only useful if a browser could get further
// than we just did. With no connection at all it cannot, and offering it invites
// them to prove the app is broken when it is not.
if (reason === 'offline') $('browser').hidden = true
else if (copy.browser) $('browser').textContent = copy.browser
if (detail) {
  $('detail').textContent = detail
  $('detail').hidden = false
  $('detail-label').hidden = false
}
if (isOss) $('server').hidden = false

// Widening backoff rather than a fixed interval: an outage that lasts an hour
// should not mean 240 reload attempts, and the app should still recover on its
// own without the user watching it.
const BACKOFF = [5, 10, 20, 40, 60]
let attempt = 0
let countdown = null
let timer = null
let busy = false

function say(text) {
  $('status').textContent = text
}

async function tryNow() {
  if (busy) return
  busy = true
  clearInterval(timer)
  $('retry').disabled = true
  say('Checking…')

  let result
  try {
    result = await window.langalphaOutage.retry()
  } catch (err) {
    // The bridge rejecting is the one failure this page cannot survive by
    // itself. Left to propagate it skips the release below, so `busy` stays
    // set, the button stays disabled and the countdown never restarts: a page
    // that exists to recover becomes the dead end it was meant to prevent.
    result = { ok: false, error: err && err.message }
  } finally {
    busy = false
    $('retry').disabled = false
  }

  // On success the shell navigates this window away; nothing more to do here.
  if (result && result.ok) return say('Reconnected. Loading…')

  attempt += 1
  schedule(result && result.error)
}

function schedule(error) {
  const wait = BACKOFF[Math.min(attempt, BACKOFF.length - 1)]
  countdown = wait
  const stamp = new Date().toLocaleTimeString()
  const tail = error ? ` ${error}` : ''

  const tick = () => {
    say(`Still unavailable as of ${stamp}.${tail} Trying again in ${countdown}s.`)
    if (countdown <= 0) return void tryNow()
    countdown -= 1
  }
  tick()
  clearInterval(timer)
  timer = setInterval(tick, 1000)
}

$('retry').addEventListener('click', () => tryNow())
// Opening the same URL in a real browser is the fastest way for a user to tell
// "the app is broken" from "the service is down", and it is the first thing
// support would ask them to do anyway.
$('browser').addEventListener('click', () => window.langalphaOutage.openExternal())
$('server').addEventListener('click', () => window.langalphaOutage.changeServer())

// The browser knows before we do when a dropped connection comes back.
window.addEventListener('online', () => tryNow())

schedule()
