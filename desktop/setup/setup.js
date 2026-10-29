'use strict'

// Runs in the setup window's isolated world, talking to main through the
// setup-preload bridge. Kept as a separate file rather than inline so the page
// can keep a script-src 'self' CSP.

const input = document.getElementById('url')
const status = document.getElementById('status')
const testButton = document.getElementById('test')
const connectButton = document.getElementById('connect')

const DEFAULT_URL = 'http://localhost:5173'

function say(message, tone) {
  status.textContent = message
  if (tone) status.dataset.tone = tone
  else delete status.dataset.tone
}

function busy(value) {
  testButton.disabled = value
  connectButton.disabled = value
}

function value() {
  const raw = input.value.trim() || DEFAULT_URL
  // A bare host is what people type; without this "localhost:5173" parses as a
  // URL whose protocol is "localhost:".
  return /^https?:\/\//i.test(raw) ? raw : `http://${raw}`
}

async function probe(url) {
  busy(true)
  say('Checking…')
  try {
    const result = await window.langalphaSetup.probe(url)
    if (result.ok) say(`Reached it (HTTP ${result.status}).`, 'good')
    else say(result.error, 'bad')
    return result.ok ? result : null
  } catch (err) {
    say(err && err.message ? err.message : 'Could not reach that address.', 'bad')
    return null
  } finally {
    busy(false)
  }
}

testButton.addEventListener('click', () => probe(value()))

connectButton.addEventListener('click', async () => {
  // Read once. The probe is a round trip the user can type through, and
  // re-reading the field afterwards would connect to whatever it says by then
  // rather than to the address that was actually reachable.
  const url = value()
  // Probe first so an unreachable address fails here, with an explanation,
  // rather than as a blank window after the navigation.
  const reached = await probe(url)
  if (!reached) return
  busy(true)
  try {
    // The origin the probe actually reached, so an address that redirects is
    // adopted as where it landed rather than where it started.
    const result = await window.langalphaSetup.use(reached.origin || url)
    // On success the shell loads the app over this window; leaving the buttons
    // disabled is correct, there is nothing left to press.
    if (result.ok) return
    say(result.error, 'bad')
  } catch (err) {
    say(err && err.message ? err.message : 'Could not switch to that server.', 'bad')
  }
  busy(false)
})

input.addEventListener('keydown', (event) => {
  if (event.key === 'Enter') connectButton.click()
})

window.langalphaSetup.current().then((stored) => {
  input.value = stored || ''
}).catch(() => {
  // Not having a previous address to prefill is a worse page, not a broken one.
}).finally(() => {
  // Outside the success path on purpose: an empty field the user can type into
  // beats a focused field that never arrives.
  input.focus()
  input.select()
})
