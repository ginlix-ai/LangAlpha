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

function focusAddress() {
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
}

// ---------------------------------------------------------------------------
// The stages. A build with no bundled stack only ever shows the connect form, and
// shows it as it always has.
// ---------------------------------------------------------------------------

const stages = ['choose', 'server', 'progress']
const stepsList = document.getElementById('steps')
const failureBox = document.getElementById('failure')
const backButton = document.getElementById('back')

function showStage(name) {
  for (const stage of stages) document.getElementById(stage).hidden = stage !== name
  if (name === 'server') focusAddress()
}

// Which stage a status asks for. A build with no stack only ever has the form.
function stageFor(status) {
  if (!status || !status.available) return 'server'
  const state = status.view && status.view.state
  return state === 'starting' || state === 'failed' ? 'progress' : 'choose'
}

function renderProgress(view) {
  stepsList.replaceChildren(...view.steps.map((step) => {
    const item = document.createElement('li')
    item.textContent = step.label
    item.dataset.status = step.status
    return item
  }))
  const failed = view.state === 'failed' && view.failure
  failureBox.hidden = !failed
  document.getElementById('progress-title').textContent = failed ? 'LangAlpha could not start' : 'Starting LangAlpha'
  document.getElementById('progress-lede').hidden = Boolean(failed)
  if (!failed) return
  // The step list above already marks which one it was.
  document.getElementById('failure-message').textContent = view.failure.message
  document.getElementById('failure-note').textContent = view.failure.dataNote || ''
  const tail = document.getElementById('failure-tail')
  tail.textContent = view.failure.tail || ''
  tail.scrollTop = tail.scrollHeight
}

function render(status) {
  const stage = stageFor(status)
  if (stage === 'progress') renderProgress(status.view)
  showStage(stage)
}

function useStack() {
  showStage('progress')
  failureBox.hidden = true
  window.langalphaSetup.useStack().catch((err) => {
    document.getElementById('failure-message').textContent = err && err.message ? err.message : 'Could not start.'
    failureBox.hidden = false
  })
}

document.getElementById('use-stack').addEventListener('click', useStack)
document.getElementById('retry').addEventListener('click', useStack)
document.getElementById('reveal').addEventListener('click', () => window.langalphaSetup.revealLogs())
const copyPrompt = document.getElementById('copy-prompt')
let copiedTimer = null
copyPrompt.addEventListener('click', async () => {
  const done = await window.langalphaSetup.copyHelpPrompt().catch(() => false)
  copyPrompt.textContent = done ? 'Copied' : 'Could not copy'
  clearTimeout(copiedTimer)
  copiedTimer = setTimeout(() => { copyPrompt.textContent = 'Copy Prompt' }, 2000)
})
for (const button of [document.getElementById('use-server'), document.getElementById('failed-server')]) {
  button.addEventListener('click', () => {
    backButton.hidden = false
    showStage('server')
  })
}
backButton.addEventListener('click', () => showStage('choose'))

// A change moves the page into progress only. One that ends it (ready, stopped)
// is followed by this window closing, or by the user's own next step, and taking
// them back to the choice would pull the form out from under their typing.
window.langalphaSetup.onStackChange((view) => {
  const status = { available: true, view }
  if (stageFor(status) === 'progress') render(status)
})

window.langalphaSetup.stackStatus().then(render).catch(() => render(null))
