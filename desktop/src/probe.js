'use strict'

const { net } = require('electron')

const TIMEOUT_MS = 5000

/**
 * Is something answering at this URL?
 *
 * Deliberately does not assert that the response looks like langalpha: a
 * self-hoster may front the stack with anything, and rejecting a working server
 * because the HTML was unfamiliar is worse than accepting a wrong URL the user
 * can correct. Any HTTP answer, including a 500, counts as reachable; telling
 * "nothing is listening" apart from "it is listening and unhappy" is the whole
 * point of the distinction the outage page draws.
 *
 * Asked over Electron's network stack rather than Node's, because this decides
 * whether a WINDOW can load a URL and so has to travel the route a window would.
 * Node knows nothing about the OS proxy, a PAC script, or a certificate trusted
 * by the system store, so on a managed machine it can call a server unreachable
 * that the window loads fine, and the outage screen would then refuse to retry
 * its way out of a network that had already come back. The agreement runs both
 * ways: Chromium refuses its unsafe-port list here exactly as it would in the
 * window, so a port a window could never load no longer probes as reachable.
 */
async function probe(rawUrl) {
  let target
  try {
    target = new URL(rawUrl)
  } catch {
    return { ok: false, error: 'That is not a valid URL.' }
  }
  if (target.protocol !== 'http:' && target.protocol !== 'https:') {
    return { ok: false, error: 'Use an http:// or https:// address.' }
  }

  // `net.request`, not `net.fetch`: fetch leaves `response.url` empty and
  // `redirected` false even when it followed a 302, so the origin that actually
  // answered is unrecoverable from it. The request API reports each hop.
  return new Promise((resolve) => {
    const request = net.request({ url: target.toString(), redirect: 'follow' })

    // The origin that answered, which is not always the one that was asked. A
    // bare hostname is prefixed `http://` and a great many stacks redirect that
    // straight to https, so reporting the typed origin means the app adopts
    // `http://host`, and its very first navigation to `https://host` is a
    // different origin: the policy reads it as somewhere else and hands it to
    // the system browser instead of loading it.
    let origin = target.origin
    let settled = false
    const settle = (value) => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      resolve(value)
    }

    const timer = setTimeout(() => {
      request.abort()
      settle({ ok: false, error: 'No response after 5 seconds.' })
    }, TIMEOUT_MS)

    // Observed, never followed by hand: in `follow` mode Chromium walks the
    // chain itself and caps it at 20 hops with ERR_TOO_MANY_REDIRECTS.
    request.on('redirect', (_status, _method, redirectUrl) => {
      try {
        origin = new URL(redirectUrl).origin
      } catch {
        // A hop that does not parse is not a server we can adopt; the last good
        // origin is the honest fallback and `adoptServer` still has the final
        // say on it.
      }
    })

    request.on('response', (res) => {
      // Nothing here reads the body, and an undrained response holds the socket.
      res.resume()
      const type = [].concat(res.headers['content-type'] || [])[0] || ''
      settle({ ok: true, status: res.statusCode, origin, contentType: type })
    })

    request.on('error', (err) => {
      settle({ ok: false, error: `Could not reach it: ${err.message}` })
    })

    request.end()
  })
}

/**
 * The probe, for an address the app is about to show as its whole window.
 *
 * Still not a check that the page is LangAlpha's. A reply in JSON is never a
 * page at all, and the address that gives one is nearly always the API port of
 * a stack whose web app is served elsewhere: connected, the window would show
 * nothing but `{"detail":"Not Found"}`.
 */
async function probeWebApp(rawUrl) {
  const result = await probe(rawUrl)
  if (result.ok && /[/+]json\b/i.test(result.contentType || '')) {
    return {
      ok: false,
      error: 'That address serves data, not a web page, so it is likely LangAlpha\u2019s API rather than its web app. Use the address you open in a browser, such as http://localhost:5173.',
    }
  }
  return result
}

module.exports = { probe, probeWebApp }
