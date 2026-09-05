/**
 * The form of a URL that is safe to put in front of a person.
 *
 * Query parameters carry session tokens and, on the OAuth callback, the
 * authorization code itself. This strips them for anything a human or a log
 * file might see: the outage screen and any screenshot of it, and the main
 * process log. Nothing functional reads this form, so it can lose whatever it
 * likes; retries use the full URL held in the window's record.
 */
function forDisplay(rawUrl) {
  try {
    const u = new URL(rawUrl)
    // An opaque URL has the *string* origin "null" and carries its whole payload
    // in the path: for `mailto:` that is somebody's address, for `file:` a path
    // on this machine. Both reach here, because mailto is an openable scheme, so
    // the scheme is the only part of them worth printing.
    if (u.origin === 'null') return `${u.protocol}…`
    return u.search || u.hash ? `${u.origin}${u.pathname}…` : `${u.origin}${u.pathname}`
  } catch {
    return rawUrl
  }
}

module.exports = { forDisplay }
