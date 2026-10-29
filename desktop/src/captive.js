'use strict'

const TIMEOUT_MS = 2000

// Plain HTTP on purpose, and this is the whole trick: a portal can only
// intercept cleartext. Over HTTPS the same request fails the handshake, which
// looks identical to the host being unreachable, so the check would answer the
// question it was written to distinguish. Every OS does it this way.
const CHECK_URL = 'http://cp.cloudflare.com/generate_204'

/**
 * A host on this machine or this LAN. Nothing a portal does can explain a
 * failure to reach one, so an OSS user pointed at their own stack never causes
 * an external request.
 */
function isLocalTarget(rawUrl) {
  let host
  try {
    host = new URL(rawUrl).hostname.toLowerCase()
  } catch {
    return true // Unparseable is not a case worth phoning out over.
  }
  host = host.replace(/^\[|\]$/g, '') // IPv6 literals arrive bracketed.

  if (host === 'localhost' || host.endsWith('.localhost')) return true
  if (host.endsWith('.local')) return true // mDNS
  // A name with no dot in it was resolved by something other than public DNS:
  // a hosts file, mDNS, or the LAN's own resolver. `http://nas:5173` is an
  // ordinary way to reach a self-hosted stack, and reading it as public is what
  // made a machine that cannot reach its own server phone a third party about it.
  if (!host.includes('.')) return true
  // The suffixes reserved for exactly this. `.home.arpa` is RFC 8375's answer to
  // everyone having used `.home` anyway, so both are here alongside the two that
  // cloud and router vendors settled on.
  if (/\.(home\.arpa|internal|intranet|lan|home|corp|private)$/.test(host)) return true
  if (host === '::1' || host === '0.0.0.0') return true
  if (host.startsWith('fe80:')) return true // IPv6 link-local
  // RFC 4193 unique-local, the IPv6 equivalent of 10/8 and 192.168/16, and the
  // range a self-hoster's own network most likely uses.
  if (/^f[cd][0-9a-f]{2}:/.test(host)) return true

  const v4 = host.match(/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/)
  if (!v4) return false
  const [a, b] = [Number(v4[1]), Number(v4[2])]
  if (a === 127 || a === 10) return true
  if (a === 192 && b === 168) return true
  if (a === 172 && b >= 16 && b <= 31) return true
  if (a === 169 && b === 254) return true // link-local
  if (a === 100 && b >= 64 && b <= 127) return true // RFC 6598 carrier-grade NAT
  return false
}

/**
 * Is the machine behind a sign-in portal?
 *
 * Only ever answers true on positive evidence: something answered, and it was
 * not the 204 the endpoint exists to return. A throw means we learned nothing,
 * which is not the same as "no portal" but is the only safe thing to report,
 * because claiming a portal that is not there sends the user hunting for a
 * sign-in page that does not exist.
 */
async function behindPortal(targetUrl) {
  if (isLocalTarget(targetUrl)) return false

  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), TIMEOUT_MS)
  try {
    const res = await fetch(CHECK_URL, {
      signal: controller.signal,
      // A portal's 302 is the signal itself, so it must not be followed.
      redirect: 'manual',
      cache: 'no-store',
    })
    return res.status !== 204
  } catch {
    return false
  } finally {
    clearTimeout(timer)
  }
}

module.exports = { behindPortal, isLocalTarget, CHECK_URL }
