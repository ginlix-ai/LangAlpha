import React, { useState } from 'react';

/**
 * Circular monogram fallback shown when a favicon fails to load (or no
 * domain is available). Renders the first character of the label.
 */
export function Monogram({ letter, size = 14 }: { letter: string; size?: number }): React.ReactElement {
  return (
    <span
      style={{
        width: size,
        height: size,
        borderRadius: size / 2,
        background: 'var(--color-bg-surface)',
        display: 'inline-flex',
        alignItems: 'center',
        justifyContent: 'center',
        fontSize: size * 0.65,
        fontWeight: 600,
        color: 'var(--color-text-secondary)',
        flexShrink: 0,
        textTransform: 'uppercase',
      }}
    >
      {letter}
    </span>
  );
}

/** Hostname suffixes that are never publicly routable. */
const NON_PUBLIC_SUFFIXES = ['.local', '.internal', '.lan', '.corp', '.home', '.test', '.localhost'];

/**
 * True only for clearly-public registrable domains. Anything that could be a
 * private/internal host (localhost, single-label names, IP literals, RFC1918 /
 * link-local ranges, internal TLDs) returns false so we never leak it to the
 * third-party favicon service. Accepts either a full URL or a bare hostname.
 */
export function isPublicHost(input: string): boolean {
  if (!input) return false;

  let host = input.trim().toLowerCase();
  // Accept a full URL or a bare host; fall back to treating input as hostname.
  try {
    host = new URL(input).hostname.toLowerCase();
  } catch {
    // Not a parseable URL — strip a leading scheme-less authority if present.
    host = host.replace(/^\/\//, '').split('/')[0].split('?')[0].split('#')[0];
    // Drop a trailing :port (IPv6 literals are rejected separately below).
    if (!host.includes('[') && (host.match(/:/g) || []).length === 1) {
      host = host.split(':')[0];
    }
  }
  // Strip brackets from IPv6 literals and any trailing dot.
  host = host.replace(/^\[|\]$/g, '').replace(/\.$/, '');

  if (!host) return false;
  if (host === 'localhost') return false;

  // IPv6 literals contain a colon.
  if (host.includes(':')) return false;

  // Single-label hostnames (no dot) — localhost, intranet, etc.
  if (!host.includes('.')) return false;

  // Internal TLD suffixes.
  if (NON_PUBLIC_SUFFIXES.some((suffix) => host.endsWith(suffix))) return false;

  // IPv4 literals — including RFC1918 / link-local private ranges.
  if (/^\d{1,3}(\.\d{1,3}){3}$/.test(host)) {
    const octets = host.split('.').map(Number);
    if (octets.some((o) => o > 255)) return false; // malformed → treat as non-public
    const [a, b] = octets;
    if (a === 10) return false; // 10.0.0.0/8
    if (a === 172 && b >= 16 && b <= 31) return false; // 172.16.0.0/12
    if (a === 192 && b === 168) return false; // 192.168.0.0/16
    if (a === 169 && b === 254) return false; // 169.254.0.0/16 link-local
    if (a === 127) return false; // loopback
    return false; // any other bare IPv4 literal is not a registrable domain
  }

  return true;
}

/**
 * Favicon for a domain, sourced from Google's s2 favicon service. Falls back
 * to a {@link Monogram} of the domain's first character when the image fails,
 * the domain is empty, or the host is non-public (never sent to Google).
 */
export function Favicon({ domain, size = 14 }: { domain: string; size?: number }): React.ReactElement {
  const [failed, setFailed] = useState(false);

  if (failed || !domain || !isPublicHost(domain)) {
    return <Monogram letter={domain.charAt(0) || '?'} size={size} />;
  }

  return (
    <img
      src={`https://www.google.com/s2/favicons?domain=${encodeURIComponent(domain)}&sz=32`}
      alt=""
      width={size}
      height={size}
      loading="lazy"
      decoding="async"
      style={{ borderRadius: size > 14 ? 3 : 2, flexShrink: 0 }}
      onError={() => setFailed(true)}
    />
  );
}
