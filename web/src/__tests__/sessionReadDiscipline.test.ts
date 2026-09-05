import { describe, it, expect } from 'vitest';
import { readdirSync, readFileSync, statSync } from 'node:fs';
import path from 'node:path';

/**
 * A request must not cost a token refresh.
 *
 * `supabase.auth.getSession()` is not an accessor: its own docs say it "returns
 * the session, refreshing it if necessary". Calling it once per outbound
 * request therefore spends a network refresh every time the session reads as
 * near expiry, and on a device whose clock is off by more than
 * `JWT_TTL - 90s` that is every single time. Supabase caps `/auth/v1/token` at
 * 1800/hr per IP with a burst of 30 and does not make it configurable, so a
 * couple of page loads exhaust it; auth-js then treats the 429 as fatal and
 * destroys the session, which the user experiences as being signed out and
 * unable to reload their way back in (issue #379).
 *
 * The whole fix is that exactly one module reads the session and everything
 * else reads its cache. Putting `getSession()` back into a request path is a
 * one-line edit that typechecks, lints, and passes every other test here. The
 * original bug arrived exactly that way, when a commit swapped a synchronous
 * module-level user id for a `getSession()` call and kept the five lines around
 * it. This is the only thing that would notice.
 *
 * Allowances are per file and deliberately narrow. Widening one means a second
 * module now reads the session, which is the regression itself.
 */
const SRC = path.resolve(__dirname, '..');

/** Where a real session read belongs, and why it is once-per-lifecycle there. */
const ALLOWED = new Map<string, string>([
  // The cache itself: one read behind a single-flight, which is the fix.
  ['lib/authToken.ts', 'the shared cache is the one reader'],
  // Bootstrap, and the OAuth popup's BroadcastChannel handover. Both run once
  // per document, not per request.
  ['contexts/AuthContext.tsx', 'once at bootstrap and once per OAuth handover'],
]);

/**
 * Deliberately not a parser. It catches the shape the regression actually
 * takes -- a plain `auth.getSession()` added to a request path, which is
 * exactly how #379 arrived -- and `getUser()` / `getClaims()` alongside it,
 * which are different endpoints but the same mistake: a network round trip per
 * request.
 * Aliasing the namespace first (`const a = sb.auth; a.getSession()`) walks
 * past it. That is accepted: someone doing that is not making the honest
 * one-line mistake this exists to catch.
 */
const SESSION_READ =
  /\bauth\s*(?:\.\s*(getSession|refreshSession|getUser|getClaims)\s*\(|\[\s*['"](getSession|refreshSession|getUser|getClaims)['"]\s*\]\s*\()/;

function sourceFiles(dir: string): string[] {
  const out: string[] = [];
  for (const entry of readdirSync(dir)) {
    const full = path.join(dir, entry);
    if (statSync(full).isDirectory()) {
      // Tests mock the client wholesale; they are not request paths.
      if (entry === '__tests__' || entry === 'test') continue;
      out.push(...sourceFiles(full));
    } else if (/\.(ts|tsx)$/.test(entry)) {
      out.push(full);
    }
  }
  return out;
}

describe('who is allowed to read the Supabase session', () => {
  it('is only the token cache and the auth bootstrap', () => {
    const offenders = sourceFiles(SRC)
      .filter((file) => SESSION_READ.test(readFileSync(file, 'utf8')))
      .map((file) => path.relative(SRC, file))
      .filter((rel) => !ALLOWED.has(rel));

    expect(offenders).toEqual([]);
  });

  it('still has a reader, so the check above is testing something', () => {
    // If the cache stopped reading the session, every assertion here would pass
    // vacuously while the app had no way to obtain a token at all.
    for (const allowed of ALLOWED.keys()) {
      const text = readFileSync(path.join(SRC, allowed), 'utf8');
      expect(SESSION_READ.test(text), `${allowed} no longer reads the session`).toBe(true);
    }
  });

  it('keeps the Bearer header in one place', () => {
    // Two call sites once built this header from their own session read, and
    // one of them grew a private pre-expiry margin that then drifted. The
    // header belongs next to the token, in `lib/authToken`.
    //
    // Both spellings the codebase uses, object literal and assignment: an
    // earlier version of this check only saw the literal, so the axios
    // interceptor's own two builds were invisible to the thing auditing them.
    const BEARER_BUILD = /Authorization["']?\s*[:=]\s*(?:`Bearer \$\{|['"]Bearer['"]\s*\+)/;
    const builders = sourceFiles(SRC)
      .filter((file) => BEARER_BUILD.test(readFileSync(file, 'utf8')))
      .map((file) => path.relative(SRC, file));

    expect(builders.sort()).toEqual([
      // The axios interceptor stamps the header it was handed. It reads the
      // cache for the token and never a session, which is the part that matters.
      'api/client.ts',
      // `syncUser` posts with the session the auth event handed it, which is
      // the one request that legitimately predates the cache being populated.
      'contexts/AuthContext.tsx',
      'lib/authToken.ts',
    ]);
  });
});
