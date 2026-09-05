import { describe, it, expect, vi, beforeEach } from 'vitest';
import { withShellReturn, shellHandoffUrl } from '@/lib/desktopAuthHandoff';

// `desktop.ts` captures the bridge at module load, so the flag has to be
// swappable from here rather than by assigning window.langalphaDesktop later.
let shellScheme: string | undefined;
vi.mock('@/lib/desktop', () => ({
  get desktop() { return shellScheme === undefined ? undefined : { scheme: shellScheme }; },
  isDesktopShell: () => shellScheme !== undefined,
}));

beforeEach(() => {
  shellScheme = undefined;
});

describe('withShellReturn', () => {
  it('leaves a browser flow unmarked', () => {
    expect(withShellReturn('https://app.langalpha.ai/auth/confirm'))
      .toBe('https://app.langalpha.ai/auth/confirm');
  });

  // A path, never a query: the email template appends its own `?`, so a marker
  // in the query pushes the token into the previous parameter's value.
  it('marks a flow that began in the shell, leaving the query free', () => {
    shellScheme = 'langalpha';
    expect(withShellReturn('https://app.langalpha.ai/auth/confirm'))
      .toBe('https://app.langalpha.ai/auth/confirm/desktop');
  });

  // The two editions install side by side on one machine and answer on
  // different schemes, so a link marked for the wrong one opens the other build.
  it('names the edition, so the link returns to the build that sent it', () => {
    shellScheme = 'langalpha-oss';
    expect(withShellReturn('http://localhost/auth/confirm'))
      .toBe('http://localhost/auth/confirm/desktop-oss');
  });

  // Feature-detected like every other bridge member: the shell ships on its own
  // cadence, so an older one falls back to redeeming in the browser.
  it('marks nothing when the shell is too old to name its scheme', () => {
    shellScheme = '';
    expect(withShellReturn('http://localhost/auth/confirm'))
      .toBe('http://localhost/auth/confirm');
  });
});

describe('shellHandoffUrl', () => {
  it('hands a marked link to the app, carrying the payload', () => {
    expect(shellHandoffUrl('/auth/confirm/desktop', '?token_hash=abc123&type=email'))
      .toBe('langalpha://callback?token_hash=abc123&type=email');
  });

  it('carries a recovery link too, not only signup confirmation', () => {
    expect(shellHandoffUrl('/reset-password/desktop', '?token_hash=abc123&type=recovery'))
      .toBe('langalpha://callback?token_hash=abc123&type=recovery');
  });

  it('sends a self-hosted link to the self-hosted scheme', () => {
    expect(shellHandoffUrl('/auth/confirm/desktop-oss', '?token_hash=abc123&type=email'))
      .toBe('langalpha-oss://callback?token_hash=abc123&type=email');
  });

  // The table is the allow list. Asking the OS to open a scheme launches an
  // application, so a segment nobody registered must never become one.
  it('refuses a segment the table does not know', () => {
    expect(shellHandoffUrl('/auth/confirm/evil', '?token_hash=abc123&type=email')).toBeNull();
  });

  it('ignores an unmarked link, which is every browser signup', () => {
    expect(shellHandoffUrl('/auth/confirm', '?token_hash=abc123&type=email')).toBeNull();
  });

  // Bouncing it back out through the OS would be a loop: the app is where the
  // token was being sent.
  it('ignores a marked link that already reached the app', () => {
    shellScheme = 'langalpha';
    expect(shellHandoffUrl('/auth/confirm/desktop', '?token_hash=abc123&type=email')).toBeNull();
  });

  // A used or expired link comes back as error params with no token. Handing
  // that to the app would replace a readable message with a silent no-op.
  it('keeps a rejected link here, where its error can be reported', () => {
    expect(shellHandoffUrl('/auth/confirm/desktop', '?error=access_denied&error_code=otp_expired'))
      .toBeNull();
  });

  // The two halves run in different windows and only meet in the mail. The
  // template appends `?token_hash={{ .TokenHash }}&type=email` to whatever the
  // app produced, so a marker in the query made that the URL's second `?` and
  // the token parsed as part of the marker's value -- a valid link reported as
  // expired, with the token never sent.
  it('survives the email template appending a query of its own', () => {
    shellScheme = 'langalpha-oss';
    const returnTo = withShellReturn('http://localhost/auth/confirm');

    shellScheme = undefined;
    const link = new URL(`${returnTo}?token_hash=pkce_abc123&type=email`);
    expect(shellHandoffUrl(link.pathname, link.search))
      .toBe('langalpha-oss://callback?token_hash=pkce_abc123&type=email');
  });
});
