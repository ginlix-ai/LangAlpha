import { describe, it, expect, vi, afterEach } from 'vitest';

/**
 * `global: { fetch: authFetch }` is the whole activation.
 *
 * Both corrections for issue #379 that live below the app -- recomputing
 * `expires_at` on the local clock, and answering a rate limit with a status
 * auth-js will retry instead of one it signs the user out over -- only happen
 * because the client was built on our fetch. Delete that one line and the two
 * modules stay fully tested and completely inert, which no other test here
 * would notice: the storm suite builds its own client to drive auth-js
 * directly, so it proves the corrections work, not that they are installed.
 */
// Typed to the real call shape so the third argument is inspectable below; an
// untyped `vi.fn` infers no parameters and `calls[0][2]` comes out as `never`.
const createBrowserClient = vi.fn(
  (_url: string, _key: string, _options?: { global?: { fetch?: typeof fetch } }) => ({ auth: {} }),
);
vi.mock('@supabase/ssr', () => ({ createBrowserClient }));

afterEach(() => {
  vi.unstubAllEnvs();
  vi.clearAllMocks();
});

describe('the Supabase client the app actually uses', () => {
  it('is built on authFetch', async () => {
    vi.stubEnv('VITE_SUPABASE_URL', 'https://project.supabase.co');
    vi.stubEnv('VITE_SUPABASE_PUBLISHABLE_KEY', 'anon-key');
    vi.resetModules();

    const { authFetch } = await import('../authFetch');
    await import('../supabase');

    expect(createBrowserClient).toHaveBeenCalledTimes(1);
    // Identity, not shape: a wrapper around it would still be a different fetch.
    expect(createBrowserClient.mock.calls[0][2]?.global?.fetch).toBe(authFetch);
  });

  it('is not built at all without the env vars, so OSS ships no client', async () => {
    vi.stubEnv('VITE_SUPABASE_URL', '');
    vi.stubEnv('VITE_SUPABASE_PUBLISHABLE_KEY', '');
    vi.resetModules();

    const { supabase } = await import('../supabase');

    expect(createBrowserClient).not.toHaveBeenCalled();
    expect(supabase).toBeNull();
  });
});
