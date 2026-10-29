/**
 * The user the app presents when it runs without an auth client: OSS mode, or a
 * platform build missing its Supabase env. `VITE_AUTH_USER_ID` overrides it.
 */
export const LOCAL_USER_ID: string = (import.meta.env.VITE_AUTH_USER_ID as string) || 'local-dev-user';

/**
 * Whether the build carries the env `lib/supabase` builds its auth client from.
 * Read off the env rather than the client, so asking does not load the SDK.
 */
export const hasAuthEnv: boolean = Boolean(
  import.meta.env.VITE_SUPABASE_URL && import.meta.env.VITE_SUPABASE_PUBLISHABLE_KEY,
);
