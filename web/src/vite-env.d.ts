/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_API_BASE_URL?: string;
  readonly VITE_HOST_MODE?: 'oss' | 'platform';
  readonly VITE_SUPABASE_URL?: string;
  readonly VITE_SUPABASE_PUBLISHABLE_KEY?: string;
  readonly VITE_AUTH_USER_ID?: string;
  readonly VITE_CDN_BASE?: string;
  // Parent domain shared by all first-party cookies (auth + locale). Unset →
  // host-only (the default); set to a parent domain for cross-subdomain SSO.
  readonly VITE_COOKIE_DOMAIN?: string;
  readonly VITE_APP_ENTRY_PATH?: string;
  // Origin (or same-host path) of the platform console — account, plans,
  // integrations. Cross-app links only; never a router target.
  readonly VITE_PLATFORM_URL?: string;
  readonly VITE_CONTACT_EMAILS?: string;
  readonly VITE_PRIVACY_URL?: string;
}

/** The installed monaco-editor release, from `scripts/monaco-version.ts`. */
declare const __MONACO_VERSION__: string;

interface ImportMeta {
  readonly env: ImportMetaEnv;
}

