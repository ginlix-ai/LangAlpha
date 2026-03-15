/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_API_BASE_URL?: string;
  readonly VITE_SUPABASE_URL?: string;
  readonly VITE_SUPABASE_PUBLISHABLE_KEY?: string;
  readonly VITE_AUTH_USER_ID?: string;
  readonly VITE_CDN_BASE?: string;
}
interface ImportMeta {
  readonly env: ImportMetaEnv;
}

