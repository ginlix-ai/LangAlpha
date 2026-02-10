import { createClient } from '@supabase/supabase-js';

const supabaseUrl = import.meta.env.VITE_SUPABASE_URL;
const supabaseKey = import.meta.env.VITE_SUPABASE_PUBLISHABLE_KEY;

// Warn only when partially configured (one set, other missing).
// Both unset is fine — auth is simply disabled for local dev.
if (supabaseUrl && !supabaseKey) {
  console.warn('[supabase] VITE_SUPABASE_URL is set but VITE_SUPABASE_PUBLISHABLE_KEY is missing');
} else if (!supabaseUrl && supabaseKey) {
  console.warn('[supabase] VITE_SUPABASE_PUBLISHABLE_KEY is set but VITE_SUPABASE_URL is missing');
}

// Only create a real client when fully configured; otherwise export null.
// AuthContext.jsx already short-circuits to local-dev mode when the URL is
// missing, so no code path will call supabase.auth.* in that case.
export const supabase =
  supabaseUrl && supabaseKey ? createClient(supabaseUrl, supabaseKey) : null;
