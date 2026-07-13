import type { TFunction } from 'i18next';

/**
 * Maps Supabase AuthError codes to i18n keys so users see actionable copy
 * instead of raw API messages. Unmapped codes fall back to the raw message.
 * Codes: https://supabase.com/docs/guides/auth/debugging/error-codes
 */
const CODE_TO_KEY: Record<string, string> = {
  invalid_credentials: 'auth.errors.invalidCredentials',
  email_not_confirmed: 'auth.errors.emailNotConfirmed',
  over_email_send_rate_limit: 'auth.errors.rateLimited',
  over_request_rate_limit: 'auth.errors.rateLimited',
  user_already_exists: 'auth.errors.userAlreadyExists',
  email_exists: 'auth.errors.userAlreadyExists',
  weak_password: 'auth.errors.weakPassword',
  otp_expired: 'auth.errors.otpExpired',
  same_password: 'auth.errors.samePassword',
};

export interface AuthErrorInfo {
  message: string;
  /** Supabase AuthError.code when present (e.g. 'email_not_confirmed'). */
  code: string | null;
}

export function authErrorMessage(err: unknown, t: TFunction): AuthErrorInfo {
  const rawCode = (err as { code?: unknown })?.code;
  const code = typeof rawCode === 'string' ? rawCode : null;
  const key = code ? CODE_TO_KEY[code] : undefined;
  if (key) return { message: t(key), code };
  const raw = (err as Error)?.message;
  return { message: raw || t('auth.errors.generic'), code };
}
