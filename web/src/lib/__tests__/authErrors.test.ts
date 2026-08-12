import { describe, it, expect } from 'vitest';
import type { TFunction } from 'i18next';
import { authErrorMessage } from '@/lib/authErrors';

// Identity stub: returns the i18n key verbatim so assertions verify the
// code→key mapping table directly, without coupling to translated copy.
const t = ((key: string) => key) as unknown as TFunction;

describe('authErrorMessage', () => {
  describe('mapped Supabase codes → i18n keys', () => {
    const cases: Array<[string, string]> = [
      ['invalid_credentials', 'auth.errors.invalidCredentials'],
      ['email_not_confirmed', 'auth.errors.emailNotConfirmed'],
      ['over_email_send_rate_limit', 'auth.errors.rateLimited'],
      ['over_request_rate_limit', 'auth.errors.rateLimited'],
      ['user_already_exists', 'auth.errors.userAlreadyExists'],
      ['email_exists', 'auth.errors.userAlreadyExists'],
      ['weak_password', 'auth.errors.weakPassword'],
      ['otp_expired', 'auth.errors.otpExpired'],
      ['same_password', 'auth.errors.samePassword'],
    ];

    it.each(cases)('maps %s to %s (mapped key wins over raw message)', (code, key) => {
      const info = authErrorMessage({ code, message: 'raw api text' }, t);
      expect(info).toEqual({ message: key, code });
    });

    it('collapses both rate-limit codes onto the same key', () => {
      const a = authErrorMessage({ code: 'over_email_send_rate_limit' }, t);
      const b = authErrorMessage({ code: 'over_request_rate_limit' }, t);
      expect(a.message).toBe(b.message);
    });

    it('collapses both account-exists codes onto the same key', () => {
      const a = authErrorMessage({ code: 'user_already_exists' }, t);
      const b = authErrorMessage({ code: 'email_exists' }, t);
      expect(a.message).toBe(b.message);
    });
  });

  describe('unmapped / fallback paths', () => {
    it('falls back to the raw message for an unknown code, preserving the code', () => {
      const info = authErrorMessage({ code: 'brand_new_code', message: 'Boom' }, t);
      expect(info).toEqual({ message: 'Boom', code: 'brand_new_code' });
    });

    it('uses the generic key when an unknown code carries no message', () => {
      const info = authErrorMessage({ code: 'brand_new_code' }, t);
      expect(info).toEqual({ message: 'auth.errors.generic', code: 'brand_new_code' });
    });

    it('nulls a non-string code and returns the raw message', () => {
      const info = authErrorMessage({ code: 42, message: 'numeric code' }, t);
      expect(info).toEqual({ message: 'numeric code', code: null });
    });

    it('returns generic + null code for null/undefined errors', () => {
      expect(authErrorMessage(null, t)).toEqual({ message: 'auth.errors.generic', code: null });
      expect(authErrorMessage(undefined, t)).toEqual({ message: 'auth.errors.generic', code: null });
    });

    it('uses a plain Error message with a null code', () => {
      const info = authErrorMessage(new Error('network down'), t);
      expect(info).toEqual({ message: 'network down', code: null });
    });
  });
});
