import { describe, it, expect } from 'vitest';
import {
  PASSWORD_REQUIREMENTS,
  passwordMeetsRequirements,
  validatePasswordPair,
} from '@/pages/Login/passwordRequirements';

// Identity stub: validatePasswordPair returns t(key), so message === key here.
const t = (key: string) => key;

describe('PASSWORD_REQUIREMENTS', () => {
  it('enforces exactly the three documented checks', () => {
    expect(PASSWORD_REQUIREMENTS.map((r) => r.key)).toEqual([
      'auth.reqLength',
      'auth.reqLetter',
      'auth.reqNumber',
    ]);
  });
});

describe('passwordMeetsRequirements', () => {
  it('accepts a password with 8+ chars, a letter, and a number', () => {
    expect(passwordMeetsRequirements('abcd1234')).toBe(true);
  });

  it('rejects an empty password', () => {
    expect(passwordMeetsRequirements('')).toBe(false);
  });

  it('rejects a password shorter than 8 chars even if it has a letter+number', () => {
    expect(passwordMeetsRequirements('abc123')).toBe(false); // 6 chars
    expect(passwordMeetsRequirements('abcdef1')).toBe(false); // 7 chars
  });

  it('rejects an all-letters password (no number)', () => {
    expect(passwordMeetsRequirements('abcdefgh')).toBe(false);
  });

  it('rejects an all-digits password (no letter)', () => {
    expect(passwordMeetsRequirements('12345678')).toBe(false);
  });

  it('accepts exactly 8 chars on the boundary', () => {
    expect(passwordMeetsRequirements('abcdefg1')).toBe(true);
  });
});

describe('validatePasswordPair', () => {
  it('returns null when the password is strong and both entries match', () => {
    expect(validatePasswordPair('abcd1234', 'abcd1234', t)).toBeNull();
  });

  it('flags a weak password before checking the match', () => {
    // Weak AND mismatched — requirements are checked first, so weakPassword wins.
    expect(validatePasswordPair('abc', 'xyz', t)).toBe('auth.errors.weakPassword');
  });

  it('flags a mismatch when the password is strong but confirm differs', () => {
    expect(validatePasswordPair('abcd1234', 'abcd1235', t)).toBe('auth.passwordsDontMatch');
  });
});
