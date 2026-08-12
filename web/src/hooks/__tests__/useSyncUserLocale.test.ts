import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { renderHook } from '@testing-library/react';
import { useSyncUserLocale } from '../useSyncUserLocale';
import { getLocaleCookie, setLocaleCookie } from '../../lib/locale';

vi.mock('../useUser', () => ({
  useUser: vi.fn(),
}));

vi.mock('react-i18next', () => ({
  useTranslation: vi.fn(),
}));

import { useUser } from '../useUser';
import { useTranslation } from 'react-i18next';

const mockUseUser = useUser as Mock;
const mockUseTranslation = useTranslation as unknown as Mock;

type I18nStub = { language: string; changeLanguage: Mock };

function makeI18n(language = 'en-US'): I18nStub {
  const stub: I18nStub = {
    language,
    changeLanguage: vi.fn((lang: string) => {
      // Mirror real i18next behavior so subsequent reads see the new language.
      stub.language = lang;
      return Promise.resolve();
    }),
  };
  return stub;
}

function clearLocaleCookie() {
  document.cookie = 'locale=; path=/; expires=Thu, 01 Jan 1970 00:00:00 GMT';
}

let mockI18n: I18nStub;

describe('useSyncUserLocale', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    clearLocaleCookie();
    mockI18n = makeI18n('en-US');
    mockUseTranslation.mockReturnValue({ i18n: mockI18n });
  });

  it('seeds locale from the server on first render (no cookie yet)', () => {
    mockUseUser.mockReturnValue({ user: { locale: 'zh-CN' } });

    renderHook(() => useSyncUserLocale());

    expect(mockI18n.changeLanguage).toHaveBeenCalledTimes(1);
    expect(mockI18n.changeLanguage).toHaveBeenCalledWith('zh-CN');
    expect(getLocaleCookie()).toBe('zh-CN');
  });

  it('does not re-apply when user.locale changes after the first sync (regression: locale race)', () => {
    mockUseUser.mockReturnValue({ user: { locale: 'zh-CN' } });

    const { rerender } = renderHook(() => useSyncUserLocale());

    expect(mockI18n.changeLanguage).toHaveBeenCalledTimes(1);
    expect(getLocaleCookie()).toBe('zh-CN');

    // Stale /users/me refetch returns the prior server value after a local pick.
    mockUseUser.mockReturnValue({ user: { locale: 'en-US' } });
    rerender();

    // Latch holds: no second changeLanguage, cookie untouched.
    expect(mockI18n.changeLanguage).toHaveBeenCalledTimes(1);
    expect(getLocaleCookie()).toBe('zh-CN');
  });

  it('an existing cookie wins — the DB value does not override it', () => {
    setLocaleCookie('en-US');
    mockUseUser.mockReturnValue({ user: { locale: 'zh-CN' } });

    renderHook(() => useSyncUserLocale());

    expect(mockI18n.changeLanguage).not.toHaveBeenCalled();
    expect(getLocaleCookie()).toBe('en-US');
  });

  it('seeds the cookie even when i18n already matches (no changeLanguage needed), and latches', () => {
    mockUseUser.mockReturnValue({ user: { locale: 'en-US' } });

    const { rerender } = renderHook(() => useSyncUserLocale());

    // i18n.language already 'en-US' → no changeLanguage, but the cookie is seeded.
    expect(mockI18n.changeLanguage).not.toHaveBeenCalled();
    expect(getLocaleCookie()).toBe('en-US');

    // Latch blocks a later differing refetch.
    mockUseUser.mockReturnValue({ user: { locale: 'zh-CN' } });
    rerender();

    expect(mockI18n.changeLanguage).not.toHaveBeenCalled();
    expect(getLocaleCookie()).toBe('en-US');
  });
});
