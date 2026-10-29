import { renderHook } from '@testing-library/react';
import { describe, it, expect, vi } from 'vitest';

/**
 * `users.locale` is left null at signup and only ever written by the Settings
 * dropdown, so the column alone gates out nearly every Chinese user. These lock
 * the Accept-Language-equivalent fallback to the active UI language.
 *
 * Lives apart from useCnNewsEligibility.test.tsx because that file mocks the
 * hook itself to isolate the widget wiring.
 */
const mockUser = vi.fn();
const mockPack = vi.fn();
const mockLanguage = vi.fn();
const mockFlagsPending = vi.fn(() => false);
const mockUserLoading = vi.fn(() => false);

vi.mock('@/hooks/useUser', () => ({ useUser: () => ({ user: mockUser(), isLoading: mockUserLoading() }) }));
vi.mock('@/hooks/useFeatures', () => ({
  useFeatureEnabled: () => mockPack(),
  useFeatures: () => ({ isPending: mockFlagsPending() }),
}));
vi.mock('@/hooks/useLocale', () => ({ useLocale: () => mockLanguage() }));

import { useCnNewsEligibility } from '../useCnNewsEligibility';

// Rendered rather than called: the compiled hook needs React's dispatcher.
function eligibilityFor(locale: string | null, uiLanguage: string, pack = true, flagsPending = false, userLoading = false): boolean | null {
  mockUserLoading.mockReturnValue(userLoading);
  mockUser.mockReturnValue({ locale });
  mockPack.mockReturnValue(pack);
  mockLanguage.mockReturnValue(uiLanguage);
  mockFlagsPending.mockReturnValue(flagsPending);
  return renderHook(() => useCnNewsEligibility()).result.current;
}

describe('useCnNewsEligibility locale fallback', () => {
  it('falls back to the UI language when users.locale is null', () => {
    expect(eligibilityFor(null, 'zh-CN')).toBe(true);
    expect(eligibilityFor(null, 'en-US')).toBe(false);
  });

  it('an explicit stored locale still wins over the UI language', () => {
    expect(eligibilityFor('en-US', 'zh-CN')).toBe(false);
    expect(eligibilityFor('zh-CN', 'en-US')).toBe(true);
  });

  it('the feature gate still dominates both signals', () => {
    expect(eligibilityFor(null, 'zh-CN', false)).toBe(false);
  });

  it('is undecided for a zh user until the flags land, and false for anyone else', () => {
    expect(eligibilityFor(null, 'zh-CN', false, true)).toBeNull();
    expect(eligibilityFor(null, 'en-US', false, true)).toBe(false);
  });

  it('is undecided while the user record loads, whatever the UI language', () => {
    expect(eligibilityFor(null, 'zh-CN', true, false, true)).toBeNull();
    expect(eligibilityFor(null, 'en-US', true, false, true)).toBeNull();
  });
});
