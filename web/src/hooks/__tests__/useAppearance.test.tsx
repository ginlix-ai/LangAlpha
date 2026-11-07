/**
 * The look follows the account: a browser applies the account's value whenever
 * it changes, and gives the account its own picks only when the account has
 * none. A theme counts as a pick only when it is light or dark, because every
 * browser stores `auto` on its first load without anyone choosing it, and
 * giving that to the account would undo another device's real pick.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { ReactNode } from 'react';
import { act, renderHook, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ThemeProvider } from '@/contexts/ThemeContext';
import { queryKeys } from '@/lib/queryKeys';
import { getCardStyle, setCardStyle, type CardStyle } from '@/lib/cardStyle';

const h = vi.hoisted(() => ({ storedStyle: false }));

// The stored style is read once at import, so whether this browser has one is
// set per test here rather than through localStorage.
vi.mock('@/lib/cardStyle', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/cardStyle')>()),
  hasStoredCardStyle: () => h.storedStyle,
}));

vi.mock('@/pages/Dashboard/utils/api', () => ({
  getPreferences: vi.fn(),
  updatePreferences: vi.fn(),
  clearPreferences: vi.fn(),
}));

import { getPreferences, updatePreferences } from '@/pages/Dashboard/utils/api';
import { useAppearance, useSyncAppearance } from '../useAppearance';

const mockGet = getPreferences as unknown as ReturnType<typeof vi.fn>;
const mockUpdate = updatePreferences as unknown as ReturnType<typeof vi.fn>;

/** The patches sent, without the context the mutation passes beside each. */
const writes = () => mockUpdate.mock.calls.map(([patch]) => patch);

interface Browser {
  theme?: string;
  cardStyle?: CardStyle;
}

/** The app's mount (useSyncAppearance in Main) and the Settings control
 *  (useAppearance), over one account row. */
async function mount(row: Record<string, unknown> | null, browser: Browser = {}) {
  if (browser.theme) localStorage.setItem('theme', browser.theme);
  if (browser.cardStyle) setCardStyle(browser.cardStyle);
  h.storedStyle = Boolean(browser.cardStyle);
  mockGet.mockResolvedValue(row);
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: Infinity }, mutations: { retry: false } },
  });
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>
      <ThemeProvider>{children}</ThemeProvider>
    </QueryClientProvider>
  );
  const rendered = renderHook(() => {
    useSyncAppearance();
    return useAppearance();
  }, { wrapper });
  await waitFor(() => expect(queryClient.getQueryData(queryKeys.user.preferences())).not.toBeUndefined());
  await act(async () => {});
  return { ...rendered, queryClient };
}

beforeEach(() => {
  mockGet.mockReset();
  mockUpdate.mockReset();
  mockUpdate.mockImplementation(async (patch: unknown) => patch);
  localStorage.clear();
  setCardStyle('balanced');
  localStorage.clear();
});

describe('seeding an account that has no look saved', () => {
  it('writes nothing from a browser nobody picked on', async () => {
    await mount(null);
    expect(mockUpdate).not.toHaveBeenCalled();
  });

  it('never gives the account a theme of auto, which every browser stores unasked', async () => {
    await mount({}, { theme: 'auto', cardStyle: 'soft' });
    expect(writes()).toEqual([{ other_preference: { card_style: 'soft' } }]);
  });

  it("gives the account this browser's picks once", async () => {
    const { queryClient, rerender } = await mount({}, { theme: 'dark', cardStyle: 'terminal' });
    expect(writes()).toEqual([{ other_preference: { card_style: 'terminal', theme: 'dark' } }]);

    mockGet.mockResolvedValue({});
    await act(async () => {
      await queryClient.invalidateQueries({ queryKey: queryKeys.user.preferences() });
    });
    rerender();
    expect(writes()).toHaveLength(1);
  });

  it('leaves alone a key the account already has', async () => {
    await mount({ other_preference: { theme: 'light' } }, { theme: 'dark', cardStyle: 'soft' });
    expect(writes()).toEqual([{ other_preference: { card_style: 'soft' } }]);
  });
});

describe("following the account's look", () => {
  it('applies the account value here, and again when another device changes it', async () => {
    const { result, queryClient } = await mount(
      { other_preference: { card_style: 'soft', theme: 'dark' } },
      { theme: 'light' },
    );
    expect(getCardStyle()).toBe('soft');
    expect(document.documentElement.dataset.theme).toBe('dark');
    expect(mockUpdate).not.toHaveBeenCalled();

    act(() => {
      queryClient.setQueryData(queryKeys.user.preferences(), {
        other_preference: { card_style: 'terminal', theme: 'light' },
      });
    });
    await waitFor(() => expect(result.current).toMatchObject({ cardStyle: 'terminal', theme: 'light' }));
    expect(getCardStyle()).toBe('terminal');
    expect(document.documentElement.dataset.theme).toBe('light');
  });

  it('a pick restyles at once and writes only its own key', async () => {
    const { result } = await mount({ other_preference: { card_style: 'soft', theme: 'dark' } });
    // What the server stores, for the refetch that follows the save.
    mockGet.mockResolvedValue({ other_preference: { card_style: 'terminal', theme: 'dark' } });
    await act(async () => {
      await result.current.pickStyle('terminal');
    });
    expect(getCardStyle()).toBe('terminal');
    expect(writes()).toEqual([{ other_preference: { card_style: 'terminal' } }]);
    await waitFor(() => expect(result.current.cardStyle).toBe('terminal'));
    expect(getCardStyle()).toBe('terminal');
  });

  it('a failed save leaves the control, and the page, on the account value', async () => {
    const { result } = await mount({ other_preference: { card_style: 'soft', theme: 'dark' } });
    mockUpdate.mockRejectedValue(new Error('offline'));

    await act(async () => {
      await result.current.pickStyle('terminal').catch(() => {});
    });
    await act(async () => {
      await result.current.pickTheme('light').catch(() => {});
    });

    await waitFor(() => expect(result.current).toMatchObject({ cardStyle: 'soft', theme: 'dark' }));
    expect(getCardStyle()).toBe('soft');
    expect(document.documentElement.dataset.theme).toBe('dark');
  });
});

describe('picks made while another is still saving', () => {
  it('return to what the server confirmed when every save fails, even with no refetch', async () => {
    const { result } = await mount({ other_preference: { card_style: 'terminal', theme: 'dark' } });
    const saves: Array<(error: Error) => void> = [];
    mockUpdate.mockImplementation(() => new Promise((_resolve, reject) => saves.push(reject)));
    mockGet.mockRejectedValue(new Error('offline'));

    // The second pick's rollback restores a row that holds the first pick.
    let first!: Promise<void>;
    let second!: Promise<void>;
    await act(async () => {
      first = result.current.pickStyle('balanced').catch(() => {});
    });
    await act(async () => {
      second = result.current.pickTheme('light').catch(() => {});
    });
    expect(getCardStyle()).toBe('balanced');

    await act(async () => {
      saves[0](new Error('offline'));
      await first;
    });
    await waitFor(() => expect(saves).toHaveLength(2));
    await act(async () => {
      saves[1](new Error('offline'));
      await second;
    });
    await act(async () => {});

    expect(result.current).toMatchObject({ cardStyle: 'terminal', theme: 'dark' });
    expect(getCardStyle()).toBe('terminal');
    expect(document.documentElement.dataset.theme).toBe('dark');
  });

  it('keep the earlier pick when only the later save fails', async () => {
    const { result } = await mount({ other_preference: { card_style: 'balanced', theme: 'dark' } });
    const saves: Array<{ resolve: (row: unknown) => void; reject: (error: Error) => void }> = [];
    mockUpdate.mockImplementation(() => new Promise((resolve, reject) => saves.push({ resolve, reject })));
    mockGet.mockRejectedValue(new Error('offline'));

    let first!: Promise<void>;
    let second!: Promise<void>;
    await act(async () => {
      first = result.current.pickStyle('terminal').catch(() => {});
    });
    await act(async () => {
      second = result.current.pickTheme('light').catch(() => {});
    });
    await act(async () => {
      saves[0].resolve({});
      await first;
    });
    await waitFor(() => expect(saves).toHaveLength(2));
    await act(async () => {
      saves[1].reject(new Error('offline'));
      await second;
    });
    await act(async () => {});

    expect(result.current).toMatchObject({ cardStyle: 'terminal', theme: 'dark' });
    expect(getCardStyle()).toBe('terminal');
    expect(document.documentElement.dataset.theme).toBe('dark');
  });
});

describe('a browser another account used', () => {
  it('never gives a second account the look it followed from the first', async () => {
    const first = await mount({ other_preference: { card_style: 'terminal', theme: 'dark' } });
    expect(getCardStyle()).toBe('terminal');
    expect(localStorage.getItem('theme')).toBe('dark');
    first.unmount();

    // Signed out and in as someone with no look saved: the page keeps the
    // look it has (stored as if picked here), but the account is not given it.
    await mount({}, { theme: 'dark', cardStyle: 'terminal' });
    expect(mockUpdate).not.toHaveBeenCalled();
    expect(getCardStyle()).toBe('terminal');
  });
});
