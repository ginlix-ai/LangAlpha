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
import { getCardStyle, restoreCardStyle, setCardStyle, type CardStyle } from '@/lib/cardStyle';

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

/** The app's mount (useSyncAppearance in App's AppearanceSync) and the Settings control
 *  (useAppearance), over one account row. */
async function mount(row: Record<string, unknown> | null, browser: Browser = {}) {
  if (browser.theme) localStorage.setItem('theme', browser.theme);
  if (browser.cardStyle) setCardStyle(browser.cardStyle);
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
  restoreCardStyle('balanced', null);
});

const marks = () => JSON.parse(localStorage.getItem('appearanceFromAccount') ?? '[]');

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
  it('takes back the look it followed from the first when the second has none', async () => {
    const first = await mount({ other_preference: { card_style: 'terminal', theme: 'dark' } });
    expect(getCardStyle()).toBe('terminal');
    expect(localStorage.getItem('theme')).toBe('dark');
    first.unmount();

    // Signed out and in as someone with no look saved: the look the page kept
    // from the first account goes back to the default, and is not given away.
    await mount({}, { theme: 'dark', cardStyle: 'terminal' });
    expect(mockUpdate).not.toHaveBeenCalled();
    expect(getCardStyle()).toBe('balanced');
    expect(localStorage.getItem('cardStyle.v2')).toBeNull();
    expect(localStorage.getItem('theme')).toBe('auto');
    expect(marks()).toEqual([]);
  });
});

describe('an account whose look was reset', () => {
  it('takes back the followed look and keeps a style picked here', async () => {
    // The style was picked here; its seed fails, so the account never takes it.
    mockUpdate.mockRejectedValue(new Error('offline'));
    const { queryClient, unmount } = await mount({ other_preference: { theme: 'dark' } }, { cardStyle: 'soft' });
    await act(async () => {});
    expect(document.documentElement.dataset.theme).toBe('dark');
    expect(marks()).toEqual(['theme']);

    // Settings' reset deletes the row and the refetch reads none.
    mockGet.mockResolvedValue(null);
    act(() => {
      queryClient.setQueryData(queryKeys.user.preferences(), null);
    });
    await waitFor(() => expect(localStorage.getItem('theme')).toBe('auto'));
    expect(getCardStyle()).toBe('soft');
    expect(localStorage.getItem('cardStyle.v2')).toBe('soft');
    expect(marks()).toEqual([]);
    unmount();

    // The style is still this browser's own, offered once the account has none.
    mockUpdate.mockReset();
    mockUpdate.mockImplementation(async (patch: unknown) => patch);
    await mount(null);
    expect(writes()).toEqual([{ other_preference: { card_style: 'soft' } }]);
  });

  it('changes nothing until the preferences have loaded', async () => {
    localStorage.setItem('appearanceFromAccount', JSON.stringify(['cardStyle', 'theme']));
    setCardStyle('terminal');
    localStorage.setItem('theme', 'dark');
    mockGet.mockReturnValue(new Promise(() => {}));
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    renderHook(() => useSyncAppearance(), {
      wrapper: ({ children }: { children: ReactNode }) => (
        <QueryClientProvider client={queryClient}>
          <ThemeProvider>{children}</ThemeProvider>
        </QueryClientProvider>
      ),
    });
    await act(async () => {});
    expect(getCardStyle()).toBe('terminal');
    expect(localStorage.getItem('theme')).toBe('dark');
  });
});

describe('a failed save', () => {
  it('leaves no style stored on a browser that held none', async () => {
    const { result } = await mount({ other_preference: { theme: 'dark' } });
    expect(localStorage.getItem('cardStyle.v2')).toBeNull();
    mockUpdate.mockRejectedValue(new Error('offline'));

    await act(async () => {
      await result.current.pickStyle('terminal').catch(() => {});
    });
    await act(async () => {});

    expect(getCardStyle()).toBe('balanced');
    expect(localStorage.getItem('cardStyle.v2')).toBeNull();
    expect(marks()).toEqual(['theme']);
  });
});

describe('another tab on the same account', () => {
  const fromOtherTab = (key: string, newValue: string | null) =>
    act(async () => {
      if (newValue === null) localStorage.removeItem(key);
      else localStorage.setItem(key, newValue);
      window.dispatchEvent(new StorageEvent('storage', { key, newValue }));
    });

  it("moves this tab's control with a pick there, so a pick back here is not a no-op", async () => {
    const { result } = await mount({ other_preference: { card_style: 'soft', theme: 'dark' } });
    expect(result.current.cardStyle).toBe('soft');

    await fromOtherTab('cardStyle.v2', 'terminal');
    await fromOtherTab('theme', 'light');
    expect(result.current.cardStyle).toBe('terminal');
    expect(result.current.theme).toBe('light');

    await act(async () => { await result.current.pickStyle('soft'); });
    expect(writes()).toEqual([{ other_preference: { card_style: 'soft' } }]);
  });

  it('leaves the row alone for a key the account does not hold', async () => {
    const { queryClient } = await mount({ other_preference: { theme: 'dark' } });
    await fromOtherTab('cardStyle.v2', 'terminal');
    const row = queryClient.getQueryData<{ other_preference?: Record<string, unknown> }>(queryKeys.user.preferences());
    expect(row?.other_preference).toEqual({ theme: 'dark' });
  });
});
