/**
 * The onboarding bar's language control: a globe and the language's own name
 * that opens a two-row menu. A pick switches the page at once, writes the
 * cookie the next load reads, and saves to the account; the save failing
 * leaves the switch standing, and picks made in a row are saved in that order.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, render, screen, waitFor } from '@testing-library/react';
import userEvent, { PointerEventsCheckLevel } from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import i18n from '@/i18n';
import { runAuthResets } from '@/lib/authResets';
import { getLocaleCookie } from '@/lib/locale';
import { queryKeys } from '@/lib/queryKeys';

const updateCurrentUser = vi.fn(async (_data: Record<string, unknown>): Promise<Record<string, unknown>> => ({}));
vi.mock('@/pages/Dashboard/utils/api', () => ({
  updateCurrentUser: (data: Record<string, unknown>) => updateCurrentUser(data),
}));

import { LanguageToggle } from '../LanguageToggle';

function clearLocaleCookie() {
  document.cookie = 'locale=; path=/; expires=Thu, 01 Jan 1970 00:00:00 GMT';
}

function renderToggle() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const invalidate = vi.spyOn(client, 'invalidateQueries');
  render(
    <QueryClientProvider client={client}>
      <LanguageToggle />
    </QueryClientProvider>,
  );
  return { invalidate };
}

const trigger = () => screen.getByTestId('language-toggle');
const option = (name: string) => screen.getByRole('menuitemradio', { name });

beforeEach(async () => {
  clearLocaleCookie();
  await act(() => i18n.changeLanguage('en-US'));
});

afterEach(async () => {
  vi.clearAllMocks();
  clearLocaleCookie();
  await act(() => i18n.changeLanguage('en-US'));
});

describe('LanguageToggle', () => {
  it('shows a globe and the current language by its own name, and says what it is', () => {
    renderToggle();
    expect(trigger()).toHaveAccessibleName('Language: English');
    expect(trigger()).toHaveTextContent('English');
    expect(trigger().querySelector('svg')).toHaveAttribute('aria-hidden', 'true');
    expect(trigger()).toHaveAttribute('aria-haspopup', 'menu');
    expect(trigger()).toHaveAttribute('aria-expanded', 'false');
  });

  it('opens a menu of both languages in their own script, the current one checked', async () => {
    const user = userEvent.setup();
    renderToggle();
    await user.click(trigger());

    const menu = await screen.findByRole('menu', { name: 'Language' });
    expect(menu).toHaveAttribute('data-no-drag');
    const english = option('English');
    const chinese = option('简体中文');
    expect(english).toHaveAttribute('aria-checked', 'true');
    expect(chinese).toHaveAttribute('aria-checked', 'false');
    // Left alone by page translation, and read out in their own language.
    expect(english).toHaveAttribute('lang', 'en');
    expect(chinese).toHaveAttribute('lang', 'zh-CN');
    expect(english).toHaveAttribute('translate', 'no');
    expect(chinese).toHaveAttribute('translate', 'no');
  });

  it.each(['{Enter}', ' ', '{ArrowDown}'])('opens from the keyboard with %s', async (key) => {
    const user = userEvent.setup();
    renderToggle();
    trigger().focus();
    await user.keyboard(key);
    expect(await screen.findByRole('menu')).toBeInTheDocument();
    expect(option('English')).toHaveFocus();
  });

  it('moves with the arrows, and Escape closes it and gives focus back', async () => {
    const user = userEvent.setup();
    renderToggle();
    trigger().focus();
    await user.keyboard('{Enter}');
    await screen.findByRole('menu');
    await user.keyboard('{ArrowDown}');
    expect(option('简体中文')).toHaveFocus();
    await user.keyboard('{ArrowUp}');
    expect(option('English')).toHaveFocus();
    await user.keyboard('{Escape}');
    await waitFor(() => expect(screen.queryByRole('menu')).not.toBeInTheDocument());
    expect(trigger()).toHaveFocus();
    expect(i18n.language).toBe('en-US');
    expect(updateCurrentUser).not.toHaveBeenCalled();
  });

  it('closes on a click outside it', async () => {
    // An open menu holds the page's pointer events off, as a modal does; the
    // press lands outside it all the same.
    const user = userEvent.setup({ pointerEventsCheck: PointerEventsCheckLevel.Never });
    renderToggle();
    await user.click(trigger());
    await screen.findByRole('menu');
    await user.click(document.body);
    await waitFor(() => expect(screen.queryByRole('menu')).not.toBeInTheDocument());
  });

  it('a pick switches the page, writes the cookie and saves it to the account', async () => {
    const user = userEvent.setup();
    const { invalidate } = renderToggle();
    await user.click(trigger());
    await user.click(await screen.findByRole('menuitemradio', { name: '简体中文' }));

    await waitFor(() => expect(i18n.language).toBe('zh-CN'));
    expect(getLocaleCookie()).toBe('zh-CN');
    expect(updateCurrentUser).toHaveBeenCalledTimes(1);
    expect(updateCurrentUser).toHaveBeenCalledWith({ locale: 'zh-CN' });
    await waitFor(() => expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.user.me() }));
    expect(screen.queryByRole('menu')).not.toBeInTheDocument();
    // The bar now names the language it is in, in that language.
    expect(trigger()).toHaveAccessibleName('语言：中文');
    expect(trigger()).toHaveTextContent('中文');
  });

  it('picked from the keyboard, it switches the same way', async () => {
    const user = userEvent.setup();
    renderToggle();
    trigger().focus();
    await user.keyboard('{Enter}');
    await screen.findByRole('menu');
    await user.keyboard('{ArrowDown}{Enter}');
    await waitFor(() => expect(i18n.language).toBe('zh-CN'));
    expect(updateCurrentUser).toHaveBeenCalledWith({ locale: 'zh-CN' });
  });

  it('a failed save keeps the switch, and says nothing', async () => {
    updateCurrentUser.mockRejectedValueOnce(new Error('offline'));
    const user = userEvent.setup();
    const { invalidate } = renderToggle();
    await user.click(trigger());
    await user.click(await screen.findByRole('menuitemradio', { name: '简体中文' }));

    await waitFor(() => expect(i18n.language).toBe('zh-CN'));
    expect(getLocaleCookie()).toBe('zh-CN');
    await waitFor(() => expect(updateCurrentUser).toHaveBeenCalledTimes(1));
    // Settled, and nothing was refetched for a save that did not land.
    await act(async () => {});
    expect(invalidate).not.toHaveBeenCalled();
    expect(i18n.language).toBe('zh-CN');
    expect(trigger()).toHaveTextContent('中文');
  });

  it('two picks in a row leave the second on the account, whichever save is answered first', async () => {
    // The server keeps whatever it writes last, and answers the newest request
    // it holds first, as a slow connection can.
    let stored: unknown = null;
    const held: Array<() => void> = [];
    const hold = (data: Record<string, unknown>) =>
      new Promise<Record<string, unknown>>((resolve) => {
        held.push(() => {
          stored = data.locale;
          resolve({});
        });
      });
    updateCurrentUser.mockImplementationOnce(hold).mockImplementationOnce(hold);
    const user = userEvent.setup();
    renderToggle();
    await user.click(trigger());
    await user.click(await screen.findByRole('menuitemradio', { name: '简体中文' }));
    await waitFor(() => expect(i18n.language).toBe('zh-CN'));
    await user.click(trigger());
    await user.click(await screen.findByRole('menuitemradio', { name: 'English' }));
    await waitFor(() => expect(i18n.language).toBe('en-US'));

    for (let answered = 0; answered < 2; answered++) {
      await waitFor(() => expect(held.length).toBeGreaterThan(0));
      await act(async () => held.pop()!());
    }
    expect(updateCurrentUser).toHaveBeenCalledTimes(2);
    expect(updateCurrentUser).toHaveBeenLastCalledWith({ locale: 'en-US' });
    expect(stored).toBe('en-US');
    expect(getLocaleCookie()).toBe('en-US');
  });

  it('a failed save does not hold back the next pick', async () => {
    updateCurrentUser.mockRejectedValueOnce(new Error('offline'));
    const user = userEvent.setup();
    renderToggle();
    await user.click(trigger());
    await user.click(await screen.findByRole('menuitemradio', { name: '简体中文' }));
    await waitFor(() => expect(i18n.language).toBe('zh-CN'));
    await user.click(trigger());
    await user.click(await screen.findByRole('menuitemradio', { name: 'English' }));

    await waitFor(() => expect(updateCurrentUser).toHaveBeenCalledTimes(2));
    expect(updateCurrentUser).toHaveBeenLastCalledWith({ locale: 'en-US' });
  });

  it('a save still queued at a sign-out is dropped, not sent as the next account', async () => {
    let answer: () => void = () => {};
    updateCurrentUser.mockImplementationOnce(
      () => new Promise<Record<string, unknown>>((resolve) => { answer = () => resolve({}); }),
    );
    const user = userEvent.setup();
    renderToggle();
    await user.click(trigger());
    await user.click(await screen.findByRole('menuitemradio', { name: '简体中文' }));
    await waitFor(() => expect(updateCurrentUser).toHaveBeenCalledTimes(1));
    await user.click(trigger());
    await user.click(await screen.findByRole('menuitemradio', { name: 'English' }));
    await waitFor(() => expect(i18n.language).toBe('en-US'));

    // Signed out, and the next account in, while the first save is out.
    runAuthResets();
    await act(async () => answer());
    await act(async () => {});
    expect(updateCurrentUser).toHaveBeenCalledTimes(1);
    expect(updateCurrentUser).toHaveBeenCalledWith({ locale: 'zh-CN' });
  });

  it('picking the language already in use changes and saves nothing', async () => {
    const user = userEvent.setup();
    renderToggle();
    await user.click(trigger());
    await user.click(await screen.findByRole('menuitemradio', { name: 'English' }));
    await waitFor(() => expect(screen.queryByRole('menu')).not.toBeInTheDocument());
    expect(updateCurrentUser).not.toHaveBeenCalled();
    expect(getLocaleCookie()).toBeNull();
  });
});
