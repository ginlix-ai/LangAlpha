/**
 * The photo picker after a failed upload: the same file picked again uploads
 * again, and nothing can start a second upload while one is in flight.
 */
import { describe, expect, it, vi } from 'vitest';
import { act, fireEvent, screen } from '@testing-library/react';
import i18n from '@/i18n';
import { renderWithProviders } from '@/test/utils';

const h = vi.hoisted(() => ({
  uploads: [] as Array<{ resolve: (v: { avatar_url: string }) => void; reject: (e: Error) => void }>,
}));

vi.mock('@/pages/Dashboard/utils/api', () => ({
  updateCurrentUser: vi.fn(),
  uploadAvatar: () => new Promise((resolve, reject) => { h.uploads.push({ resolve, reject }); }),
}));
vi.mock('@/contexts/AuthContext', () => ({ useAuth: () => ({ logout: vi.fn() }) }));
vi.mock('@/hooks/useUser', () => ({ useUser: () => ({ user: { name: 'Ada', email: 'ada@example.com' } }) }));

import { AccountPanel } from '../AccountPanel';

describe('AccountPanel photo', () => {
  it('uploads the same file again after a failure, and holds both buttons while one runs', async () => {
    const { container } = renderWithProviders(<AccountPanel />);
    const input = container.querySelector<HTMLInputElement>('input[type="file"]')!;
    const photo = new File(['x'], 'me.png', { type: 'image/png' });
    // A browser fires no change for the file the input already holds, which
    // jsdom does not model, so the test watches for the input being cleared.
    let picked = 'C:\\fakepath\\me.png';
    Object.defineProperty(input, 'value', { configurable: true, get: () => picked, set: (v: string) => { picked = v; } });

    const opened = vi.spyOn(input, 'click').mockImplementation(() => {});

    fireEvent.change(input, { target: { files: [photo] } });
    expect(h.uploads).toHaveLength(1);
    expect(picked).toBe('');
    const label = i18n.t('settings:changeAvatar');
    const held = [...screen.getAllByRole('button', { name: label }), screen.getByRole('button', { name: i18n.t('settings:uploading') })];
    for (const button of held) {
      // Held, not disabled, so the button the picker returns focus to keeps it.
      expect(button).toHaveAttribute('aria-disabled', 'true');
      expect(button).toBeEnabled();
      fireEvent.click(button);
    }
    expect(opened).not.toHaveBeenCalled();

    await act(async () => { h.uploads[0].reject(new Error('502')); });
    expect(screen.getByText(i18n.t('settings:failedToUploadAvatar'))).toBeInTheDocument();
    for (const button of screen.getAllByRole('button', { name: label })) expect(button).toHaveAttribute('aria-disabled', 'false');
    fireEvent.click(screen.getAllByRole('button', { name: label })[0]);
    expect(opened).toHaveBeenCalledTimes(1);

    fireEvent.change(input, { target: { files: [photo] } });
    expect(h.uploads).toHaveLength(2);
  });
});
