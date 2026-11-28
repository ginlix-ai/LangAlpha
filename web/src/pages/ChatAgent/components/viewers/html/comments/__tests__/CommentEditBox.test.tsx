import { describe, expect, it, vi } from 'vitest';
import { fireEvent, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { ApiError } from '@/api/client';
import { renderWithProviders } from '@/test/utils';
import { CommentEditBox } from '../CommentEditBox';

const keyboard = vi.hoisted(() => ({ up: false }));
vi.mock('@/lib/visualViewport', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  onScreenKeyboardUp: () => keyboard.up,
}));

describe("editing a comment's words", () => {
  it('saves the new words with Enter, trimmed', async () => {
    const onSave = vi.fn().mockResolvedValue(undefined);
    const user = userEvent.setup();
    renderWithProviders(<CommentEditBox initial="fix it" onSave={onSave} onCancel={vi.fn()} />);

    const field = screen.getByRole('textbox', { name: 'Edit the comment' });
    await user.clear(field);
    await user.type(field, ' fix the margin  {Enter}');

    expect(onSave).toHaveBeenCalledWith('fix the margin');
  });

  it('breaks the line on Enter with an on-screen keyboard up, as the comment box does, for Save to save', async () => {
    keyboard.up = true;
    const onSave = vi.fn().mockResolvedValue(undefined);
    const user = userEvent.setup();
    renderWithProviders(<CommentEditBox initial="fix it" onSave={onSave} onCancel={vi.fn()} />);

    await user.type(screen.getByRole('textbox'), '{Enter}now');
    expect(onSave).not.toHaveBeenCalled();
    expect(screen.getByRole('textbox')).toHaveValue('fix it\nnow');
    keyboard.up = false;
  });

  it('cannot save empty words', async () => {
    const onSave = vi.fn();
    const user = userEvent.setup();
    renderWithProviders(<CommentEditBox initial="fix it" onSave={onSave} onCancel={vi.fn()} />);

    await user.clear(screen.getByRole('textbox'));
    await user.keyboard('{Enter}');

    expect(screen.getByRole('button', { name: 'Save' })).toBeDisabled();
    expect(onSave).not.toHaveBeenCalled();
  });

  it('cancels with Esc before the box around it hears the key', () => {
    const onCancel = vi.fn();
    // The box's own Esc listens on the document, in its capture phase.
    const boxHeard = vi.fn();
    document.addEventListener('keydown', boxHeard, { capture: true });
    renderWithProviders(<CommentEditBox initial="fix it" onSave={vi.fn()} onCancel={onCancel} />);

    fireEvent.keyDown(screen.getByRole('textbox'), { key: 'Escape' });

    expect(onCancel).toHaveBeenCalledTimes(1);
    expect(boxHeard).not.toHaveBeenCalled();
    document.removeEventListener('keydown', boxHeard, { capture: true });
  });

  it("keeps the words and says the server's reason when the send took them meanwhile", async () => {
    const refused = new ApiError('Request failed with status code 409', 'ERR_BAD_REQUEST', {
      status: 409, statusText: '', headers: {},
      data: { detail: { code: 'comment_sent', message: 'These words have reached the agent, so they can no longer be edited.' } },
    });
    const user = userEvent.setup();
    renderWithProviders(<CommentEditBox initial="fix it" onSave={vi.fn().mockRejectedValue(refused)} onCancel={vi.fn()} />);

    await user.type(screen.getByRole('textbox'), ' now{Enter}');

    await waitFor(() => expect(screen.getByRole('alert')).toHaveTextContent('reached the agent'));
    expect(screen.getByRole('textbox')).toHaveValue('fix it now');
  });

  it('holds Esc while a save is out, so a refusal still finds the words', async () => {
    let refuse: (error: unknown) => void = () => {};
    const onSave = vi.fn(() => new Promise<void>((_, reject) => { refuse = reject; }));
    const onCancel = vi.fn();
    const user = userEvent.setup();
    renderWithProviders(<CommentEditBox initial="fix it" onSave={onSave} onCancel={onCancel} />);

    await user.type(screen.getByRole('textbox'), ' now{Enter}');
    fireEvent.keyDown(screen.getByRole('textbox'), { key: 'Escape' });
    expect(onCancel).not.toHaveBeenCalled();

    refuse(new Error('refused'));
    await waitFor(() => expect(screen.getByRole('alert')).toBeInTheDocument());
    expect(screen.getByRole('textbox')).toHaveValue('fix it now');

    fireEvent.keyDown(screen.getByRole('textbox'), { key: 'Escape' });
    expect(onCancel).toHaveBeenCalledTimes(1);
  });
});
