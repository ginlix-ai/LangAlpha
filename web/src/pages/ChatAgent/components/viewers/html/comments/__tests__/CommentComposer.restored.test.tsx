/**
 * A draft taken up again from where it was kept (a reload, a return to its
 * file) shows with its words but leaves the keyboard where it is: a key meant
 * for something else must not send it. A draft just picked takes the focus.
 */
import { screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { renderWithProviders } from '@/test/utils';
import { CommentComposer, type ComposerMode } from '../CommentComposer';

const mode = (quiet: boolean): ComposerMode => ({
  kind: 'list',
  forAgent: true,
  onForAgentChange: vi.fn(),
  destination: null,
  confirmDiscard: false,
  onKeepWriting: vi.fn(),
  onDiscard: vi.fn(),
  onClose: vi.fn(),
  takesFocus: !quiet,
});

function render(quiet: boolean) {
  renderWithProviders(
    <CommentComposer value="Kept words" onChange={vi.fn()} placeholder="Comment" canQueue={false} canSend onSubmit={vi.fn()} mode={mode(quiet)} />,
  );
  return screen.getByRole('textbox');
}

describe('a comment box opening', () => {
  it('takes the focus for a draft just picked', () => {
    expect(render(false)).toHaveFocus();
  });

  it('leaves the focus alone for a draft taken up again', () => {
    expect(render(true)).not.toHaveFocus();
  });
});
