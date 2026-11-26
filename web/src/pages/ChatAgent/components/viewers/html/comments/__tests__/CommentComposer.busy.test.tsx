/**
 * While a send is out the field takes no more words: the box closes when it
 * comes back, and anything written meanwhile would go with it.
 */
import { act, fireEvent, screen, waitFor } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { renderWithProviders } from '@/test/utils';
import { CommentComposer } from '../CommentComposer';

describe('a comment being sent', () => {
  it('holds its words until the send comes back', async () => {
    let done: () => void = () => {};
    renderWithProviders(
      <CommentComposer
        value="Check this"
        onChange={() => {}}
        placeholder="Comment"
        canQueue={false}
        canSend
        onSubmit={() => new Promise<void>((resolve) => { done = resolve; })}
        mode={{ kind: 'reply' }}
      />,
    );
    const field = screen.getByRole('textbox');
    expect(field).not.toHaveAttribute('readonly');

    fireEvent.keyDown(field, { key: 'Enter' });
    expect(field).toHaveAttribute('readonly');

    await act(async () => done());
    await waitFor(() => expect(field).not.toHaveAttribute('readonly'));
  });
});
