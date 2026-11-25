/**
 * A press outside a draft's box (the chat, a toolbar, a menu) leaves words
 * written as they are; only a press on the page asks whether to discard them,
 * and that one reaches the layer as the page's own report, not here. An empty
 * draft still goes with a press anywhere outside.
 */
import { act, fireEvent, render } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { CommentPopover } from '../CommentPopover';

async function pressOutside(words: boolean) {
  const onDismiss = vi.fn();
  const elsewhere = document.createElement('button');
  document.body.appendChild(elsewhere);
  render(
    <CommentPopover
      open
      point={{ x: 40, y: 40 }}
      narrow={false}
      boundary={null}
      width={320}
      label="Comment"
      focusSelf={false}
      arrival={false}
      replaced={false}
      draft={{ words }}
      onDismiss={onDismiss}
      onEscape={() => {}}
      onGone={() => {}}
    >
      <textarea aria-label="words" />
    </CommentPopover>,
  );
  // Radix starts listening for presses outside a tick after it opens.
  await act(() => new Promise((resolve) => setTimeout(resolve, 0)));
  // A primary press is taken as one outside once its click lands.
  fireEvent.pointerDown(elsewhere);
  fireEvent.click(elsewhere);
  fireEvent.focusIn(elsewhere);
  await act(() => new Promise((resolve) => setTimeout(resolve, 0)));
  elsewhere.remove();
  return onDismiss;
}

describe('a press outside a draft\'s box', () => {
  it('leaves a draft with words as it is', async () => {
    expect(await pressOutside(true)).not.toHaveBeenCalled();
  });

  it('lets an empty draft go', async () => {
    expect(await pressOutside(false)).toHaveBeenCalled();
  });
});
