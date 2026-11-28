/**
 * A comment whose chat stopped with its answer still owed asks for attention
 * on its pin as a new reply does, and says why to a screen reader.
 */
import { screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { renderWithProviders } from '@/test/utils';
import type { ArtifactComment } from '@/types/api';
import { CommentLayerContext, type CommentLayerValue } from '../commentLayerContext';
import { CommentPins } from '../CommentPin';

const flags = vi.hoisted(() => ({ isRunning: false, status: 'failed' as string }));
vi.mock('@/lib/threadLifecycle/store', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  useThreadFlags: () => flags,
}));

const comment = (over: Partial<ArtifactComment> = {}) => ({
  comment_id: 'c-1',
  number: 3,
  status: 'open',
  delivery: 'sent',
  thread_id: 'th-other',
  unread: 0,
  messages: [{ message_id: 'm1', seq: 1, author: 'user', body: 'Check the margin', sent_at: null, request_key: null, thread_id: 'th-other', created_at: '' }],
  ...over,
}) as unknown as ArtifactComment;

function render(c: ArtifactComment) {
  const layer = {
    actions: { sendState: () => ({ sending: false, pending: null, refusal: null }) },
    besideThreadId: null,
    destinations: {},
    runningFor: () => false,
  } as unknown as CommentLayerValue;
  renderWithProviders(
    <CommentLayerContext value={layer}>
      <CommentPins
        pins={[{ comment: c, x: 40, y: 40 }]}
        shownId={null}
        draftSerial={null}
        pulse={new Set()}
        madeId={null}
        size={{ width: 400, height: 400 }}
        pointerIn={0}
        hidden={false}
        scrolling={false}
        onPreview={vi.fn()}
        onPin={vi.fn()}
      />
    </CommentLayerContext>,
  );
  return screen.getByRole('button', { name: /Comment 3/ });
}

describe('a pin whose chat needs a retry', () => {
  it('wears the attention mark and says the chat needs a retry', () => {
    flags.status = 'failed';
    const pin = render(comment());
    expect(pin).toHaveAttribute('data-unread');
    expect(pin).toHaveAccessibleName('Comment 3, its chat needs a retry');
  });

  it('reads as any other while its chat is well', () => {
    flags.status = 'idle';
    const pin = render(comment());
    expect(pin).not.toHaveAttribute('data-unread');
    expect(pin).toHaveAccessibleName('Comment 3');
  });
});
