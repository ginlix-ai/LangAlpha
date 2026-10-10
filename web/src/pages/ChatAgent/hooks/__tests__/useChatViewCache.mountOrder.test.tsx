/**
 * Cached chat views are rendered in creation order. Going back to a thread
 * promotes its entry to the front of the cache, and a view React moves to
 * follow that order is re-inserted into the document, which resets every
 * scroll position inside it: the thread came back at the top of its
 * transcript instead of where the reader left it.
 */
import React from 'react';
import { describe, it, expect } from 'vitest';
import { render, act } from '@testing-library/react';
import { useChatViewCache } from '../useChatViewCache';

let cache: ReturnType<typeof useChatViewCache>;

function Views() {
  const api = useChatViewCache();
  React.useEffect(() => {
    cache = api;
  });
  return (
    <div data-testid="views">
      {api.mounted.map((entry) => (
        <div key={entry.instanceId} data-thread={entry.threadId} />
      ))}
    </div>
  );
}

const open = (threadId: string) =>
  act(() => {
    cache.touch({ workspaceId: 'ws-1', threadId, workspaceName: 'Workspace' });
  });

describe('useChatViewCache — mount order', () => {
  it('keeps creation order while a promotion reorders the cache', () => {
    render(<Views />);
    open('a');
    open('b');
    open('c');
    expect(cache.entries.map((e) => e.threadId)).toEqual(['c', 'b', 'a']);
    expect(cache.mounted.map((e) => e.threadId)).toEqual(['a', 'b', 'c']);

    open('a');
    expect(cache.entries.map((e) => e.threadId)).toEqual(['a', 'c', 'b']);
    expect(cache.mounted.map((e) => e.threadId)).toEqual(['a', 'b', 'c']);
  });

  it('never re-inserts a view when going back and forth between threads', () => {
    const { getByTestId } = render(<Views />);
    open('a');
    open('b');
    const views = getByTestId('views');
    const moves = new MutationObserver(() => {});
    moves.observe(views, { childList: true });

    open('a');
    open('b');
    open('a');
    open('c');

    const removed = moves.takeRecords().flatMap((r) => [...r.removedNodes]);
    moves.disconnect();
    expect(removed).toEqual([]);
    expect([...views.children].map((el) => (el as HTMLElement).dataset.thread)).toEqual(['a', 'b', 'c']);
  });

  it('drops the least recently used view without moving the others', () => {
    const { getByTestId } = render(<Views />);
    for (const id of ['a', 'b', 'c', 'd', 'e']) open(id);
    open('a');
    const views = getByTestId('views');
    const moves = new MutationObserver(() => {});
    moves.observe(views, { childList: true });

    open('f');

    const removed = moves.takeRecords().flatMap((r) => [...r.removedNodes]);
    moves.disconnect();
    expect(removed.map((el) => (el as HTMLElement).dataset.thread)).toEqual(['b']);
    expect([...views.children].map((el) => (el as HTMLElement).dataset.thread)).toEqual(['a', 'c', 'd', 'e', 'f']);
  });
});
