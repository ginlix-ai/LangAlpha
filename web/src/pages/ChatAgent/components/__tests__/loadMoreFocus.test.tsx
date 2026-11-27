/**
 * A "show more" row leaves while its page loads, so a keyboard press would
 * drop the focus on the page: it goes to the first item the page added, or
 * back to the row when none came, or to the last item when the row went too,
 * and a click leaves it alone.
 */
import { describe, expect, it } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import { useLoadMoreFocus } from '../loadMoreFocus';

function List({ items, busy, more = true }: { items: string[]; busy: boolean; more?: boolean }) {
  const press = useLoadMoreFocus(items, busy, (scope, id) =>
    scope.querySelector(id ? `[data-item="${id}"]` : '[data-more]'));
  return (
    <section>
      {items.map((item) => <span key={item} data-item={item} tabIndex={0}>{item}</span>)}
      {!busy && more && (
        <button type="button" data-more onClick={(e) => press(e.currentTarget.closest('section'))}>
          more
        </button>
      )}
    </section>
  );
}

function pressFromKeyboard() {
  fireEvent.keyDown(window, { key: 'Enter' });
  const more = screen.getByRole('button', { name: 'more' });
  more.focus();
  fireEvent.click(more);
}

describe('useLoadMoreFocus', () => {
  it('focuses the first item the page added', () => {
    const view = render(<List items={['a', 'b']} busy={false} />);
    pressFromKeyboard();
    view.rerender(<List items={['a', 'b']} busy />);
    view.rerender(<List items={['a', 'b', 'c', 'd']} busy={false} />);
    expect(document.activeElement).toBe(screen.getByText('c'));
  });

  it('finds the first added item by id when the list already held one from further on', () => {
    // The current workspace is kept in view past the first page.
    const view = render(<List items={['w0', 'w1', 'w9']} busy={false} />);
    pressFromKeyboard();
    view.rerender(<List items={['w0', 'w1', 'w2', 'w9']} busy={false} more={false} />);
    expect(document.activeElement).toBe(screen.getByText('w2'));
  });

  it('goes back to the row when the page brought nothing', () => {
    const view = render(<List items={['a']} busy={false} />);
    pressFromKeyboard();
    view.rerender(<List items={['a']} busy />);
    view.rerender(<List items={['a']} busy={false} />);
    expect(document.activeElement).toBe(screen.getByRole('button', { name: 'more' }));
  });

  it('falls back to the last item when the page brought nothing and the row went too', () => {
    const view = render(<List items={['a', 'b']} busy={false} />);
    pressFromKeyboard();
    view.rerender(<List items={['a', 'b']} busy />);
    view.rerender(<List items={['a', 'b']} busy={false} more={false} />);
    expect(document.activeElement).toBe(screen.getByText('b'));
  });

  it('waits for the page through renders that change nothing', () => {
    const view = render(<List items={['a']} busy={false} />);
    pressFromKeyboard();
    view.rerender(<List items={['a']} busy={false} />);
    view.rerender(<List items={['a']} busy />);
    view.rerender(<List items={['a', 'b']} busy={false} />);
    expect(document.activeElement).toBe(screen.getByText('b'));
  });

  it('leaves focus alone after a click', () => {
    const view = render(<List items={['a']} busy={false} />);
    const more = screen.getByRole('button', { name: 'more' });
    fireEvent.pointerDown(more);
    fireEvent.click(more);
    view.rerender(<List items={['a']} busy />);
    view.rerender(<List items={['a', 'b']} busy={false} />);
    expect(document.activeElement).toBe(document.body);
  });
});
