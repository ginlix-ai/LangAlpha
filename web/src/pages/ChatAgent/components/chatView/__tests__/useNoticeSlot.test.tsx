/**
 * The notice row is filled while any notice in it renders, including one that
 * comes or goes on its own render, with the host left as it was.
 */
import { describe, it, expect } from 'vitest';
import { StrictMode, useState } from 'react';
import { fireEvent, render, waitFor } from '@testing-library/react';
import { useNoticeSlot } from '../useNoticeSlot';

function Notice() {
  const [shown, setShown] = useState(true);
  return shown ? <button type="button" onClick={() => setShown(false)}>dismiss</button> : null;
}

function Host({ notice }: { notice: boolean }) {
  const [attach, filled] = useNoticeSlot();
  return (
    <>
      <span data-testid="filled">{String(filled)}</span>
      <div ref={attach} hidden={!filled}>
        {notice && <Notice />}
      </div>
    </>
  );
}

describe('useNoticeSlot', () => {
  it('reads filled while a notice renders, and empty once it is gone', async () => {
    const view = render(<Host notice={false} />);
    const filled = () => view.getByTestId('filled').textContent;
    expect(filled()).toBe('false');

    view.rerender(<Host notice />);
    await waitFor(() => expect(filled()).toBe('true'));

    fireEvent.click(view.getByRole('button', { name: 'dismiss', hidden: true }));
    await waitFor(() => expect(filled()).toBe('false'));
  });

  it('reads filled when it mounts with a notice in it, under StrictMode too', async () => {
    const view = render(<StrictMode><Host notice /></StrictMode>);
    expect(view.getByTestId('filled').textContent).toBe('true');
    fireEvent.click(view.getByRole('button', { name: 'dismiss', hidden: true }));
    await waitFor(() => expect(view.getByTestId('filled').textContent).toBe('false'));
  });
});
