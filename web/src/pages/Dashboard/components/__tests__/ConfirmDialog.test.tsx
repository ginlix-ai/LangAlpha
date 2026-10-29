import { describe, it, expect, vi } from 'vitest';
import { useState } from 'react';
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import ConfirmDialog from '../ConfirmDialog';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (k: string) => ({ 'common.cancel': 'Cancel' })[k] ?? k }),
}));
vi.mock('@/hooks/useIsMobile', () => ({ useIsMobile: () => false }));

function Harness({ onConfirm }: { onConfirm: () => void }) {
  const [open, setOpen] = useState(false);
  return (
    <>
      <button type="button" onClick={() => setOpen(true)}>Open</button>
      <ConfirmDialog open={open} title="Save changes" message="Save?" confirmLabel="Save" onConfirm={onConfirm} onOpenChange={setOpen} focusConfirm />
    </>
  );
}

async function openFromKeyboard() {
  const opener = screen.getByRole('button', { name: 'Open' });
  opener.focus();
  act(() => { opener.click(); });
  await screen.findByRole('dialog');
  return opener;
}

describe('ConfirmDialog', () => {
  it('opens on the confirm button, so Enter confirms', async () => {
    const onConfirm = vi.fn();
    render(<Harness onConfirm={onConfirm} />);
    await openFromKeyboard();

    expect(document.activeElement).toBe(screen.getByRole('button', { name: 'Save' }));
  });

  it('hands focus back to what opened it after a keyboard answer', async () => {
    // It opens from state with no Dialog.Trigger for Radix to return to, so
    // focus used to land on <body> and a keyboard user lost their place.
    const onConfirm = vi.fn();
    render(<Harness onConfirm={onConfirm} />);
    const opener = await openFromKeyboard();

    fireEvent.keyDown(document.activeElement!, { key: 'Escape' });

    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull());
    await waitFor(() => expect(document.activeElement).toBe(opener));
    expect(onConfirm).not.toHaveBeenCalled();
  });
});
