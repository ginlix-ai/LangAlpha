import { useState } from 'react';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { useIsMobile } from '@/hooks/useIsMobile';
import { Dialog, DialogContent, DialogTitle } from '../dialog';

vi.mock('@/hooks/useIsMobile', () => ({ useIsMobile: vi.fn(() => false) }));

describe('DialogContent', () => {
  // jsdom lays nothing out, so this holds the class that does it. Without it a
  // long file name widened the desktop dialog's column to over twice the dialog.
  it.each([
    ['desktop', false],
    ['mobile sheet', true],
  ])('lets its items be narrower than their longest line (%s)', (_shape, mobile) => {
    vi.mocked(useIsMobile).mockReturnValue(mobile);
    render(
      <Dialog open>
        <DialogContent>
          <DialogTitle>Title</DialogTitle>
        </DialogContent>
      </Dialog>,
    );
    const grid = screen.getByText('Title').parentElement;
    expect(grid?.className).toContain('*:min-w-0');
  });
});

// Opened from state, as a confirm opened from a row's action is: no
// Dialog.Trigger for Radix to hand focus back to.
function OpenedFromState() {
  const [open, setOpen] = useState(false);
  return (
    <>
      <button type="button" onClick={() => setOpen(true)}>Delete</button>
      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent>
          <DialogTitle>Delete it?</DialogTitle>
          <button type="button" onClick={() => setOpen(false)}>Cancel</button>
        </DialogContent>
      </Dialog>
    </>
  );
}

// A rename dialog: its field autofocuses, which moves focus in before Radix's
// own open focus runs.
function RenameFromState() {
  const [open, setOpen] = useState(false);
  return (
    <>
      <button type="button" onClick={() => setOpen(true)}>Rename</button>
      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent>
          <DialogTitle>Rename</DialogTitle>
          <input aria-label="Title" autoFocus />
        </DialogContent>
      </Dialog>
    </>
  );
}

describe('DialogContent focus on close', () => {
  it('hands focus back to its opener when a field inside took focus first', async () => {
    vi.mocked(useIsMobile).mockReturnValue(false);
    render(<RenameFromState />);
    const opener = screen.getByRole('button', { name: 'Rename' });
    opener.focus();
    fireEvent.keyDown(opener, { key: 'Enter' });
    fireEvent.click(opener);
    const field = await screen.findByRole('textbox', { name: 'Title' });
    await waitFor(() => expect(field).toHaveFocus());
    fireEvent.keyDown(field, { key: 'Escape' });
    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull());
    await waitFor(() => expect(opener).toHaveFocus());
  });

  it('hands focus back to what opened it from the keyboard, with no trigger to return to', async () => {
    vi.mocked(useIsMobile).mockReturnValue(false);
    render(<OpenedFromState />);
    const opener = screen.getByRole('button', { name: 'Delete' });
    opener.focus();
    fireEvent.keyDown(opener, { key: 'Enter' });
    fireEvent.click(opener);
    const dialog = await screen.findByRole('dialog');
    await waitFor(() => expect(dialog.contains(document.activeElement)).toBe(true));
    fireEvent.keyDown(document.activeElement ?? dialog, { key: 'Escape' });
    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull());
    await waitFor(() => expect(opener).toHaveFocus());
  });

  it('leaves focus off after a click closes it, as every overlay does', async () => {
    vi.mocked(useIsMobile).mockReturnValue(false);
    render(<OpenedFromState />);
    const opener = screen.getByRole('button', { name: 'Delete' });
    opener.focus();
    fireEvent.keyDown(opener, { key: 'Enter' });
    fireEvent.click(opener);
    const cancel = await screen.findByRole('button', { name: 'Cancel' });
    fireEvent.pointerDown(cancel);
    fireEvent.click(cancel);
    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull());
    await new Promise((r) => setTimeout(r, 10));
    expect(opener).not.toHaveFocus();
  });
});
