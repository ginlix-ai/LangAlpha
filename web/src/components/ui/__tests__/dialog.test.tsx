import { render, screen } from '@testing-library/react';
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
