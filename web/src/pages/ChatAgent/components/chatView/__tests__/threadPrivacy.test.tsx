/**
 * Who sees a shared workspace's conversation, as its header and composer say
 * it: a glyph and words for each state, the change only its starter may make
 * and only while no colleague has written, the note under a private one, and
 * the chip's switch and share question.
 */
import { describe, it, expect, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import { PrivacyChip, ThreadPrivacyRow } from '../threadPrivacy';
import type { ThreadPrivacy } from '../useThreadPrivacy';

const privacy = (over: Partial<ThreadPrivacy> = {}): ThreadPrivacy => ({
  workspaceName: 'Desk Real', shared: false, canChange: true, request: vi.fn(), ...over,
});

describe('ThreadPrivacyRow', () => {
  it('a private thread says so, offers sharing, and says what private covers', () => {
    const p = privacy();
    render(<ThreadPrivacyRow privacy={p} isMobile={false} />);
    expect(screen.getByTestId('thread-privacy-state').textContent).toBe('Only you');
    expect(screen.getByTestId('thread-privacy-note').textContent).toContain('Files it saves are in Desk Real');
    fireEvent.click(screen.getByTestId('thread-privacy-change'));
    expect(p.request).toHaveBeenCalledWith(true);
  });

  it('says the link went off when making it private turned one off', () => {
    const { rerender } = render(<ThreadPrivacyRow privacy={privacy({ linkTurnedOff: true })} isMobile={false} />);
    expect(screen.getByTestId('thread-privacy-note').textContent).toContain('Its link was turned off.');
    rerender(<ThreadPrivacyRow privacy={privacy()} isMobile={false} />);
    expect(screen.getByTestId('thread-privacy-note').textContent).not.toContain('link');
  });

  it('a shared thread names its workspace and offers making it private', () => {
    const p = privacy({ shared: true });
    render(<ThreadPrivacyRow privacy={p} isMobile={false} />);
    expect(screen.getByTestId('thread-privacy-state').textContent).toBe('Shared with Desk Real');
    expect(screen.queryByTestId('thread-privacy-note')).toBeNull();
    fireEvent.click(screen.getByTestId('thread-privacy-change'));
    expect(p.request).toHaveBeenCalledWith(false);
  });

  it('offers no change to anyone but its starter, nor once a colleague has written', () => {
    render(<ThreadPrivacyRow privacy={privacy({ shared: true, canChange: false })} isMobile={false} />);
    expect(screen.queryByTestId('thread-privacy-change')).toBeNull();
  });
});

describe('PrivacyChip', () => {
  it("is a switch on a new thread, and on a private one asks to share it", () => {
    const p = privacy();
    const { rerender } = render(<PrivacyChip privacy={p} />);
    expect(screen.getByTestId('composer-privacy-chip').textContent).toContain('Private');
    fireEvent.click(screen.getByRole('button'));
    expect(p.request).toHaveBeenCalledWith(true);
    rerender(<PrivacyChip privacy={privacy({ shared: true })} />);
    expect(screen.getByTestId('composer-privacy-chip').textContent).toContain('Shared with Desk Real');
  });
});
