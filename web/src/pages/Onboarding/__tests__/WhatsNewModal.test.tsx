import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { WhatsNewModal } from '../engine/WhatsNewModal';
import type { AnnouncementDef } from '../registry/types';

function ann(key: string, releaseVersion: string): AnnouncementDef {
  return { key, releaseVersion, modalTitleKey: `${key}-title`, modalBodyKey: `${key}-body` };
}

describe('WhatsNewModal', () => {
  it('renders nothing when there are no announcements', () => {
    const { container } = render(<WhatsNewModal announcements={[]} onAcknowledge={vi.fn()} />);
    expect(container).toBeEmptyDOMElement();
    expect(document.querySelector('[role="dialog"]')).toBeNull();
  });

  it('groups announcements by release, newest version first', () => {
    render(
      <WhatsNewModal
        announcements={[ann('older', '2026.4'), ann('newer', '2026.6'), ann('sibling', '2026.6')]}
        onAcknowledge={vi.fn()}
      />
    );
    const versions = screen.getAllByText(/^2026\.\d+$/).map((el) => el.textContent);
    expect(versions).toEqual(['2026.6', '2026.4']);
    // both 2026.6 items render under one group
    expect(screen.getByText('newer-title')).toBeInTheDocument();
    expect(screen.getByText('sibling-title')).toBeInTheDocument();
    expect(screen.getByText('older-body')).toBeInTheDocument();
  });

  it('Got it acknowledges once', () => {
    const onAcknowledge = vi.fn();
    render(<WhatsNewModal announcements={[ann('a', '2026.5')]} onAcknowledge={onAcknowledge} />);
    fireEvent.click(screen.getByRole('button', { name: 'Got it' }));
    expect(onAcknowledge).toHaveBeenCalledTimes(1);
  });

  it('closing via the dialog X also acknowledges (no un-acknowledged dismissal path)', () => {
    const onAcknowledge = vi.fn();
    render(<WhatsNewModal announcements={[ann('a', '2026.5')]} onAcknowledge={onAcknowledge} />);
    fireEvent.click(screen.getByRole('button', { name: 'Close' }));
    expect(onAcknowledge).toHaveBeenCalledTimes(1);
  });
});
