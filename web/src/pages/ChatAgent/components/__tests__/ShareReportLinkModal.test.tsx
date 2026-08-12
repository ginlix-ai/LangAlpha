import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';

import ShareReportLinkModal from '../ShareReportLinkModal';

// t returns the key verbatim so we can query by key.
vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

function setup(overrides: Partial<React.ComponentProps<typeof ShareReportLinkModal>> = {}) {
  const onCopyShareable = vi.fn().mockResolvedValue(undefined);
  const onCopyDirect = vi.fn().mockResolvedValue(undefined);
  const onClose = vi.fn();
  render(
    <ShareReportLinkModal
      open
      fileName="report.html"
      onCopyShareable={onCopyShareable}
      onCopyDirect={onCopyDirect}
      onClose={onClose}
      {...overrides}
    />,
  );
  return { onCopyShareable, onCopyDirect, onClose };
}

describe('ShareReportLinkModal', () => {
  it('offers both the shareable and direct link options', () => {
    setup();
    expect(screen.getByText('filePanel.shareableLinkOption')).toBeInTheDocument();
    expect(screen.getByText('filePanel.directLinkOption')).toBeInTheDocument();
  });

  it('runs the shareable action and closes on success', async () => {
    const { onCopyShareable, onCopyDirect, onClose } = setup();
    fireEvent.click(screen.getByText('filePanel.shareableLinkOption'));
    await waitFor(() => expect(onClose).toHaveBeenCalled());
    expect(onCopyShareable).toHaveBeenCalledTimes(1);
    expect(onCopyDirect).not.toHaveBeenCalled();
  });

  it('runs the direct action and closes on success', async () => {
    const { onCopyDirect, onClose } = setup();
    fireEvent.click(screen.getByText('filePanel.directLinkOption'));
    await waitFor(() => expect(onClose).toHaveBeenCalled());
    expect(onCopyDirect).toHaveBeenCalledTimes(1);
  });

  // A failed copy must keep the chooser open so the user can retry or switch.
  it('stays open when the copy action throws', async () => {
    const onCopyShareable = vi.fn().mockRejectedValue(new Error('nope'));
    const onClose = vi.fn();
    setup({ onCopyShareable, onClose });
    fireEvent.click(screen.getByText('filePanel.shareableLinkOption'));
    await waitFor(() => expect(onCopyShareable).toHaveBeenCalled());
    expect(onClose).not.toHaveBeenCalled();
  });
});
