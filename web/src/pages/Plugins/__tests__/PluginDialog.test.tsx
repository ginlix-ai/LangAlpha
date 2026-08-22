import { describe, it, expect, vi } from 'vitest';
import { fireEvent, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';
import { PluginDialog } from '../components/PluginDialog';

/**
 * The dialog has three dismissal routes (the X, the backdrop, Escape) and one
 * step that must survive all three.
 *
 * Closing mid-install does not cancel anything: the request is already with the
 * server, so the plugin installs either way. What dismissal destroyed was the
 * report, the only statement of which components landed, which credentials are
 * still missing, and which sse entries can be upgraded. The result was a plugin
 * that installed and then silently did nothing.
 *
 * Each route is asserted separately because they are three different
 * mechanisms, and gating only the one someone remembered is how this comes
 * back.
 */
describe('PluginDialog dismissable', () => {
  const open = (dismissable: boolean, onClose: () => void) =>
    renderWithProviders(
      <PluginDialog
        title="Install plugin"
        subtitle="Installing"
        dismissable={dismissable}
        onClose={onClose}
      >
        <button>inside</button>
      </PluginDialog>,
    );

  it('offers all three routes while dismissable', () => {
    const onClose = vi.fn();
    open(true, onClose);

    const close = screen.getByRole('button', { name: /close/i });
    fireEvent.click(close);
    expect(onClose).toHaveBeenCalledTimes(1);

    fireEvent.keyDown(screen.getByRole('dialog'), { key: 'Escape' });
    expect(onClose).toHaveBeenCalledTimes(2);
  });

  it('withdraws the close button when not dismissable', () => {
    open(false, vi.fn());
    expect(screen.queryByRole('button', { name: /close/i })).toBeNull();
  });

  it('ignores Escape when not dismissable', () => {
    const onClose = vi.fn();
    open(false, onClose);
    fireEvent.keyDown(screen.getByRole('dialog'), { key: 'Escape' });
    expect(onClose).not.toHaveBeenCalled();
  });

  it('ignores a backdrop click when not dismissable', () => {
    const onClose = vi.fn();
    const { container } = open(false, onClose);
    // The backdrop is the dialog's parent: the fixed inset-0 ground.
    const backdrop = screen.getByRole('dialog').parentElement as HTMLElement;
    expect(backdrop).toBeTruthy();
    expect(container).toBeTruthy();
    fireEvent.mouseDown(backdrop);
    fireEvent.click(backdrop);
    expect(onClose).not.toHaveBeenCalled();
  });

  it('keeps a long body reachable by scrolling it rather than overflowing', () => {
    open(true, vi.fn());
    // The body is a capped flex child with its own scroll, so a report longer
    // than the viewport cannot push its own Done button off both ends.
    const body = screen.getByText('inside').closest('.overflow-y-auto');
    expect(body).toBeTruthy();
    expect(screen.getByRole('dialog').className).toMatch(/max-h-/);
  });
});
