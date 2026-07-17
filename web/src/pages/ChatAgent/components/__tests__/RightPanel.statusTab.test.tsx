/**
 * RightPanel Status tab: the tab appears whenever a watch is active or an
 * explicit chip click (a `panelTarget` of kind 'status') is pending, snaps to
 * Status on that click, and falls back to Files when both signals clear while
 * Status is open.
 */
import { describe, it, expect, vi } from 'vitest';
import { screen, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';
import RightPanel from '../RightPanel';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string) => key,
  }),
}));

// Replace lazy children with cheap stubs so we can assert which one renders.
vi.mock('../FilePanel', () => ({
  default: () => <div data-testid="file-panel">files</div>,
}));
vi.mock('../MemoryPanel', () => ({
  default: () => <div data-testid="memory-panel">memory</div>,
}));
vi.mock('../MemoPanel', () => ({
  default: () => <div data-testid="memo-panel">memo</div>,
}));
vi.mock('../SourcesPanel', () => ({
  default: () => <div data-testid="sources-panel">sources</div>,
}));
vi.mock('../StatusPanel', () => ({
  default: () => <div data-testid="status-panel">status</div>,
}));

vi.mock('@/components/ui/animated-tabs', () => ({
  AnimatedTabs: ({ tabs, value, onChange }: {
    tabs: { id: string; label: string }[];
    value: string;
    onChange: (id: string) => void;
  }) => (
    <div data-testid="tabs" data-active={value}>
      {tabs.map((t) => (
        <button key={t.id} data-tab={t.id} onClick={() => onChange(t.id)}>
          {t.label}
        </button>
      ))}
    </div>
  ),
}));

const baseProps = {
  workspaceId: 'ws-1',
  onClose: () => {},
};

const statusTab = () => screen.queryByRole('button', { name: 'rightPanel.tabs.status' });

describe('RightPanel Status tab', () => {
  it('hides the Status tab when there is no watch and no target', async () => {
    renderWithProviders(<RightPanel {...baseProps} initialTab="files" />);
    await waitFor(() => {
      expect(screen.getByTestId('tabs')).toBeInTheDocument();
    });
    expect(statusTab()).not.toBeInTheDocument();
  });

  it('shows the Status tab whenever a watch is active (symbols present)', async () => {
    renderWithProviders(
      <RightPanel
        {...baseProps}
        initialTab="files"
        marketWatch={{ symbols: ['NVDA', 'TSLA'] }}
      />,
    );
    await waitFor(() => {
      expect(statusTab()).toBeInTheDocument();
    });
    // No explicit target, so it doesn't steal focus from the initial tab.
    expect(screen.getByTestId('tabs').getAttribute('data-active')).toBe('files');
  });

  it('snaps to the Status tab when a status target is set', async () => {
    renderWithProviders(
      <RightPanel
        {...baseProps}
        initialTab="files"
        panelTarget={{ kind: 'status' }}
        marketWatch={{ symbols: ['NVDA'] }}
      />,
    );
    await waitFor(() => {
      expect(screen.getByTestId('tabs').getAttribute('data-active')).toBe('status');
    });
    expect(statusTab()).toBeInTheDocument();
    expect(await screen.findByTestId('status-panel')).toBeInTheDocument();
  });

  it('opens on a status target even with no symbols yet (chip click before first stamp)', async () => {
    renderWithProviders(
      <RightPanel {...baseProps} initialTab="files" panelTarget={{ kind: 'status' }} />,
    );
    await waitFor(() => {
      expect(screen.getByTestId('tabs').getAttribute('data-active')).toBe('status');
    });
    expect(statusTab()).toBeInTheDocument();
  });

  it('falls back to Files when the Status tab disappears (target cleared, no symbols)', async () => {
    const { rerender } = renderWithProviders(
      <RightPanel {...baseProps} initialTab="files" panelTarget={{ kind: 'status' }} />,
    );
    await waitFor(() => {
      expect(screen.getByTestId('tabs').getAttribute('data-active')).toBe('status');
    });
    // The chip-click target clears and no watch is active → the tab is gone.
    rerender(<RightPanel {...baseProps} initialTab="files" panelTarget={null} />);
    await waitFor(() => {
      expect(screen.getByTestId('tabs').getAttribute('data-active')).toBe('files');
    });
    expect(statusTab()).not.toBeInTheDocument();
  });
});
