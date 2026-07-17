/**
 * RightPanel target snapping: the panel snaps to the tab that owns the current
 * `panelTarget.kind`. The union makes multiple simultaneous targets
 * unrepresentable, so the former precedence ladder collapses to a single switch
 * — each kind is tested in isolation here.
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

describe('RightPanel target snapping', () => {
  it('starts on the initialTab when no target is set', async () => {
    renderWithProviders(<RightPanel {...baseProps} initialTab="memory" />);
    await waitFor(() => {
      expect(screen.getByTestId('tabs').getAttribute('data-active')).toBe('memory');
    });
  });

  it('snaps to Files when a file target is set', async () => {
    renderWithProviders(
      <RightPanel
        {...baseProps}
        initialTab="memory"
        panelTarget={{ kind: 'file', path: 'work/notes.md' }}
      />,
    );
    await waitFor(() => {
      expect(screen.getByTestId('tabs').getAttribute('data-active')).toBe('files');
    });
  });

  it('snaps to Files when a directory-only file target is set', async () => {
    renderWithProviders(
      <RightPanel
        {...baseProps}
        initialTab="memory"
        panelTarget={{ kind: 'file', dir: 'work/reports' }}
      />,
    );
    await waitFor(() => {
      expect(screen.getByTestId('tabs').getAttribute('data-active')).toBe('files');
    });
  });

  it('snaps to Memo when a memo target is set', async () => {
    renderWithProviders(
      <RightPanel
        {...baseProps}
        initialTab="files"
        panelTarget={{ kind: 'memo', key: 'report.pdf' }}
      />,
    );
    await waitFor(() => {
      expect(screen.getByTestId('tabs').getAttribute('data-active')).toBe('memo');
    });
  });

  it('snaps to Memory when a memory target is set', async () => {
    renderWithProviders(
      <RightPanel
        {...baseProps}
        initialTab="files"
        panelTarget={{ kind: 'memory', key: 'risk-preferences.md', tier: 'user' }}
      />,
    );
    await waitFor(() => {
      expect(screen.getByTestId('tabs').getAttribute('data-active')).toBe('memory');
    });
  });

  it('treats an empty-string memo key as a memo-tab open (no entry)', async () => {
    // Used by ChatView for memo-index routing.
    renderWithProviders(
      <RightPanel
        {...baseProps}
        initialTab="files"
        panelTarget={{ kind: 'memo', key: '' }}
      />,
    );
    await waitFor(() => {
      expect(screen.getByTestId('tabs').getAttribute('data-active')).toBe('memo');
    });
  });

  it('snaps to Sources when a sources target is set and shows the Sources tab', async () => {
    renderWithProviders(
      <RightPanel
        {...baseProps}
        initialTab="files"
        panelTarget={{ kind: 'sources', messageId: 'msg-1' }}
        sourcesRecords={{}}
      />,
    );
    await waitFor(() => {
      expect(screen.getByTestId('tabs').getAttribute('data-active')).toBe('sources');
    });
    // The Sources tab only appears when a turn's provenance is being shown.
    expect(screen.getByRole('button', { name: 'rightPanel.tabs.sources' })).toBeInTheDocument();
    // Body is lazy-loaded — wait for the SourcesPanel stub to resolve.
    expect(await screen.findByTestId('sources-panel')).toBeInTheDocument();
  });

  it('hides the Sources tab when no sources target is set', async () => {
    renderWithProviders(<RightPanel {...baseProps} initialTab="files" />);
    await waitFor(() => {
      expect(screen.getByTestId('tabs')).toBeInTheDocument();
    });
    expect(screen.queryByRole('button', { name: 'rightPanel.tabs.sources' })).not.toBeInTheDocument();
  });
});
