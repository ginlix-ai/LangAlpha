/**
 * RightPanel snap-back precedence: when multiple target props are set in one
 * render, the panel must converge on memory > memo > file. The parent
 * (ChatView) clears siblings before setting one, but this effect is the
 * second line of defense.
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

describe('RightPanel snap-back precedence', () => {
  it('starts on the initialTab when no targets are set', async () => {
    renderWithProviders(<RightPanel {...baseProps} initialTab="memory" />);
    await waitFor(() => {
      expect(screen.getByTestId('tabs').getAttribute('data-active')).toBe('memory');
    });
  });

  it('snaps to Files when targetFile is set', async () => {
    renderWithProviders(
      <RightPanel
        {...baseProps}
        initialTab="memory"
        targetFile="work/notes.md"
      />,
    );
    await waitFor(() => {
      expect(screen.getByTestId('tabs').getAttribute('data-active')).toBe('files');
    });
  });

  it('snaps to Memo when targetMemoKey is set', async () => {
    renderWithProviders(
      <RightPanel
        {...baseProps}
        initialTab="files"
        targetMemoKey="report.pdf"
      />,
    );
    await waitFor(() => {
      expect(screen.getByTestId('tabs').getAttribute('data-active')).toBe('memo');
    });
  });

  it('snaps to Memory when targetMemoryKey is set', async () => {
    renderWithProviders(
      <RightPanel
        {...baseProps}
        initialTab="files"
        targetMemoryKey="risk-preferences.md"
        targetMemoryTier="user"
      />,
    );
    await waitFor(() => {
      expect(screen.getByTestId('tabs').getAttribute('data-active')).toBe('memory');
    });
  });

  it('honors precedence (memory > memo > file) when all three are set', async () => {
    renderWithProviders(
      <RightPanel
        {...baseProps}
        initialTab="files"
        targetFile="work/notes.md"
        targetMemoKey="report.pdf"
        targetMemoryKey="risk-preferences.md"
        targetMemoryTier="user"
      />,
    );
    await waitFor(() => {
      expect(screen.getByTestId('tabs').getAttribute('data-active')).toBe('memory');
    });
  });

  it('honors precedence (memo > file) when memory is null', async () => {
    renderWithProviders(
      <RightPanel
        {...baseProps}
        initialTab="files"
        targetFile="work/notes.md"
        targetMemoKey="report.pdf"
      />,
    );
    await waitFor(() => {
      expect(screen.getByTestId('tabs').getAttribute('data-active')).toBe('memo');
    });
  });

  it('treats empty-string targetMemoKey as a memo-tab open (no entry)', async () => {
    // Used by ChatView for memo-index routing.
    renderWithProviders(
      <RightPanel
        {...baseProps}
        initialTab="files"
        targetMemoKey=""
      />,
    );
    await waitFor(() => {
      expect(screen.getByTestId('tabs').getAttribute('data-active')).toBe('memo');
    });
  });

  it('snaps to Sources when targetSources is set and shows the Sources tab', async () => {
    renderWithProviders(
      <RightPanel
        {...baseProps}
        initialTab="files"
        targetSources="msg-1"
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

  it('hides the Sources tab when no targetSources is set', async () => {
    renderWithProviders(<RightPanel {...baseProps} initialTab="files" />);
    await waitFor(() => {
      expect(screen.getByTestId('tabs')).toBeInTheDocument();
    });
    expect(screen.queryByRole('button', { name: 'rightPanel.tabs.sources' })).not.toBeInTheDocument();
  });

  it('honors precedence (sources > memory) when both are set', async () => {
    renderWithProviders(
      <RightPanel
        {...baseProps}
        initialTab="files"
        targetSources="msg-1"
        sourcesRecords={{}}
        targetMemoryKey="risk-preferences.md"
        targetMemoryTier="user"
      />,
    );
    await waitFor(() => {
      expect(screen.getByTestId('tabs').getAttribute('data-active')).toBe('sources');
    });
  });
});
