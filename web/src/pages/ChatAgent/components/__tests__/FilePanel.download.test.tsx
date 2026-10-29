import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen, waitFor, fireEvent } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';

// `renderWithProviders` mounts no Toaster, so a toast has nowhere to appear in
// the DOM. The spy stands in for that viewport, and the assertion pins the
// sentence the reader would have read, not merely that a toast happened.
const { toastSpy } = vi.hoisted(() => ({ toastSpy: vi.fn() }));
vi.mock('@/components/ui/use-toast', async (importOriginal) => {
  const orig = await importOriginal<Record<string, unknown>>();
  return { ...orig, toast: toastSpy };
});

vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => {
  const orig = await importOriginal<Record<string, unknown>>();
  return {
    ...orig,
    readWorkspaceFile: vi.fn(),
    readWorkspaceFileFull: vi.fn(),
    triggerFileDownload: vi.fn(),
    resolveWorkspaceFile: vi.fn(),
  };
});
vi.mock('@/hooks/useWorkspace', () => ({ useWorkspace: () => ({ data: { status: 'running', name: 'ws' } }) }));
vi.mock('@/contexts/ThemeContext', () => ({ useTheme: () => ({ theme: 'dark' }) }));
vi.mock('@/pages/ChatAgent/components/FilePanelMemo', () => ({
  memoMimeForName: () => null,
  useAddToMemo: () => vi.fn(),
  useWorkspaceMemoIndex: () => new Map(),
  useMemoStaleCheck: () => ({ status: null, sandboxText: null, refresh: () => {} }),
  MemoStaleBanner: () => null,
  MemoDiffModal: () => null,
}));
vi.mock('@/pages/ChatAgent/components/SandboxSettingsPanel', () => ({ SandboxSettingsContent: () => null }));

// The boundary only has something to catch if a viewer throws. CSV is the one
// that renders from text alone, so no ArrayBuffer fixture is needed to reach it.
vi.mock('@/pages/ChatAgent/components/viewers/CsvViewer', () => ({
  default: () => {
    throw new Error('viewer blew up');
  },
}));

import * as api from '@/pages/ChatAgent/utils/api';
import FilePanel from '@/pages/ChatAgent/components/FilePanel';

beforeEach(() => {
  vi.clearAllMocks();
  (api.readWorkspaceFile as ReturnType<typeof vi.fn>).mockResolvedValue({
    content: 'a,b\n1,2\n',
    mime: 'text/csv',
    truncated: false,
  });
});

describe('a download started from a viewer that could not render', () => {
  // The offer is the whole interaction: nothing opens, nothing navigates and
  // the browser shows no file, so a failure that only reaches the console
  // leaves the reader looking at a button that did nothing.
  it('says the save went nowhere instead of only logging it', async () => {
    (api.triggerFileDownload as ReturnType<typeof vi.fn>).mockRejectedValue(
      new Error('sandbox stopped'),
    );
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {});

    renderWithProviders(
      <FilePanel workspaceId="ws" onClose={() => {}} files={['data.csv']} target={{ kind: 'file', path: 'data.csv' }} />,
    );

    const offer = await screen.findByText('Download instead');
    fireEvent.click(offer);

    await waitFor(() => {
      expect(api.triggerFileDownload).toHaveBeenCalledWith('ws', 'data.csv');
      expect(toastSpy).toHaveBeenCalledWith({
        description: "Couldn't download the file",
        variant: 'destructive',
      });
    });
    logged.mockRestore();
  });

  it('leaves the explanation in place rather than replacing it with a second error', async () => {
    // The control for the choice of a toast over `fileError`: raising one here
    // would swap out the fallback the reader is already reading.
    (api.triggerFileDownload as ReturnType<typeof vi.fn>).mockRejectedValue(
      new Error('sandbox stopped'),
    );
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {});

    renderWithProviders(
      <FilePanel workspaceId="ws" onClose={() => {}} files={['data.csv']} target={{ kind: 'file', path: 'data.csv' }} />,
    );
    fireEvent.click(await screen.findByText('Download instead'));

    await waitFor(() => expect(toastSpy).toHaveBeenCalled());
    expect(screen.getByText('Unable to preview this file')).toBeTruthy();
    logged.mockRestore();
  });
});
