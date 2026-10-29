import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { screen, fireEvent } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';

const read = vi.hoisted(() => ({ fail: false, html: '<html><body><h2 id="risks">Risks</h2></body></html>' }));
vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => {
  const orig = await importOriginal<Record<string, unknown>>();
  const answer = async () => {
    if (read.fail) throw Object.assign(new Error('Request failed with status code 500'), { response: { status: 500 } });
    return { content: read.html, mime: 'text/html', truncated: false };
  };
  return {
    ...orig,
    readWorkspaceFile: vi.fn(answer),
    readWorkspaceFileFull: vi.fn(answer),
    writeWorkspaceFile: vi.fn(),
    downloadWorkspaceFileAsArrayBuffer: vi.fn(),
    triggerFileDownload: vi.fn(),
    resolveWorkspaceFile: vi.fn(),
    getPreviewUrl: vi.fn(),
  };
});
vi.mock('@/hooks/useWorkspace', () => ({ useWorkspace: () => ({ data: { status: 'running', name: 'ws' } }) }));
vi.mock('@/contexts/ThemeContext', () => ({ useTheme: () => ({ theme: 'dark' }) }));
vi.mock('@/hooks/useWorkspaceFileGrant', () => ({
  useWorkspaceFileGrant: () => ({ data: { prefix: '/api/v1/wsfiles/g/grant-1/', expires_in: 3600 }, error: null, refetch: vi.fn() }),
}));
vi.mock('@/hooks/useShareLink', () => ({ useShareLink: () => ({ data: undefined }) }));
vi.mock('@/pages/ChatAgent/components/FilePanelMemo', () => ({
  memoMimeForName: () => null,
  useAddToMemo: () => vi.fn(),
  useWorkspaceMemoIndex: () => new Map(),
  useMemoStaleCheck: () => ({ status: null, sandboxText: null, refresh: () => {} }),
  MemoStaleBanner: () => null,
  MemoDiffModal: () => null,
}));
vi.mock('@/pages/ChatAgent/components/SandboxSettingsPanel', () => ({ SandboxSettingsContent: () => null }));
vi.mock('@/pages/ChatAgent/components/SyntaxHighlighter', () => ({
  default: ({ children }: { children: string }) => <pre>{children}</pre>,
  oneDark: {},
  oneLight: {},
}));

import FilePanel from '@/pages/ChatAgent/components/FilePanel';
import { fileTarget } from '@/pages/ChatAgent/components/filePanel/types';

const panel = (seq = 1) => (
  <FilePanel
    workspaceId="ws"
    onClose={() => {}}
    files={['report.html']}
    target={fileTarget('report.html', { location: { anchor: 'risks' } }, seq)}
    onTargetHandled={() => {}}
  />
);

beforeEach(() => { localStorage.clear(); read.fail = false; });
afterEach(() => { vi.restoreAllMocks(); });

/** The panel holds Source versus the page, and the file header switches it. */
describe('FilePanel HTML view', () => {
  // Coming back to a tab puts the reader back at its section, which is not
  // a new ask for the page, so the markup they chose stays up.
  it('keeps the source up when the reader comes back to a tab with a section', async () => {
    renderWithProviders(panel());
    fireEvent.click(await screen.findByRole('button', { name: 'View source' }));
    expect(screen.getByRole('button', { name: 'View page' })).toBeTruthy();

    fireEvent.click(screen.getByRole('button', { name: 'New tab' }));
    fireEvent.click(screen.getByRole('tab', { name: /report\.html/ }));

    expect(await screen.findByRole('button', { name: 'View page' })).toBeTruthy();
  });

  // A step lands under a new tab id, and it is still a return, not an ask.
  it('keeps the source up when Back returns to a file with a section', async () => {
    const { rerender } = renderWithProviders(panel(1));
    fireEvent.click(await screen.findByRole('button', { name: 'View source' }));
    rerender(
      <FilePanel workspaceId="ws" onClose={() => {}} files={['report.html', 'appendix.html']} target={fileTarget('appendix.html', {}, 2)} onTargetHandled={() => {}} />,
    );
    await screen.findByText('appendix.html', { selector: '.file-panel-tab-name' });

    fireEvent.click(screen.getByRole('button', { name: 'Back' }));
    await screen.findByText('report.html', { selector: '.file-panel-tab-name' });
    expect(await screen.findByRole('button', { name: 'View page' })).toBeTruthy();
    await new Promise((r) => setTimeout(r, 50));
    expect(screen.queryByRole('button', { name: 'View source' })).toBeNull();
  });

  it('returns to the page when a reference asks for the section again', async () => {
    const { rerender } = renderWithProviders(panel(1));
    fireEvent.click(await screen.findByRole('button', { name: 'View source' }));
    rerender(panel(2));
    expect(await screen.findByRole('button', { name: 'View source' })).toBeTruthy();
  });

  it('offers no switch over a file that failed to load', async () => {
    read.fail = true;
    renderWithProviders(panel());
    await screen.findByText('report.html', { selector: '.file-panel-tab-name' });
    await new Promise((r) => setTimeout(r, 50));
    expect(screen.queryByRole('button', { name: 'View source' })).toBeNull();
  });
});
