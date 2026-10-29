/**
 * The panel reads the chat's transcript through a subscription rather than
 * by re-rendering on every streamed chunk, so what it shows off the
 * transcript has to follow the transcript on its own: a tool tab shows its
 * call's new record when the record is replaced, and a chunk that leaves the
 * record alone renders nothing in the panel at all.
 */
import React, { Profiler } from 'react';
import { describe, it, expect, vi } from 'vitest';
import { act, screen } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';

vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => {
  const orig = await importOriginal<Record<string, unknown>>();
  return {
    ...orig,
    readWorkspaceFile: vi.fn(async () => ({ content: '# Notes', mime: 'text/markdown', truncated: false })),
    readWorkspaceFileFull: vi.fn(async () => ({ content: '# Notes' })),
    resolveWorkspaceFile: vi.fn(),
    getPreviewUrl: vi.fn(),
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

import FilePanel from '@/pages/ChatAgent/components/FilePanel';
import type { ToolCallProcessRecord } from '@/pages/ChatAgent/components/ToolCallDetailView';
import { createTranscriptStore } from '@/pages/ChatAgent/components/filePanel/transcriptStore';

const running: ToolCallProcessRecord = {
  toolName: 'bash',
  toolCall: { name: 'bash', args: { command: 'echo first' } },
  isComplete: false,
};
const settled: ToolCallProcessRecord = { ...running, isComplete: true, toolCallResult: { content: 'the-new-output' } };

/** One assistant turn holding the call, with `text` streamed so far. */
const turn = (proc: ToolCallProcessRecord, text: string) => [
  { id: 'm1', role: 'assistant', content: text, toolCallProcesses: { 'call-1': proc } },
];

describe('FilePanel transcript reads', () => {
  it('follows a tool call as its record is replaced, and renders nothing for a chunk that leaves it alone', async () => {
    const store = createTranscriptStore({ messages: turn(running, '') });
    const onRender = vi.fn();
    renderWithProviders(
      <Profiler id="panel" onRender={onRender}>
        <FilePanel
          workspaceId="ws"
          onClose={() => {}}
          files={[]}
          target={{ kind: 'tool', toolCallId: 'call-1', seq: 1 }}
          transcript={store.reader}
        />
      </Profiler>,
    );
    // The tab's body is a lazy chunk, slower to land under a loaded suite.
    await screen.findByText('No result content', undefined, { timeout: 5000 });
    await act(async () => {});

    onRender.mockClear();
    act(() => store.publish({ messages: turn(running, 'A chunk of the reply') }));
    expect(onRender).not.toHaveBeenCalled();

    act(() => store.publish({ messages: turn(settled, 'A chunk of the reply, and more') }));
    expect(await screen.findByText('the-new-output')).toBeInTheDocument();
  });
});
