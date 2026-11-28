import React from 'react';
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { waitFor, renderHook } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router';
import type { Artifact, ArtifactDestinations, ArtifactTarget, DestinationThread } from '@/types/api';
import { CommentSurfaceContext } from '../commentSurface';
import { useLayerSending } from '../useLayerSending';
import type { CommentUi } from '../useCommentUi';

const api = vi.hoisted(() => ({
  findArtifact: vi.fn(),
  getArtifactDestinations: vi.fn(),
  findDestinationThread: vi.fn(),
}));
vi.mock('@/pages/ChatAgent/utils/api/artifacts', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/ChatAgent/utils/api/artifacts')>()),
  ...api,
}));

const drive: ArtifactTarget = { kind: 'drive', path: 'e1' };
const record = { artifact_id: 'art-1', workspace_id: 'home', kind: 'drive', path: 'e1' } as unknown as Artifact;
const destinations = {
  default: { rule: 'new', thread: null, workspace_id: 'home' }, bound: null, beside: null, creator: null, worked: null,
  home_workspace_id: 'home',
} as unknown as ArtifactDestinations;
const picked: DestinationThread = {
  thread_id: 'th-picked', title: 'Q3 notes', workspace_id: 'ws-2', workspace_name: 'Research', home: false, updated_at: null,
};

/** The layer on a Drive file, with a draft kept from an earlier visit that picked `to` (read each render). */
function layer(to: { threadId: string } | (() => { threadId: string } | null), onTo = vi.fn()) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const wrapper = ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={qc}>
      <MemoryRouter>
        <CommentSurfaceContext value={{ enter: 'send', pickAnyChat: true }}>{children}</CommentSurfaceContext>
      </MemoryRouter>
    </QueryClientProvider>
  );
  const ui = { sends: { get: () => undefined } } as unknown as CommentUi;
  return renderHook(
    () => useLayerSending({ workspaceId: 'home', doc: drive, draft: drive, ui, to: typeof to === 'function' ? to() : to, onTo }),
    { wrapper },
  );
}

beforeEach(() => {
  api.findArtifact.mockResolvedValue(record);
  api.getArtifactDestinations.mockResolvedValue(destinations);
  api.findDestinationThread.mockReset();
});

describe('a draft kept with a chat picked through "Another chat…"', () => {
  it('goes there when it comes back, and its "To:" line says so', async () => {
    api.findDestinationThread.mockResolvedValue(picked);
    const { result } = layer({ threadId: 'th-picked' });

    await waitFor(() => expect(result.current.draftRoute('send')).toEqual({ kind: 'send', to: { threadId: 'th-picked' } }));
    expect(api.findDestinationThread).toHaveBeenCalledWith('art-1', 'th-picked');
    expect(result.current.destination.status).toEqual({ kind: 'ready', selected: 'thread:th-picked' });
    expect(result.current.destination.choices.find((c) => c.key === 'thread:th-picked')?.label).toBe('Q3 notes');
  });

  it('holds the send until the server has said, rather than sending it to the default meanwhile', async () => {
    api.findDestinationThread.mockImplementation(() => new Promise(() => {}));
    const { result } = layer({ threadId: 'th-picked' });

    await waitFor(() => expect(api.findDestinationThread).toHaveBeenCalled());
    expect(result.current.draftRoute('send')).toBeNull();
    expect(result.current.draftRoute('note')).toEqual({ kind: 'note' });
  });

  it('goes to the default when that chat no longer takes the comments', async () => {
    api.findDestinationThread.mockResolvedValue(null);
    const { result } = layer({ threadId: 'th-archived' });

    await waitFor(() => expect(result.current.draftRoute('send')).toEqual({ kind: 'send', to: { newThreadIn: 'home' } }));
    expect(result.current.destination.status).toEqual({ kind: 'ready', selected: 'new:home' });
  });
});

describe('a chat picked through "Another chat…"', () => {
  it('counts at once, the search having said it takes the comments', async () => {
    let to: { threadId: string } | null = null;
    const { result, rerender } = layer(() => to, vi.fn((next) => { to = next; }));
    await waitFor(() => expect(result.current.destination.search).not.toBeNull());

    result.current.destination.search!.onPick(picked);
    rerender();
    expect(to).toEqual({ threadId: 'th-picked' });
    expect(result.current.destination.status).toEqual({ kind: 'ready', selected: 'thread:th-picked' });
    expect(result.current.draftRoute('send')).toEqual({ kind: 'send', to: { threadId: 'th-picked' } });
    expect(api.findDestinationThread).not.toHaveBeenCalled();
  });
});
