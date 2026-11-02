/**
 * A status read copies the file's whole list off the computer, and only a new
 * comment stores what it answers, so opening a file with comments asks nothing.
 */
import { describe, expect, it, vi } from 'vitest';
import { renderWithProviders } from '@/test/utils';

const { status, owned, comments } = vi.hoisted(() => ({
  status: vi.fn((..._args: unknown[]) => ({ data: undefined })),
  owned: {
    record: { artifact_id: 'a1', open_comments: 2, latest_seq: 3, shared: true, path: 'r.html' },
    error: null,
  },
  comments: { data: [], isSuccess: true },
}));
vi.mock('@/hooks/useArtifact', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  useOwnedArtifact: () => owned,
  useArtifactComments: () => comments,
  useArtifactStatus: (...args: unknown[]) => status(...args),
}));

import CommentLayer from '../CommentLayer';

describe('CommentLayer', () => {
  it('reads no status for a file with comments until one is being written', () => {
    const showDoc = () => () => {};
    const ui = { modeTab: null, listTab: null, showDoc, setList: vi.fn(), setMode: vi.fn() };
    const bridge = { session: 's', path: 'r.html', ready: true, post: vi.fn(), subscribe: () => () => {} };
    renderWithProviders(
      <CommentLayer
        tabId="t"
        workspaceId="ws"
        path="r.html"
        bridge={bridge as never}
        view={{ key: 'k', versionView: { version: null, record: null, entry: null } } as never}
        active
        ui={ui as never}
      />,
    );

    expect(status).toHaveBeenCalled();
    for (const [, options] of status.mock.calls) expect((options as { enabled: boolean }).enabled).toBe(false);
  });
});
