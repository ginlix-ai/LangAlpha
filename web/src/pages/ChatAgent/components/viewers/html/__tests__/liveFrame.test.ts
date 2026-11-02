import { renderHook } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

vi.mock('@/hooks/useWorkspaceFileGrant', () => ({
  useWorkspaceFileGrant: () => ({ data: null, error: null, refetch: vi.fn() }),
}));
vi.mock('../../../../contexts/WorkspaceContext', () => ({ useWorkspaceFolders: () => null }));

import { useLiveFrame } from '../liveFrame';

describe('useLiveFrame', () => {
  it('holds a first fragment with its file, as the viewer stays mounted from tab to tab', () => {
    const { result, rerender } = renderHook(
      ({ filePath, anchor }: { filePath: string; anchor: string | null }) =>
        useLiveFrame({ workspaceId: 'w', filePath, prefix: '/serve/', inject: 'comments', revision: 'r', anchor }),
      { initialProps: { filePath: 'a.html', anchor: 'revenue' as string | null } },
    );
    expect(result.current.source?.src).toMatch(/a\.html.*#revenue$/);

    rerender({ filePath: 'b.html', anchor: null });
    expect(result.current.source?.src).toMatch(/b\.html/);
    expect(result.current.source?.src).not.toContain('#');

    // A later ask in the same file goes to the document, not the address.
    rerender({ filePath: 'b.html', anchor: 'margin' });
    expect(result.current.source?.src).not.toContain('#');

    rerender({ filePath: 'c.html', anchor: 'margin' });
    expect(result.current.source?.src).toMatch(/c\.html.*#margin$/);
  });

  it('takes the same path in another workspace for another file', () => {
    const { result, rerender } = renderHook(
      ({ workspaceId, anchor }: { workspaceId: string; anchor: string | null }) =>
        useLiveFrame({ workspaceId, filePath: 'report.html', prefix: `/serve/${workspaceId}/`, inject: 'view', anchor }),
      { initialProps: { workspaceId: 'a', anchor: 'revenue' as string | null } },
    );
    const before = result.current.source;
    expect(before?.src).toContain('#revenue');

    rerender({ workspaceId: 'b', anchor: null });
    // Under one key the frame would read the new address as a renewed grant and keep A's document.
    expect(result.current.source?.key).not.toBe(before?.key);
    expect(result.current.source?.src).not.toContain('#');
  });
});
