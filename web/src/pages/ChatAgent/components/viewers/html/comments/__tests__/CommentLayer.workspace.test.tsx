/**
 * A draft keeps its file's path but not its workspace, so it is kept for the
 * workspace it was written in: a move to another lets it go from the layer,
 * which starts on that workspace's own draft, if any, and a move back brings
 * it again.
 */
import { act } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { runAuthResets } from '@/lib/authResets';
import { renderWithProviders } from '@/test/utils';
import type { PageToViewer } from '../../pageProtocol';
import { keepDraft, readDraft } from '../commentDrafts';
import { DRAFT_ID } from '../pinPlaces';

// Stable across renders, as the query hooks' results are.
const { record, comments } = vi.hoisted(() => ({
  record: {
    artifact: { artifact_id: 'a1', open_comments: 0, latest_seq: 0, shared: false, path: 'NVDA:1day' },
    error: null,
    failed: false,
    retry: () => {},
    loaded: true,
    hashable: false,
  },
  comments: { data: [], isSuccess: true },
}));
vi.mock('@/hooks/useArtifact', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  useCommentRecord: () => record,
  useArtifactComments: () => comments,
  useArtifactStatus: () => ({ data: undefined }),
}));

import CommentLayer from '../CommentLayer';

const CHART = 'NVDA:1day';

/** A draft written on the chart in `workspaceId`, on the spot named `spot`. */
function kept(workspaceId: string, spot: string) {
  keepDraft(workspaceId, 'chart', {
    draft: {
      anchor: { kind: 'chart', marks: [{ type: 'point', time: spot, price: 100 }] },
      rect: { x: 10, y: 20, width: 0, height: 0 },
      path: CHART,
      title: null,
    },
    text: `words in ${workspaceId}`,
    forAgent: true,
    to: null,
  });
}

function setup(workspaceId: string) {
  let send: (message: PageToViewer) => void = () => {};
  const post = vi.fn();
  const ui = { modeTab: 't', listTab: null, showDoc: () => () => {}, setList: vi.fn(), setMode: vi.fn() };
  const bridge = {
    session: 1,
    path: CHART,
    ready: true,
    post,
    subscribe: (listener: (message: PageToViewer) => void) => {
      send = listener;
      return () => {};
    },
  };
  const layer = (ws: string) => (
    <CommentLayer
      tabId="t"
      workspaceId={ws}
      target={{ kind: 'chart', path: CHART }}
      bridge={bridge as never}
      view={{ key: 'k', versionView: { version: null, record: null, entry: null } } as never}
      active
      ui={ui as never}
    />
  );
  const { rerender } = renderWithProviders(layer(workspaceId));
  /** The draft's spot the page is asked to find, by its time; null for none. */
  const draftSpot = () => {
    const last = post.mock.calls.map(([m]) => m).filter((m) => m.type === 'page:anchors').at(-1);
    const item = (last?.items ?? []).find((i: { id: string }) => i.id === DRAFT_ID);
    return item ? (item.anchor.marks[0].time as string) : null;
  };
  const pick = () => act(() => send({
    type: 'page:pick',
    anchor: { kind: 'element', path: '#a' },
    rect: { x: 10, y: 20, width: 100, height: 30 },
    title: null,
    press: null,
  } as PageToViewer));
  return { draftSpot, pick, post, moveTo: (ws: string) => rerender(layer(ws)) };
}

afterEach(() => {
  localStorage.clear();
  runAuthResets();
});

describe('a draft and its workspace', () => {
  it('opens on the draft kept for this workspace and chart', () => {
    kept('ws-a', '2026-10-01');
    const { draftSpot } = setup('ws-a');
    expect(draftSpot()).toBe('2026-10-01');
  });

  it('is kept for its workspace on a move, and the layer starts on the new one\'s own draft', () => {
    kept('ws-a', '2026-10-01');
    kept('ws-b', '2026-10-02');
    const { draftSpot, moveTo } = setup('ws-a');

    moveTo('ws-b');
    expect(draftSpot()).toBe('2026-10-02');
    expect(readDraft('ws-a', 'chart', CHART)?.text).toBe('words in ws-a');

    moveTo('ws-a');
    expect(draftSpot()).toBe('2026-10-01');
  });

  it('lets an empty draft go on a move, to a workspace with none of its own', () => {
    const { draftSpot, pick, post, moveTo } = setup('ws-a');
    pick();
    expect(post.mock.calls.some(([m]) => m.type === 'page:anchors' && m.items.some((i: { id: string }) => i.id === DRAFT_ID))).toBe(true);
    moveTo('ws-b');
    expect(draftSpot()).toBeNull();
  });

  it('stays while the workspace does', () => {
    kept('ws-a', '2026-10-01');
    const { draftSpot, moveTo } = setup('ws-a');
    moveTo('ws-a');
    expect(draftSpot()).toBe('2026-10-01');
  });
});
