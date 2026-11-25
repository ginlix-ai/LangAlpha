/**
 * A draft keeps its file's path but not its workspace, so it goes when the
 * layer moves to another workspace rather than landing on that workspace's
 * file of the same path. A host asks first where it has words (MarketView).
 */
import { act } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { renderWithProviders } from '@/test/utils';
import type { PageToViewer } from '../../pageProtocol';
import { DRAFT_ID } from '../pinPlaces';

// Stable across renders, as the query hooks' results are.
const { record, comments } = vi.hoisted(() => ({
  record: {
    artifact: { artifact_id: 'a1', open_comments: 0, latest_seq: 0, shared: false, path: 'NVDA:1day' },
    error: null,
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

function setup() {
  let send: (message: PageToViewer) => void = () => {};
  const post = vi.fn();
  const ui = { modeTab: 't', listTab: null, showDoc: () => () => {}, showWriting: () => {}, setList: vi.fn(), setMode: vi.fn() };
  const bridge = {
    session: 1,
    path: 'NVDA:1day',
    ready: true,
    post,
    subscribe: (listener: (message: PageToViewer) => void) => {
      send = listener;
      return () => {};
    },
  };
  const layer = (workspaceId: string) => (
    <CommentLayer
      tabId="t"
      workspaceId={workspaceId}
      target={{ kind: 'chart', path: 'NVDA:1day' }}
      bridge={bridge as never}
      view={{ key: 'k', versionView: { version: null, record: null, entry: null } } as never}
      active
      ui={ui as never}
    />
  );
  const { rerender } = renderWithProviders(layer('ws-a'));
  const draftShown = () => {
    const last = post.mock.calls.map(([m]) => m).filter((m) => m.type === 'page:anchors').at(-1);
    return (last?.items ?? []).some((item: { id: string }) => item.id === DRAFT_ID);
  };
  const pick = () => act(() => send({
    type: 'page:pick',
    anchor: { kind: 'element', path: '#a' },
    rect: { x: 10, y: 20, width: 100, height: 30 },
    title: null,
    press: null,
  } as PageToViewer));
  return { draftShown, pick, moveTo: (ws: string) => rerender(layer(ws)) };
}

describe('a draft and its workspace', () => {
  it('goes with a move to another workspace', () => {
    const { draftShown, pick, moveTo } = setup();
    pick();
    expect(draftShown()).toBe(true);
    moveTo('ws-b');
    expect(draftShown()).toBe(false);
  });

  it('stays while the workspace does', () => {
    const { draftShown, pick, moveTo } = setup();
    pick();
    moveTo('ws-a');
    expect(draftShown()).toBe(true);
  });
});
