/**
 * Esc steps back one layer whichever way it arrives (the viewer's keys or the
 * page's, reported over the bridge): an empty draft goes first, and only the
 * next press leaves comment mode.
 */
import { act, fireEvent } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { renderWithProviders } from '@/test/utils';
import type { PageToViewer } from '../../pageProtocol';
import { DRAFT_ID } from '../pinPlaces';

const { owned, comments } = vi.hoisted(() => ({
  owned: {
    record: { artifact_id: 'a1', open_comments: 0, latest_seq: 0, shared: false, path: 'r.html' },
    error: null,
  },
  comments: { data: [], isSuccess: true },
}));
vi.mock('@/hooks/useArtifact', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  useCommentRecord: () => ({ artifact: owned.record, error: owned.error, loaded: true, hashable: false }),
  useArtifactComments: () => comments,
  useArtifactStatus: () => ({ data: undefined }),
}));

import CommentLayer from '../CommentLayer';

function setup() {
  let send: (message: PageToViewer) => void = () => {};
  const post = vi.fn();
  const ui = { modeTab: 't', listTab: null, showDoc: () => () => {}, showWriting: () => {}, setList: vi.fn(), setMode: vi.fn() };
  const bridge = {
    session: 's',
    path: 'r.html',
    ready: true,
    post,
    subscribe: (listener: (message: PageToViewer) => void) => {
      send = listener;
      return () => {};
    },
  };
  renderWithProviders(
    <CommentLayer
      tabId="t"
      workspaceId="ws"
      target={{ kind: 'file', path: 'r.html' }}
      bridge={bridge as never}
      view={{ key: 'k', versionView: { version: null, record: null, entry: null } } as never}
      active
      ui={ui as never}
    />,
  );
  const draftShown = () => {
    const last = post.mock.calls.map(([m]) => m).filter((m) => m.type === 'page:anchors').at(-1);
    return (last?.items ?? []).some((item: { id: string }) => item.id === DRAFT_ID);
  };
  const pick = () => act(() => send({
    type: 'page:pick',
    anchor: { kind: 'element', path: '#a' },
    rect: { x: 10, y: 20, width: 100, height: 30 },
    title: 'Report',
    press: null,
  } as PageToViewer));
  return { ui, send, draftShown, pick };
}

describe('Esc in comment mode', () => {
  it('lets an empty draft go first, then leaves the mode, pressed in the viewer', () => {
    const { ui, draftShown, pick } = setup();
    pick();
    expect(draftShown()).toBe(true);

    fireEvent.keyDown(document, { key: 'Escape' });
    expect(draftShown()).toBe(false);
    expect(ui.setMode).not.toHaveBeenCalled();

    fireEvent.keyDown(document, { key: 'Escape' });
    expect(ui.setMode).toHaveBeenCalledWith('t', false);
  });

  it('steps back the same way when the page reports the key', () => {
    const { ui, send, draftShown, pick } = setup();
    pick();

    act(() => send({ type: 'page:key', key: 'Escape' } as PageToViewer));
    expect(draftShown()).toBe(false);
    expect(ui.setMode).not.toHaveBeenCalled();

    act(() => send({ type: 'page:key', key: 'Escape' } as PageToViewer));
    expect(ui.setMode).toHaveBeenCalledWith('t', false);
  });

  it('leaves an IME composition its own Esc', () => {
    const { ui } = setup();
    fireEvent.keyDown(document, { key: 'Escape', keyCode: 229 });
    expect(ui.setMode).not.toHaveBeenCalled();
  });
});
