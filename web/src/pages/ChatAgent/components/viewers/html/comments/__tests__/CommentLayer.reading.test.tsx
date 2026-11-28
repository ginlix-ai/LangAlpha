/**
 * A layer read only (an earlier version's comments) takes no mode and starts
 * no comment, and draws only the comments it is given to show; any layer's
 * list closes on Esc, marked taken; and the mode hint gives way only to its
 * own layer's box, not another pane's.
 */
import { act, fireEvent } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { renderWithProviders } from '@/test/utils';
import type { ArtifactComment } from '@/types/api';
import type { PageToViewer } from '../../pageProtocol';
import { CommentSurfaceContext, type CommentSurface } from '../commentSurface';
import { DRAFT_ID } from '../pinPlaces';

const comment = (id: string, number: number) => ({
  comment_id: id,
  artifact_id: 'a1',
  number,
  kind: 'drive',
  status: 'open',
  delivery: 'note',
  thread_id: null,
  unread: 0,
  anchor: { kind: 'element', path: `#${id}` },
  messages: [],
}) as unknown as ArtifactComment;

const { comments, recordsRead } = vi.hoisted(() => ({
  comments: { data: [] as unknown[], isSuccess: true },
  recordsRead: [] as { kind: string; path: string }[],
}));
vi.mock('@/hooks/useArtifact', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  useCommentRecord: (_workspaceId: string, target: { kind: string; path: string }) => {
    recordsRead.push(target);
    return {
      artifact: { artifact_id: 'a1', open_comments: 0, latest_seq: 0, shared: false, path: 'e1' },
      error: null,
      loaded: true,
      hashable: false,
    };
  },
  useArtifactComments: () => comments,
  useArtifactStatus: () => ({ data: undefined }),
}));

import CommentLayer from '../CommentLayer';

afterEach(() => {
  vi.restoreAllMocks();
  document.querySelectorAll('.foreign').forEach((el) => el.remove());
});

function setup({ surface = {} as CommentSurface, modeTab = 't', listTab = null as string | null, pagePath = 'e1' } = {}) {
  let send: (message: PageToViewer) => void = () => {};
  const post = vi.fn();
  const ui = { modeTab, listTab, showDoc: () => () => {}, setList: vi.fn(), setMode: vi.fn(), registerWordsTaker: vi.fn(() => () => {}) };
  const bridge = {
    session: 's',
    path: pagePath,
    ready: true,
    post,
    subscribe: (listener: (message: PageToViewer) => void) => {
      send = listener;
      return () => {};
    },
  };
  const view = render();
  function render() {
    return renderWithProviders(
      <CommentSurfaceContext value={surface}>
        <CommentLayer
          tabId="t"
          workspaceId="home"
          target={{ kind: 'drive', path: 'e1' }}
          bridge={bridge as never}
          view={{ key: 'k', shown: true } as never}
          active
          ui={ui as never}
        />
      </CommentSurfaceContext>,
    );
  }
  const anchors = () => {
    const last = post.mock.calls.map(([m]) => m).filter((m) => m.type === 'page:anchors').at(-1);
    return ((last?.items ?? []) as { id: string }[]).map((item) => item.id);
  };
  const pick = () => act(() => send({
    type: 'page:pick',
    anchor: { kind: 'element', path: '#a' },
    rect: { x: 10, y: 20, width: 100, height: 30 },
    title: 'Report',
    press: null,
  } as PageToViewer));
  return { ui, send, anchors, pick, view };
}

describe('a read-only comment layer', () => {
  it('draws only the comments it shows, takes no mode and starts no comment', () => {
    comments.data = [comment('c1', 1), comment('c2', 2)];
    const { ui, send, anchors, pick } = setup({ surface: { readOnly: true, shows: (c: ArtifactComment) => c.comment_id === 'c2' } });
    expect(anchors()).toEqual(['c2']);

    // Neither its own key nor the page's turns a mode on.
    fireEvent.keyDown(document, { key: 'c' });
    act(() => send({ type: 'page:key', key: 'c' } as PageToViewer));
    expect(ui.setMode).not.toHaveBeenCalled();

    pick();
    expect(anchors()).not.toContain(DRAFT_ID);
    expect(document.querySelector('.page-comment-box')).toBeNull();
    // No hint, since there is no mode, though the tab holds it.
    expect(document.querySelector('.page-comments-hint')).toBeNull();
  });

  it('offers no selection toolbar its words', () => {
    comments.data = [];
    const { ui } = setup({ surface: { readOnly: true } });
    expect(ui.registerWordsTaker).not.toHaveBeenCalled();
  });
});

describe('a Drive file whose page names another file', () => {
  it("reads the comments of the file it was opened on, never the one its page's hello names", () => {
    comments.data = [];
    recordsRead.length = 0;
    setup({ pagePath: 'another-entry' });
    expect(recordsRead.length).toBeGreaterThan(0);
    expect(recordsRead.every((target) => target.kind === 'drive' && target.path === 'e1')).toBe(true);
  });
});

describe('Esc with the comment list open', () => {
  it('closes the list, marked taken, with the mode off', () => {
    comments.data = [];
    const { ui } = setup({ modeTab: null as unknown as string, listTab: 't' });
    const event = new KeyboardEvent('keydown', { key: 'Escape', bubbles: true, cancelable: true });
    document.body.dispatchEvent(event);
    expect(ui.setList).toHaveBeenCalledWith('t', false);
    expect(event.defaultPrevented).toBe(true);
  });

  it('leaves the mode first where both are on', () => {
    comments.data = [];
    const { ui } = setup({ listTab: 't' });
    fireEvent.keyDown(document, { key: 'Escape' });
    expect(ui.setMode).toHaveBeenCalledWith('t', false);
    expect(ui.setList).not.toHaveBeenCalled();
  });
});

describe('the mode hint', () => {
  it('gives way to its own layer\'s box, never to another pane\'s', async () => {
    comments.data = [];
    // Another pane's box sits over where this layer's hint is.
    const foreign = document.createElement('div');
    foreign.className = 'page-comment-box foreign';
    document.body.prepend(foreign);
    const far = { left: 900, right: 1000, top: 900, bottom: 1000, width: 100, height: 100, x: 900, y: 900, toJSON: () => ({}) };
    const near = { left: 0, right: 200, top: 0, bottom: 40, width: 200, height: 40, x: 0, y: 0, toJSON: () => ({}) };
    vi.spyOn(Element.prototype, 'getBoundingClientRect').mockImplementation(function (this: Element) {
      return (this.classList.contains('page-comments-hint') || this.classList.contains('foreign') ? near : far) as DOMRect;
    });
    const { pick } = setup();
    pick();
    expect(document.querySelector('.page-comment-box:not(.foreign)')).not.toBeNull();
    await act(() => new Promise<void>((done) => requestAnimationFrame(() => requestAnimationFrame(() => done()))));
    expect(document.querySelector('.page-comments-hint')).not.toHaveAttribute('data-covered');
  });
});
