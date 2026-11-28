/**
 * An empty comment box gives way to the press that starts a new target: the
 * press closes it and its own pick opens a box there, in one gesture. A box
 * with words keeps a press to one layer (it asks), and a press on the box's
 * own spot only closes it.
 */
import { act, fireEvent, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { renderWithProviders } from '@/test/utils';
import type { ArtifactCommentAnchor } from '@/types/api';
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

const RECT = { x: 10, y: 20, width: 100, height: 30 };
const FIRST: ArtifactCommentAnchor = { kind: 'text', quote: 'revenue grew', point: { x: 0.5, y: 0.5 } };
const SECOND: ArtifactCommentAnchor = { kind: 'text', quote: 'margins held', point: { x: 0.2, y: 0.5 } };
const BLOCK: ArtifactCommentAnchor = { kind: 'element', path: '#a', point: { x: 0.1, y: 0.4 } };

function setup() {
  let send: (message: PageToViewer) => void = () => {};
  const post = vi.fn();
  const ui = { modeTab: 't', listTab: null, showDoc: () => () => {}, setList: vi.fn(), setMode: vi.fn() };
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
      view={{ key: 'k', shown: true, versionView: { version: null, record: null, entry: null } } as never}
      active
      ui={ui as never}
    />,
  );
  const posted = (type: string) => post.mock.calls.map(([m]) => m).filter((m) => m.type === type);
  /** The draft's spot as last told to the page, null with no draft. */
  const draft = (): ArtifactCommentAnchor | null => {
    const items: { id: string; anchor: ArtifactCommentAnchor }[] = posted('page:anchors').at(-1)?.items ?? [];
    return items.find((item) => item.id === DRAFT_ID)?.anchor ?? null;
  };
  const press = (n: number) => act(() => send({ type: 'page:press', press: n }));
  const words = (anchor: ArtifactCommentAnchor, n: number) => act(() => send({
    type: 'page:selection',
    selection: { anchor, rect: RECT, title: 'Report', press: n, compose: true },
  } as PageToViewer));
  const pick = (anchor: ArtifactCommentAnchor, n: number) => act(() => send({
    type: 'page:pick', anchor, rect: RECT, title: 'Report', press: n,
  } as PageToViewer));
  /** Where the page says the draft is, which places its box. */
  const placed = () => act(() => {
    send({ type: 'page:positions', items: [{ id: DRAFT_ID, rect: RECT }] });
    send({ type: 'page:settled' } as PageToViewer);
  });
  const escape = () => act(() => send({ type: 'page:key', key: 'Escape' }));
  return { post, posted, draft, press, words, pick, placed, escape };
}

describe('an empty comment box gives way to a new target', () => {
  // A box left with words is kept for the file, and would open again in the next test.
  beforeEach(() => {
    localStorage.clear();
    sessionStorage.clear();
  });

  it('opens the box on words the press selects, in the same gesture', () => {
    const { posted, draft, press, words } = setup();
    press(1);
    words(FIRST, 1);
    expect(draft()).toEqual(FIRST);

    press(2);
    words(SECOND, 2);
    expect(draft()).toEqual(SECOND);
    expect(posted('page:declined')).toEqual([]);
    // The press dropped the first words' selection as it began its own: a
    // clear now would take the words just selected.
    expect(posted('page:clear-selection')).toEqual([]);
  });

  it('opens the box on an element the press clicks', () => {
    const { draft, press, words, pick } = setup();
    press(1);
    words(FIRST, 1);
    press(2);
    pick(BLOCK, 2);
    expect(draft()).toEqual(BLOCK);
  });

  it('only closes on a press that picks the box\'s own spot, wherever on it', () => {
    const { posted, draft, press, pick } = setup();
    press(1);
    pick(BLOCK, 1);
    press(2);
    pick({ ...BLOCK, point: { x: 0.9, y: 0.6 } }, 2);
    expect(draft()).toBeNull();
    expect(posted('page:declined')).toEqual([]);
  });

  it('lets the same words go, their selection with them', () => {
    const { posted, draft, press, words } = setup();
    press(1);
    words(FIRST, 1);
    press(2);
    words(FIRST, 2);
    expect(draft()).toBeNull();
    expect(posted('page:clear-selection')).toHaveLength(1);
  });

  it('keeps a box with words: the press asks, and its pick opens nothing', async () => {
    const { posted, draft, press, words, placed } = setup();
    press(1);
    words(FIRST, 1);
    placed();
    fireEvent.change(await screen.findByPlaceholderText('Add a comment'), { target: { value: 'why' } });

    press(2);
    words(SECOND, 2);
    expect(draft()).toEqual(FIRST);
    expect(posted('page:declined')).toEqual([{ type: 'page:declined', press: 2 }]);
    expect(posted('page:clear-selection')).toHaveLength(1);
  });

  it('still clears the words of a box closed another way', () => {
    const { posted, draft, press, words, escape } = setup();
    press(1);
    words(FIRST, 1);
    escape();
    expect(draft()).toBeNull();
    expect(posted('page:clear-selection')).toHaveLength(1);
  });
});
