import { afterEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import type { LibraryItem, PublishedLookup } from '@/types/orgLibrary';
import { LibraryPublishRow } from '../LibraryPublishRow';

const state: { enabled: boolean; lookup: PublishedLookup | undefined } = { enabled: true, lookup: undefined };
const publish = vi.fn();
const remove = vi.fn();
const asked: { enabled?: boolean } = {};

vi.mock('@/hooks/useOrgLibrary', () => ({
  useLibraryEnabled: () => state.enabled,
  usePublished: (_target: unknown, enabled: boolean) => {
    asked.enabled = enabled;
    return { data: enabled ? state.lookup : undefined };
  },
  usePublishToLibrary: () => ({ mutate: publish, isPending: false }),
  useRemoveFromLibrary: () => ({ mutate: remove, isPending: false }),
}));
// The account is read for real, from a member of Northfield whose
// organization set the rule on publishing.
vi.mock('@/hooks/useUser', () => ({
  useUser: () => ({
    user: { user_id: 'u-1', organization: { id: 'org-1', name: 'Northfield' }, policy: { set_by: { 'org_library.publish': 'organization' } } },
  }),
}));

const ITEM: LibraryItem = {
  item_id: 'i1',
  kind: 'thread',
  title: 'Q3 notes',
  summary: '',
  publisher_name: 'Mia',
  mine: true,
  url: '/s/tok',
  name: null,
  audience: 'organization',
  published_at: '2026-11-01T00:00:00Z',
  updated_at: '2026-11-01T00:00:00Z',
  skill_name: null,
  installed: false,
  update_available: false,
};

function renderRow(linkOn = true) {
  return render(
    <MemoryRouter>
      <LibraryPublishRow target={{ kind: 'thread', threadId: 't1' }} linkOn={linkOn} hint="Members open it through its link." />
    </MemoryRouter>,
  );
}

afterEach(() => {
  state.enabled = true;
  state.lookup = undefined;
  publish.mockReset();
  remove.mockReset();
});

describe('LibraryPublishRow', () => {
  it('renders nothing and asks nothing while the library is off for the account', () => {
    state.enabled = false;
    state.lookup = { item: null, can_publish: true };
    const { container } = renderRow();
    expect(container).toBeEmptyDOMElement();
    expect(asked.enabled).toBe(false);
  });

  it('renders nothing while the link is off, since the library opens an item through it', () => {
    state.lookup = { item: null, can_publish: true };
    const { container } = renderRow(false);
    expect(container).toBeEmptyDOMElement();
  });

  it('offers to publish a thread that is not in the library', () => {
    state.lookup = { item: null, can_publish: true };
    renderRow();
    expect(screen.getByText('Members open it through its link.')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: /publish/i }));
    expect(publish).toHaveBeenCalledWith({ target: { kind: 'thread', threadId: 't1' } }, expect.anything());
  });

  it('names who does not allow publishing instead of offering it', () => {
    state.lookup = { item: null, can_publish: false };
    renderRow();
    expect(screen.queryByRole('button', { name: /publish/i })).not.toBeInTheDocument();
    expect(screen.getByText(/Northfield/)).toBeInTheDocument();
  });

  it('shows a published thread in the organization library and takes it out', () => {
    state.lookup = { item: ITEM, can_publish: true };
    renderRow();
    expect(screen.getByText(/Northfield/)).toBeInTheDocument();
    expect(screen.getByRole('link')).toHaveAttribute('href', '/library');
    fireEvent.click(screen.getByRole('button'));
    expect(remove).toHaveBeenCalledWith('i1', expect.anything());
  });

  it("says a shared workspace's managers are why, naming who can", () => {
    state.lookup = { item: null, can_publish: false, workspace_rule: 'managers' };
    const { unmount } = renderRow();
    expect(screen.getByText('Only a manager of this workspace can publish it to the library.')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Publish to library' })).not.toBeInTheDocument();
    unmount();
    state.lookup = { item: null, can_publish: false, workspace_rule: 'none' };
    renderRow();
    expect(screen.getByText(/Nothing in this workspace can be published to the library/)).toBeInTheDocument();
  });
});
