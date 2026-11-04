/**
 * The crumb's Update publishes in one click, but not from a list older than
 * the page: it runs the page out of sight first, as the Publish dialog does,
 * and the publish names the code it ran, so a page the agent changed since the
 * crumb read its status is reviewed as it is now. What running it adds goes to
 * the dialog. Network calls are mocked at the artifacts api leaf.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { renderWithProviders } from '@/test/utils';
import { runAuthResets } from '@/lib/authResets';
import type { Artifact, ArtifactStatus, ArtifactVersion, ShareFileEntry } from '@/types/api';
import type { ArtifactVersionView } from '../useArtifactVersionView';

vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));
vi.mock('../../viewers/html/ArtifactVersionPicker', () => ({ ArtifactVersionPicker: () => null }));

// The page run out of sight: it answers at once unless a test holds it.
const scout = vi.hoisted(() => ({ settle: null as (() => void) | null, hold: false, runs: 0 }));
vi.mock('../../viewers/html/BridgedFrame', () => ({
  BridgedFrame: ({ onUnbridged }: { onUnbridged?: () => void }) => {
    React.useEffect(() => {
      scout.runs += 1;
      if (scout.hold) scout.settle = onUnbridged ?? null;
      else onUnbridged?.();
    }, [onUnbridged]);
    return null;
  },
}));

vi.mock('@/pages/ChatAgent/utils/api/artifacts', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/pages/ChatAgent/utils/api/artifacts')>();
  return { ...actual, getArtifactStatus: vi.fn(), publishArtifact: vi.fn() };
});

import { getArtifactStatus, publishArtifact } from '@/pages/ChatAgent/utils/api/artifacts';
import { ArtifactCrumbs } from '../ArtifactCrumbs';

const mockStatus = getArtifactStatus as Mock;
const mockPublish = publishArtifact as Mock;

const ID = '6f1c2a7e-3b4d-4e5f-8a9b-0c1d2e3f4a5b';
const ENTRY = 'results/report.html';
const ENTRY_ROW: ShareFileEntry = { path: ENTRY, size: 100, reason: 'entry', optional: false };
const DATA_ROW: ShareFileEntry = { path: 'results/data/msft.csv', size: 10, reason: 'loaded', optional: false };
const NO_DRIFT = { added: [], removed: [], changed: [] };

const record: Artifact = {
  artifact_id: ID,
  code: 'k7f2m9q1x4z8',
  url: '/a/k7f2m9q1x4z8',
  workspace_id: 'ws-1',
  path: ENTRY,
  name: 'report.html',
  shared: true,
  shared_at: '2026-10-31T00:00:00Z',
  keep_updated: false,
  latest_seq: 1,
  bound_thread_id: null,
  open_comments: 0,
  unread: 0,
  created_at: '2026-10-31T00:00:00Z',
};

const view: ArtifactVersionView = {
  record,
  versions: [],
  version: null,
  entry: null,
  missing: false,
  failed: false,
  retry: () => {},
  updatedTo: null,
  pick: () => {},
} as unknown as ArtifactVersionView;

function status(rows: ShareFileEntry[], overrides: Partial<ArtifactStatus> = {}): ArtifactStatus {
  return {
    latest_seq: 1,
    live_sha256: 'a'.repeat(64),
    page_sha256: 'c'.repeat(64),
    files: rows,
    left_out: [],
    total_size: rows.reduce((s, r) => s + (r.size ?? 0), 0),
    changed: true,
    drift: NO_DRIFT,
    ...overrides,
  };
}

const published = (seq: number): { version: ArtifactVersion; cut: boolean } => ({
  version: { seq, entry_path: ENTRY, origin: 'owner', title: null, label: null, byte_len: 100, published_at: '2026-11-03T00:00:00Z' },
  cut: true,
});

function renderCrumbs(onReview = vi.fn()) {
  renderWithProviders(<ArtifactCrumbs view={view} onReview={onReview} readAt={1} />);
  return onReview;
}

const updateButton = () => screen.findByRole('button', { name: 'Update' });

beforeEach(() => {
  vi.clearAllMocks();
  runAuthResets();
  scout.hold = false;
  scout.settle = null;
  scout.runs = 0;
});

describe('ArtifactCrumbs Update', () => {
  it('runs the page, then publishes the list it read with the code it ran', async () => {
    mockStatus.mockResolvedValue(status([ENTRY_ROW]));
    mockPublish.mockResolvedValue(published(2));
    scout.hold = true;
    renderCrumbs();

    await userEvent.click(await updateButton());
    await waitFor(() => expect(scout.settle).not.toBeNull());
    // Nothing goes out while the page runs.
    expect(mockPublish).not.toHaveBeenCalled();
    expect(screen.getByRole('button', { name: 'Update' })).toBeDisabled();

    act(() => scout.settle?.());
    await waitFor(() => expect(mockPublish).toHaveBeenCalledTimes(1));
    expect(scout.runs).toBe(1);
    expect(mockPublish.mock.calls[0][0]).toBe(ID);
    expect(mockPublish.mock.calls[0][1]).toMatchObject({ files: [ENTRY], page_sha256: 'c'.repeat(64) });
  });

  it('sends a file the page asks for now to the dialog, not out', async () => {
    mockStatus.mockResolvedValue(status([ENTRY_ROW]));
    const onReview = renderCrumbs();
    const button = await updateButton();
    // Running the page, the list gains what its code builds a path to.
    mockStatus.mockResolvedValue(status([ENTRY_ROW, DATA_ROW], { drift: { ...NO_DRIFT, added: [DATA_ROW.path] } }));

    await userEvent.click(button);

    await waitFor(() => expect(onReview).toHaveBeenCalledTimes(1));
    expect(mockPublish).not.toHaveBeenCalled();
    await waitFor(() => expect(screen.getByRole('button', { name: 'Update' })).toBeEnabled());
  });

  it('sends a list it could not read to the dialog', async () => {
    mockStatus.mockResolvedValue(status([ENTRY_ROW]));
    const onReview = renderCrumbs();
    const button = await updateButton();
    // A refusal, asked once: the dialog says why.
    mockStatus.mockRejectedValue(Object.assign(new Error('gone'), { response: { status: 404, data: {} } }));

    await userEvent.click(button);

    await waitFor(() => expect(onReview).toHaveBeenCalledTimes(1));
    expect(mockPublish).not.toHaveBeenCalled();
  });
});
