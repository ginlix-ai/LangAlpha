/**
 * The Publish dialog is the review step: the list it shows is exactly what a
 * publish sends, and the server's 409 is the one answer that never reads as
 * success. Network calls are mocked at the artifacts api leaf; the hooks and
 * the error shape helpers (`filesChangedIn`, `shareCapIn`) stay real so the
 * tests read an API client error the way the dialog ships. The record comes
 * from the get-or-create (`ensureArtifact`), and a write's refresh reads it
 * back by path (`findArtifact`), so `serve` answers both.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { renderWithProviders } from '@/test/utils';
import type { Artifact, ArtifactStatus, ArtifactVersion, ShareFileEntry } from '@/types/api';

vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));

vi.mock('@/pages/ChatAgent/utils/api/artifacts', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/pages/ChatAgent/utils/api/artifacts')>();
  return {
    ...actual,
    findArtifact: vi.fn(),
    ensureArtifact: vi.fn(),
    getArtifactStatus: vi.fn(),
    listArtifactVersions: vi.fn(async () => []),
    publishArtifact: vi.fn(),
    unshareArtifact: vi.fn(),
    setArtifactKeepUpdated: vi.fn(),
  };
});

import { toast } from '@/components/ui/use-toast';
import {
  ensureArtifact,
  findArtifact,
  getArtifactStatus,
  listArtifactVersions,
  publishArtifact,
  unshareArtifact,
} from '@/pages/ChatAgent/utils/api/artifacts';
import PublishDialog from '../PublishDialog';

const mockFind = findArtifact as Mock;
const mockEnsure = ensureArtifact as Mock;
const mockStatus = getArtifactStatus as Mock;
const mockVersions = listArtifactVersions as Mock;
const mockPublish = publishArtifact as Mock;
const mockUnshare = unshareArtifact as Mock;

const WS = 'ws-1';
const ID = '6f1c2a7e-3b4d-4e5f-8a9b-0c1d2e3f4a5b';
const CODE = 'k7f2m9q1x4z8';
const ENTRY = 'results/report.html';
const HREF = `${window.location.origin}/a/${CODE}`;

function artifact(overrides: Partial<Artifact> = {}): Artifact {
  return {
    artifact_id: ID,
    code: CODE,
    url: `/a/${CODE}`,
    workspace_id: WS,
    path: ENTRY,
    name: 'report.html',
    shared: false,
    shared_at: null,
    keep_updated: false,
    latest_seq: 0,
    bound_thread_id: null,
    open_comments: 0,
    unread: 0,
    created_at: '2026-10-31T00:00:00Z',
    ...overrides,
  };
}

function version(seq: number, overrides: Partial<ArtifactVersion> = {}): ArtifactVersion {
  return {
    seq,
    entry_path: ENTRY,
    origin: 'owner',
    title: 'Q3 report',
    label: null,
    byte_len: 43_520,
    published_at: '2026-10-31T12:00:00Z',
    ...overrides,
  };
}

const ENTRY_ROW: ShareFileEntry = { path: ENTRY, size: 40_960, reason: 'entry' };
const STYLE_ROW: ShareFileEntry = { path: 'results/style.css', size: 512, reason: 'style' };
const CHART_ROW: ShareFileEntry = { path: 'results/charts/rev.png', size: 2_048, reason: 'page' };

function status(rows: ShareFileEntry[], overrides: Partial<ArtifactStatus> = {}): ArtifactStatus {
  return {
    latest_seq: 0,
    live_sha256: 'a'.repeat(64),
    files: rows,
    total_size: rows.reduce((s, r) => s + r.size, 0),
    changed: null,
    drift: null,
    ...overrides,
  };
}

const NO_DRIFT = { added: [], removed: [], changed: [] };

/** The record the server answers from now on, whether made or read. */
function serve(record: Artifact) {
  mockEnsure.mockResolvedValue(record);
  mockFind.mockResolvedValue(record);
}

/**
 * A write's answer, and the record the server reads back from then on: the
 * write invalidates the dialog's read by path, which reads it again.
 */
function answers(next: Artifact, rest: { version?: ArtifactVersion; cut?: boolean } = {}) {
  return async () => {
    serve(next);
    return { artifact: next, version: rest.version ?? version(next.latest_seq), cut: rest.cut ?? true };
  };
}

function apiError(status: number, detail: unknown) {
  return Object.assign(new Error(`Request failed with status code ${status}`), {
    response: { status, data: { detail } },
  });
}

function renderDialog(path = ENTRY) {
  return renderWithProviders(<PublishDialog open workspaceId={WS} path={path} onClose={() => {}} />);
}

/** The review list, named by its heading. */
const filesList = (count: number) => screen.getByRole('list', { name: new RegExp(`shares ${count} files?$`) });
const publishButton = () => screen.getByRole('button', { name: 'Publish' });
const updateButton = () => screen.getByRole('button', { name: 'Update' });

describe('PublishDialog', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockVersions.mockResolvedValue([]);
  });

  describe('a private file', () => {
    beforeEach(() => {
      serve(artifact());
      mockStatus.mockResolvedValue(status([ENTRY_ROW, STYLE_ROW]));
    });

    it('shows the link, Private, and the files a publish would carry with each reason', async () => {
      renderDialog();

      expect(await screen.findByText(HREF)).toBeInTheDocument();
      expect(screen.getByText('Publish report.html')).toBeInTheDocument();
      expect(screen.getByText('Private')).toBeInTheDocument();
      // One get-or-create, and no read by path beside it.
      expect(mockEnsure).toHaveBeenCalledTimes(1);
      expect(mockEnsure).toHaveBeenCalledWith(WS, ENTRY);
      expect(mockFind).not.toHaveBeenCalled();

      expect(await screen.findByText('Publishing shares 2 files')).toBeInTheDocument();
      const rows = within(filesList(2)).getAllByRole('listitem');
      expect(rows).toHaveLength(2);
      expect(within(rows[0]).getByText('report.html')).toBeInTheDocument();
      expect(within(rows[0]).getByText('this file')).toBeInTheDocument();
      expect(within(rows[1]).getByText('in CSS')).toBeInTheDocument();
      expect(screen.queryByRole('button', { name: 'Unshare' })).not.toBeInTheDocument();
      expect(screen.queryByText('View as visitor')).not.toBeInTheDocument();
    });

    it('Publish sends exactly the reviewed list, the trimmed note and keep updated', async () => {
      mockPublish.mockImplementation(answers(
        artifact({ shared: true, latest_seq: 1, keep_updated: true }),
        { version: version(1, { label: 'First cut' }) },
      ));
      const user = userEvent.setup();
      renderDialog();

      await screen.findByText('Publishing shares 2 files');
      await waitFor(() => expect(publishButton()).toBeEnabled());
      await user.type(screen.getByRole('textbox'), '  First cut  ');
      await user.click(screen.getByRole('switch', { name: 'Keep updated' }));
      // Nothing is sent until Publish: the switch is part of the publish.
      expect(mockPublish).not.toHaveBeenCalled();
      await user.click(publishButton());

      expect(mockPublish).toHaveBeenCalledTimes(1);
      expect(mockPublish).toHaveBeenCalledWith(ID, {
        files: [ENTRY, STYLE_ROW.path],
        label: 'First cut',
        keep_updated: true,
      });
      expect(await screen.findByText('Published v1')).toBeInTheDocument();
      expect(updateButton()).toBeInTheDocument();
      expect(screen.getByRole('link', { name: 'View as visitor' })).toHaveAttribute('href', `${HREF}?as=visitor`);
    });

    it('an empty note sends null, and keep updated starts off', async () => {
      mockPublish.mockImplementation(answers(artifact({ shared: true, latest_seq: 1 })));
      const user = userEvent.setup();
      renderDialog();

      await waitFor(() => expect(publishButton()).toBeEnabled());
      expect(screen.getByRole('switch', { name: 'Keep updated' })).toHaveAttribute('aria-checked', 'false');
      await user.type(screen.getByRole('textbox'), '   ');
      await user.click(publishButton());

      expect(mockPublish).toHaveBeenCalledWith(ID, { files: [ENTRY, STYLE_ROW.path], label: null, keep_updated: false });
    });
  });

  it('starts every open afresh: a note left behind is gone', async () => {
    serve(artifact());
    mockStatus.mockResolvedValue(status([ENTRY_ROW]));
    const user = userEvent.setup();
    const dialog = (open: boolean) => <PublishDialog open={open} workspaceId={WS} path={ENTRY} onClose={() => {}} />;
    const { rerender } = renderWithProviders(dialog(true));

    await user.type(await screen.findByRole('textbox'), 'draft');
    rerender(dialog(false));
    await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument());
    rerender(dialog(true));

    expect(await screen.findByRole('textbox')).toHaveValue('');
  });

  it('makes the record of a file never recorded, then lists it', async () => {
    mockFind.mockResolvedValue(null);
    mockEnsure.mockResolvedValue(artifact());
    mockStatus.mockResolvedValue(status([ENTRY_ROW]));
    renderDialog('/home/workspace/results/report.html');

    expect(await screen.findByText('Publishing shares 1 file')).toBeInTheDocument();
    expect(mockEnsure).toHaveBeenCalledTimes(1);
    expect(mockEnsure).toHaveBeenCalledWith(WS, ENTRY);
    expect(screen.getByText(HREF)).toBeInTheDocument();
  });

  it('a 409 files_changed shows the new list and waits for a second confirm', async () => {
    serve(artifact());
    const serverList = [ENTRY_ROW, STYLE_ROW, CHART_ROW];
    mockStatus
      .mockResolvedValueOnce(status([ENTRY_ROW, STYLE_ROW]))
      .mockResolvedValue(status(serverList));
    mockPublish
      .mockRejectedValueOnce(apiError(409, { code: 'files_changed', files: serverList }))
      .mockImplementationOnce(answers(artifact({ shared: true, latest_seq: 1 })));
    const user = userEvent.setup();
    renderDialog();

    await screen.findByText('Publishing shares 2 files');
    await waitFor(() => expect(publishButton()).toBeEnabled());
    await user.click(publishButton());

    expect(await screen.findByRole('status')).toHaveTextContent(
      'The file changed while this was open. Review the list and publish again.',
    );
    // The list is the server's now, and nothing pretended the publish went through.
    expect(await screen.findByText('Publishing shares 3 files')).toBeInTheDocument();
    expect(screen.getByText('rev.png')).toBeInTheDocument();
    expect(screen.getByText('Private')).toBeInTheDocument();
    expect(mockPublish).toHaveBeenCalledTimes(1);
    expect(toast).not.toHaveBeenCalled();

    // The second attempt confirms the list the owner was just shown.
    await waitFor(() => expect(publishButton()).toBeEnabled());
    await user.click(publishButton());
    expect(mockPublish).toHaveBeenCalledTimes(2);
    expect(mockPublish).toHaveBeenLastCalledWith(ID, {
      files: serverList.map((f) => f.path),
      label: null,
      keep_updated: false,
    });
    expect(await screen.findByText('Published v1')).toBeInTheDocument();
    expect(screen.queryByRole('status')).not.toBeInTheDocument();
  });

  describe('a published file', () => {
    it('Unshare stops the link and keeps the versions', async () => {
      serve(artifact({ shared: true, shared_at: '2026-10-31T12:00:00Z', latest_seq: 2 }));
      mockStatus.mockResolvedValue(status([ENTRY_ROW, STYLE_ROW], { latest_seq: 2, changed: false, drift: NO_DRIFT }));
      mockVersions.mockResolvedValue([version(2, { label: 'Q3 numbers' }), version(1)]);
      mockUnshare.mockImplementation(async () => (await answers(artifact({ latest_seq: 2 }))()).artifact);
      const user = userEvent.setup();
      renderDialog();

      expect(await screen.findByText('Published v2')).toBeInTheDocument();
      await user.click(screen.getByRole('button', { name: 'Unshare' }));

      expect(mockUnshare).toHaveBeenCalledWith(ID);
      expect(await screen.findByText('Private')).toBeInTheDocument();
      expect(screen.getByText('Not shared. Its versions are kept, and publishing again uses the same link.')).toBeInTheDocument();
      expect(screen.queryByRole('button', { name: 'Unshare' })).not.toBeInTheDocument();
      expect(screen.getByText('Q3 numbers')).toBeInTheDocument();
      expect(mockPublish).not.toHaveBeenCalled();
    });

    it('an unchanged file holds Update, since it would cut nothing', async () => {
      serve(artifact({ shared: true, latest_seq: 3 }));
      mockStatus.mockResolvedValue(status([ENTRY_ROW], { latest_seq: 3, changed: false, drift: NO_DRIFT }));
      renderDialog();

      expect(await screen.findByText('Visitors see v3. Nothing has changed since.')).toBeInTheDocument();
      expect(screen.getByText('Visitors can open 1 file')).toBeInTheDocument();
      expect(updateButton()).toBeDisabled();
    });

    it('lists the versions newest first, marking the ones keep updated cut', async () => {
      serve(artifact({ shared: true, latest_seq: 3, keep_updated: true }));
      mockStatus.mockResolvedValue(status([ENTRY_ROW], { latest_seq: 3, changed: false, drift: NO_DRIFT }));
      mockVersions.mockResolvedValue([
        version(3, { origin: 'keep_updated' }),
        version(2, { label: 'Q3 numbers' }),
        version(1),
      ]);
      renderDialog();

      const list = await screen.findByRole('list', { name: 'Versions' });
      await waitFor(() => expect(within(list).getAllByRole('listitem')).toHaveLength(3));
      const rows = within(list).getAllByRole('listitem');
      expect(within(rows[0]).getByText('v3')).toBeInTheDocument();
      expect(within(rows[0]).getByText('kept updated')).toBeInTheDocument();
      expect(within(rows[0]).getByText('Oct 31')).toBeInTheDocument();
      expect(within(rows[1]).getByText('Q3 numbers')).toBeInTheDocument();
      expect(within(list).getAllByText('kept updated')).toHaveLength(1);
      expect(screen.getByRole('switch', { name: 'Keep updated' })).toHaveAttribute('aria-checked', 'true');
    });

    it('files the entry newly reads wait for Update: a notice, the rows marked, and Update carries them', async () => {
      serve(artifact({ shared: true, latest_seq: 1 }));
      mockStatus.mockResolvedValue(status([ENTRY_ROW, STYLE_ROW, CHART_ROW], {
        latest_seq: 1,
        changed: true,
        drift: { added: [CHART_ROW.path], removed: [], changed: [] },
      }));
      mockPublish.mockImplementation(answers(artifact({ shared: true, latest_seq: 2 })));
      const user = userEvent.setup();
      renderDialog();

      expect(await screen.findByText(
        "1 new file waits for Update. Visitors won't get it until you update.",
      )).toBeInTheDocument();
      expect(screen.getByText('Changed since v1. Visitors see v1 until you update.')).toBeInTheDocument();
      expect(screen.getByText('Updating shares 3 files')).toBeInTheDocument();
      const rows = within(filesList(3)).getAllByRole('listitem');
      expect(within(rows[2]).getByText('rev.png')).toBeInTheDocument();
      expect(within(rows[2]).getByText('not shared yet')).toBeInTheDocument();
      // Only the added row carries the mark; the others keep their reason.
      expect(screen.getAllByText('not shared yet')).toHaveLength(1);
      expect(within(rows[0]).getByText('this file')).toBeInTheDocument();

      await waitFor(() => expect(updateButton()).toBeEnabled());
      await user.click(updateButton());
      // An Update leaves keep updated as the record has it.
      expect(mockPublish).toHaveBeenCalledWith(ID, {
        files: [ENTRY, STYLE_ROW.path, CHART_ROW.path],
        label: null,
      });
      expect(mockPublish.mock.calls[0][1]).not.toHaveProperty('keep_updated');
    });
  });

  describe('answers that are not a new version', () => {
    it('a publish that cut nothing says so quietly', async () => {
      // Shared before and stopped: publishing again resumes at the latest version.
      serve(artifact({ latest_seq: 2 }));
      mockStatus.mockResolvedValue(status([ENTRY_ROW], { latest_seq: 2, changed: false }));
      mockPublish.mockImplementation(answers(artifact({ shared: true, latest_seq: 2 }), { cut: false }));
      const user = userEvent.setup();
      renderDialog();

      await waitFor(() => expect(publishButton()).toBeEnabled());
      await user.click(publishButton());

      expect(await screen.findByRole('status')).toHaveTextContent('Nothing changed since v2, so no new version was made.');
      expect(screen.getByText('Published v2')).toBeInTheDocument();
      expect(toast).not.toHaveBeenCalled();
    });

    it('a 422 cap on the status names it and holds Publish', async () => {
      serve(artifact());
      mockStatus.mockRejectedValue(apiError(422, { code: 'too_many_bytes', limit: 256 * 1024 * 1024 }));
      renderDialog();

      expect(await screen.findByText(
        'The files this one references add up to more than 256 MB, which is more than a link can carry.',
      )).toHaveAttribute('role', 'status');
      expect(screen.queryByRole('list')).not.toBeInTheDocument();
      expect(publishButton()).toBeDisabled();
    });

    it('a 404 file_missing on publish says the file is gone', async () => {
      serve(artifact());
      mockStatus.mockResolvedValue(status([ENTRY_ROW]));
      mockPublish.mockRejectedValue(apiError(404, { code: 'file_missing' }));
      const user = userEvent.setup();
      renderDialog();

      await waitFor(() => expect(publishButton()).toBeEnabled());
      await user.click(publishButton());

      expect(await screen.findByText('This file is no longer in the workspace.')).toHaveAttribute('role', 'status');
      expect(toast).not.toHaveBeenCalled();
    });

    it('any other failure toasts and leaves the file private', async () => {
      serve(artifact());
      mockStatus.mockResolvedValue(status([ENTRY_ROW]));
      mockPublish.mockRejectedValue(apiError(500, 'boom'));
      vi.spyOn(console, 'error').mockImplementation(() => {});
      const user = userEvent.setup();
      renderDialog();

      await waitFor(() => expect(publishButton()).toBeEnabled());
      await user.click(publishButton());

      await waitFor(() => expect(toast).toHaveBeenCalledWith(expect.objectContaining({
        description: "Couldn't publish. Please try again.",
        variant: 'destructive',
      })));
      expect(screen.getByText('Private')).toBeInTheDocument();
    });
  });
});
