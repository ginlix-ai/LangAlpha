/**
 * The share dialog is the review step: the list it shows is exactly what a
 * share PATCH sends, and the server's 409 is the one answer that never reads
 * as success. Network calls are mocked at the share-links api leaf; the error
 * shape helpers (`shareConflictIn`, `shareCapIn`, `shareLinkHref`) stay
 * real so the tests exercise the same reading of an API client error the dialog
 * ships with.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { renderWithProviders } from '@/test/utils';
import type { FileShareLink, ShareFileEntry, ShareLinkFiles } from '@/types/api';

vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));

vi.mock('@/pages/ChatAgent/utils/api/shareLinks', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/pages/ChatAgent/utils/api/shareLinks')>();
  return {
    ...actual,
    createShareLink: vi.fn(),
    getShareLinkFiles: vi.fn(),
    patchShareLink: vi.fn(),
    listSharedLinks: vi.fn(async () => ({ links: [] })),
  };
});

import { toast } from '@/components/ui/use-toast';
import { createShareLink, getShareLinkFiles, patchShareLink } from '@/pages/ChatAgent/utils/api/shareLinks';
import ShareLinkDialog from '../ShareLinkDialog';

const mockCreate = createShareLink as Mock;
const mockFiles = getShareLinkFiles as Mock;
const mockPatch = patchShareLink as Mock;

const WS = 'ws-1';
const CODE = 'k7f2m9q1x4z8';
const ENTRY = 'results/report.html';
const HREF = `${window.location.origin}/a/${CODE}`;

function link(overrides: Partial<FileShareLink> = {}): FileShareLink {
  return {
    code: CODE,
    workspace_id: WS,
    url: `/a/${CODE}`,
    kind: 'file',
    path: ENTRY,
    port: null,
    title: null,
    shared: false,
    shared_at: null,
    shared_files: null,
    created_at: '2026-09-24T00:00:00Z',
    ...overrides,
  };
}

const ENTRY_ROW: ShareFileEntry = { path: ENTRY, size: 40_960, reason: 'entry' };
const STYLE_ROW: ShareFileEntry = { path: 'results/style.css', size: 512, reason: 'style' };
const CHART_ROW: ShareFileEntry = { path: 'results/charts/rev.png', size: 2_048, reason: 'page' };

function files(rows: ShareFileEntry[], drift: ShareLinkFiles['drift'] = null): ShareLinkFiles {
  return { files: rows, total_size: rows.reduce((s, r) => s + r.size, 0), drift };
}

function apiError(status: number, detail: unknown) {
  return Object.assign(new Error(`Request failed with status code ${status}`), {
    response: { status, data: { detail } },
  });
}

function renderDialog() {
  return renderWithProviders(
    <ShareLinkDialog open workspaceId={WS} filePath={ENTRY} onClose={() => {}} />,
  );
}

const theSwitch = () => screen.getByRole('switch', { name: 'Anyone with the link' });

describe('ShareLinkDialog', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('private link', () => {
    beforeEach(() => {
      mockCreate.mockResolvedValue(link());
      mockFiles.mockResolvedValue(files([ENTRY_ROW, STYLE_ROW]));
    });

    it('shows the /a/<code> link, an off switch, and no visitor view', async () => {
      renderDialog();

      expect(await screen.findByText(HREF)).toBeInTheDocument();
      expect(screen.getByText('Share report.html')).toBeInTheDocument();
      expect(mockCreate).toHaveBeenCalledWith(WS, { kind: 'file', path: ENTRY });

      await waitFor(() => expect(theSwitch()).toHaveAttribute('aria-checked', 'false'));
      expect(screen.queryByText('View as visitor')).not.toBeInTheDocument();
    });

    it('lists what visitors could open, with each row\'s reason', async () => {
      renderDialog();

      expect(await screen.findByText('This link covers 2 files')).toBeInTheDocument();
      const rows = screen.getAllByRole('listitem');
      expect(rows).toHaveLength(2);
      expect(within(rows[0]).getByText('report.html')).toBeInTheDocument();
      expect(within(rows[0]).getByText('this file')).toBeInTheDocument();
      expect(within(rows[1]).getByText('style.css')).toBeInTheDocument();
      expect(within(rows[1]).getByText('in CSS')).toBeInTheDocument();
      expect(screen.queryByText('not shared yet')).not.toBeInTheDocument();
    });

    it('uses the singular for one file', async () => {
      mockFiles.mockResolvedValue(files([ENTRY_ROW]));
      renderDialog();
      expect(await screen.findByText('This link covers 1 file')).toBeInTheDocument();
    });

    it('copies the link to the clipboard', async () => {
      // user-event installs its own clipboard on setup; spy on that one.
      const user = userEvent.setup();
      const writeText = vi.spyOn(navigator.clipboard, 'writeText');
      renderDialog();

      await screen.findByText(HREF);
      await user.click(screen.getByRole('button', { name: 'Copy' }));

      expect(writeText).toHaveBeenCalledWith(HREF);
      expect(await screen.findByText('Copied')).toBeInTheDocument();
    });

    it('turning sharing on sends exactly the reviewed list', async () => {
      mockPatch.mockResolvedValue(link({ shared: true, shared_files: [ENTRY, STYLE_ROW.path] }));
      const user = userEvent.setup();
      renderDialog();

      await screen.findByText('This link covers 2 files');
      await waitFor(() => expect(theSwitch()).toBeEnabled());
      await user.click(theSwitch());

      expect(mockPatch).toHaveBeenCalledTimes(1);
      expect(mockPatch).toHaveBeenCalledWith(WS, CODE, { shared: true, files: [ENTRY, STYLE_ROW.path] });
      await waitFor(() => expect(theSwitch()).toHaveAttribute('aria-checked', 'true'));
      expect(screen.getByText('View as visitor')).toBeInTheDocument();
    });
  });

  describe('shared link', () => {
    beforeEach(() => {
      mockCreate.mockResolvedValue(link({ shared: true, shared_at: '2026-09-24T01:00:00Z', shared_files: [ENTRY, STYLE_ROW.path] }));
      mockFiles.mockResolvedValue(files([ENTRY_ROW, STYLE_ROW], { added: [], removed: [] }));
    });

    it('shows the switch on and the visitor view link', async () => {
      renderDialog();

      await waitFor(() => expect(theSwitch()).toHaveAttribute('aria-checked', 'true'));
      const visitor = screen.getByRole('link', { name: 'View as visitor' });
      expect(visitor).toHaveAttribute('href', `${HREF}?as=visitor`);
      expect(visitor).toHaveAttribute('target', '_blank');
      // No drift: the plain heading, no banner.
      expect(await screen.findByText('This link covers 2 files')).toBeInTheDocument();
      expect(screen.queryByText('Update sharing')).not.toBeInTheDocument();
    });

    it('turning sharing off sends {shared: false}', async () => {
      mockPatch.mockResolvedValue(link({ shared: false }));
      const user = userEvent.setup();
      renderDialog();

      await waitFor(() => expect(theSwitch()).toHaveAttribute('aria-checked', 'true'));
      await user.click(theSwitch());

      expect(mockPatch).toHaveBeenCalledWith(WS, CODE, { shared: false });
      await waitFor(() => expect(theSwitch()).toHaveAttribute('aria-checked', 'false'));
      expect(screen.queryByText('View as visitor')).not.toBeInTheDocument();
    });
  });

  describe('drift', () => {
    it('with added files: banner, "not shared yet" rows, after-update heading, and an update PATCH', async () => {
      mockCreate.mockResolvedValue(link({ shared: true, shared_files: [ENTRY, STYLE_ROW.path] }));
      mockFiles.mockResolvedValue(files([ENTRY_ROW, STYLE_ROW, CHART_ROW], { added: [CHART_ROW.path], removed: [] }));
      mockPatch.mockResolvedValue(link({ shared: true, shared_files: [ENTRY, STYLE_ROW.path, CHART_ROW.path] }));
      const user = userEvent.setup();
      renderDialog();

      expect(await screen.findByText(
        "The file now uses files that aren't shared. Visitors won't see them until you update sharing.",
      )).toBeInTheDocument();
      expect(screen.getByText('After the update, visitors can open 3 files')).toBeInTheDocument();

      const rows = screen.getAllByRole('listitem');
      expect(within(rows[2]).getByText('rev.png')).toBeInTheDocument();
      expect(within(rows[2]).getByText('not shared yet')).toBeInTheDocument();
      // Only the added row carries the notice; the confirmed rows keep their reason.
      expect(screen.getAllByText('not shared yet')).toHaveLength(1);
      expect(within(rows[0]).getByText('this file')).toBeInTheDocument();

      await user.click(screen.getByRole('button', { name: 'Update sharing' }));
      expect(mockPatch).toHaveBeenCalledWith(WS, CODE, {
        shared: true,
        files: [ENTRY, STYLE_ROW.path, CHART_ROW.path],
      });
    });

    it('with only removed files: the removed-only copy', async () => {
      mockCreate.mockResolvedValue(link({ shared: true, shared_files: [ENTRY, STYLE_ROW.path, CHART_ROW.path] }));
      mockFiles.mockResolvedValue(files([ENTRY_ROW, STYLE_ROW], { added: [], removed: [CHART_ROW.path] }));
      renderDialog();

      expect(await screen.findByText(
        'The file no longer uses some shared files. Update sharing to stop sharing them.',
      )).toBeInTheDocument();
      expect(screen.getByRole('button', { name: 'Update sharing' })).toBeInTheDocument();
      expect(screen.queryByText('not shared yet')).not.toBeInTheDocument();
    });

    it('is never shown for a private link, whatever the drift says', async () => {
      mockCreate.mockResolvedValue(link({ shared: false }));
      mockFiles.mockResolvedValue(files([ENTRY_ROW, CHART_ROW], { added: [CHART_ROW.path], removed: [] }));
      renderDialog();

      expect(await screen.findByText('This link covers 2 files')).toBeInTheDocument();
      expect(screen.queryByText('Update sharing')).not.toBeInTheDocument();
      expect(screen.queryByText('not shared yet')).not.toBeInTheDocument();
    });
  });

  describe('errors', () => {
    it('a 409 files_changed re-reads the list and stays private', async () => {
      mockCreate.mockResolvedValue(link());
      const serverList = [ENTRY_ROW, STYLE_ROW, CHART_ROW];
      mockFiles
        .mockResolvedValueOnce(files([ENTRY_ROW, STYLE_ROW]))
        .mockResolvedValue(files(serverList));
      mockPatch
        .mockRejectedValueOnce(apiError(409, { code: 'files_changed', files: serverList }))
        .mockResolvedValueOnce(link({ shared: true, shared_files: serverList.map((f) => f.path) }));
      const user = userEvent.setup();
      renderDialog();

      await screen.findByText('This link covers 2 files');
      await waitFor(() => expect(theSwitch()).toBeEnabled());
      await user.click(theSwitch());

      expect(await screen.findByRole('status')).toHaveTextContent(
        'The file changed while this was open. Review the list and turn sharing on again.',
      );
      // The list is the server's now, and nothing pretended the share went through.
      expect(await screen.findByText('This link covers 3 files')).toBeInTheDocument();
      expect(mockFiles).toHaveBeenCalledTimes(2);
      expect(screen.getByText('rev.png')).toBeInTheDocument();
      expect(theSwitch()).toHaveAttribute('aria-checked', 'false');
      expect(toast).not.toHaveBeenCalled();

      // The second attempt confirms the list the owner was just shown.
      await waitFor(() => expect(theSwitch()).toBeEnabled());
      await user.click(theSwitch());
      expect(mockPatch).toHaveBeenLastCalledWith(WS, CODE, {
        shared: true,
        files: serverList.map((f) => f.path),
      });
      await waitFor(() => expect(theSwitch()).toHaveAttribute('aria-checked', 'true'));
      expect(screen.queryByRole('status')).not.toBeInTheDocument();
    });

    it('a 422 too_many_files names the cap and disables the switch', async () => {
      mockCreate.mockResolvedValue(link());
      mockFiles.mockRejectedValue(apiError(422, { code: 'too_many_files', limit: 200 }));
      renderDialog();

      expect(await screen.findByText(
        'This file references more than 200 files, which is more than a link can carry.',
      )).toHaveAttribute('role', 'status');
      expect(theSwitch()).toBeDisabled();
      expect(screen.queryByRole('list')).not.toBeInTheDocument();
      expect(mockPatch).not.toHaveBeenCalled();
    });

    it('a 422 too_many_bytes names the size cap, not a listing failure', async () => {
      mockCreate.mockResolvedValue(link());
      mockFiles.mockRejectedValue(apiError(422, { code: 'too_many_bytes', limit: 256 * 1024 * 1024 }));
      renderDialog();

      expect(await screen.findByText(
        'The files this one references add up to more than 256 MB, which is more than a link can carry.',
      )).toHaveAttribute('role', 'status');
      expect(screen.queryByText("Couldn't list the files this one references.")).not.toBeInTheDocument();
      expect(theSwitch()).toBeDisabled();
      expect(mockPatch).not.toHaveBeenCalled();
    });

    it('a 409 files_changed whose re-read list is over a cap shows the cap, not the old list', async () => {
      mockCreate.mockResolvedValue(link());
      mockFiles
        .mockResolvedValueOnce(files([ENTRY_ROW, STYLE_ROW]))
        .mockRejectedValue(apiError(422, { code: 'too_many_files', limit: 200 }));
      mockPatch.mockRejectedValueOnce(apiError(409, { code: 'files_changed', files: [] }));
      const user = userEvent.setup();
      renderDialog();

      await screen.findByText('This link covers 2 files');
      await waitFor(() => expect(theSwitch()).toBeEnabled());
      await user.click(theSwitch());

      expect(await screen.findByText(
        'This file references more than 200 files, which is more than a link can carry.',
      )).toBeInTheDocument();
      expect(screen.queryByText('This link covers 2 files')).not.toBeInTheDocument();
      expect(theSwitch()).toBeDisabled();
    });

    it('a 409 link_changed toasts and re-reads the link', async () => {
      mockCreate
        .mockResolvedValueOnce(link())
        .mockResolvedValue(link({ shared: true, shared_files: [ENTRY] }));
      mockFiles.mockResolvedValue(files([ENTRY_ROW]));
      mockPatch.mockRejectedValueOnce(apiError(409, { code: 'link_changed' }));
      const user = userEvent.setup();
      renderDialog();

      await screen.findByText('This link covers 1 file');
      await waitFor(() => expect(theSwitch()).toBeEnabled());
      await user.click(theSwitch());

      await waitFor(() => expect(toast).toHaveBeenCalledWith({
        description: 'This link was changed somewhere else. It now shows its current settings.',
      }));
      await waitFor(() => expect(theSwitch()).toHaveAttribute('aria-checked', 'true'));
      expect(mockCreate).toHaveBeenCalledTimes(2);
    });

    it('a link that fails to load offers a retry instead of a spinner', async () => {
      mockCreate
        .mockRejectedValueOnce(apiError(400, 'Home is not on a computer yet'))
        .mockResolvedValue(link());
      mockFiles.mockResolvedValue(files([ENTRY_ROW]));
      const user = userEvent.setup();
      renderDialog();

      expect(await screen.findByText("Couldn't create the link. Please try again.")).toBeInTheDocument();
      expect(screen.queryByText('Checking what the file references…')).not.toBeInTheDocument();
      expect(theSwitch()).toBeDisabled();

      await user.click(screen.getByRole('button', { name: 'Retry' }));
      expect(await screen.findByText(HREF)).toBeInTheDocument();
      expect(await screen.findByText('This link covers 1 file')).toBeInTheDocument();
    });

    it('any other share failure toasts and leaves the link private', async () => {
      mockCreate.mockResolvedValue(link());
      mockFiles.mockResolvedValue(files([ENTRY_ROW]));
      mockPatch.mockRejectedValue(apiError(500, 'boom'));
      const user = userEvent.setup();
      vi.spyOn(console, 'error').mockImplementation(() => {});
      renderDialog();

      await screen.findByText('This link covers 1 file');
      await waitFor(() => expect(theSwitch()).toBeEnabled());
      await user.click(theSwitch());

      await waitFor(() => expect(toast).toHaveBeenCalledWith(expect.objectContaining({
        description: "Couldn't change sharing. Please try again.",
        variant: 'destructive',
      })));
      expect(theSwitch()).toHaveAttribute('aria-checked', 'false');
    });
  });
});
