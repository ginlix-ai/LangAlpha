import { describe, it, expect, vi, beforeAll } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import i18n from '@/i18n';
import { PanelNotices } from '../PanelNotices';
import type { BackupResult } from '../types';

function renderNotice(backupResult: BackupResult, onDismiss = vi.fn()) {
  render(
    <PanelNotices
      uploadProgress={null}
      error={null}
      onDismissError={vi.fn()}
      busy={false}
      backupResult={backupResult}
      onDismissBackupResult={onDismiss}
      editing={false}
    />,
  );
  return onDismiss;
}

describe('PanelNotices: a backup that skipped files', () => {
  beforeAll(async () => {
    await i18n.changeLanguage('en-US');
  });

  it('names each skipped file with what to do about it', () => {
    renderNotice({
      synced: 259,
      skipped: 80,
      unsaved: [
        { path: 'e2e/growing.log', reason: 'changed', size: 255970560 },
        { path: 'data/ticks.parquet', reason: 'too_large', size: 6_549_825_126 },
      ],
      unsaved_count: 2,
    });
    expect(screen.getByText(/Backed up 259 files/)).toBeInTheDocument();
    expect(screen.getByText('2 not saved')).toBeInTheDocument();
    expect(screen.getByText('e2e/growing.log')).toBeInTheDocument();
    expect(screen.getByText('Changed while saving. The next backup retries.')).toBeInTheDocument();
    expect(screen.getByText('Too large to back up here (6.1 GB). Move or split it.')).toBeInTheDocument();
  });

  it('summarises past the named files, counting from the full total', () => {
    const unsaved = Array.from({ length: 100 }, (_, i) => ({
      path: `logs/run-${i}.log`,
      reason: 'failed' as const,
    }));
    renderNotice({ synced: 3, unsaved, unsaved_count: 240 });
    expect(screen.getByText('240 not saved')).toBeInTheDocument();
    expect(screen.getAllByText("Didn't upload. The next backup retries.")).toHaveLength(5);
    expect(screen.getByText('and 235 more files')).toBeInTheDocument();
  });

  it('keeps the plain confirmation when nothing was skipped', () => {
    renderNotice({ synced: 4, skipped: 10, unsaved: [], unsaved_count: 0 });
    expect(screen.getByText('Backed up 4 files, 10 unchanged')).toBeInTheDocument();
    expect(screen.queryByText(/not saved/)).not.toBeInTheDocument();
  });

  it('dismisses on the close button', () => {
    const onDismiss = renderNotice({
      synced: 1,
      unsaved: [{ path: 'a.bin', reason: 'unreadable' }],
      unsaved_count: 1,
    });
    fireEvent.click(screen.getByRole('button'));
    expect(onDismiss).toHaveBeenCalledOnce();
  });
});
