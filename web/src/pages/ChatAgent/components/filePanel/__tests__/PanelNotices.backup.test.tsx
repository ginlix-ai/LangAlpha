import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { PanelNotices } from '../PanelNotices';

describe('PanelNotices: an away machine', () => {
  it('keeps the backup notice when the listing failed too', () => {
    render(
      <PanelNotices
        uploadProgress={null}
        error={null}
        onDismissError={vi.fn()}
        filesError="Couldn't load files"
        backupNotice="Showing the backup from 2 hours ago"
        busy={false}
        backupResult={null}
        onDismissBackupResult={vi.fn()}
        editing={false}
      />,
    );
    expect(screen.getByText('Showing the backup from 2 hours ago')).toBeInTheDocument();
    expect(screen.getByText("Couldn't load files")).toBeInTheDocument();
  });
});
