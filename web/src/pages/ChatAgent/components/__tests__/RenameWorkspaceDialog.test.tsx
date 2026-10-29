/**
 * The rename field holds as many characters as the server accepts, counted the
 * way the server counts them: an emoji is one character, not two UTF-16 units.
 */
import { describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import type { Workspace } from '@/types/api';
import RenameWorkspaceDialog from '../RenameWorkspaceDialog';

const TARGET = { workspace_id: 'ws-1', name: 'Research' } as Workspace;

describe('RenameWorkspaceDialog', () => {
  it('submits a name of 41 emoji, which the server accepts', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn();
    render(<RenameWorkspaceDialog target={TARGET} onClose={vi.fn()} onSubmit={onSubmit} busy={false} />);

    const field = screen.getByRole('textbox');
    await user.clear(field);
    await user.paste('\u{1F4C8}'.repeat(41));
    await user.click(screen.getByRole('button', { name: /^save$/i }));

    expect(onSubmit).toHaveBeenCalledWith('\u{1F4C8}'.repeat(41));
  });
});
