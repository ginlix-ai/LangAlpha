/**
 * A file pill opens its file in the panel, which reads this computer. A call
 * that named another of the user's computers read or wrote that one's file,
 * so its pill opens the call instead of a namesake here.
 */
import { describe, it, expect, vi } from 'vitest';
import { fireEvent, screen } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import { FileToolGroupRow } from '../FileToolGroupRow';
import type { ToolActivityItem } from '../activityTypes';

const read = (args: Record<string, unknown>): ToolActivityItem => ({
  type: 'tool_call', id: 'a', toolCallId: 'a', toolName: 'Read', _liveState: 'completed',
  toolCall: { args }, toolCallResult: { content: 'ok' },
});

describe('FileToolGroupRow', () => {
  it('opens a file read on this computer', () => {
    const onOpenFile = vi.fn();
    const onToolCallClick = vi.fn();
    renderWithProviders(<FileToolGroupRow items={[read({ file_path: 'data/q3.csv' })]} onOpenFile={onOpenFile} onToolCallClick={onToolCallClick} />);
    fireEvent.click(screen.getByRole('button', { name: 'q3.csv' }));
    expect(onOpenFile).toHaveBeenCalledWith('data/q3.csv');
    expect(onToolCallClick).not.toHaveBeenCalled();
  });

  it('opens the call, not a file here, for a read on another computer', () => {
    const onOpenFile = vi.fn();
    const onToolCallClick = vi.fn();
    const item = read({ file_path: 'data/q3.csv', computer: 'Studio Mac' });
    renderWithProviders(<FileToolGroupRow items={[item]} onOpenFile={onOpenFile} onToolCallClick={onToolCallClick} />);
    fireEvent.click(screen.getByRole('button', { name: 'q3.csv' }));
    expect(onOpenFile).not.toHaveBeenCalled();
    expect(onToolCallClick).toHaveBeenCalledWith(item);
  });
});
