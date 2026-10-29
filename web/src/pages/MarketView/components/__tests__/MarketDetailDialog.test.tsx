import { render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import MarketDetailDialog from '../MarketDetailDialog';
import type { ToolCallProcessRecord } from '../../../ChatAgent/components/ToolCallDetailView';

vi.mock('../../../ChatAgent/components/ToolCallDetailView', () => ({
  default: ({ toolCallProcess }: { toolCallProcess: ToolCallProcessRecord }) => (
    <div data-testid="detail-view">{String(toolCallProcess.status)}</div>
  ),
}));
vi.mock('../../../ChatAgent/components/viewers/PreviewViewer', () => ({ default: () => null }));

const running: ToolCallProcessRecord = {
  toolCallId: 'tc-1',
  toolName: 'web_search',
  status: 'running',
  toolCall: { name: 'web_search', args: { query: 'AAPL' } },
} as unknown as ToolCallProcessRecord;

describe('MarketDetailDialog', () => {
  it('reads the live record through the lookup, so a call settles while open', () => {
    const lookup = vi.fn().mockReturnValue(running);
    const { rerender } = render(
      <MarketDetailDialog payload={{ type: 'toolcall', toolCallId: 'tc-1' }} onClose={() => {}} getToolCallProcess={lookup} />,
    );
    expect(screen.getByTestId('detail-view')).toHaveTextContent('running');

    lookup.mockReturnValue({ ...running, status: 'completed' });
    rerender(
      <MarketDetailDialog payload={{ type: 'toolcall', toolCallId: 'tc-1' }} onClose={() => {}} getToolCallProcess={lookup} />,
    );
    expect(screen.getByTestId('detail-view')).toHaveTextContent('completed');
  });

  it('shows the gone placeholder when the record has left the transcript', () => {
    render(
      <MarketDetailDialog payload={{ type: 'toolcall', toolCallId: 'tc-1' }} onClose={() => {}} getToolCallProcess={() => undefined} />,
    );
    expect(screen.queryByTestId('detail-view')).toBeNull();
    expect(screen.getByText(/no longer in the chat/i)).toBeInTheDocument();
  });
});
