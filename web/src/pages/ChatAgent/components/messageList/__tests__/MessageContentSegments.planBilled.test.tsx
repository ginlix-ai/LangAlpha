/**
 * The notice under a reply whose last request was billed to usage credits
 * reads as the quiet notice line after the reply's prose, worded in the
 * reader's zone, not as a divider.
 */
import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import { MemoryRouter } from 'react-router';
import type React from 'react';
import { MessageContentSegments } from '../MessageContentSegments';

vi.mock('@/hooks/useClaudeCodePlan', () => ({
  usePlanClock: () => ({ locale: 'en-US', zone: 'America/New_York', now: Date.parse('2026-11-06T19:05:00Z') }),
}));

type SegmentsProps = React.ComponentProps<typeof MessageContentSegments>;

describe('MessageContentSegments: a reply billed to usage credits', () => {
  it('draws the notice line after the prose', () => {
    render(
      <MemoryRouter>
        <MessageContentSegments
          segments={[
            { type: 'text', order: 1, content: 'Here is the summary.' },
            { type: 'notification', order: 2, planBilled: { window: 'five_hour', until: '2026-11-06T20:40:00Z' } },
          ] as SegmentsProps['segments']}
          reasoningProcesses={{}}
          toolCallProcesses={{}}
          todoListProcesses={{}}
          subagentTasks={{}}
          isStreaming={false}
        />
      </MemoryRouter>,
    );
    const notice = screen.getByRole('status');
    expect(notice).toHaveTextContent(
      "Your Claude plan's 5-hour limit ran out during this reply, so its last request was billed to your usage credits. "
      + 'Claude Code turns wait until it resets at 3:40 PM.',
    );
    const prose = screen.getByText('Here is the summary.', { exact: false });
    expect(prose.compareDocumentPosition(notice) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });
});
