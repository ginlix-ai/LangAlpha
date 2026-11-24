/**
 * A reply whose leading compaction notices the list draws above the turn's
 * message keeps them out when it rebuilds itself for the clock (a live tool
 * row aging), as it was built: the notice is never drawn twice.
 */
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import { MessageActionsProvider } from '../messageList/MessageActionsContext';
import { MessageContentSegments } from '../messageList/MessageContentSegments';
import { contextHead, projectMessageContent } from '../messageList/contentProjection';
import type { MessageRecord } from '../messageList/types';

describe('MessageContentSegments: the notices drawn above the turn', () => {
  it('leaves them out of a rebuild for the clock', () => {
    const segments = [
      { type: 'notification', content: 'Context compacted · 14 messages → summary', order: 0, context: true },
      { type: 'text', content: 'Looking it up.', order: 1 },
    ];
    const message = { id: 'a1', role: 'assistant', contentSegments: segments, reasoningProcesses: {}, toolCallProcesses: {} } as MessageRecord;
    const rest = contextHead(projectMessageContent(message, false, null), 1)!.rest;
    render(
      <MessageActionsProvider actions={{}}>
        <MessageContentSegments
          segments={segments as never}
          contentProjection={{ ...rest, nextExpiry: 1 }}
          reasoningProcesses={{}}
          toolCallProcesses={{}}
          subagentTasks={{}}
        />
      </MessageActionsProvider>,
    );
    expect(screen.getByText('Looking it up.')).toBeInTheDocument();
    expect(screen.queryByText(/Context compacted/)).toBeNull();
  });
});
