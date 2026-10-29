/**
 * `ActivityBlock` is memoized so a streamed token does not re-render every
 * tool row under it, and `MessageActionsContext` is documented as
 * identity-stable for the same reason one level up. A handler built inline in
 * the render body hands the memo a fresh identity on every render and spends
 * that work anyway, so the identity is asserted rather than assumed.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render } from '@testing-library/react';
import { MessageContentSegments } from '../MessageContentSegments';
import { projectMessageContent } from '../contentProjection';
import { MessageActionsProvider } from '../MessageActionsContext';
import type { ContentSegmentRecord, MessageRecord } from '../types';

const captured: Record<string, unknown>[] = [];

vi.mock('@/hooks/useUser', () => ({ useUser: () => ({ user: null }) }));
vi.mock('@/contexts/ThemeContext', () => ({ useTheme: () => ({ theme: 'light' }) }));
vi.mock('react-i18next', () => ({ useTranslation: () => ({ t: (key: string) => key }) }));
vi.mock('../../Markdown', () => ({
  default: ({ content }: { content: string }) => <div>{content}</div>,
}));
vi.mock('../../ActivityBlock', () => ({
  default: (props: Record<string, unknown>) => {
    captured.push(props);
    return <div data-testid="activity-block" />;
  },
}));

const segments: ContentSegmentRecord[] = [
  { type: 'tool_call', toolCallId: 't1', order: 0 } as ContentSegmentRecord,
  { type: 'text', content: 'Done.\n\n', order: 1 },
];

const toolCallProcesses = {
  t1: { toolName: 'bash', toolCall: { id: 't1', name: 'bash', args: {} }, toolCallResult: { output: 'ok' }, order: 0 },
};

const message = {
  id: 'a0', role: 'assistant', content: 'Done.\n\n', contentType: 'text',
  contentSegments: segments, reasoningProcesses: {}, toolCallProcesses,
} as unknown as MessageRecord;

// One projection object across both renders: that is what a render advancing
// only the typewriter hands down, and it keeps every other memo input equal so
// the handler is the one thing under test.
const contentProjection = projectMessageContent(message);

// The host builds this once; a fresh object here would be a different bug.
const actions = { onToolCallDetailClick: () => {} };

function tree(isTurnTail: boolean) {
  return (
    <MessageActionsProvider actions={actions}>
      <MessageContentSegments
        segments={segments}
        contentProjection={contentProjection}
        reasoningProcesses={{}}
        toolCallProcesses={toolCallProcesses}
        todoListProcesses={{}}
        subagentTasks={{}}
        isTurnTail={isTurnTail}
      />
    </MessageActionsProvider>
  );
}

describe('ActivityBlock props across a re-render', () => {
  it('keeps the tool-click handler identity, so the memo can still bail out', () => {
    captured.length = 0;
    const { rerender } = render(tree(false));
    rerender(tree(true));

    expect(captured.length).toBeGreaterThanOrEqual(2);
    const [first, second] = [captured[0], captured[captured.length - 1]];
    // Proof the rest of the memo's inputs held: if `items` had changed, a stable
    // handler would buy nothing and this assertion would prove nothing.
    expect(second.items).toBe(first.items);
    expect(second.onToolCallClick).toBe(first.onToolCallClick);
  });
});
