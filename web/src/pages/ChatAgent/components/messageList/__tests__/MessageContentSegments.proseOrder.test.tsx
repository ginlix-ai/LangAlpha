/**
 * Nothing that narrates the work may overtake the prose that introduces it.
 *
 * Paragraph delivery holds a sentence until its blank line arrives, so a card
 * mounted the moment its event lands sits above prose written before it. The
 * guard used to cover only an `activity` block directly after the text, which
 * left a task card or a widget to render straight through, and a card between
 * two rows let the row after it through as well.
 *
 * HITL cards are excluded on purpose: their turn is interrupted waiting on the
 * reader, so holding the control behind a typewriter makes each wait on the
 * other. That exclusion is asserted here so it stays a decision.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render } from '@testing-library/react';
import { MessageContentSegments } from '../MessageContentSegments';
import { projectMessageContent } from '../contentProjection';
import { TranscriptDisplayContext } from '@/lib/transcriptDisplay';
import type { ContentSegmentRecord, MessageRecord } from '../types';

vi.mock('@/hooks/useUser', () => ({ useUser: () => ({ user: null }) }));
vi.mock('@/contexts/ThemeContext', () => ({ useTheme: () => ({ theme: 'light' }) }));
vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));
vi.mock('../../Markdown', () => ({
  default: ({ content }: { content: string }) => <div data-testid="markdown-content">{content}</div>,
}));
vi.mock('../../ActivityBlock', () => ({
  default: () => <div data-testid="activity-block" />,
  ActivityBlock: () => <div data-testid="activity-block" />,
}));
// Counted, not just queried. The old guard let the card through on the commit
// that first painted the prose and removed it once the effect reported, so by
// the time a query runs the flash has already been and gone.
// A spawn draws through its run and a workflow through WorkflowRunCard; both
// are the same card to the gate. A declaration, so the hoisted factories can
// name it before this line runs.
const cardPaints = { n: 0 };
function paintedCard() {
  cardPaints.n += 1;
  return <div data-testid="subagent-card" />;
}
vi.mock('../../WorkflowRunCard', () => ({ default: paintedCard }));
vi.mock('../SubagentRun', () => ({ default: paintedCard, SubagentRun: paintedCard }));
vi.mock('../../UserQuestionCard', () => ({
  default: () => <div data-testid="question-card" />,
}));
vi.mock('../../charts/InlineArtifactCards', async (orig) => ({
  ...(await orig<Record<string, unknown>>()),
  INLINE_ARTIFACT_MAP: { quote: () => <div data-testid="artifact-card" /> },
}));

/** A paragraph with no blank line after it: the gate holds its tail. */
const HELD = 'The first paragraph is complete.\n\nThe second one is still being';

// A completed inline-artifact tool call, which `buildRenderBlocks` promotes
// out of the activity timeline into a `compact_artifact` block of its own.
const ARTIFACT_PROCS = {
  t1: {
    toolName: 'get_quote',
    toolCall: { id: 't1', name: 'get_quote', args: {} },
    toolCallResult: { artifact: { type: 'quote', symbol: 'AAPL' } },
    order: 1,
  },
};

function renderSegments(
  text: string,
  after: 'subagent_task' | 'user_question' | 'compact_artifact' | 'notification' = 'subagent_task',
) {
  const trailing = after === 'subagent_task'
    ? { type: 'subagent_task', order: 1, subagentId: 'task-1' }
    : after === 'user_question'
      ? { type: 'user_question', order: 1, questionId: 'q-1' }
      : after === 'notification'
        ? { type: 'notification', order: 1, content: 'Switched to gpt-5 after 2 retries' }
        : { type: 'tool_call', order: 1, toolCallId: 't1' };
  const toolCallProcesses = after === 'compact_artifact' ? ARTIFACT_PROCS : {};
  const segments: ContentSegmentRecord[] = [
    { type: 'text', content: text, order: 0 },
    trailing as ContentSegmentRecord,
  ];
  const message = {
    id: 'a0', role: 'assistant', content: text, contentType: 'text', isStreaming: true,
    contentSegments: segments, reasoningProcesses: {}, toolCallProcesses,
  } as unknown as MessageRecord;
  return render(
    <TranscriptDisplayContext.Provider value={{ turnDisplay: 'lean', streamingMode: 'paragraph' }}>
      <MessageContentSegments
        segments={segments}
        contentProjection={projectMessageContent(message)}
        reasoningProcesses={{}}
        toolCallProcesses={toolCallProcesses}
        todoListProcesses={{}}
        subagentTasks={{ 'task-1': { status: 'running' } as never }}
        userQuestions={{ 'q-1': { question: 'Which tickers?', options: [], interruptId: 'q-1', status: 'pending' } }}
        isStreaming
      />
    </TranscriptDisplayContext.Provider>,
  );
}

const present = (c: HTMLElement, id: string) => c.querySelector(`[data-testid="${id}"]`) !== null;

describe('block order while a paragraph is held', () => {
  beforeEach(() => { cardPaints.n = 0; });

  it('holds a task card that lands under prose the gate has not released', () => {
    const { container } = renderSegments(HELD);

    expect(present(container, 'markdown-content')).toBe(true);
    expect(present(container, 'subagent-card')).toBe(false);
  });

  it('never paints the card, not even on the commit that first shows the prose', () => {
    // Text and the block after it can arrive in one commit. The prose reports
    // its progress from an effect, so on that first paint there is no answer
    // yet, and treating the silence as "revealed" showed the card for a frame.
    renderSegments(HELD);

    expect(cardPaints.n).toBe(0);
  });

  it('lets a question card through, because its turn is waiting on the reader', () => {
    // Holding the control the reader has to answer behind a typewriter would
    // leave the turn waiting on the reader and the reader waiting on the turn.
    const { container } = renderSegments(HELD, 'user_question');

    expect(present(container, 'question-card')).toBe(true);
  });

  it('holds an inline artifact card, which narrates work the same way', () => {
    // A chart or quote card is promoted out of the activity timeline into a
    // block of its own, so the activity hold never saw it.
    const { container } = renderSegments(HELD, 'compact_artifact');

    expect(present(container, 'artifact-card')).toBe(false);
  });

  it('holds a fallback notice, which is the record rather than the live signal', () => {
    // The `ModelStatus` pill above the composer announces the switch while it
    // happens. This block is what the transcript keeps, and a record printed
    // above the sentence it follows dates the switch a sentence too early.
    const { container } = renderSegments(HELD, 'notification');

    expect(container.textContent).not.toContain('Switched to gpt-5');
  });

  it('releases the notice once the prose it follows has landed whole', () => {
    const whole = 'The first paragraph is complete.\n\nSo is the second one.\n\n';
    const { container } = renderSegments(whole, 'notification');

    expect(container.textContent).toContain('Switched to gpt-5');
  });

  it('lets the card through once the prose it follows has landed whole', () => {
    const whole = 'The first paragraph is complete.\n\nSo is the second one.\n\n';
    const { container } = renderSegments(whole);

    expect(present(container, 'subagent-card')).toBe(true);
  });
});
