/**
 * Consecutive delivery pills share one wrapping row; anything else between
 * them ends the row, and other compact cards are never gathered.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
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
  default: ({ content }: { content: string }) => <div data-testid="text">{content}</div>,
}));
vi.mock('../../ActivityBlock', () => ({
  default: () => <div data-testid="activity-block" />,
  ActivityBlock: () => <div data-testid="activity-block" />,
}));
vi.mock('../../charts/InlineArtifactCards', async (orig) => ({
  ...(await orig<Record<string, unknown>>()),
  INLINE_ARTIFACT_MAP: {
    message_delivery: () => <button data-testid="pill" />,
    quote: () => <div data-testid="quote" />,
  },
}));

const proc = (id: string, type: string, order: number) => ({
  toolName: type === 'quote' ? 'get_quote' : 'send_message',
  toolCall: { id, name: 'x', args: {} },
  toolCallResult: { artifact: { type } },
  order,
});

type Item = 'pill' | 'quote' | 'text';

function renderItems(items: Item[]) {
  const toolCallProcesses: Record<string, unknown> = {};
  const segments = items.map((kind, i) => {
    if (kind === 'text') return { type: 'text', content: `para ${i}`, order: i };
    const id = `t${i}`;
    toolCallProcesses[id] = proc(id, kind === 'pill' ? 'message_delivery' : 'quote', i);
    return { type: 'tool_call', toolCallId: id, order: i };
  }) as ContentSegmentRecord[];
  const message = {
    id: 'a0', role: 'assistant', content: '', contentType: 'text', isStreaming: false,
    contentSegments: segments, reasoningProcesses: {}, toolCallProcesses,
  } as unknown as MessageRecord;
  return render(
    <TranscriptDisplayContext.Provider value={{ turnDisplay: 'lean', streamingMode: 'paragraph' }}>
      <MessageContentSegments
        segments={segments}
        contentProjection={projectMessageContent(message)}
        reasoningProcesses={{}}
        toolCallProcesses={toolCallProcesses as never}
        todoListProcesses={{}}
        subagentTasks={{}}
      />
    </TranscriptDisplayContext.Provider>,
  ).container;
}

const rows = (c: HTMLElement) => Array.from(c.querySelectorAll('[data-pill-row]'));

describe('delivery pill rows', () => {
  it('puts two consecutive pills in one row', () => {
    const c = renderItems(['pill', 'pill']);
    expect(rows(c)).toHaveLength(1);
    expect(rows(c)[0].querySelectorAll('[data-testid="pill"]')).toHaveLength(2);
  });

  it('keeps a lone pill outside any row', () => {
    const c = renderItems(['pill']);
    expect(rows(c)).toHaveLength(0);
    expect(c.querySelectorAll('[data-testid="pill"]')).toHaveLength(1);
  });

  it('starts a new row after a text block', () => {
    const c = renderItems(['pill', 'pill', 'text', 'pill', 'pill']);
    expect(rows(c)).toHaveLength(2);
    rows(c).forEach((r) => expect(r.querySelectorAll('[data-testid="pill"]')).toHaveLength(2));
  });

  it('breaks the run at a non-pill card and never groups those', () => {
    const c = renderItems(['pill', 'quote', 'pill', 'quote', 'quote']);
    expect(rows(c)).toHaveLength(0);
    expect(c.querySelectorAll('[data-testid="pill"]')).toHaveLength(2);
    expect(c.querySelectorAll('[data-testid="quote"]')).toHaveLength(3);
  });

  it('groups only the pills when a quote sits beside the run', () => {
    const c = renderItems(['quote', 'pill', 'pill']);
    expect(rows(c)).toHaveLength(1);
    expect(rows(c)[0].querySelector('[data-testid="quote"]')).toBeNull();
  });
});
