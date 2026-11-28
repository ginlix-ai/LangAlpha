import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, render } from '@testing-library/react';
import '@testing-library/jest-dom';
import ActivityBlock from '../ActivityBlock';
import { TranscriptDisplayContext } from '@/lib/transcriptDisplay';
import { STATUS_HOLD_MS } from '../messageList/liveZoneTiming';

// Browser tests own animation frames; these pin what the line names and counts.
vi.mock('@/lib/framer', async (importOriginal) => ({
  ...await importOriginal<typeof import('@/lib/framer')>(),
  AnimatePresence: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}));

type ActivityItem = Parameters<typeof ActivityBlock>[0]['items'][number];

const thought: ActivityItem = { type: 'reasoning', id: 'thought', content: 'Planning the searches.', _liveState: 'completed' };

function search(id: string, running = true): ActivityItem {
  return {
    type: 'tool_call', id, toolCallId: id, toolName: 'WebSearch',
    toolCall: { args: { query: `query ${id}` } },
    _liveState: running ? 'active' : 'completed', isComplete: !running,
  };
}

const STATUS = { turnDisplay: 'lean', streamingMode: 'token', toolCallLayout: 'status' } as const;

function renderBlock(items: ActivityItem[]) {
  const view = (next: ActivityItem[]) => (
    <TranscriptDisplayContext.Provider value={STATUS}>
      <ActivityBlock items={next} isStreaming holdsLine presentation="folded" />
    </TranscriptDisplayContext.Provider>
  );
  const result = render(view(items));
  return { ...result, update: (next: ActivityItem[]) => result.rerender(view(next)) };
}

/** The line, the step it names, and its "+N" as shown and as spoken. */
function readLine(container: HTMLElement) {
  const lines = container.querySelectorAll('[data-status-line]');
  expect(lines).toHaveLength(1);
  const line = lines[0] as HTMLElement;
  const more = line.querySelector('.titem-more');
  return {
    text: line.textContent ?? '',
    shown: more?.querySelector('[aria-hidden]')?.textContent ?? null,
    spoken: more?.querySelector('.sr-only')?.textContent ?? null,
  };
}

const advance = (ms: number) => act(() => { vi.advanceTimersByTime(ms); });

describe('ActivityBlock status line, the count of other steps in flight', () => {
  beforeEach(() => { vi.useFakeTimers(); });
  afterEach(() => { vi.useRealTimers(); });

  it('names the newest of a parallel batch and counts the rest down as they land', () => {
    const { container, update } = renderBlock([thought, search('a'), search('b'), search('c')]);
    let line = readLine(container);
    expect(line.text).toContain('query c');
    expect(line.shown).toBe('+2');
    expect(line.spoken).toBe('and 2 more running');

    update([thought, search('a'), search('b', false), search('c')]);
    line = readLine(container);
    expect(line.text).toContain('query c');
    expect(line.shown).toBe('+1');
    expect(line.spoken).toBe('and 1 more running');

    update([thought, search('a', false), search('b', false), search('c')]);
    expect(readLine(container).shown).toBeNull();
  });

  it('keeps the count steady while the line hands a landed step to one still running', () => {
    const { container, update } = renderBlock([thought, search('a'), search('b'), search('c')]);
    advance(STATUS_HOLD_MS * 2);
    // The step on the line lands with two others in flight: the newer takes
    // the line, and the count never shows the one taking it on the way.
    update([thought, search('a'), search('b'), search('c', false)]);
    expect(readLine(container).shown).toBe('+1');
    advance(STATUS_HOLD_MS);
    const line = readLine(container);
    expect(line.text).toContain('query b');
    expect(line.shown).toBe('+1');
  });

  it('shows no count for a step waiting out its hold to take the line', () => {
    const { container, update } = renderBlock([thought, search('a')]);
    update([thought, search('a', false)]);
    update([thought, search('a', false), search('b')]);
    let line = readLine(container);
    expect(line.text).toContain('query a');
    expect(line.shown).toBeNull();
    advance(STATUS_HOLD_MS);
    line = readLine(container);
    expect(line.text).toContain('query b');
    expect(line.shown).toBeNull();
  });

  it('does not count the step the block opened on, drawn running above the line', () => {
    const { container } = renderBlock([search('a'), search('b'), search('c')]);
    expect(container.querySelector('.activity-lead')?.textContent).toContain('query a');
    const line = readLine(container);
    expect(line.text).toContain('query c');
    expect(line.shown).toBe('+1');
  });
});
