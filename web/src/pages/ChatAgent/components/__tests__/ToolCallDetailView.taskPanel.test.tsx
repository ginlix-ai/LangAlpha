/**
 * The detail panel for a spawned `Task`.
 *
 * The `Task` tool returns the instant the subagent is dispatched, and nothing
 * ever overwrites that reply — so the only "result" this panel can reach is
 * the dispatch boilerplate. It used to render exactly that under a heading
 * reading "Result", telling the reader to go call `TaskOutput(...)`. The
 * panel's job is what was asked, how it is going, and the way through to the
 * subagent's tab, where the real output lives.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

vi.mock('@/hooks/useIsMobile', () => ({ useIsMobile: () => false }));

vi.mock('../Markdown', () => ({
  default: ({ content }: { content: string }) => (
    <div data-testid="markdown-content">{content}</div>
  ),
  CodeBlock: ({ code }: { code: string }) => <pre data-testid="code-block">{code}</pre>,
}));

import ToolCallDetailView from '../ToolCallDetailView';

/** What `Task` actually replies with — verbatim in shape, neutral in content. */
const DISPATCH_REPLY = [
  'Background subagent deployed: **Task-A1b2C3**',
  '- Type: research',
  '- Status: Running in background',
  '',
  'You can:',
  '- Use `TaskOutput(task_id="task_a1b2c3")` to get progress or result',
].join('\n');

function makeProcess(status: string) {
  return {
    toolName: 'Task',
    toolCall: {
      id: 'call-1',
      name: 'Task',
      args: { subagent_type: 'research', description: 'Summarize the filing' },
    },
    toolCallResult: { content: DISPATCH_REPLY },
    isComplete: true,
    _subagentStatus: status,
  };
}

describe('ToolCallDetailView — task panel', () => {
  it('never shows the dispatch reply as the task result', () => {
    render(
      <ToolCallDetailView
        toolCallProcess={makeProcess('completed')}
        onOpenSubagentTask={() => {}}
      />,
    );

    const rendered = screen
      .queryAllByTestId('markdown-content')
      .map((node) => node.textContent ?? '');
    expect(rendered).toContain('Summarize the filing');
    expect(rendered.some((text) => text.includes('TaskOutput'))).toBe(false);
    expect(rendered.some((text) => text.includes('deployed'))).toBe(false);
  });

  it('shows the task status and the way through to its output', () => {
    render(
      <ToolCallDetailView
        toolCallProcess={makeProcess('running')}
        onOpenSubagentTask={() => {}}
      />,
    );

    // getAllByText: a running chip carries the key twice — the visible text
    // and the ascii loader's screen-reader label.
    expect(screen.getAllByText('chat.taskCard.statusRunning').length).toBeGreaterThan(0);
    expect(screen.getByText('toolArtifact.goToSubagentTab')).toBeInTheDocument();
  });

  it('reports a failed task as failed rather than as a missing result', () => {
    // The old panel had no status of its own: a task that errored showed
    // "No result available", which reads as "nothing happened yet".
    render(<ToolCallDetailView toolCallProcess={makeProcess('error')} />);

    expect(screen.getByText('chat.taskCard.statusFailed')).toBeInTheDocument();
  });
});
