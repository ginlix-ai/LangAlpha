/**
 * The model often writes a bare "\n\n" between two tool calls. It says
 * nothing, so it must neither draw nor split the launches on either side of
 * it into two runs, for subagents and analyst hand-offs alike, and while it
 * streams it must not hold the launch after it behind prose that never comes.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import { MessageContentSegments } from '../MessageContentSegments';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));
vi.mock('../../Markdown', () => ({
  default: ({ content }: { content: string }) => <div data-testid="markdown-content">{content}</div>,
}));
// The runs stand in for themselves: what is pinned here is how the blocks are
// gathered, not how a run draws.
vi.mock('../SubagentRun', () => {
  const SubagentRun = ({ members }: { members: Array<{ subagentId: string }> }) => (
    <div data-testid="subagent-run">{members.map((m) => m.subagentId).join(',')}</div>
  );
  return { SubagentRun, default: SubagentRun };
});
vi.mock('../AnalystHandoffRun', () => {
  const AnalystHandoffRun = ({ members }: { members: Array<{ proposalId: string }> }) => (
    <div data-testid="analyst-run">{members.map((m) => m.proposalId).join(',')}</div>
  );
  return { AnalystHandoffRun, default: AnalystHandoffRun };
});

type SegmentsProps = React.ComponentProps<typeof MessageContentSegments>;

const base: SegmentsProps = {
  segments: [],
  reasoningProcesses: {},
  toolCallProcesses: {},
  todoListProcesses: {},
  subagentTasks: {},
  isStreaming: false,
};

const spawn = (id: string) => ({
  subagentId: id, description: `Research ${id}`, prompt: '', type: 'research', action: 'init', status: 'running',
});

describe('MessageContentSegments: blank text between launches', () => {
  it.each([false, true])('keeps two spawns one run and draws nothing between them (streaming: %s)', (isStreaming) => {
    render(
      <MessageContentSegments
        {...base}
        isStreaming={isStreaming}
        segments={[
          { type: 'subagent_task', order: 0, subagentId: 'a' },
          { type: 'text', order: 1, content: '\n\n' },
          { type: 'subagent_task', order: 2, subagentId: 'b' },
        ] as SegmentsProps['segments']}
        subagentTasks={{ a: spawn('a'), b: spawn('b') } as SegmentsProps['subagentTasks']}
      />,
    );
    expect(screen.getAllByTestId('subagent-run').map((el) => el.textContent)).toEqual(['a,b']);
    expect(screen.queryByTestId('markdown-content')).toBeNull();
  });

  it('keeps two hand-offs one run', () => {
    const proposal = (workspace: string) => ({ status: 'approved' as const, workspace_id: workspace, question: 'Job' });
    render(
      <MessageContentSegments
        {...base}
        segments={[
          { type: 'ptc_agent', order: 0, proposalId: 'p1' },
          { type: 'text', order: 1, content: ' \n\n ' },
          { type: 'ptc_agent', order: 2, proposalId: 'p2' },
        ] as SegmentsProps['segments']}
        ptcAgentProposals={{ p1: proposal('ws-a'), p2: proposal('ws-b') }}
      />,
    );
    expect(screen.getAllByTestId('analyst-run').map((el) => el.textContent)).toEqual(['p1,p2']);
    expect(screen.queryByTestId('markdown-content')).toBeNull();
  });

  it('still splits launches that real prose stands between', () => {
    render(
      <MessageContentSegments
        {...base}
        segments={[
          { type: 'subagent_task', order: 0, subagentId: 'a' },
          { type: 'text', order: 1, content: 'Now the margins.' },
          { type: 'subagent_task', order: 2, subagentId: 'b' },
        ] as SegmentsProps['segments']}
        subagentTasks={{ a: spawn('a'), b: spawn('b') } as SegmentsProps['subagentTasks']}
      />,
    );
    expect(screen.getAllByTestId('subagent-run').map((el) => el.textContent)).toEqual(['a', 'b']);
    expect(screen.getByTestId('markdown-content')).toHaveTextContent('Now the margins.');
  });
});
