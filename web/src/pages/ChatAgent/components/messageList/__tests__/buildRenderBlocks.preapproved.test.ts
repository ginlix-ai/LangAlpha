/**
 * A hidden secretary tool call the server dispatched without asking (no
 * approval interrupt, result marked `preapproved`) still needs a card: the
 * normal `HIDDEN_TOOL_CALL_NAMES` filter would otherwise drop it silently
 * since it never got a proposal segment of its own. The card's own shape is
 * pinned in utils/__tests__/preapprovedCards.test.ts.
 */
import { describe, it, expect } from 'vitest';
import { buildRenderBlocks } from '../buildRenderBlocks';
import { projectContent, type ContentProjection } from '../contentProjection';
import type { ContentSegmentRecord } from '../types';
import type { ToolCallProcessRecord } from '../../ToolCallDetailView';

const TOOL_SEGMENT: ContentSegmentRecord = { type: 'tool_call', toolCallId: 'tc1', order: 0 };

const handoffResult = () => ({
  content: JSON.stringify({
    success: true,
    preapproved: true,
    workspace_id: 'ws-1',
    workspace_name: 'Semiconductors',
    thread_id: 'thread-1',
    report_back: true,
  }),
});

function run(toolCallProcesses: Record<string, ToolCallProcessRecord>) {
  return buildRenderBlocks([TOOL_SEGMENT], {
    reasoningProcesses: {},
    toolCallProcesses,
    isStreaming: false,
  });
}

describe('buildRenderBlocks: preapproved secretary calls', () => {
  it('draws an approved ptc_agent card for a preapproved hand-off', () => {
    const { blocks } = run({
      tc1: {
        toolName: 'ptc_agent',
        toolCall: { args: { question: 'Is NVDA overvalued?' } },
        toolCallResult: handoffResult(),
        isComplete: true,
      },
    });

    expect(blocks).toMatchObject([
      { type: 'ptc_agent', key: 'ptc-agent-tc1', segment: TOOL_SEGMENT, proposal: { status: 'approved' } },
    ]);
  });

  it('draws an approved create_workspace card for a preapproved manage_workspaces call', () => {
    const { blocks } = run({
      tc1: {
        toolName: 'manage_workspaces',
        toolCall: { args: { action: 'create' } },
        toolCallResult: {
          content: JSON.stringify({ success: true, preapproved: true, workspace_name: 'New Workspace' }),
        },
        isComplete: true,
      },
    });

    expect(blocks).toMatchObject([
      { type: 'create_workspace', key: 'workspace-tc1', segment: TOOL_SEGMENT, proposal: { status: 'approved' } },
    ]);
  });

  it('renders no block (stays hidden) when the same call asked first (no preapproved flag)', () => {
    const { blocks } = run({
      tc1: {
        toolName: 'ptc_agent',
        toolCall: { args: { question: 'Is NVDA overvalued?' } },
        toolCallResult: { content: JSON.stringify({ success: true }) },
        isComplete: true,
      },
    });

    expect(blocks).toEqual([]);
  });

  it('renders no block while the hidden call has no result yet', () => {
    const { blocks } = run({
      tc1: {
        toolName: 'ptc_agent',
        toolCall: { args: { question: 'Is NVDA overvalued?' } },
        isInProgress: true,
      },
    });

    expect(blocks).toEqual([]);
  });

  it('keeps the same card block while the turn streams on past it', () => {
    // The stream hands every chunk a new tool-call record, but the result it
    // already holds keeps its identity, and so must the card drawn from it.
    const result = handoffResult();
    const chunk = (text: string) => ({
      segments: [TOOL_SEGMENT, { type: 'text', content: text, order: 1 }],
      reasoningProcesses: {},
      toolCallProcesses: {
        tc1: { toolName: 'ptc_agent', toolCall: { args: { question: 'Q' } }, toolCallResult: result, isComplete: true },
      },
      isStreaming: true,
    });
    const card = (p: ContentProjection) => p.blocks.find((b) => b.type === 'ptc_agent');

    const first = projectContent(chunk('Handed'));
    const second = projectContent(chunk('Handed off.'), first);

    expect(second).not.toBe(first);
    expect(card(first)).toBeDefined();
    expect(card(second)).toBe(card(first));
  });
});
