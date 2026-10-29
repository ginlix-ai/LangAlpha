// @vitest-environment node
/**
 * A hand-off or workspace creation the user approved in advance carries no
 * proposal: the server marks the tool result `preapproved` and the card is
 * drawn straight from it (`src/tools/secretary/approvals.py` on the server
 * side). These tests pin that contract; the cache behind `preapprovedCardOf`
 * is pinned where it pays off, in buildRenderBlocks.preapproved.test.ts.
 */
import { describe, it, expect } from 'vitest';
import { preapprovedCard, preapprovedReportBack, preapprovedCardOf } from '../preapprovedCards';

const ptcResult = (overrides: Record<string, unknown> = {}) =>
  JSON.stringify({
    success: true,
    preapproved: true,
    workspace_id: 'ws-1',
    workspace_name: 'Semiconductors',
    thread_id: 'thread-1',
    report_back: true,
    ...overrides,
  });
// A call's result record, as the stream and the transcript carry it.
const ptcTool = (overrides: Record<string, unknown> = {}) => ({ content: ptcResult(overrides) });

describe('preapprovedCard', () => {
  it.each(['delegate_to_analyst', 'ptc_agent'])('builds an approved ptc_agent proposal for %s', (toolName) => {
    const card = preapprovedCard(toolName, { question: 'Is NVDA overvalued?' }, ptcTool());
    expect(card).toEqual({
      type: 'ptc_agent',
      proposal: {
        status: 'approved',
        question: 'Is NVDA overvalued?',
        workspace_id: 'ws-1',
        workspace_name: 'Semiconductors',
        thread_id: 'thread-1',
        report_back: true,
      },
    });
  });

  it('reads report_back false when the result does not say true', () => {
    const card = preapprovedCard('ptc_agent', { question: 'Q' }, ptcTool({ report_back: false }));
    expect(card?.proposal.report_back).toBe(false);
  });

  it('falls back to an empty question when args carry no string one', () => {
    expect(preapprovedCard('ptc_agent', { question: 42 }, ptcTool())?.proposal.question).toBe('');
    expect(preapprovedCard('ptc_agent', undefined, ptcTool())?.proposal.question).toBe('');
  });

  it('builds an approved create_workspace proposal for manage_workspaces', () => {
    const card = preapprovedCard(
      'manage_workspaces',
      { action: 'create' },
      {
        content: JSON.stringify({
          success: true,
          preapproved: true,
          workspace_name: 'New Workspace',
          workspace_description: 'A fresh workspace',
        }),
      },
    );
    expect(card).toEqual({
      type: 'create_workspace',
      proposal: {
        status: 'approved',
        workspace_name: 'New Workspace',
        workspace_description: 'A fresh workspace',
      },
    });
  });

  it('returns null when the result carries no preapproved flag', () => {
    expect(preapprovedCard('ptc_agent', { question: 'Q' }, { content: JSON.stringify({ success: true }) })).toBeNull();
  });

  // The server marks a failure preapproved only once the hand-off was past its
  // approval point (cap, busy thread, refused or unknown dispatch).
  it('draws a failed hand-off approved in advance as approved and failed', () => {
    const result = JSON.stringify({
      success: false,
      error: 'dispatch_failed',
      preapproved: true,
      workspace_id: 'ws-1',
      workspace_name: 'Semiconductors',
    });
    expect(preapprovedCard('ptc_agent', { question: 'Q' }, { content: result })).toEqual({
      type: 'ptc_agent',
      proposal: {
        status: 'approved',
        question: 'Q',
        workspace_id: 'ws-1',
        workspace_name: 'Semiconductors',
        thread_id: undefined,
        report_back: false,
        dispatch_failed: true,
      },
    });
  });

  it('keeps the thread an unknown outcome names, where its run may yet turn up', () => {
    const card = preapprovedCard(
      'delegate_to_analyst',
      { question: 'Q' },
      ptcTool({ success: false, outcome: 'unknown_retained', report_back: undefined }),
    );
    expect(card?.proposal).toMatchObject({ thread_id: 'thread-1', dispatch_failed: true, dispatch_unknown: true, status: 'approved' });
  });

  it('marks a failure that named its thread definite, unless its outcome is unknown', () => {
    // A continuation refused outright names the thread, whose other runs are not this one.
    const card = preapprovedCard('delegate_to_analyst', { question: 'Q' }, ptcTool({ success: false, error: 'dispatch_failed' }));
    expect(card?.proposal).toMatchObject({ thread_id: 'thread-1', dispatch_failed: true });
    expect(card?.proposal).not.toHaveProperty('dispatch_unknown');
  });

  it('pins the hand-off to the run its dispatch named, and names it by its call', () => {
    const card = preapprovedCard('delegate_to_analyst', { question: 'Q' }, ptcTool({ status: 'dispatched', run_id: 'run-1' }), 'call-1');
    expect(card?.proposal).toMatchObject({ run_id: 'run-1', tool_call_id: 'call-1' });
    // A failed dispatch's run, if it names one, is not the hand-off's.
    const failed = preapprovedCard('delegate_to_analyst', { question: 'Q' }, ptcTool({ success: false, run_id: 'run-1' }));
    expect(failed?.proposal).not.toHaveProperty('run_id');
  });

  it('takes a steered hand-off\'s steer from the result\'s artifact, which the model never reads', () => {
    const steered = ptcResult({ status: 'steered', run_id: 'run-1' });
    const card = preapprovedCardOf({
      toolName: 'delegate_to_analyst',
      toolCall: { id: 'call-7', args: { question: 'Q' } },
      toolCallResult: { content: steered, artifact: { steer_id: 'steer-1' } },
    });
    expect(card?.proposal).toMatchObject({ run_id: 'run-1', steer_id: 'steer-1' });
    // Nowhere else: not from the content, and not for a dispatch that failed.
    expect(preapprovedCard('delegate_to_analyst', { question: 'Q' }, ptcTool({ steer_id: 'steer-1' }))?.proposal)
      .not.toHaveProperty('steer_id');
    const failed = { content: ptcResult({ success: false }), artifact: { steer_id: 'steer-1' } };
    expect(preapprovedCard('delegate_to_analyst', { question: 'Q' }, failed)?.proposal).not.toHaveProperty('steer_id');
  });

  it('reads the call id off the tool call it draws from', () => {
    const card = preapprovedCardOf({
      toolName: 'delegate_to_analyst',
      toolCall: { id: 'call-7', args: { question: 'Q' } },
      toolCallResult: { content: ptcResult({ run_id: 'run-7' }) },
    });
    expect(card?.proposal).toMatchObject({ run_id: 'run-7', tool_call_id: 'call-7' });
  });

  it('returns null for a hand-off that failed before anything was approved', () => {
    const result = JSON.stringify({ success: false, error: 'dispatch_failed' });
    expect(preapprovedCard('ptc_agent', { question: 'Q' }, { content: result })).toBeNull();
  });

  it('returns null for a workspace creation approved in advance that did not succeed', () => {
    const result = JSON.stringify({ success: false, preapproved: true, workspace_name: 'New Workspace' });
    expect(preapprovedCard('manage_workspaces', { action: 'create' }, { content: result })).toBeNull();
  });

  it('returns null for non-JSON content', () => {
    expect(preapprovedCard('ptc_agent', { question: 'Q' }, { content: 'plain text result' })).toBeNull();
  });

  it('returns null for non-string content', () => {
    expect(preapprovedCard('ptc_agent', { question: 'Q' }, { content: { success: true, preapproved: true } })).toBeNull();
  });

  it('returns null for a tool name that is not a recognized hand-off or workspace action', () => {
    expect(preapprovedCard('bash', {}, ptcTool())).toBeNull();
  });
});

describe('preapprovedReportBack', () => {
  it('is true only for a preapproved, dispatched result with report_back true', () => {
    expect(preapprovedReportBack(ptcResult({ status: 'dispatched', report_back: true }))).toBe(true);
  });

  it('is true for a preapproved hand-off steered into a running analyst turn', () => {
    expect(preapprovedReportBack(ptcResult({ status: 'steered', report_back: true }))).toBe(true);
  });

  it('is false for a steered hand-off that does not report back', () => {
    expect(preapprovedReportBack(ptcResult({ status: 'steered', report_back: false }))).toBe(false);
  });

  it('is false when report_back is not true', () => {
    expect(preapprovedReportBack(ptcResult({ status: 'dispatched', report_back: false }))).toBe(false);
  });

  it('is false when the status is not dispatched', () => {
    expect(preapprovedReportBack(ptcResult({ status: 'other', report_back: true }))).toBe(false);
  });

  it('is false for a dispatched result that was not preapproved (asked first)', () => {
    expect(
      preapprovedReportBack(
        JSON.stringify({ success: true, status: 'dispatched', report_back: true }),
      ),
    ).toBe(false);
  });

  it('is false for non-JSON content', () => {
    expect(preapprovedReportBack('nope')).toBe(false);
  });
});

describe('preapprovedCardOf', () => {
  it('returns null when there is no result', () => {
    expect(preapprovedCardOf(undefined)).toBeNull();
    expect(preapprovedCardOf({ toolName: 'ptc_agent' })).toBeNull();
  });
});
