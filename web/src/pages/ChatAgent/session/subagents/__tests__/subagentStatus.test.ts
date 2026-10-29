// @vitest-environment node
/**
 * Locks the three-phase display contract of deriveSubagentStatus. The regression
 * this guards: a still-running subagent whose detail card carried an explicit
 * 'running'/'active' status but no locally-accumulated messages was rendered as
 * "Initializing" (the empty-messages branch discarded the live status), so its
 * detail view contradicted its inline chip's "Running".
 */
import { describe, it, expect } from 'vitest';
import {
  deriveSubagentStatus,
  isToolResultFailure,
  normalizeWireStatus,
} from '../subagentStatus';

describe('isToolResultFailure', () => {
  it('matches the bare failure ToolMessage case-insensitively', () => {
    // A failed spawn opens no channel — this signal alone must settle the
    // card, for both the "Error: " convention and persisted "ERROR: " turns
    // (pre-normalization RunWorkflow).
    expect(isToolResultFailure({ content: 'Error: could not start Task-x' })).toBe(true);
    expect(isToolResultFailure({ content: "ERROR: Provide exactly one of 'script', 'script_path', or 'workflow'." })).toBe(true);
    expect(isToolResultFailure({ content: '  error: lowercase too' })).toBe(true);
  });

  it('requires the failure envelope, not just the word', () => {
    // The false positive this guards: a Bash command that exited 0 with
    // "Error rate: 0%" on stdout, whose provenance artifact was emptied and so
    // never rode the wire, rendered as a failed tool call.
    expect(isToolResultFailure({ content: 'Error rate: 0%' })).toBe(false);
    expect(isToolResultFailure({ content: 'Errors were logged but the run passed.' })).toBe(false);
    expect(isToolResultFailure({ content: 'error handling is covered by 12 tests' })).toBe(false);
  });

  it('accepts every envelope a real producer writes', () => {
    // One case per distinct shape found in the sweep: the "ERROR: "/"Error: "
    // convention shared by the file, search, sandbox and preview tools, the
    // Task and RunWorkflow refusals, ExecuteCode's newline form, and the bare
    // envelope an empty stderr trims down to.
    expect(isToolResultFailure({ content: 'ERROR: Sandbox not initialized' })).toBe(true);
    expect(isToolResultFailure({ content: 'ERROR: Command failed (exit code 1)\nboom' })).toBe(true);
    expect(isToolResultFailure({ content: 'Error: could not start Task-3, admission refused.' })).toBe(true);
    expect(isToolResultFailure({ content: "Error: Provide exactly one of 'script', 'script_path', or 'workflow'." })).toBe(true);
    expect(isToolResultFailure({ content: 'ERROR\nTraceback (most recent call last):' })).toBe(true);
    expect(isToolResultFailure({ content: '  ERROR\n  ' })).toBe(true);
  });

  it('never flags results with artifacts or non-error content', () => {
    expect(isToolResultFailure({ content: 'Error: x', artifact: { task_id: 't1' } })).toBe(false);
    expect(isToolResultFailure({ content: 'dispatched ok' })).toBe(false);
    expect(isToolResultFailure({ content: ['Error: x'] })).toBe(false);
  });

  it('takes the wire status as authoritative, whatever the content reads like', () => {
    expect(isToolResultFailure({ content: '{"ok": true}', status: 'error' })).toBe(true);
    expect(isToolResultFailure({ content: 'anything', artifact: { a: 1 }, status: 'error' })).toBe(true);
  });

  it('lets an explicit success status outrank the "Refused:" rule', () => {
    // A direct MCP tool is free to print "Refused:" as ordinary output, which
    // is the false positive the wire status was carried to fix.
    expect(isToolResultFailure({ content: 'fine', status: 'success' })).toBe(false);
    expect(
      isToolResultFailure({
        content: 'Refused: 7 applications',
        status: 'success',
        toolName: 'mcp__moomoo__place_order',
      }),
    ).toBe(false);
  });

  it('keeps the "Error" rule firing under a success status', () => {
    // Producers do not all stamp their failures: a refused Task launch used to
    // ride the wire with the default 'success', and trusting it left the card
    // spinning forever because no channel ever opens to settle it.
    expect(
      isToolResultFailure({
        content: 'Error: could not start Task-3, admission refused.',
        status: 'success',
      }),
    ).toBe(true);
  });

  it('flags a refused direct MCP call from its content when no status rode along', () => {
    expect(
      isToolResultFailure({
        content: 'Refused: moomoo trading is not enabled for this connection.',
        toolName: 'mcp__moomoo__place_order',
      }),
    ).toBe(true);
    expect(
      isToolResultFailure({ content: 'refused: lowercase too', toolName: 'mcp__moomoo__place_order' }),
    ).toBe(true);
  });

  it('leaves "Refused:" output of a non-MCP tool alone', () => {
    // The false positive this guards: a successful Bash run whose stdout opens
    // with "Refused: 7 applications" rendered as a failed tool call. The
    // refusal envelope belongs to direct MCP tools; every other tool owns its
    // own stdout.
    expect(isToolResultFailure({ content: 'Refused: 7 applications', toolName: 'bash' })).toBe(false);
    // No name resolved (a result that outran its tool_calls event) keeps the
    // rule off rather than guessing.
    expect(isToolResultFailure({ content: 'Refused: 7 applications' })).toBe(false);
  });
});

describe('normalizeWireStatus', () => {
  it('passes terminal display statuses through', () => {
    expect(normalizeWireStatus('completed')).toBe('completed');
    expect(normalizeWireStatus('cancelled')).toBe('cancelled');
    expect(normalizeWireStatus('error')).toBe('error');
  });

  it('collapses failure spellings to error — including interrupted', () => {
    // Task HITL is descoped: an interrupted task run is a failure, matching
    // the server's history stamping. Before this lane existed, a live
    // chan_close{outcome: interrupted} fell through to 'completed' while a
    // history reload stamped the same task 'error'.
    expect(normalizeWireStatus('failed')).toBe('error');
    expect(normalizeWireStatus('interrupted')).toBe('error');
  });

  it('collapses live spellings to active', () => {
    expect(normalizeWireStatus('in_progress')).toBe('active');
    expect(normalizeWireStatus('running')).toBe('active');
    expect(normalizeWireStatus('active')).toBe('active');
  });

  it('returns null for unknown/absent values so callers keep their default', () => {
    expect(normalizeWireStatus(undefined)).toBeNull();
    expect(normalizeWireStatus(null)).toBeNull();
    expect(normalizeWireStatus('weird-legacy')).toBeNull();
  });
});

describe('deriveSubagentStatus', () => {
  it('returns terminal statuses verbatim, regardless of message shape', () => {
    for (const status of ['completed', 'cancelled', 'error'] as const) {
      expect(deriveSubagentStatus({ status, messages: [] })).toBe(status);
      expect(deriveSubagentStatus({ status, messages: [{}, {}] })).toBe(status);
    }
  });

  it('honors an explicit live status even with no messages (the bug)', () => {
    // A card known-running via a task event / active_tasks snapshot / accepted
    // resume must never regress to "initializing" just because its transcript
    // has not accumulated locally yet.
    expect(deriveSubagentStatus({ status: 'active', messages: [] })).toBe('active');
    expect(deriveSubagentStatus({ status: 'running', messages: [] })).toBe('active');
  });

  it('holds an explicit initializing until content streams, then promotes', () => {
    expect(deriveSubagentStatus({ status: 'initializing', messages: [] })).toBe('initializing');
    expect(deriveSubagentStatus({ status: 'initializing', messages: undefined })).toBe('initializing');
    // Streamed content is a positive signal — promote even if a late status
    // write still says 'initializing'.
    expect(deriveSubagentStatus({ status: 'initializing', messages: [{}] })).toBe('active');
  });

  it('falls back to transcript shape for a missing/unknown status', () => {
    expect(deriveSubagentStatus({ messages: [] })).toBe('initializing');
    expect(deriveSubagentStatus({ messages: undefined })).toBe('initializing');
    expect(deriveSubagentStatus({ status: 'weird-legacy', messages: [] })).toBe('initializing');
    expect(deriveSubagentStatus({ messages: [{}] })).toBe('active');
    expect(deriveSubagentStatus({ status: 'weird-legacy', messages: [{}] })).toBe('active');
  });
});
