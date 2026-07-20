/**
 * Locks the three-phase display contract of deriveSubagentStatus. The regression
 * this guards: a still-running subagent whose detail card carried an explicit
 * 'running'/'active' status but no locally-accumulated messages was rendered as
 * "Initializing" (the empty-messages branch discarded the live status), so its
 * detail view contradicted its inline chip's "Running".
 */
import { describe, it, expect } from 'vitest';
import { deriveSubagentStatus } from '../subagentStatus';

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
