import { describe, expect, it } from 'vitest';
import { takesWorkspaces } from '../computerStatus';

describe('takesWorkspaces', () => {
  // The server refuses a move to, or a primary flip onto, every one it rejects.
  it.each([
    ['running', 'sbx-1', true],
    ['stopped', 'sbx-1', true],
    ['deleted', 'sbx-1', false],
    ['error', 'sbx-1', false],
    ['creating', null, false],
  ] as const)('%s with ref %s takes workspaces: %s', (status, provider_ref, expected) => {
    expect(takesWorkspaces({ status, provider_ref })).toBe(expected);
  });
});
