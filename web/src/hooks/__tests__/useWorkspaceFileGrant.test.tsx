/**
 * The grant is what a live report iframe loads under, so it is re-minted
 * while an hour is still left on it: a frame never sees its prefix expire.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import type { Mock } from 'vitest';
import { act } from '@testing-library/react';
import { focusManager } from '@tanstack/react-query';

import { renderHookWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';

vi.mock('@/pages/ChatAgent/utils/api/shareLinks', () => ({
  createFileGrant: vi.fn(),
}));

import { createFileGrant } from '@/pages/ChatAgent/utils/api/shareLinks';
import { useWorkspaceFileGrant } from '../useWorkspaceFileGrant';

const mockMint = createFileGrant as Mock;
const WS = 'ws-1';
const HOUR = 60 * 60_000;
const T0 = Date.parse('2026-09-24T12:00:00Z');

function grant(hoursLeft: number, prefix = '/api/v1/wsfiles/g/2f9c1a7e/') {
  return { prefix, expires_in: hoursLeft * 3600 };
}

async function tick(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms);
  });
}

describe('useWorkspaceFileGrant', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(T0);
    mockMint.mockReset();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('re-mints one hour before the grant expires, not before', async () => {
    mockMint.mockImplementation(async () => grant(12));
    const { result, queryClient } = renderHookWithProviders(() => useWorkspaceFileGrant(WS));

    await tick(0);
    expect(mockMint).toHaveBeenCalledTimes(1);
    expect(result.current.data?.prefix).toBe('/api/v1/wsfiles/g/2f9c1a7e/');

    // Fresh until the margin: a second consumer mounting now reads the cache.
    await tick(11 * HOUR - 60_000);
    expect(mockMint).toHaveBeenCalledTimes(1);
    expect(queryClient.getQueryState(queryKeys.fileGrants.workspace(WS))?.data).toBeDefined();

    // With one hour left, the entry renews on its own clock.
    mockMint.mockImplementation(async () => grant(12, '/api/v1/wsfiles/g/8e1b4d3c/'));
    await tick(60_000);
    expect(mockMint).toHaveBeenCalledTimes(2);
    await vi.waitFor(() => expect(result.current.data?.prefix).toBe('/api/v1/wsfiles/g/8e1b4d3c/'));
  });

  it('a window focus reads the cache until the renewal deadline', async () => {
    // A re-mint hands every open report a new prefix and reloads it.
    mockMint.mockImplementation(async () => grant(12));
    renderHookWithProviders(() => useWorkspaceFileGrant(WS));
    const refocus = () => act(() => {
      focusManager.setFocused(false);
      focusManager.setFocused(true);
    });

    await tick(0);
    await tick(6 * HOUR);
    refocus();
    await tick(0);
    expect(mockMint).toHaveBeenCalledTimes(1);

    // A tab that sleeps past the deadline renews the moment it is back.
    focusManager.setFocused(false);
    await tick(5 * HOUR);
    refocus();
    await tick(0);
    expect(mockMint).toHaveBeenCalledTimes(2);
    focusManager.setFocused(undefined);
  });

  it('a grant already inside the margin is renewed a minute later, never in a tight loop', async () => {
    mockMint.mockImplementation(async () => grant(0.5));
    renderHookWithProviders(() => useWorkspaceFileGrant(WS));

    await tick(0);
    expect(mockMint).toHaveBeenCalledTimes(1);
    await tick(59_000);
    expect(mockMint).toHaveBeenCalledTimes(1);
    await tick(1_000);
    expect(mockMint).toHaveBeenCalledTimes(2);
  });

  it('a refused mint is not asked again on the renewal clock', async () => {
    mockMint.mockRejectedValue(Object.assign(new Error('Not found'), { response: { status: 404 } }));
    const { result } = renderHookWithProviders(() => useWorkspaceFileGrant(WS));

    await tick(0);
    expect(mockMint).toHaveBeenCalledTimes(1);
    await vi.waitFor(() => expect(result.current.isError).toBe(true));
    await tick(10 * 60_000);
    expect(mockMint).toHaveBeenCalledTimes(1);
  });

  it('a workspace invalidation leaves the grant alone', async () => {
    // Settings, computer and row actions all invalidate the workspace family;
    // a re-mint there would hand every open report a new prefix and reload it.
    mockMint.mockImplementation(async () => grant(12));
    const { queryClient } = renderHookWithProviders(() => useWorkspaceFileGrant(WS));

    await tick(0);
    expect(mockMint).toHaveBeenCalledTimes(1);
    await act(async () => {
      await queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.all });
    });
    await tick(0);
    expect(mockMint).toHaveBeenCalledTimes(1);
  });

  it('does not mint without a workspace', async () => {
    renderHookWithProviders(() => useWorkspaceFileGrant(null));
    await tick(0);
    expect(mockMint).not.toHaveBeenCalled();
  });
});
