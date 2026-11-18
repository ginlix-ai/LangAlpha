// @vitest-environment node
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';

vi.mock('@/api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/api/client')>();
  return { ...actual, api: { post: vi.fn() } };
});

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('@/lib/authToken', () => ({
  getAuthHeaders: vi.fn(async () => ({})),
  getAccessToken: vi.fn(async () => null),
}));

import { api, ApiError } from '@/api/client';
import { getBrowserView, navigateBrowser, setBrowserControl } from '../sandbox';

const mockPost = api.post as unknown as Mock;

function refusal(status: number, detail = 'Not Found'): ApiError {
  return new ApiError(`Request failed with status code ${status}`, 'ERR_BAD_REQUEST', {
    data: { detail }, status, statusText: '', headers: {},
  });
}

beforeEach(() => {
  mockPost.mockReset();
});

describe('the browser routes', () => {
  it('asks for the view, and to start it only when told, giving a start longer to answer', async () => {
    mockPost.mockResolvedValue({ data: { state: 'idle' } });
    await getBrowserView('ws1');
    const [, look, { timeout: lookTimeout }] = mockPost.mock.lastCall!;
    expect(mockPost).toHaveBeenLastCalledWith('/api/v1/workspaces/ws1/sandbox/browser-view', {}, expect.anything());
    expect(look).toEqual({});
    await getBrowserView('ws1', true);
    const [, start, { timeout: startTimeout }] = mockPost.mock.lastCall!;
    expect(start).toEqual({ start: true });
    // Bounded, so a request the server never answers ends in the tab's Try again.
    expect(lookTimeout).toBeGreaterThan(0);
    expect(startTimeout).toBeGreaterThan(lookTimeout);
  });

  it('reads a server without the view route as no browser on offer', async () => {
    mockPost.mockRejectedValue(refusal(404));
    await expect(getBrowserView('ws1')).resolves.toEqual({ state: 'unavailable' });
  });

  it('leaves a workspace the route cannot find to the caller, as a failure', async () => {
    mockPost.mockRejectedValue(refusal(404, 'Workspace not found'));
    await expect(getBrowserView('ws1')).rejects.toBeInstanceOf(ApiError);
    await expect(setBrowserControl('ws1', 'h1', 'take')).rejects.toBeInstanceOf(ApiError);
  });

  it('still fails on any other refusal, which asking again may get past', async () => {
    mockPost.mockRejectedValue(refusal(503));
    await expect(getBrowserView('ws1')).rejects.toBeInstanceOf(ApiError);
    mockPost.mockRejectedValue(new Error('offline'));
    await expect(getBrowserView('ws1')).rejects.toThrow('offline');
  });

  it('names the holder and what it does with its hold', async () => {
    mockPost.mockResolvedValue({ data: { held: true } });
    await expect(setBrowserControl('ws1', 'h1', 'renew')).resolves.toBe(true);
    expect(mockPost).toHaveBeenLastCalledWith(
      '/api/v1/workspaces/ws1/sandbox/browser-control', { holder: 'h1', action: 'renew' }, expect.anything(),
    );
    // Bounded under the hold's minute, so a renewal that never comes back fails in time.
    const [, , { timeout }] = mockPost.mock.lastCall!;
    expect(timeout).toBeGreaterThan(0);
    expect(timeout).toBeLessThan(60_000);
    mockPost.mockResolvedValue({ data: { held: false } });
    await expect(setBrowserControl('ws1', 'h1', 'release')).resolves.toBe(false);
  });

  it('sends an address only with an open, as the route has always taken it', async () => {
    mockPost.mockResolvedValue({ data: { done: true } });
    await expect(navigateBrowser('ws1', 'h1', { action: 'open', url: 'example.com' })).resolves.toBeNull();
    expect(mockPost).toHaveBeenLastCalledWith(
      '/api/v1/workspaces/ws1/sandbox/browser-navigate', { holder: 'h1', action: 'open', url: 'example.com' }, expect.anything(),
    );
    await navigateBrowser('ws1', 'h1', { action: 'back' });
    expect(mockPost).toHaveBeenLastCalledWith(
      '/api/v1/workspaces/ws1/sandbox/browser-navigate', { holder: 'h1', action: 'back' }, expect.anything(),
    );
    // Bounded, so a navigation the server never answers unlocks the address.
    expect(mockPost.mock.lastCall![2].timeout).toBeGreaterThan(0);
  });

  it('reads why a navigation did nothing, and leaves a failed request to the caller', async () => {
    mockPost.mockResolvedValue({ data: { done: false, reason: 'bad_address' } });
    await expect(navigateBrowser('ws1', 'h1', { action: 'open', url: 'x' })).resolves.toBe('bad_address');
    mockPost.mockResolvedValue({ data: { done: false } });
    await expect(navigateBrowser('ws1', 'h1', { action: 'reload' })).resolves.toBe('failed');
    mockPost.mockRejectedValue(refusal(503));
    await expect(navigateBrowser('ws1', 'h1', { action: 'reload' })).rejects.toBeInstanceOf(ApiError);
  });

  it('reads a server without the control route as nothing held', async () => {
    mockPost.mockRejectedValue(refusal(404));
    await expect(setBrowserControl('ws1', 'h1', 'take')).resolves.toBe(false);
    mockPost.mockRejectedValue(refusal(500));
    await expect(setBrowserControl('ws1', 'h1', 'take')).rejects.toBeInstanceOf(ApiError);
  });
});
