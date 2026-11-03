/**
 * The review waits on the page from the moment its document has loaded, since
 * a slow grant or image would otherwise spend the wait before the page settled.
 */
import { act, render } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { SETTLE_CAP_MS } from '../pageProtocol';

type Live = {
  source: null;
  bridge: { ready: boolean; subscribe: () => () => void } | null;
  failed: boolean;
  onBridge: () => void;
};
const live = vi.hoisted(() => ({ value: null as unknown as Live }));
const frame = vi.hoisted(() => ({ onLoad: null as null | (() => void) }));
vi.mock('../liveFrame', () => ({ useLiveFrame: () => live.value }));
vi.mock('../BridgedFrame', () => ({
  BridgedFrame: ({ onLoad }: { onLoad?: () => void }) => {
    frame.onLoad = onLoad ?? null;
    return null;
  },
}));

import { PageScout } from '../PageScout';

const scout = (onSettled: () => void, onFailed: () => void = () => {}) => (
  <PageScout workspaceId="ws" path="a.html" onSettled={onSettled} onFailed={onFailed} />
);

describe('PageScout', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    live.value = { source: null, bridge: { ready: true, subscribe: () => () => {} }, failed: false, onBridge: () => {} };
    frame.onLoad = null;
  });
  afterEach(() => vi.useRealTimers());

  it('waits from the page loading, not from its hello', () => {
    const onSettled = vi.fn();
    render(scout(onSettled));
    act(() => { vi.advanceTimersByTime(SETTLE_CAP_MS + 5000); });
    expect(onSettled).not.toHaveBeenCalled();

    act(() => frame.onLoad?.());
    act(() => { vi.advanceTimersByTime(SETTLE_CAP_MS + 1000); });
    expect(onSettled).not.toHaveBeenCalled();
    act(() => { vi.advanceTimersByTime(1000); });
    expect(onSettled).toHaveBeenCalled();
  });

  it('lets the review go when the page never finishes loading', () => {
    const onSettled = vi.fn();
    render(scout(onSettled));
    act(() => { vi.advanceTimersByTime(30_000); });
    expect(onSettled).toHaveBeenCalled();
  });

  it('says at once that the page could not run when its grant never came', () => {
    const onSettled = vi.fn();
    const onFailed = vi.fn();
    live.value = { ...live.value, failed: true };
    render(scout(onSettled, onFailed));
    // Not settled: the list would go out without what the page's code builds.
    expect(onFailed).toHaveBeenCalled();
    expect(onSettled).not.toHaveBeenCalled();
  });
});
