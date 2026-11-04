/**
 * A visitor's frame keeps the address it loaded from when the page renews its
 * grant, so a long read is not reloaded under them. That address lapses with
 * its grant, so a frame that loses its document on an address since renewed
 * loads the renewed one, once. Lost on the current address, it stays: a reload
 * there could not help, and would repeat for a document that never says hello.
 */
import { act } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { renderWithProviders } from '@/test/utils';
import SharedPageView from '../SharedPageView';

const frames: { key: string; src: string; lose: () => void }[] = [];

vi.mock('@/pages/ChatAgent/components/viewers/html/BridgedFrame', () => ({
  BridgedFrame: ({ source, onUnbridged }: { source: { key: string; src: string }; onUnbridged: () => void }) => {
    frames.push({ key: source.key, src: source.src, lose: onUnbridged });
    return null;
  },
}));

function metadata(base: string) {
  return {
    kind: 'file', artifact_id: 'a1', seq: 2, path: 'r.html', name: 'r.html', title: null,
    frame_base: base, isolated: false,
  } as never;
}

const last = () => frames[frames.length - 1];

describe('SharedPageView: a frame and its grant', () => {
  beforeEach(() => { frames.length = 0; });

  it('keeps its document through a renewal while it holds', () => {
    const view = renderWithProviders(<SharedPageView code="c" metadata={metadata('/g1/')} onStale={() => {}} />);
    const first = last().key;
    view.rerender(<SharedPageView code="c" metadata={metadata('/g2/')} onStale={() => {}} />);
    expect(last().key).toBe(first);
  });

  it('loads the renewed address when it loses its document on an old one, once', () => {
    const onStale = vi.fn();
    const view = renderWithProviders(<SharedPageView code="c" metadata={metadata('/g1/')} onStale={onStale} />);
    const first = last().key;
    view.rerender(<SharedPageView code="c" metadata={metadata('/g2/')} onStale={onStale} />);

    act(() => last().lose());
    const reloaded = last();
    expect(reloaded.key).not.toBe(first);
    expect(reloaded.src).toContain('/g2/');

    act(() => reloaded.lose());
    expect(last().key).toBe(reloaded.key);
    expect(onStale).toHaveBeenCalledTimes(2);
  });

  it('stays where it is when it loses its document on the current address', () => {
    const onStale = vi.fn();
    renderWithProviders(<SharedPageView code="c" metadata={metadata('/g1/')} onStale={onStale} />);
    const first = last().key;

    act(() => last().lose());

    expect(last().key).toBe(first);
    expect(onStale).toHaveBeenCalledTimes(1);
  });
});
