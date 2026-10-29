import { Suspense } from 'react';
import { describe, expect, it, vi } from 'vitest';
import { act, render, screen } from '@testing-library/react';
import { lazyWithMotion, preloadableLazy } from '../lazyWithMotion';

const Page = () => <span>page</span>;

function mount(Component: React.ComponentType) {
  return render(
    <Suspense fallback={<span>fallback</span>}>
      <Component />
    </Suspense>,
  );
}

describe.each([
  ['lazyWithMotion', lazyWithMotion],
  ['preloadableLazy', preloadableLazy],
] as const)('%s', (_name, lazyLoad) => {
  it('suspends on a first render nothing loaded ahead', async () => {
    const Lazy = lazyLoad(() => Promise.resolve({ default: Page }));
    mount(Lazy);
    expect(screen.getByText('fallback')).toBeInTheDocument();
    expect(await screen.findByText('page')).toBeInTheDocument();
  });

  it('renders at once, with no fallback, once preloaded', async () => {
    const load = vi.fn(() => Promise.resolve({ default: Page }));
    const Lazy = lazyLoad(load);
    await Lazy.preload();
    act(() => {
      mount(Lazy);
    });
    expect(screen.queryByText('fallback')).toBeNull();
    expect(screen.getByText('page')).toBeInTheDocument();
    // The preload's load is the mount's: the chunk is asked for once.
    expect(load).toHaveBeenCalledTimes(1);
  });

  it('loads again after a preload that failed', async () => {
    const load = vi
      .fn<() => Promise<{ default: typeof Page }>>()
      .mockRejectedValueOnce(new Error('chunk gone'))
      .mockResolvedValue({ default: Page });
    const Lazy = lazyLoad(load);
    await expect(Lazy.preload()).rejects.toThrow('chunk gone');
    mount(Lazy);
    expect(await screen.findByText('page')).toBeInTheDocument();
    expect(load).toHaveBeenCalledTimes(2);
  });
});
