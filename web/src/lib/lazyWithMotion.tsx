import { lazy, type ComponentProps, type ComponentType, type LazyExoticComponent } from 'react';

/** A component from `lazyWithMotion`, which can be loaded before it renders. */
export type PreloadableLazy<T extends ComponentType<any>> = LazyExoticComponent<T> & {
  /** Loads the component and framer beside it, so its first render does not
   *  suspend. Rejects as the load does. */
  preload: () => Promise<unknown>;
};

/**
 * `React.lazy` for a component that eager code renders: the seam where a
 * subtree first reaches framer-motion. framer is off the entry chunk, so no
 * `MotionConfig` can sit at the app root; each seam wraps what it loads in
 * `reducedMotion="user"` instead, which collapses transform and layout
 * animations to instant for a reader who asked the OS for less motion (opacity
 * still animates). Context reaches portals, so one wrapper covers everything
 * the subtree opens. framer loads beside the chunk, never after it.
 *
 * An eager `React.lazy` that skips this helper leaves its motion ignoring the
 * reader's setting, silently. The reverse costs a download: a root with no
 * motion anywhere below it stays on plain `React.lazy`, or its first render
 * waits on framer for nothing.
 */
export function lazyWithMotion<
  // React.lazy's own constraint: `any` is what admits every props type.
  T extends ComponentType<any>,
>(
  load: () => Promise<{ default: T }>,
): PreloadableLazy<T> {
  return preloadableLazy(async () => {
    const [{ default: Component }, MotionConfig] = await Promise.all([
      load(),
      // Destructured right in the `.then`, the shape the bundler reads as
      // "only MotionConfig". Handed the whole namespace, it re-exports all of
      // framer from a chunk of its own, 11 kB gz on every route.
      import('./framer').then(({ MotionConfig }) => MotionConfig),
    ]);
    function WithMotion(props: ComponentProps<T>) {
      return (
        <MotionConfig reducedMotion="user">
          <Component {...props} />
        </MotionConfig>
      );
    }
    // Props pass through untouched, so the wrapper keeps T's contract.
    return { default: WithMotion as unknown as T };
  });
}

/**
 * `React.lazy` that can be loaded before it renders, for a root with no motion
 * below it. Loaded ahead, it is handed over by a thenable that answers at
 * once, which React.lazy takes as resolved and renders without suspending. A
 * settled promise still answers a tick later, and a boundary mounting fresh
 * shows its fallback for that tick, then holds the content behind it for
 * React's fallback throttle, 300ms: so warming a chunk alone does not spare a
 * first visit the loader.
 */
export function preloadableLazy<
  // React.lazy's own constraint: `any` is what admits every props type.
  T extends ComponentType<any>,
>(
  load: () => Promise<{ default: T }>,
): PreloadableLazy<T> {
  let loaded: { default: T } | null = null;
  let loading: Promise<{ default: T }> | null = null;
  const resolve = () =>
    (loading ??= load().then(
      (module) => (loaded = module),
      (error: unknown) => {
        // Not kept: the next try imports again, as a mount after a failed
        // preload always has.
        loading = null;
        throw error;
      },
    ));
  const component = lazy(() => {
    const ready = loaded;
    if (!ready) return resolve();
    // React reads any thenable here, though the type asks for a promise.
    return { then: (fulfil: (m: { default: T }) => void) => fulfil(ready) } as unknown as Promise<{ default: T }>;
  });
  return Object.assign(component, { preload: resolve });
}
