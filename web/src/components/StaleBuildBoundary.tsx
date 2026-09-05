import React from 'react';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';
import { isStaleBuildError, reportStaleBuild } from '@/lib/staleBuild';

/**
 * Catches a lazy route whose chunk a deploy deleted, and rethrows everything
 * else.
 *
 * Without this, a failed `React.lazy` import has no boundary above it anywhere:
 * `<Suspense>` handles pending promises, not rejections, so the throw walks past
 * every provider and React unmounts the whole root. The user gets a blank page
 * rather than a broken pane.
 */

function StaleBuildFallback({ variant }: { variant: 'app' | 'pane' }) {
  const { t } = useTranslation();
  const ref = React.useRef<HTMLDivElement>(null);

  // An assertive live region announces its subtree but does not move the caret,
  // and the view the user navigated to never mounted — so focus is left on a
  // detached node and falls back to document.body. A keyboard user lands at the
  // top of the document with no sign the pane changed. Put focus where the only
  // working control is.
  React.useEffect(() => {
    ref.current?.focus();
  }, []);

  // `app` replaces the whole document, so its title is the page's only heading.
  const Heading = variant === 'app' ? 'h1' : 'h2';

  return (
    <div
      ref={ref}
      tabIndex={-1}
      className={cn(
        'flex flex-col items-center justify-center gap-4 px-6 text-center outline-none',
        // Not h-screen: 100vh sits behind mobile Safari's toolbar, which is
        // where the Reload button would land — and it is the only way out of
        // this screen. App.css makes the same swap for .app-layout.
        variant === 'app' ? 'h-[100dvh]' : 'h-full',
      )}
      style={{ color: 'var(--color-text-secondary)' }}
    >
      {/* This is the blocking case, not the ambient one: the user asked for a
          view and it did not open. The toast copy ("a new version is
          available") would describe it as optional news. */}
      <div role="alert">
        <Heading
          className="title-font text-base font-medium"
          style={{ color: 'var(--color-text-primary)' }}
        >
          {t('common.staleBuild.blockedTitle')}
        </Heading>
        <p className="mt-2 text-sm">{t('common.staleBuild.blockedDescription')}</p>
      </div>
      <Button onClick={() => window.location.reload()}>{t('common.staleBuild.reload')}</Button>
    </div>
  );
}

interface Props {
  children: React.ReactNode;
  /** `app` fills the viewport, `pane` fills the routed content area. */
  variant?: 'app' | 'pane';
}

interface State {
  /** Whether anything was thrown, which `error` alone cannot say — a rejected
   *  loader may hand us `null` or `undefined`, and reading that as "no error"
   *  re-renders the children that just threw, in a loop. */
  caught: boolean;
  error: unknown;
  stale: boolean;
}

export class StaleBuildBoundary extends React.Component<Props, State> {
  state: State = { caught: false, error: null, stale: false };

  static getDerivedStateFromError(error: unknown): State {
    return { caught: true, error, stale: isStaleBuildError(error) };
  }

  componentDidCatch(error: unknown): void {
    // Reached only on the stale branch. componentDidCatch runs in the commit
    // phase, and the rethrow below happens during render, so for an ordinary
    // error this never commits and never logs — React reports that one itself.
    // Logging the stale branch is the part that matters: absorbing a chunk
    // failure silently would hide a real /assets/* 404 regression behind a
    // friendly reload prompt.
    console.error('[StaleBuildBoundary]', error);
    // silent: this boundary is about to render the same message as a full-pane
    // card, and the toast would put a byte-identical second notice beside it.
    // The call still takes the once-only latch, so a later signal cannot stack
    // a toast on top of the card.
    if (isStaleBuildError(error)) reportStaleBuild('chunk', { silent: true });
  }

  render(): React.ReactNode {
    const { caught, error, stale } = this.state;

    if (caught && !stale) {
      // React has no "decline to handle" API. Returning null here would make
      // React consider the error handled, re-render the same children, and
      // throw again in a loop. Throwing during render propagates to the next
      // boundary up; with none above, that unmounts the root exactly as it did
      // before this boundary existed, so no ordinary bug is masked by it.
      throw error;
    }

    if (stale) return <StaleBuildFallback variant={this.props.variant ?? 'app'} />;
    return this.props.children;
  }
}

export default StaleBuildBoundary;
