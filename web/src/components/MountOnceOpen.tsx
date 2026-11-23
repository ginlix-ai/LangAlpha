import { Suspense, type ReactNode } from 'react';
import { StaleBuildBoundary } from '@/components/StaleBuildBoundary';
import { useOnceTrue } from '@/hooks/useOnceTrue';

/**
 * Renders a lazy dialog from the first time it opens, and keeps it mounted
 * after so a close animates out rather than cutting to nothing. Until then
 * its chunk is never asked for, which is what keeps a dialog hosted on every
 * page out of the first load. A tab left open across a deploy finds that
 * chunk gone: the dialog then stays shut and the stale-build toast offers the
 * reload, rather than the failure taking the page or the app down with it.
 */
export function MountOnceOpen({ open, children }: { open: boolean; children: ReactNode }) {
  if (!useOnceTrue(open)) return null;
  return (
    <StaleBuildBoundary variant="toast">
      <Suspense fallback={null}>{children}</Suspense>
    </StaleBuildBoundary>
  );
}
