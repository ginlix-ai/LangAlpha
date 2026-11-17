/**
 * Holds the app behind the desktop app's first run until it has its answer.
 * The app under it (Home's warm-up and the welcome chat among it) mounts
 * only then, so nothing builds a cloud computer before the user says where
 * their agents work.
 */
import { Suspense, useState, type ReactNode } from 'react';

import PageLoading from '@/components/PageLoading/PageLoading';
import { StaleBuildBoundary } from '@/components/StaleBuildBoundary';
import { lazyWithMotion } from '@/lib/lazyWithMotion';

import { useComputerChoiceUnasked } from './computerChoiceGate';

const ThisMacFirstRun = lazyWithMotion(() => import('./ThisMacFirstRun'));

export function ThisMacFirstRunGate({ children }: { children: ReactNode }) {
  const unasked = useComputerChoiceUnasked();
  // Once shown, the first run stays until it says it is over, answered or
  // with nothing to ask (this Mac is already a computer, or this server
  // cannot take it): it marks this Mac asked midway, and the page still has
  // its last step to show. Over is for the rest of the session.
  const [run, setRun] = useState<'idle' | 'showing' | 'over'>('idle');
  if (unasked && run === 'idle') setRun('showing');
  if (run === 'showing' || (unasked && run === 'idle')) {
    return (
      <Suspense fallback={<PageLoading />}>
        <StaleBuildBoundary>
          <ThisMacFirstRun onDone={() => setRun('over')} />
        </StaleBuildBoundary>
      </Suspense>
    );
  }
  return children;
}
