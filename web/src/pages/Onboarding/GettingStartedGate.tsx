import { lazy, Suspense } from 'react';
import { useOnboarding } from './OnboardingProvider';
import type { GettingStartedLauncherProps } from './engine/GettingStarted';

// Lazy so the checklist, its styles and icons stay out of the main chunk: most
// sessions belong to someone who finished or hid it, and never fetch it.
const GettingStartedLauncher = lazy(() =>
  import('./engine/GettingStarted').then((m) => ({ default: m.GettingStartedLauncher }))
);

/** The sidebar's getting-started entry, loaded only while it has something to show. */
export function GettingStartedGate(props: GettingStartedLauncherProps) {
  const { gettingStarted } = useOnboarding();
  if (!gettingStarted.visible) return null;
  return (
    <Suspense fallback={null}>
      <GettingStartedLauncher {...props} />
    </Suspense>
  );
}
