import { useEffect, useState, type ReactNode } from 'react';
import PageLoading from '@/components/PageLoading/PageLoading';
import { hostedSignInUrl, leaveForHostedSignIn } from '@/lib/hostedSignIn';

/** How long the window may sit on its way out before the hand-off is taken to have failed. */
const HAND_OFF_STALL_MS = 10_000;

/**
 * Renders this app's sign-in page (`children`), or sends a desktop window to
 * the account portal's (see `hostedSignInUrl`). Decided once on mount, so a
 * render on the way out cannot flash this app's page. A window still here once
 * the stall time has passed was not let go (the shell held the navigation, or
 * the hand-off could not be recorded), and gets this app's page rather than a
 * spinner it cannot leave.
 */
export function HostedSignInGate({ children }: { children: ReactNode }) {
  const [hosted] = useState(hostedSignInUrl);
  const [stalled, setStalled] = useState(false);

  useEffect(() => {
    if (!hosted) return;
    leaveForHostedSignIn(hosted);
    const timer = setTimeout(() => setStalled(true), HAND_OFF_STALL_MS);
    return () => clearTimeout(timer);
  }, [hosted]);

  return hosted && !stalled ? <PageLoading /> : children;
}
