import { useEffect } from 'react';

/** Leaves the app for a page outside it, such as the hosted onboarding, in place. */
export function ExternalRedirect({ to }: { to: string }) {
  useEffect(() => {
    window.location.replace(to);
  }, [to]);
  return null;
}
