import { useCallback, useEffect, useState } from 'react';

/**
 * Countdown gate for "resend email" buttons — mirrors the server-side send
 * rate limit (one email per 60s). Starts counting on mount (an email was
 * just sent) and restarts after each successful resend.
 */
export function useResendCooldown(seconds = 60) {
  const [secondsLeft, setSecondsLeft] = useState(seconds);

  useEffect(() => {
    if (secondsLeft <= 0) return;
    const id = setTimeout(() => setSecondsLeft((s) => s - 1), 1000);
    return () => clearTimeout(id);
  }, [secondsLeft]);

  const start = useCallback(() => setSecondsLeft(seconds), [seconds]);

  return { secondsLeft, isCoolingDown: secondsLeft > 0, start };
}
