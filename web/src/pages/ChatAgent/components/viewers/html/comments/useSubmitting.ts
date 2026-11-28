import { useState } from 'react';

/**
 * One write from a comment's controls at a time: `busy` while it is out,
 * `failed` with what it threw once it was refused, cleared by the next.
 */
export function useSubmitting(): {
  busy: boolean;
  failed: { error: unknown } | null;
  run: (act: () => Promise<unknown>) => Promise<void>;
} {
  const [busy, setBusy] = useState(false);
  const [failed, setFailed] = useState<{ error: unknown } | null>(null);
  const run = async (act: () => Promise<unknown>) => {
    if (busy) return;
    setBusy(true);
    setFailed(null);
    try {
      await act();
    } catch (error) {
      setFailed({ error });
    } finally {
      setBusy(false);
    }
  };
  return { busy, failed, run };
}
