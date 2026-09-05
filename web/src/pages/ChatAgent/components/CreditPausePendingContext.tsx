import React, { createContext, useContext, useMemo } from 'react';
import type { CreditPauseState } from '@/types/chat';

// Consumed at the leaf (SubagentTaskMessageContent) so the task cards read the
// message's pause state without every block in between carrying a prop for it.
// Same pattern as SubagentTelemetryContext.
const CreditPausePendingContext = createContext(false);

export function useCreditPausePending(): boolean {
  return useContext(CreditPausePendingContext);
}

/** `resuming` still counts as pending: the request is in flight and no run has
 *  opened, so the tasks really are stopped until admission answers. */
export function CreditPausePendingProvider({
  creditPauses,
  children,
}: {
  creditPauses?: Record<string, CreditPauseState>;
  children: React.ReactNode;
}): React.ReactElement {
  const pending = useMemo(
    () => Object.values(creditPauses ?? {}).some((p) => p.status !== 'resumed'),
    [creditPauses],
  );
  return (
    <CreditPausePendingContext.Provider value={pending}>
      {children}
    </CreditPausePendingContext.Provider>
  );
}
