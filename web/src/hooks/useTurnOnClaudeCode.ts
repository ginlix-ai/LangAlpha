import { useEffect, useRef } from 'react';
import type { ClaudeCodeSetup } from '@/api/model';
import { useFeatures, useSetFeatureOverride } from '@/hooks/useFeatures';
import { authSessionCheck } from '@/lib/authResets';
import { CLAUDE_CODE_FEATURE } from '@/lib/claudeCode';

/**
 * Choosing Claude Code (`answered`) turns it on, once, where the user may,
 * so the token saved next runs. A turn-on that failed is offered
 * again (`failed`, `retry`), as is a read of the flags that failed, without
 * which it is not known whether Claude Code is on. Once per account: a new
 * account turns it on for itself.
 */
export function useTurnOnClaudeCode(answered: boolean, setup: ClaudeCodeSetup | null) {
  const features = useFeatures();
  const flag = features.data?.find((f) => f.key === CLAUDE_CODE_FEATURE)?.gate;
  const toggle = useSetFeatureOverride();
  const turnedOn = useRef<(() => boolean) | null>(null);
  const off = answered && setup?.on === false;
  const mayTurnOn = off && (flag === 'opt_in' || flag === 'opt_out');
  useEffect(() => {
    if (!mayTurnOn || turnedOn.current?.()) return;
    turnedOn.current = authSessionCheck();
    toggle.mutate({ key: CLAUDE_CODE_FEATURE, enabled: true });
  }, [mayTurnOn, toggle]);

  const flagsUnread = answered && features.isError && !features.data;
  return {
    pending: (mayTurnOn && !toggle.isSuccess && !toggle.isError) || toggle.isPending || (off && features.isLoading),
    // Still off once this account's turn-on ended, refused or not.
    failed: flagsUnread || (mayTurnOn && (toggle.isError || toggle.isSuccess)),
    retry: () => (flagsUnread ? void features.refetch() : toggle.mutate({ key: CLAUDE_CODE_FEATURE, enabled: true })),
  };
}
