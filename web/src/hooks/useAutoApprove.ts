import { usePreferences } from './usePreferences';
import { useUpdatePreferences } from './useUpdatePreferences';

/** The agent actions a user can approve in advance, by the `other_preference`
 *  key that holds each. The server reads the same keys when the model proposes
 *  the action (`src/tools/secretary/approvals.py`). */
export const AUTO_APPROVE = {
  handoffs: 'auto_approve_handoffs',
  workspaceCreation: 'auto_approve_workspace_creation',
} as const;

export type AutoApproveKey = (typeof AUTO_APPROVE)[keyof typeof AUTO_APPROVE];

export function useAutoApprove(key: AutoApproveKey) {
  const { preferences } = usePreferences();
  const update = useUpdatePreferences();
  const other = preferences?.other_preference as Record<string, unknown> | undefined;
  return {
    enabled: other?.[key] === true,
    // Asking is the default, so turning this off deletes the key (null).
    set: (on: boolean) => update.mutateAsync({ other_preference: { [key]: on ? true : null } }),
  };
}

/**
 * The "Always allow" button on an approval card: saves the setting, then
 * answers the card. Only the write, since the card never shows the setting.
 * `saving` follows the write, so a card whose answer went nowhere (another
 * card holds the pending interrupt) is answerable again once it settles.
 */
export function useApproveAlways(key: AutoApproveKey) {
  const update = useUpdatePreferences();
  return {
    // Saved before the answer goes out, so the turn it resumes already sees the
    // setting. A failed save still approves this one; the next one asks again.
    approveAlways: (then: () => void) => {
      void update.mutateAsync({ other_preference: { [key]: true } }).catch(() => {}).finally(then);
    },
    saving: update.isPending,
  };
}
