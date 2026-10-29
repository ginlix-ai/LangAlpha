/**
 * Copy for a request refused by the user's plan. Workspace and computer
 * actions both hit the same entitlement gate, so they read its refusals here.
 */
import type { useTranslation } from 'react-i18next';

import { isPlatformMode } from '@/config/hostMode';
import type { ResourceTier } from '@/types/api';

import { apiErrorDetailMessage, apiErrorStatus, formatApiErrorDetail } from './api';
import { tierLabel } from '../components/tierUi';

type Translate = ReturnType<typeof useTranslation>['t'];

/** Map entitlement failures (403 plan-gate / 429 quota) to actionable copy in
 *  platform mode; generic API detail otherwise. */
export function entitlementErrorMessage(err: unknown, t: Translate, tier?: ResourceTier): string {
  if (isPlatformMode) {
    const status = apiErrorStatus(err);
    if (status === 403) {
      return t('workspace.notOnPlan', 'Not available on your plan. Upgrade to unlock.');
    }
    if (status === 429) {
      // Prefer the platform's structured quota message when it forwards one
      // (detail: { message, type, current, limit, remaining }); fall back to
      // the localized generic copy otherwise.
      const platformMessage = apiErrorDetailMessage(err);
      if (platformMessage) return platformMessage;
      if (tier) {
        return t('workspace.tierLimitReached', "You've reached your {{tier}} workspace limit.", {
          tier: tierLabel(t, tier),
        });
      }
      return t('workspace.workspaceLimitReached', "You've reached your workspace limit.");
    }
  }
  return formatApiErrorDetail(err);
}
