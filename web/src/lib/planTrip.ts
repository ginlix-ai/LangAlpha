/**
 * The trip to the account console's plan page and back, as the console
 * reads it: `return`, the app path to come back to once the plan changes,
 * `for`, why the user came, which the page words itself by, and `largest`,
 * the biggest computer the app can build. The one
 * place the app builds that address, so the two params stay in step with
 * the console.
 */
import { PLAN_URL } from '@/config/supportLinks';
import type { ResourceTier } from '@/types/api';

const PLAN_RETURN_PARAM = 'return';
const PLAN_REASON_PARAM = 'for';
const PLAN_LARGEST_PARAM = 'largest';

/** Why the user goes to change plan, where the plan page says so. */
export type PlanReason = 'claude-code';

/** The plan page, asked to send the user back to `back`; null where the
 *  build names no plan page. `largest` is the largest computer the user's
 *  provider builds, so the page names no plan's computer bigger than that. */
export function planPageUrl(back: string, reason?: PlanReason, largest?: ResourceTier | null): string | null {
  if (!PLAN_URL) return null;
  const params = new URLSearchParams({ [PLAN_RETURN_PARAM]: back });
  if (reason) params.set(PLAN_REASON_PARAM, reason);
  if (largest) params.set(PLAN_LARGEST_PARAM, largest);
  return `${PLAN_URL}?${params.toString()}`;
}
