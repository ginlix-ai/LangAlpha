/**
 * The readable name of a subscription plan. The token carries a bare code
 * (`plus`, `team`) that is no name a user knows the plan by; an unknown code
 * gets no label rather than the code itself.
 */

/** ChatGPT plans by the code in the sign-in token. OpenAI renamed Team to
 *  Business, and both codes are still issued. */
const CHATGPT_PLANS: Record<string, string> = {
  free: 'ChatGPT Free',
  go: 'ChatGPT Go',
  plus: 'ChatGPT Plus',
  pro: 'ChatGPT Pro',
  team: 'ChatGPT Business',
  business: 'ChatGPT Business',
  enterprise: 'ChatGPT Enterprise',
  edu: 'ChatGPT Edu',
  education: 'ChatGPT Edu',
};

/** Null for a plan with no known name, and for Claude, whose plan is not read. */
export function planLabel(provider: string, planType: string | null | undefined): string | null {
  if (!planType) return null;
  if (provider !== 'codex-oauth') return null;
  return CHATGPT_PLANS[planType.trim().toLowerCase()] ?? null;
}
