/**
 * Terms of Service a data vendor asks each user to accept before LangAlpha
 * sets up an account with it for them. The server marks a vendor pending the
 * first time the agent needs it for a user who has not accepted; the app puts
 * the terms to the user on whatever page is open.
 */
import { api } from '@/api/client';

export interface PendingVendorTerms {
  /** The vendor's id in the routes, e.g. `firecrawl`. */
  vendor: string;
  /** Its display name, which the copy names. */
  name: string;
  terms_url: string;
  requested_at: string;
}

/** The vendors whose terms wait on the user. */
export async function getPendingVendorTerms(): Promise<PendingVendorTerms[]> {
  const { data } = await api.get<{ pending: PendingVendorTerms[] }>('/api/v1/vendor-terms/pending');
  return data.pending;
}

/**
 * Accepting sets up the user's account with the vendor, so it answers only
 * once the account exists, which takes seconds.
 */
export async function acceptVendorTerms(vendor: string): Promise<{ vendor: string }> {
  const { data } = await api.post<{ vendor: string }>(`/api/v1/vendor-terms/${encodeURIComponent(vendor)}/accept`);
  return data;
}

/** "Not now": the server stops asking until the agent needs the vendor again. */
export async function dismissVendorTerms(vendor: string): Promise<void> {
  await api.post(`/api/v1/vendor-terms/${encodeURIComponent(vendor)}/dismiss`);
}
