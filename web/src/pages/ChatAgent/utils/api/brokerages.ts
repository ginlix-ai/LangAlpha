import { api } from '@/api/client';
import type { CatalogServer } from './mcp';

/**
 * One shipped brokerage connector, as the backend describes it.
 *
 * The registry arrives over the wire rather than living in the client: a build
 * that hard-coded a broker's host would be a second place for it to be wrong,
 * and the host is where an OAuth token ends up. The quirks arrive as booleans
 * and the sentence each becomes is translated, so it lives with the copy.
 */
export interface Brokerage {
  name: string;
  label: string;
  url: string;
  /** The broker's own website, which is not the endpoint's host: an MCP
   *  endpoint sits on an API subdomain with no page behind it. */
  site: string;
  description: string;
  /**
   * The vendor's authorization server refuses a hosted callback, so only the
   * desktop shell's loopback listener can finish the flow. A browser tab gets
   * no error back either: the refusal happens on the vendor's own page.
   */
  native_callback_only: boolean;
  /**
   * The vendor allows one connected AI platform per account and drops the
   * previous one, so connecting is destructive to a connection elsewhere.
   */
  exclusive_connection: boolean;
  /**
   * What this vendor's tools can be granted in, in display order. Empty for a
   * vendor nothing is curated for, which reads as "nothing to choose" rather
   * than "everything": the backend answers the same way, and a connect that
   * names no group is granted none of them.
   */
  capabilities: CapabilityGroup[];
}

/**
 * One consent toggle offered when connecting a brokerage.
 *
 * The key is the fact and also the translation key; the words are this client's,
 * the same contract the quirk booleans above keep. `tone` is how loudly to draw
 * the row -- `neutral` is public or personal data, `caution` is the user's own
 * positions and money, `danger` places real orders -- and is left a plain string
 * because a tone this build has no styling for must still render.
 */
export interface CapabilityGroup {
  key: string;
  tone: string;
  /**
   * One of the steps between reading and placing an order (paper, preview,
   * staged, live). A fact about the group rather than a reading of its key, so
   * a group added later reaches the badges with no release here. Absent on a
   * backend that predates it, which reads as "not a rung" and costs a badge.
   */
  rung?: boolean;
  /**
   * Other groups this one needs granted to be in force. The consent dialog
   * links its switches by it, so no vendor's rule is restated in this build.
   * Absent on a backend that predates it, which links nothing.
   */
  requires?: string[];
}

export async function getBrokerages(): Promise<Brokerage[]> {
  const { data } = await api.get<{ brokerages: Brokerage[] }>('/api/v1/mcp/brokerages');
  return data.brokerages ?? [];
}

/** Turn one on or off; the backend creates its catalog row the first time. */
export async function setBrokerageEnabled(
  name: string,
  enabled: boolean,
): Promise<CatalogServer> {
  const { data } = await api.patch<CatalogServer>(
    `/api/v1/mcp/brokerages/${name}/enabled`,
    { enabled },
  );
  return data;
}
