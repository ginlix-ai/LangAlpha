/**
 * Provenance record construction shared by the live and replay paths.
 */

import type { ProvenanceEvent } from '@/types/sse';
import type { ProvenanceRecord } from '@/types/chat';
import { provenanceMcpKey } from '@/types/chat';

/**
 * Builds the immutable `provenanceRecords` key for a provenance record.
 *
 * web_search emits multiple records that share one `tool_call_id` (one per
 * result URL), so the key prefixes the tool_call_id with `source_type` +
 * `identifier` to keep every distinct source — grouping by tool_call_id while
 * never dropping a sibling URL. For `mcp_tool` the identifier is `"server:tool"`
 * (shared across calls), so the per-access discriminator (args fingerprint +
 * result hash, see {@link provenanceMcpKey}) is appended too — otherwise two
 * calls to one tool with different args (get_stock_data for AAPL vs NVDA), or
 * even the same args returning different data (live market data), collide and
 * the last silently overwrites the first. Market-watch stamps have the same
 * problem with neither safeguard (`tool_call_id` is null by design and one
 * symbol refreshes many times per turn), so they discriminate on
 * `result_sha256` — each refresh is a distinct snapshot the agent reasoned
 * over, while byte-identical throttled replays still collapse.
 */
export function provenanceRecordKey(record: {
  tool_call_id?: string;
  source_type: string;
  identifier: string;
  detail?: string | null;
  args_fingerprint?: Record<string, unknown> | null;
  result_sha256?: string | null;
}): string {
  const base = `${record.tool_call_id || ''}:${record.source_type}:${record.identifier}`;
  const mcp = provenanceMcpKey(
    record as Pick<ProvenanceRecord, 'source_type' | 'args_fingerprint' | 'result_sha256'>,
  );
  if (mcp) return `${base}:${mcp}`;
  if (record.detail === 'market_watch' && record.result_sha256) {
    return `${base}:${record.result_sha256}`;
  }
  return base;
}

/**
 * Maps a flat `provenance` SSE event to a `ProvenanceRecord`. Shared by the live
 * and replay paths — both go through `handleProvenance` — so a new field on the
 * event only has to be wired up in one place.
 */
export function provenanceEventToRecord(event: ProvenanceEvent): ProvenanceRecord {
  return {
    record_id: event.record_id,
    agent: event.agent,
    timestamp: event.timestamp,
    source_type: event.source_type,
    identifier: event.identifier,
    title: event.title,
    detail: event.detail,
    provider: event.provider,
    tool_call_id: event.tool_call_id,
    args_fingerprint: event.args_fingerprint,
    args: event.args,
    result_sha256: event.result_sha256,
    result_size: event.result_size,
    result_snippet: event.result_snippet,
  };
}
