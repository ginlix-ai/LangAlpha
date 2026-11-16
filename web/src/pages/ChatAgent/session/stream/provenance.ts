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
 * Maps a flat `provenance` SSE event to a `ProvenanceRecord`: the event minus its
 * envelope, so a new field on the event reaches the record with no wiring.
 * Shared by the live and replay paths, which both go through `handleProvenance`.
 */
export function provenanceEventToRecord(event: ProvenanceEvent): ProvenanceRecord {
  const {
    event: _event, _eventId, turn_index: _turn, response_id: _response, thread_id: _thread, ...record
  } = event;
  return record;
}
