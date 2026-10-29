import { useState } from 'react';
import { unreachableResult, verdictIs } from '@/pages/ChatAgent/components/mcp/mcpProbeVerdict';
import {
  formatApiErrorDetail,
  probeMcpServer,
  type CatalogServer,
  type McpProbeResult,
} from '@/pages/ChatAgent/utils/api';

/**
 * What saving a key from the callout led to, for the row it was saved on. A
 * remote row is checked again at once, from the host, with the headers it
 * already holds; one with no host-side check is only said to be saved.
 */
type KeyCheck = { row: string; at: string | null } & KeyOutcome;

export type KeyOutcome =
  | { phase: 'checking' }
  | { phase: 'checked'; result: McpProbeResult }
  | { phase: 'saved' };

export interface KeyFixState {
  /** The answer to the last key saved here, while it is still about this
   *  version of the row. */
  outcome: KeyOutcome | null;
  /** The re-check's verdict when it refused the new value. */
  failed: McpProbeResult | null;
  /** Whether a save is followed by a check, which the save button says. */
  checkable: boolean;
  recheck: () => Promise<void>;
}

/**
 * A key saved from a connector's page and the check that follows it. Its
 * verdict stands once it lands: the save purges the row's last check, so for
 * a moment the catalog says nothing at all, and the answer to "did that key
 * work" is this one. An edit to the row is a new question, so the verdict is
 * held to the version it was about.
 */
export function useKeyFix(catalog: CatalogServer | null): KeyFixState {
  const [check, setCheck] = useState<KeyCheck | null>(null);
  const outcome =
    catalog && check?.row === catalog.name && check.at === catalog.updated_at ? check : null;
  const failed =
    outcome?.phase === 'checked' && !verdictIs(outcome.result.verdict, 'ok') ? outcome.result : null;
  const checkable = catalog?.transport === 'http' && !!catalog.url;
  // Checked the way the add form checks an address, with the headers the row
  // holds and their refs resolved on the host, so the verdict is about the
  // value just written rather than whatever the background check reaches first.
  const recheck = async () => {
    if (!catalog) return;
    const row = catalog.name;
    const at = catalog.updated_at;
    if (!checkable || !catalog.url) {
      setCheck({ row, at, phase: 'saved' });
      return;
    }
    setCheck({ row, at, phase: 'checking' });
    let result: McpProbeResult;
    try {
      result = await probeMcpServer({ url: catalog.url, headers: catalog.headers });
    } catch (err) {
      result = unreachableResult(formatApiErrorDetail(err));
    }
    setCheck((prev) => (prev?.row === row ? { row, at, phase: 'checked', result } : prev));
  };
  return { outcome, failed, checkable, recheck };
}
