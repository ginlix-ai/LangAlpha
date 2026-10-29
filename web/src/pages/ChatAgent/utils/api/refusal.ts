import { formatApiErrorDetail } from '@/api/errors';
import { mcpCheckFailures, type McpCheckFailure } from './mcp';
import { fatalDiagnostics, type PluginDiagnostic } from './plugins';

/**
 * Why a save or install did not happen, as one value. A check failure names
 * its servers in `failures`, and its `message` then only restates the first
 * of them; a plugin refusal carries its findings in `diagnostics`.
 */
export interface Refusal {
  message: string;
  failures: McpCheckFailure[] | null;
  diagnostics: PluginDiagnostic[];
}

export function refusalOf(err: unknown): Refusal {
  return {
    message: formatApiErrorDetail(err),
    failures: mcpCheckFailures(err),
    diagnostics: fatalDiagnostics(err),
  };
}

/** A refusal worded on this side, for a failure that never reached the server. */
export function refusalSaying(message: string): Refusal {
  return { message, failures: null, diagnostics: [] };
}
