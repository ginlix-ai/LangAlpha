import type { CatalogServer, McpProbeResult } from '@/pages/ChatAgent/utils/api';

/**
 * Fixture builders for API shapes several suites need.
 *
 * `CatalogServer` had two of these, one per test file, agreeing on the type and
 * disagreeing on which of its optional fields a fixture sets. That is the drift
 * that matters: a new required field breaks one file and leaves the other
 * quietly building a shape the app never sees. The field list lives here; what a
 * particular suite's fixtures mean stays local to that suite.
 */

/** Every field of a catalog row, at its most ordinary. */
export function catalogServer(over: Partial<CatalogServer> = {}): CatalogServer {
  return {
    name: 'placeholder_server',
    transport: 'stdio',
    command: 'npx',
    args: [],
    url: null,
    env_refs: [],
    header_refs: [],
    env: {},
    headers: {},
    description: '',
    instruction: '',
    tool_exposure_mode: 'summary',
    discovery_uses_secrets: false,
    enabled: true,
    created_at: null,
    updated_at: null,
    plugin_name: null,
    plugin_enabled: null,
    probe_kicked_at: null,
    ...over,
  };
}

/** A remote row: the only kind that carries the OAuth connect lifecycle. */
export function httpCatalogServer(over: Partial<CatalogServer> = {}): CatalogServer {
  return catalogServer({
    name: 'remote_connector',
    transport: 'http',
    command: null,
    url: 'https://mcp.example.com/sse',
    ...over,
  });
}

/**
 * A row's projection of the host-side check. The catalog never carries the
 * tool list (only the add form's own check does), so `tools` stays empty here.
 */
export function catalogProbe(over: Partial<McpProbeResult> = {}): McpProbeResult {
  return {
    verdict: 'ok',
    tools: [],
    server_info: null,
    error: '',
    http_status: null,
    missing_secrets: [],
    probed_at: '2026-01-01T00:00:00Z',
    ...over,
  };
}
