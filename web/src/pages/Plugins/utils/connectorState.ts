import type { CatalogServer, McpOauthStatus, PluginInfo } from '@/pages/ChatAgent/utils/api';
import { needsOauthConnect, probeRowState } from '@/pages/ChatAgent/components/mcp/mcpState';
import { serverKeyRefs } from '@/hooks/useSecretUsage';
import { isEffectivelyEnabled } from './provenance';

/**
 * The key fix a failing connector offers in place of its warning: a key the
 * check found missing opens Secrets' add form with its name filled in, and a
 * key the server refused opens that key's own edit form.
 */
export interface KeyFix {
  kind: 'add' | 'replace';
  key: string;
}

/**
 * The one state a connector cell, its page, the rail's attention dots and the
 * page's summary line all read, so none of them disagree about what needs the
 * user. The Connect rule is the catalog row's, unchanged: an http row offers
 * Connect unless a check settled that it wants no sign-in.
 *
 * A problem outranks Off on purpose. A connector cannot be switched on until
 * it is connected, so for an unconnected one the fix is the way back on, and
 * hiding it behind "Off" would leave the user nothing to press.
 */
export type ConnectorState =
  /** Needs a sign-in, or a new one: the trailing slot is an outlined Connect.
   *  `holdsSwitch` is whether the switch waits for it. A known missing
   *  sign-in holds it; an http row nothing has checked yet does not, since
   *  switching a row on is the only thing that checks it, and holding it there
   *  would leave a header-authenticated server no way to ever be checked. */
  | { kind: 'connect'; status: McpOauthStatus | null; holdsSwitch: boolean }
  /** A check refused it (key rejected, unreachable, a key or vault value
   *  missing). `noteKey` is the reason; `wire` the server's own line when it
   *  sent one worth reading; `keys` the vault names it is about, missing ones
   *  first, then every ref on a rejection; `fix` the key to add or replace,
   *  null when the check named none. */
  | {
      kind: 'failing';
      noteKey: string;
      wire: string | null;
      severe: boolean;
      keys: string[];
      fix: KeyFix | null;
    }
  | { kind: 'off' }
  | { kind: 'on' };

export function connectorState(server: CatalogServer): ConnectorState {
  const status = server.oauth_status ?? null;
  const probe = probeRowState(server.probe?.verdict);
  // A revoked claim on a server a check found wants no sign-in is a leftover,
  // not a connection to repair.
  const claim = status === 'revoked' && probe?.oauth === 'no' ? null : status;
  // A check that settled something other than "sign in" (a key refused or
  // missing, nothing answering) is the problem to show, even on an http row
  // that still offers Connect: the fix it names comes first.
  if (!claim && probe?.noteKey && probe.oauth !== 'wants') {
    const missing = server.probe?.missing_secrets ?? [];
    const keys =
      missing.length > 0
        ? missing
        : server.probe?.verdict === 'credential_rejected'
          ? serverKeyRefs(server)
          : [];
    return {
      kind: 'failing',
      noteKey: probe.noteKey,
      wire: probe.wire ? server.probe?.error || null : null,
      severe: probe.tone === 'warning',
      keys,
      fix: keys.length > 0 ? { kind: missing.length > 0 ? 'add' : 'replace', key: keys[0] } : null,
    };
  }
  const unconnected =
    server.transport === 'http' &&
    needsOauthConnect(claim) &&
    (!!claim || !probe || probe.oauth !== 'no');
  if (unconnected) {
    return { kind: 'connect', status: claim, holdsSwitch: !!claim || probe?.oauth === 'wants' };
  }
  return isEffectivelyEnabled(server) ? { kind: 'on' } : { kind: 'off' };
}

/**
 * Why a connector cannot be switched on yet, or null when it can: a sign-in
 * it is known to need, or a check that refused it.
 */
export type SwitchOnBlock = 'connect' | 'failing' | null;

export function switchOnBlock(state: ConnectorState): SwitchOnBlock {
  if (state.kind === 'failing') return 'failing';
  return state.kind === 'connect' && state.holdsSwitch ? 'connect' : null;
}

/**
 * Which part of a summary a failing check's reason belongs to, so the rail's
 * tally and the page's summary line file each reason under the same words.
 */
export type FailureBucket = 'key' | 'rejected' | 'unreachable' | 'signIn';

export function failureBucket(noteKey: string): FailureBucket {
  switch (noteKey) {
    case 'mcp.probe.rowCredentialRejected':
      return 'rejected';
    case 'mcp.probe.rowUnreachable':
      return 'unreachable';
    // A server off http that answered with a sign-in: Connect cannot speak
    // for it, but what it wants is still a sign-in.
    case 'mcp.probe.rowOauth':
      return 'signIn';
    default:
      return 'key';
  }
}

/** The connectors a plugin installed, by its `plugin_name`. */
export function pluginConnectors(plugin: Pick<PluginInfo, 'name'>, servers: CatalogServer[]): CatalogServer[] {
  return servers.filter((s) => s.plugin_name === plugin.name);
}

/**
 * What a plugin's summary says about it: the worst thing among its
 * connectors. A key a check refused names the key; otherwise a pending
 * sign-in. Null when every connector it owns is fine (or it owns none).
 */
export type PluginProblem =
  | { kind: 'failing'; noteKey: string; count: number }
  | { kind: 'connect'; count: number };

export function pluginProblem(plugin: Pick<PluginInfo, 'name'>, servers: CatalogServer[]): PluginProblem | null {
  const states = pluginConnectors(plugin, servers).map(connectorState);
  const failing = states.filter((s): s is Extract<ConnectorState, { kind: 'failing' }> => s.kind === 'failing');
  if (failing.length > 0) {
    const worst = failing.find((s) => s.severe) ?? failing[0];
    return { kind: 'failing', noteKey: worst.noteKey, count: failing.length };
  }
  const connect = states.filter((s) => s.kind === 'connect').length;
  return connect > 0 ? { kind: 'connect', count: connect } : null;
}
