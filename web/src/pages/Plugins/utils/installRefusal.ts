import { mcpSignInNeeded, type McpSignInNeeded } from '@/pages/ChatAgent/utils/api/mcp';
import {
  multiplePluginCandidates,
  pluginSecretsRequired,
  type PluginCandidate,
  type PluginSecretRequest,
} from '@/pages/ChatAgent/utils/api/plugins';
import { refusalOf, type Refusal } from '@/pages/ChatAgent/utils/api/refusal';

/**
 * Why an install or update did not happen, read once for every surface that
 * sends one (the wizard, a catalog entry, an update). Nothing was written in
 * any of them, so each is answered by changing the input: pick a plugin, type
 * a key, sign in, or change what was sent. `refusal` is the error as said
 * anywhere that cannot act on the rest.
 */
export type InstallError =
  /** A repository that holds several plugins: pick one. */
  | { kind: 'choose'; candidates: PluginCandidate[]; refusal: Refusal }
  /** Keys the plugin declares that the vault does not hold. */
  | { kind: 'keys'; required: PluginSecretRequest[]; refusal: Refusal }
  /** A check found a key with no value, which the keys form can take too. */
  | { kind: 'missingKey'; refusal: Refusal }
  /** A server signs in first; the install travels with the sign-in. */
  | { kind: 'signIn'; servers: McpSignInNeeded[]; refusal: Refusal }
  | { kind: 'refused'; refusal: Refusal };

export function readInstallError(err: unknown): InstallError {
  const refusal = refusalOf(err);
  const candidates = multiplePluginCandidates(err);
  if (candidates) return { kind: 'choose', candidates, refusal };
  const required = pluginSecretsRequired(err);
  if (required) return { kind: 'keys', required, refusal };
  if (refusal.failures?.some((f) => f.missing_secrets.length > 0)) return { kind: 'missingKey', refusal };
  const servers = mcpSignInNeeded(err);
  if (servers) return { kind: 'signIn', servers, refusal };
  return { kind: 'refused', refusal };
}

/** A key connectors asked for that the plugin does not declare, and which
 *  connectors asked. */
export interface UndeclaredKey {
  name: string;
  servers: string[];
}

/** What the keys form asks for, and why the last attempt with it failed. */
export interface KeysAsk {
  required: PluginSecretRequest[];
  undeclared: UndeclaredKey[];
  refusal: Refusal | null;
}

/**
 * The keys form after an attempt's error, or null when the error is not the
 * form's to say: it neither asks for keys nor finds a form open.
 *
 * Undeclared keys accumulate rather than being read off the latest failure: a
 * check stops calling a key missing once it has a value, so a field drawn only
 * from the last refusal would vanish on the retry that sent it. A key a later
 * answer declares moves to the declared fields.
 */
export function nextKeysAsk(ask: KeysAsk | null, error: InstallError): KeysAsk | null {
  switch (error.kind) {
    case 'keys':
      return {
        required: error.required,
        undeclared: undeclaredKeys(error.required, ask?.undeclared ?? [], null),
        refusal: null,
      };
    case 'missingKey': {
      const required = ask?.required ?? [];
      return {
        required,
        undeclared: undeclaredKeys(required, ask?.undeclared ?? [], error.refusal),
        refusal: error.refusal,
      };
    }
    case 'refused':
      return ask && { ...ask, refusal: error.refusal };
    default:
      return null;
  }
}

function undeclaredKeys(
  required: PluginSecretRequest[],
  previous: readonly UndeclaredKey[],
  refusal: Refusal | null,
): UndeclaredKey[] {
  const declared = new Set(required.map((r) => r.name));
  const next = previous
    .filter((u) => !declared.has(u.name))
    .map((u) => ({ ...u, servers: [...u.servers] }));
  for (const failure of refusal?.failures ?? []) {
    for (const name of failure.missing_secrets) {
      if (declared.has(name)) continue;
      const known = next.find((u) => u.name === name);
      if (!known) next.push({ name, servers: [failure.server] });
      else if (!known.servers.includes(failure.server)) known.servers.push(failure.server);
    }
  }
  return next;
}
