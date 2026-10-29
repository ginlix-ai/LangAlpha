import { useBrokerages, useBuiltinMcpServers, useMcpCatalog } from '@/hooks/useMcpServers';
import {
  useCreateUserVaultSecret,
  useUpdateUserVaultSecret,
  useUserVaultSecrets,
} from '@/hooks/useUserVault';
import { useSecretReaders } from '@/hooks/useSecretUsage';

/** What the key menu says about one stored key beside its name. */
export interface SecretFacts {
  description: string;
  /** The connectors whose headers or env read this key; undefined while
   *  that is unknown. */
  usedBy: readonly string[] | undefined;
}

/** Why a name is not free: a connector of the user's own, or a name the app reserves. */
export type NameOwner = 'yours' | 'reserved';

/**
 * The names already in use on the account, so a clash is said while the name
 * is being chosen rather than after Add: the same names the backend refuses at
 * creation, the account's own rows, then the built-in servers and the shipped
 * brokerages it reserves. Read from the queries the Connectors list already
 * holds, so opening the form costs nothing the page has not fetched.
 */
export function useTakenNames(): ReadonlyMap<string, NameOwner> {
  const { data: catalog } = useMcpCatalog();
  const { data: builtins } = useBuiltinMcpServers();
  const { data: brokerages } = useBrokerages();
  const taken = new Map<string, NameOwner>();
  for (const b of builtins?.servers ?? []) taken.set(b.name, 'reserved');
  for (const b of brokerages ?? []) taken.set(b.name, 'reserved');
  for (const s of catalog?.servers ?? []) taken.set(s.name, 'yours');
  return taken;
}

/**
 * The stored keys a value field offers, each with its description and who
 * reads it, so picking one is not a guess between names, and the writes that
 * keep a typed value as one. Every `${vault:NAME}` ref resolves against the
 * account's Secrets, whichever surface opened the form, so that is where the
 * field reads and writes.
 */
export function useVaultKeys() {
  const { data: vault } = useUserVaultSecrets();
  const readers = useSecretReaders();
  const create = useCreateUserVaultSecret();
  const update = useUpdateUserVaultSecret();
  const facts = new Map<string, SecretFacts>();
  for (const s of vault?.secrets ?? []) {
    facts.set(s.name, {
      description: s.description ?? '',
      usedBy: readers ? (readers.get(s.name) ?? []) : undefined,
    });
  }
  return {
    names: [...facts.keys()],
    facts,
    create: create.mutateAsync,
    update: (name: string, value: string) => update.mutateAsync({ name, body: { value } }),
  };
}
