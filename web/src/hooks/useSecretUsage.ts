import { useMcpCatalog } from '@/hooks/useMcpServers';
import type { CatalogServer } from '@/pages/ChatAgent/utils/api';

const VAULT_REF = /\$\{vault:([A-Za-z_][A-Za-z0-9_]{0,127})\}/g;

/**
 * The stored keys one connector reads, each once: its headers and env, which
 * the row reports, and its args and address, where a ref is resolved all the
 * same (an import writes `--token=${vault:NAME}`).
 */
export function serverKeyRefs(
  server: Pick<CatalogServer, 'header_refs' | 'env_refs' | 'args' | 'url'>,
): string[] {
  const inline = [...server.args, server.url ?? ''].flatMap((value) =>
    [...value.matchAll(VAULT_REF)].map((m) => m[1]),
  );
  return [...new Set([...server.header_refs, ...server.env_refs, ...inline])];
}

/**
 * Which connectors read each stored key, from the `${vault:NAME}` refs every
 * catalog row already reports. One reading for every surface that names a key
 * (the Secrets list, the key picker, a delete or rotate confirm, a plugin's
 * keys) so none of them can disagree about what changing that key breaks.
 */
export function secretUsage(servers: readonly CatalogServer[]): Map<string, CatalogServer[]> {
  const usage = new Map<string, CatalogServer[]>();
  for (const server of servers) {
    for (const name of serverKeyRefs(server)) {
      const users = usage.get(name);
      if (users) users.push(server);
      else usage.set(name, [server]);
    }
  }
  return usage;
}

/**
 * The connectors that read each stored key, by name. Undefined until the
 * catalog loads, and left so if it never does: an empty reading would say
 * nothing reads a key, which is a reason to delete it.
 */
export function useSecretReaders(): ReadonlyMap<string, readonly string[]> | undefined {
  const { data } = useMcpCatalog();
  if (!data) return undefined;
  return new Map(
    [...secretUsage(data.servers)].map(([name, servers]) => [name, servers.map((s) => s.name)]),
  );
}

/**
 * Who reads a key, in a surface's own three sentences: nobody, the one
 * connector by name, or how many. The surfaces differ in what follows from the
 * count (delete it, rotate it, pick it), so each brings its keys; every key
 * may interpolate `connector` and `count`.
 */
export function readersSentence(
  readers: readonly string[],
  keys: { none: string; one: string; many: string },
): { key: string; values: { connector: string; count: number } } {
  const key = readers.length === 0 ? keys.none : readers.length === 1 ? keys.one : keys.many;
  return { key, values: { connector: readers[0] ?? '', count: readers.length } };
}
