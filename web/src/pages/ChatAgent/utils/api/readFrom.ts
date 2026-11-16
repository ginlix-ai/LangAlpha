// Apart from files.ts because the message API, which the entry loads, lists
// files too: importing these from files.ts put the whole file API, with its
// download progress UI, on the first page load.

/**
 * Sent with every read, listing, resolve and download: this app shows a
 * backup answered for a computer that is away as one, and never edits it, so
 * the server may answer from it. A client without it gets the machine's 503.
 */
export const BACKUP_OK = { backup_ok: true } as const;

/**
 * Where a read's answer came from: the computer, or the backup standing in
 * for it. `unreachable` is the connect code of a computer that did not
 * answer, null for a stopped workspace's backup.
 */
export type ReadFrom = { kind: 'live' } | { kind: 'backup'; unreachable: string | null };

export const LIVE: ReadFrom = { kind: 'live' };

/** A listing or a text read marks an answer from the backup in its body. */
export function readFromBody(data: { source?: unknown; unreachable?: unknown } | null | undefined): ReadFrom {
  if (data?.source !== 'database') return LIVE;
  const code = data.unreachable;
  return { kind: 'backup', unreachable: typeof code === 'string' && code ? code : null };
}
