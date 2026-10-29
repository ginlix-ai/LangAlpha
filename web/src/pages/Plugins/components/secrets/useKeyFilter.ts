import { useState } from 'react';
import type { UserVaultSecret } from '@/pages/ChatAgent/utils/api';

/** From this many keys a name is easier typed than found by eye. */
const FILTER_MIN = 8;

export type KeyFilterState = ReturnType<typeof useKeyFilter>;

/**
 * The Secrets list's narrowing: a typed query over name and description, and
 * the keys no connector reads. `pinned` names the row a form or a confirm is
 * open on, which stays whatever the filter says: typing in the filter must not
 * fold away the edit in progress.
 */
export function useKeyFilter(
  secrets: readonly UserVaultSecret[],
  readers: ReadonlyMap<string, readonly string[]> | undefined,
  pinned: string | null,
) {
  const [query, setQuery] = useState('');
  const [unreadOnly, setUnreadOnly] = useState(false);
  const unread = readers ? secrets.filter((s) => !readers.get(s.name)?.length) : undefined;
  const unreadNames = new Set(unread?.map((s) => s.name));
  const needle = query.trim().toLowerCase();
  const filtering = needle.length > 0 || unreadOnly;
  const visible = secrets.filter(
    (s) =>
      s.name === pinned ||
      ((!needle || s.name.toLowerCase().includes(needle) || s.description.toLowerCase().includes(needle)) &&
        (!unreadOnly || unreadNames.has(s.name))),
  );
  return {
    query,
    setQuery,
    unreadOnly,
    setUnreadOnly,
    /** Undefined while what reads each key is unknown. */
    unreadCount: unread?.length,
    filtering,
    /** Offered on a long list, and kept while it narrows one. */
    offered: secrets.length >= FILTER_MIN || filtering,
    visible,
    clear() {
      setQuery('');
      setUnreadOnly(false);
    },
    showUnread() {
      setQuery('');
      setUnreadOnly(true);
    },
  };
}
