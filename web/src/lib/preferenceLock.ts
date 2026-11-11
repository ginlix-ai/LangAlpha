const LOCK = 'langalpha:preference-key-write';

/**
 * One at a time across this browser's tabs: a write that replaces a whole key
 * of the preferences row, from its read to its response, and a reset, from
 * its delete to what it saves back. Each tab has a queue of its own, and this
 * is what they share. A page served over plain http has no locks: there, two
 * tabs writing inside one read-to-write gap can still drop one of the writes.
 *
 * Taken before the tab's own write queue, never inside it: a holder waiting
 * on that queue behind one waiting on the lock would never be let through.
 */
export function preferenceWriteLock<R>(run: () => Promise<R>): Promise<R> {
  const locks = typeof navigator === 'undefined' ? undefined : (navigator as { locks?: LockManager }).locks;
  return locks ? (locks.request(LOCK, run) as Promise<R>) : run();
}
