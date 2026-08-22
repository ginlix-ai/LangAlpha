/**
 * Client-side fan-out for bulk actions: the backend has only single-item
 * endpoints, and at this page's caps (20 plugins / 50 skills / dozens of
 * servers) a small worker pool beats new API surface. Failures are isolated
 * per item so one bad row never aborts the rest.
 */

export interface BulkResult<T> {
  ok: T[];
  failed: { item: T; error: unknown }[];
}

export async function runBulk<T>(
  items: readonly T[],
  worker: (item: T) => Promise<unknown>,
  opts?: { concurrency?: number; onProgress?: (done: number, total: number) => void },
): Promise<BulkResult<T>> {
  const concurrency = Math.max(1, opts?.concurrency ?? 4);
  const result: BulkResult<T> = { ok: [], failed: [] };
  let next = 0;
  let done = 0;

  async function lane() {
    while (next < items.length) {
      const item = items[next++];
      try {
        await worker(item);
        result.ok.push(item);
      } catch (error) {
        result.failed.push({ item, error });
      }
      done += 1;
      opts?.onProgress?.(done, items.length);
    }
  }

  await Promise.all(
    Array.from({ length: Math.min(concurrency, items.length) }, () => lane()),
  );
  return result;
}
