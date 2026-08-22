import { describe, it, expect, vi } from 'vitest';
import { runBulk } from '../utils/bulkRun';

describe('runBulk', () => {
  it('runs every item and partitions successes from failures', async () => {
    const worker = vi.fn(async (n: number) => {
      if (n % 2 === 0) throw new Error(`boom ${n}`);
      return n;
    });
    const result = await runBulk([1, 2, 3, 4, 5], worker);

    expect(worker).toHaveBeenCalledTimes(5);
    expect(result.ok.sort()).toEqual([1, 3, 5]);
    expect(result.failed.map((f) => f.item).sort()).toEqual([2, 4]);
    expect((result.failed[0].error as Error).message).toMatch(/^boom/);
  });

  it('reports monotonic progress up to the total', async () => {
    const seen: Array<[number, number]> = [];
    await runBulk([1, 2, 3], async () => {}, {
      onProgress: (done, total) => seen.push([done, total]),
    });
    expect(seen).toEqual([
      [1, 3],
      [2, 3],
      [3, 3],
    ]);
  });

  it('caps concurrent workers at the configured lane count', async () => {
    let inFlight = 0;
    let peak = 0;
    const result = await runBulk(
      Array.from({ length: 10 }, (_, i) => i),
      async () => {
        inFlight += 1;
        peak = Math.max(peak, inFlight);
        await new Promise((r) => setTimeout(r, 1));
        inFlight -= 1;
      },
      { concurrency: 3 },
    );
    expect(result.ok).toHaveLength(10);
    expect(peak).toBeLessThanOrEqual(3);
  });

  it('handles an empty item list without calling the worker', async () => {
    const worker = vi.fn();
    const result = await runBulk([], worker);
    expect(worker).not.toHaveBeenCalled();
    expect(result).toEqual({ ok: [], failed: [] });
  });
});
