import { describe, it, expect } from 'vitest';
import {
  ZERO_USAGE,
  extractTokenUsageDelta,
  accumulateTokenUsage,
  type SubagentTokenUsage,
} from '../tokenUsage';

describe('extractTokenUsageDelta', () => {
  it('reads input/output/total straight off a well-formed event', () => {
    expect(extractTokenUsageDelta({ input_tokens: 100, output_tokens: 50, total_tokens: 150 }))
      .toEqual({ input: 100, output: 50, total: 150 });
  });

  it('falls back to input + output when total_tokens is missing', () => {
    expect(extractTokenUsageDelta({ input_tokens: 80, output_tokens: 20 }))
      .toEqual({ input: 80, output: 20, total: 100 });
  });

  it('coerces stringified numbers (defensive against older SSE shapes)', () => {
    expect(extractTokenUsageDelta({ input_tokens: '10', output_tokens: '5', total_tokens: '15' }))
      .toEqual({ input: 10, output: 5, total: 15 });
  });

  it('treats missing/NaN/negative fields as zero', () => {
    expect(extractTokenUsageDelta({})).toEqual(ZERO_USAGE);
    expect(extractTokenUsageDelta({ input_tokens: NaN, output_tokens: -5 })).toEqual(ZERO_USAGE);
    expect(extractTokenUsageDelta({ input_tokens: 'foo' })).toEqual(ZERO_USAGE);
  });
});

describe('accumulateTokenUsage', () => {
  it('adds delta into prev across all three axes', () => {
    const prev: SubagentTokenUsage = { input: 100, output: 50, total: 150 };
    const delta: SubagentTokenUsage = { input: 30, output: 20, total: 50 };
    expect(accumulateTokenUsage(prev, delta)).toEqual({ input: 130, output: 70, total: 200 });
  });

  it('is commutative — order of equal events produces the same total', () => {
    const a: SubagentTokenUsage = { input: 10, output: 5, total: 15 };
    const b: SubagentTokenUsage = { input: 7, output: 3, total: 10 };
    const ab = accumulateTokenUsage(accumulateTokenUsage(ZERO_USAGE, a), b);
    const ba = accumulateTokenUsage(accumulateTokenUsage(ZERO_USAGE, b), a);
    expect(ab).toEqual(ba);
  });

  it('ZERO_USAGE is the additive identity', () => {
    const x: SubagentTokenUsage = { input: 42, output: 17, total: 59 };
    expect(accumulateTokenUsage(ZERO_USAGE, x)).toEqual(x);
    expect(accumulateTokenUsage(x, ZERO_USAGE)).toEqual(x);
  });
});
