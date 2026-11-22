// @vitest-environment node
/**
 * The cost marks a model row shows: a band from the model's figure at its
 * default level, calibrated on the catalog's real figures; an upper bound, or
 * an estimate from its list price on the same scale, where it has no measured
 * one; and none on a subscription.
 */
import { describe, it, expect } from 'vitest';
import { costBand, costBarFraction, costMarks } from '../taskCost';

describe('costBand', () => {
  it.each([
    ['GPT-6 Luna', 0.04, 1],
    ['Claude Haiku 5.5', 0.1, 1],
    ['DeepSeek Flash', 0.41, 2],
    ['GPT-6.2 Sol', 0.49, 2],
    ['Claude Sonnet 5.5', 1.0, 3],
    ['GPT-6.2 Astra', 1.97, 4],
    ['Claude Opus 5.5', 2.76, 4],
    ['Kimi K3.1 Pro', 3.47, 4],
    ['GLM-5.3', 4.15, 4],
    ['Claude Fable 5.2', 11.0, 5],
  ])('puts %s (%f) in band %i', (_, value, band) => {
    expect(costBand(value)).toBe(band);
  });

  it('turns over at each edge', () => {
    expect([0.149, 0.15, 0.499, 0.5, 1.499, 1.5, 4.99, 5].map(costBand)).toEqual([1, 2, 2, 3, 3, 4, 4, 5]);
  });
});

describe('costMarks', () => {
  const ladder = { reasoning_efforts: ['low', 'medium', 'high'], reasoning_effort_default: 'medium' };

  it('reads the figure at the default level', () => {
    const meta = { ...ladder, task_cost: { low: { value: 0.5, measured: true }, medium: { value: 2.76, measured: true } } };
    expect(costMarks(meta, 'credits')).toEqual({ kind: 'measured', band: 4, value: 2.76, level: 'medium' });
  });

  it('reads "any" for a model measured without levels', () => {
    expect(costMarks({ task_cost: { any: { value: 0.1, measured: true } } }, 'own_key'))
      .toMatchObject({ kind: 'measured', band: 1, level: 'any' });
  });

  it('calls a figure carried down from the level above a bound', () => {
    const meta = { ...ladder, task_cost: { medium: { value: 1, measured: false } } };
    expect(costMarks(meta, 'credits')).toMatchObject({ kind: 'bound', band: 3 });
  });

  it('falls back on the list-price estimate, banded as a figure is', () => {
    expect(costMarks({ task_cost_estimate: 2.76 }, 'server_key')).toEqual({ kind: 'estimate', band: 4 });
  });

  it('puts a model estimated at the baseline\'s price in the baseline\'s band', () => {
    const baseline = { ...ladder, task_cost: { medium: { value: 1, measured: true } } };
    expect(costMarks({ task_cost_estimate: 1 }, 'own_key')?.band).toBe(costMarks(baseline, 'credits')?.band);
  });

  it('prefers a figure to an estimate', () => {
    const meta = { task_cost: { any: { value: 0.1, measured: true } }, task_cost_estimate: 3 };
    expect(costMarks(meta, 'credits')).toMatchObject({ kind: 'measured', band: 1 });
  });

  it('marks nothing on a subscription, nor with neither figure nor price', () => {
    const meta = { task_cost: { any: { value: 1, measured: true } }, task_cost_estimate: 1 };
    expect(costMarks(meta, 'subscription:claude-oauth')).toBeNull();
    expect(costMarks({}, 'credits')).toBeNull();
    expect(costMarks(undefined, 'credits')).toBeNull();
  });
});

describe('costBarFraction', () => {
  const range: [number, number] = [0.04, 11];

  it('spans the catalog on a log scale', () => {
    expect(costBarFraction(11, range)).toBe(1);
    // The geometric middle of the catalog draws half a bar.
    expect(costBarFraction(Math.sqrt(0.04 * 11), range)).toBeCloseTo(0.5);
  });

  it('keeps the cheapest level visible', () => {
    expect(costBarFraction(0.04, range)).toBeGreaterThan(0);
  });
});
