import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { describe, expect, it } from 'vitest';

import { EFFORT_ORDER, TUNING_FIELDS, resolveEffort, resolveTuning, resolveTuningField } from '@/lib/modelTuning';
import type { ModelProfile, TuningField } from '@/lib/modelTuning';

/**
 * `resolveEffort` mirrors `ModelConfig.resolve_reasoning_effort`, which decides
 * the level the server actually runs. Nothing but this file stops the two from
 * drifting, so both read one table: change the rule on either side and the
 * other's suite fails, instead of the UI quietly naming a level that is not
 * what the request uses.
 */
type ClampContract = {
  levels: string[];
  cases: { ladder: string[]; requested: string; expected: string | null }[];
};

const FIXTURES = resolve(dirname(fileURLToPath(import.meta.url)), '../../../../tests/fixtures');
const read = <T,>(name: string): T => JSON.parse(readFileSync(resolve(FIXTURES, name), 'utf-8'));

const contract: ClampContract = read('reasoning_clamp.json');

describe('resolveEffort honors the shared clamp contract', () => {
  it('orders levels the same way REASONING_LEVELS does', () => {
    expect([...EFFORT_ORDER]).toEqual(contract.levels);
  });

  it.each(contract.cases)(
    'ladder [$ladder] asked for $requested runs $expected',
    ({ ladder, requested, expected }) => {
      expect(resolveEffort(requested, ladder)).toBe(expected);
    },
  );
});

/**
 * `resolveTuningField` mirrors `resolve_tuning_field`, which decides whose
 * value a turn runs. Same story as the clamp above: the two read one table, so
 * a change on either side fails the other's suite rather than leaving the
 * composer naming a setting the server will not use.
 */
type PrecedenceContract = {
  fields: TuningField[];
  cases: {
    name: string;
    account: ModelProfile;
    profile: ModelProfile;
    field: TuningField;
    expected: unknown;
  }[];
};

const precedence: PrecedenceContract = read('tuning_precedence.json');

describe('resolveTuningField honors the shared precedence contract', () => {
  it('covers the same four fields Tuning declares', () => {
    expect([...TUNING_FIELDS].sort()).toEqual([...precedence.fields].sort());
  });

  it.each(precedence.cases)('$name', ({ account, profile, field, expected }) => {
    expect(resolveTuningField(account, profile, field) ?? null).toEqual(expected);
  });
});

describe('resolveTuning layers the manifest under the two stored ones', () => {
  const meta = {
    reasoning_efforts: ['low', 'high'],
    reasoning_effort_default: 'low',
    prompt_guidance: 'lean',
    compaction_profile: 'relaxed',
  };

  it('names what the model itself declares when neither layer has a value', () => {
    const { inherited } = resolveTuning({}, {}, meta);
    expect(inherited).toEqual({
      prompt_guidance: 'lean',
      compaction_profile: 'relaxed',
      reasoning_effort: 'low',
      fast_mode: false,
    });
  });

  it('clamps the account level into the ladder the model actually offers', () => {
    expect(resolveTuning({ reasoning_effort: 'medium' }, {}, meta).inherited.reasoning_effort)
      .toBe('low');
  });

  it('reads an empty account level as unset rather than as a level to clamp', () => {
    // resolveEffort('') returns null, which used to swallow the manifest default.
    expect(resolveTuning({ reasoning_effort: '' }, {}, meta).inherited.reasoning_effort)
      .toBe('low');
  });

  it('offers only levels the vocabulary can name', () => {
    expect(resolveTuning({}, {}, { reasoning_efforts: ['low', 'ludicrous'] }).efforts)
      .toEqual(['low']);
  });

  it('falls back to the detailed guidance the server uses when nobody declares one', () => {
    expect(resolveTuning({}, {}, {}).inherited.prompt_guidance).toBe('detailed');
  });

  // resolve_prompt_guidance reads the deployment pin before the manifest row, so
  // a pinned deployment that labelled "Default" with the declaration would name
  // a level no turn ever runs.
  it('lets the deployment pin outrank what the model declares', () => {
    expect(resolveTuning({}, {}, meta, 'detailed').inherited.prompt_guidance).toBe('detailed');
    expect(resolveTuning({}, {}, meta, null).inherited.prompt_guidance).toBe('lean');
    expect(resolveTuning({}, {}, meta, 'auto').inherited.prompt_guidance).toBe('lean');
  });

  it('keeps the account value above the pin', () => {
    expect(resolveTuning({ prompt_guidance: 'lean' }, {}, {}, 'detailed').inherited.prompt_guidance)
      .toBe('lean');
  });

  it('runs the override, then the account value, then the inherited one', () => {
    expect(resolveTuning({ reasoning_effort: 'high' }, { reasoning_effort: 'low' }, meta)
      .effective.reasoning_effort).toBe('low');
    expect(resolveTuning({ reasoning_effort: 'high' }, {}, meta)
      .effective.reasoning_effort).toBe('high');
    expect(resolveTuning({}, {}, meta).effective.reasoning_effort).toBe('low');
  });
});
