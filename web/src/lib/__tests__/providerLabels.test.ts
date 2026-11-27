// @vitest-environment node
import type { TFunction } from 'i18next';
import { describe, expect, it } from 'vitest';

import { providerLabel } from '../providerLabels';

const t = ((key: string) => key) as unknown as TFunction;

describe('providerLabel', () => {
  it('labels LangAlpha Data under its id and the id saved before the rename', () => {
    expect(providerLabel('langalpha-data', t)).toBe('LangAlpha Data');
    expect(providerLabel('ginlix-data', t)).toBe('LangAlpha Data');
  });

  it('passes an unknown source through verbatim', () => {
    expect(providerLabel('some-vendor', t)).toBe('some-vendor');
  });
});
