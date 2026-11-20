/**
 * The cloud steps' words for what is missing: by the step to take, and by
 * size, never by a plan's name.
 */
import { describe, expect, it } from 'vitest';
import i18n from '@/i18n';

import { stepsMissing, type ClaudeCodeSteps } from '../useClaudeCodeSteps';

const steps = (over: Partial<ClaudeCodeSteps>): ClaudeCodeSteps => ({
  status: 'pending',
  next: 'token',
  home: null,
  plan: { done: true, current: 'Tier A' },
  size: 'performance',
  largest: 'max',
  medium: { state: 'done', change: null },
  token: { state: 'active' },
  ...over,
});

describe('stepsMissing', () => {
  const t = i18n.getFixedT('en-US');
  it('says the step to take now', () => {
    expect(stepsMissing(t, steps({ next: 'plan', plan: { done: false, current: 'Starter' }, medium: { state: 'locked', change: null } })))
      .toBe('Needs a plan with a Medium or larger computer.');
    expect(stepsMissing(t, steps({ next: 'medium', medium: { state: 'active', change: null } }))).toBe('Needs a Medium computer.');
    expect(stepsMissing(t, steps({ next: 'resizing', plan: null, medium: { state: 'running', change: null } }))).toBe('Its computer is changing to Medium.');
    expect(stepsMissing(t, steps({}))).toBe('Needs you to sign in to Claude.');
  });
});
