// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { planLabel } from '../planLabel';

describe('planLabel', () => {
  it.each([
    ['plus', 'ChatGPT Plus'],
    ['pro', 'ChatGPT Pro'],
    ['team', 'ChatGPT Business'],
    ['business', 'ChatGPT Business'],
    ['enterprise', 'ChatGPT Enterprise'],
    ['edu', 'ChatGPT Edu'],
    ['PRO', 'ChatGPT Pro'],
  ])('names the ChatGPT plan %s as %s', (code, name) => {
    expect(planLabel('codex-oauth', code)).toBe(name);
  });

  it('shows no label for a code it does not know, rather than the code', () => {
    expect(planLabel('codex-oauth', 'k12_trial')).toBeNull();
    expect(planLabel('codex-oauth', null)).toBeNull();
  });

  it('labels no Claude plan', () => {
    expect(planLabel('claude-oauth', 'max')).toBeNull();
  });
});
