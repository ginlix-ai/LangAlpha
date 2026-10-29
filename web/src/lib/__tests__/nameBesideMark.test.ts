/** Where room is short, a model's name beside its maker's mark drops the
 *  brand word the mark already shows. */
import { describe, it, expect } from 'vitest';
import { nameBesideMark } from '../brandArt';

describe('nameBesideMark', () => {
  it('drops the brand word its mark stands for, keeping it for a screen reader', () => {
    expect(nameBesideMark('Claude Opus 5.5', 'anthropic')).toEqual({ shown: 'Opus 5.5', brand: 'Claude' });
    expect(nameBesideMark('Claude Opus 5.5', 'claude-oauth').shown).toBe('Opus 5.5');
  });

  it('drops it before a version, whether a space, a hyphen or nothing comes between', () => {
    expect(nameBesideMark('Grok 4.7', 'grok-oauth').shown).toBe('4.7');
    expect(nameBesideMark('GPT-6.1 Sol', 'codex-oauth').shown).toBe('6.1 Sol');
    expect(nameBesideMark('DeepSeek-V4.1-Flash', 'deepseek').shown).toBe('V4.1-Flash');
    expect(nameBesideMark('Qwen3.8 Max', 'dashscope-intl').shown).toBe('3.8 Max');
    expect(nameBesideMark('GLM-5.3 (CN)', 'z-ai-cn').shown).toBe('5.3 (CN)');
  });

  it('keeps a name that only begins with the same letters, or is the brand word alone', () => {
    expect(nameBesideMark('Grokker 2', 'xai').shown).toBe('Grokker 2');
    expect(nameBesideMark('Claude', 'anthropic').shown).toBe('Claude');
    expect(nameBesideMark('Claude ', 'anthropic').shown).toBe('Claude ');
  });

  it('keeps the name of a model whose host serves many makers, or that has no mark', () => {
    expect(nameBesideMark('Claude Opus 5.5', 'openrouter').shown).toBe('Claude Opus 5.5');
    expect(nameBesideMark('Claude Opus 5.5', 'my-gateway').shown).toBe('Claude Opus 5.5');
    expect(nameBesideMark('Claude Opus 5.5', undefined).shown).toBe('Claude Opus 5.5');
  });
});
