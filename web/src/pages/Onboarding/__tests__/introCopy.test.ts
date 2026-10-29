import { describe, it, expect } from 'vitest';
import enUS from '@/locales/intros/en-US.json';
import zhCN from '@/locales/intros/zh-CN.json';
import { PAGE_INTROS } from '../registry';

// The intro copy sits outside the main catalogs, where locales/__tests__/
// keys.test.ts never looks, so this pins the same contract for it: every
// registered step has a title and a body in both locales, and neither catalog
// holds copy that no step reads.
type IntroCatalog = Record<string, Record<string, Record<string, string>>>;
const CATALOGS: Record<string, IntroCatalog> = { 'en-US': enUS, 'zh-CN': zhCN };

const stepKeys = PAGE_INTROS.flatMap((intro) =>
  intro.steps.flatMap((step) => [`${intro.id}.${step.id}.title`, `${intro.id}.${step.id}.body`])
).sort();

describe.each(Object.entries(CATALOGS))('the %s intro copy', (_, catalog) => {
  const held = Object.entries(catalog).flatMap(([introId, steps]) =>
    Object.entries(steps).flatMap(([stepId, copy]) =>
      Object.entries(copy).map(([field, text]) => ({ key: `${introId}.${stepId}.${field}`, text }))
    )
  );

  it('has a title and a body for every step, and nothing else', () => {
    expect(held.map((h) => h.key).sort()).toEqual(stepKeys);
  });

  it('has no empty copy', () => {
    for (const { key, text } of held) expect(text.trim(), key).not.toBe('');
  });
});
