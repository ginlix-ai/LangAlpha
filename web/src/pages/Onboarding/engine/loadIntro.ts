import { use } from 'react';
import { loadCopy } from '@/i18n';
import { useLocale } from '@/hooks/useLocale';
import { loadIntroScenes, useIntroScenes, type IntroScene } from './introScenes';
import type { PageIntroDef } from '../registry';

const loads = new Map<string, Promise<boolean>>();

/**
 * Fetches an intro's copy and scenes side by side; one promise per intro and
 * language, kept for `use`. Resolves to whether the copy is in, since steps
 * without it would read as raw keys. Never rejects; a scene that failed is
 * left out of the visual panel instead.
 */
export function loadIntro(intro: PageIntroDef, lng: string): Promise<boolean> {
  const key = `${intro.id}|${lng}`;
  let load = loads.get(key);
  if (!load) {
    load = Promise.all([
      loadCopy(['intros']).then(() => true, () => false),
      loadIntroScenes(intro.steps.map((s) => s.visual)),
    ]).then(([copied]) => copied);
    loads.set(key, load);
  }
  return load;
}

/**
 * The intro's scenes in step order, suspending until they and its copy are in
 * so the modal opens whole; null when the copy did not load.
 */
export function useIntro(intro: PageIntroDef): (IntroScene | undefined)[] | null {
  const locale = useLocale();
  const copied = use(loadIntro(intro, locale));
  // Read from the cache the load above settled: nothing downloads here.
  const scenes = useIntroScenes(intro.steps.map((s) => s.visual));
  return copied ? scenes : null;
}
