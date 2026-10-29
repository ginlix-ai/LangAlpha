import { use } from 'react';
import i18n from '@/i18n';
import { useLocale } from '@/hooks/useLocale';
import { loadIntroScenes, useIntroScenes, type IntroScene } from './introScenes';
import type { PageIntroDef } from '../registry';

const loads = new Map<string, Promise<boolean>>();

/**
 * Fetches an intro's copy and scenes side by side; one promise per intro and
 * language, kept for `use`. Resolves to whether the copy is in: a copy chunk
 * that failed to load (a deploy retired it) still counts as loaded to
 * i18next, and its steps would read as raw keys. Never rejects; a scene that
 * failed is left out of the visual panel instead.
 */
export function loadIntro(intro: PageIntroDef, lng: string): Promise<boolean> {
  const key = `${intro.id}|${lng}`;
  let load = loads.get(key);
  if (!load) {
    load = Promise.all([
      i18n.loadNamespaces('intros'),
      loadIntroScenes(intro.steps.map((s) => s.visual)),
    ]).then(() => i18n.hasResourceBundle(lng, 'intros'));
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
