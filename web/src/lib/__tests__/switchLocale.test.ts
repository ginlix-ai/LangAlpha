import i18next, { type BackendModule } from 'i18next';
import { describe, expect, it } from 'vitest';
import { switchLocale } from '../locale';

// Against a real i18next instance, because the contract is about how its loader
// behaves: changeLanguage alone switches even when the catalog failed, and every
// string then renders as its raw key.

const CATALOGS: Record<string, Record<string, string>> = {
  'en-US': { greeting: 'Hello' },
  'zh-CN': { greeting: '你好' },
};

async function setup(dead: string[] = []) {
  const backend: BackendModule = {
    type: 'backend',
    init() {},
    read(lng, _ns, callback) {
      queueMicrotask(() => {
        if (dead.includes(lng)) callback(new Error(`Failed to fetch dynamically imported module: /assets/${lng}.js`), false);
        else callback(null, CATALOGS[lng]);
      });
    },
  };
  const i18n = i18next.createInstance();
  await i18n.use(backend).init({ lng: 'en-US', fallbackLng: false, load: 'currentOnly' });
  return i18n;
}

describe('switchLocale', () => {
  it('switches once the catalog is in', async () => {
    const i18n = await setup();
    await switchLocale(i18n, 'zh-CN');
    expect(i18n.language).toBe('zh-CN');
    expect(i18n.t('greeting')).toBe('你好');
  });

  it('keeps the current language when the catalog fails to load', async () => {
    const i18n = await setup(['zh-CN']);
    await switchLocale(i18n, 'zh-CN');
    expect(i18n.language).toBe('en-US');
    expect(i18n.t('greeting')).toBe('Hello');
  });
});
