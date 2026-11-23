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

/**
 * An instance read the way src/i18n.ts reads: a failed read is retryable and
 * `maxRetries` is 0. `dead` names what fails to load, a language or `lng|ns`
 * for one namespace; `failOnce` fails a `lng|ns` the first time only,
 * `slow` delays one, and `onRead` sees every read.
 */
async function setup(
  dead: string[] = [],
  { failOnce = new Set<string>(), slow = '', onRead = (_key: string) => {} } = {},
) {
  const backend: BackendModule = {
    type: 'backend',
    init() {},
    read(lng, ns, callback) {
      const key = `${lng}|${ns}`;
      onRead(key);
      setTimeout(() => {
        if (dead.includes(lng) || dead.includes(key) || failOnce.delete(key)) {
          callback(new Error(`Failed to fetch dynamically imported module: /assets/${lng}.js`), true);
        } else callback(null, CATALOGS[lng]);
      }, key === slow ? 30 : 0);
    },
  };
  const i18n = i18next.createInstance();
  await i18n.use(backend).init({ lng: 'en-US', fallbackLng: false, load: 'currentOnly', maxRetries: 0 });
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

  it('keeps the current language when a namespace already in use fails to load', async () => {
    const i18n = await setup(['zh-CN|settings']);
    await i18n.loadNamespaces('settings');
    await switchLocale(i18n, 'zh-CN');
    expect(i18n.language).toBe('en-US');
  });

  it('waits for a namespace a page brings in while the switch runs', async () => {
    const i18n = await setup([], { slow: 'zh-CN|settings' });
    const switching = switchLocale(i18n, 'zh-CN');
    void i18n.loadNamespaces('settings');
    await switching;
    expect(i18n.language).toBe('zh-CN');
  });

  it('reads a namespace again on the next switch after its read failed', async () => {
    const i18n = await setup([], { failOnce: new Set(['zh-CN|settings']) });
    await i18n.loadNamespaces('settings');
    await switchLocale(i18n, 'zh-CN');
    expect(i18n.language).toBe('en-US');
    await switchLocale(i18n, 'zh-CN');
    expect(i18n.language).toBe('zh-CN');
  });

  it('leaves a later page loading its copy in the current language only', async () => {
    const reads: string[] = [];
    const i18n = await setup([], { onRead: (key) => reads.push(key) });
    await switchLocale(i18n, 'zh-CN');
    await switchLocale(i18n, 'en-US');
    reads.length = 0;
    await i18n.loadNamespaces('settings');
    expect(reads).toEqual(['en-US|settings']);
  });
});
