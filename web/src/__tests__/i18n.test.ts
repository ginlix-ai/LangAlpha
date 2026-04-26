import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import i18n from '../i18n';

// The cross-tab `storage` listener in i18n.ts is the only locale-sync mechanism
// between tabs. Browsers fire `storage` only in OTHER tabs, never the writer,
// so the handler doesn't need to dedupe its own writes — but it MUST ignore
// foreign keys, junk values, unsupported locales, and same-locale events
// (the last is a recursion guard against synthetic re-dispatches).
describe('i18n cross-tab storage listener', () => {
  let originalLang: string;

  beforeEach(() => {
    originalLang = i18n.language;
  });

  afterEach(async () => {
    if (i18n.language !== originalLang) {
      await i18n.changeLanguage(originalLang);
    }
  });

  function fire(key: string | null, newValue: string | null) {
    window.dispatchEvent(
      new StorageEvent('storage', { key, newValue, storageArea: localStorage }),
    );
  }

  it('ignores storage events for unrelated keys', async () => {
    await i18n.changeLanguage('en-US');
    const spy = vi.spyOn(i18n, 'changeLanguage');
    fire('user-prefs', 'zh-CN');
    expect(spy).not.toHaveBeenCalled();
    spy.mockRestore();
  });

  it('ignores storage events with null/empty newValue (key was removed)', async () => {
    await i18n.changeLanguage('en-US');
    const spy = vi.spyOn(i18n, 'changeLanguage');
    fire('locale', null);
    fire('locale', '');
    expect(spy).not.toHaveBeenCalled();
    spy.mockRestore();
  });

  it('ignores storage events with unsupported locales', async () => {
    await i18n.changeLanguage('en-US');
    const spy = vi.spyOn(i18n, 'changeLanguage');
    fire('locale', 'fr-FR');
    fire('locale', 'zh-TW'); // valid BCP-47 but not in SUPPORTED_LOCALES
    fire('locale', '<script>alert(1)</script>');
    expect(spy).not.toHaveBeenCalled();
    spy.mockRestore();
  });

  it('switches language when a supported locale lands via storage', async () => {
    await i18n.changeLanguage('en-US');
    fire('locale', 'zh-CN');
    expect(i18n.language).toBe('zh-CN');
  });

  it('no-ops when the storage value matches the current language', async () => {
    await i18n.changeLanguage('zh-CN');
    const spy = vi.spyOn(i18n, 'changeLanguage');
    fire('locale', 'zh-CN');
    expect(spy).not.toHaveBeenCalled();
    spy.mockRestore();
  });
});
