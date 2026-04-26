import i18n from 'i18next';
import { initReactI18next } from 'react-i18next';
import enUS from './locales/en-US.json';
import zhCN from './locales/zh-CN.json';

const SUPPORTED_LOCALES = ['en-US', 'zh-CN'] as const;

function detectLocale(): string {
  // 1. Explicit user choice persisted in localStorage
  const stored = localStorage.getItem('locale');
  if (stored && SUPPORTED_LOCALES.includes(stored as typeof SUPPORTED_LOCALES[number])) {
    return stored;
  }

  // 2. Browser language — try exact match first, then prefix match
  const browserLang = navigator.language; // e.g. "zh-CN", "zh-TW", "en-GB"
  if (SUPPORTED_LOCALES.includes(browserLang as typeof SUPPORTED_LOCALES[number])) {
    return browserLang;
  }
  const prefix = browserLang.split('-')[0]; // "zh", "en"
  const prefixMatch = SUPPORTED_LOCALES.find((l) => l.startsWith(prefix + '-'));
  if (prefixMatch) return prefixMatch;

  // 3. Fallback
  return 'en-US';
}

i18n.use(initReactI18next).init({
  resources: {
    'en-US': { translation: enUS },
    'zh-CN': { translation: zhCN },
  },
  lng: detectLocale(),
  fallbackLng: 'en-US',
  interpolation: { escapeValue: false },
});

// Cross-tab locale sync. Settings.tsx writes localStorage('locale') and calls
// changeLanguage in the active tab; without a listener, other open tabs would
// stay in the prior locale until reload. The dashboard prefs sync (PR-1) goes
// to lengths to keep everything else cross-tab consistent — locale should too.
//
// `storage` events only fire in OTHER tabs, never in the writing tab, so this
// won't recurse. Guarded for SSR / non-browser test environments.
if (typeof window !== 'undefined') {
  window.addEventListener('storage', (e) => {
    if (e.key !== 'locale' || !e.newValue) return;
    if (!SUPPORTED_LOCALES.includes(e.newValue as typeof SUPPORTED_LOCALES[number])) return;
    if (i18n.language === e.newValue) return;
    i18n.changeLanguage(e.newValue);
  });
}

export default i18n;
