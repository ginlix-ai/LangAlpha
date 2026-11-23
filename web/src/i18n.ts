import i18n, { type BackendModule, type Resource, type ResourceKey } from 'i18next';
import { initReactI18next } from 'react-i18next';
import { detectLocale, isSupported, type Locale } from './lib/locale';

// Locale resolution (cookie → browser language → English) lives in ./lib/locale,
// shared with the cookie helpers that components use. The cross-tab `storage`
// listener was removed along with localStorage-based locale: locale now rides a
// shared cookie (readable server-side, unlike localStorage); other tabs adopt a
// change on their next navigation.
//
// The document's language follows the app's, so the browser shapes CJK text
// as Chinese and `:lang()` rules can key on it. Bound before init, which
// reports the first language through the same event.
i18n.on('languageChanged', (lng) => {
  if (typeof document !== 'undefined') document.documentElement.lang = lng;
});

// One chunk per locale. Bundled, the catalogs were about 100 kB gz of the entry,
// half of it in a language the visitor never sees. The active one still does
// not wait for the entry: scripts/locale-preload.ts has index.html start
// fetching it alongside.
type Catalog = () => Promise<{ default: ResourceKey }>;

const CATALOGS: Record<Locale, Catalog> = {
  'en-US': () => import('./locales/en-US.json'),
  'zh-CN': () => import('./locales/zh-CN.json'),
};

// Every other namespace is a folder of its own, `locales/<ns>/<locale>.json`,
// fetched the first time something needs it rather than on every load: the
// page intros, the desktop app's first run, and the sections of the app whose
// copy only lazy code reads (Settings, Plugins, the file panel, ...). Their
// keys name the namespace, `setup:steps.title`, and the lazy import that first
// reaches one loads it beside its chunk with `withCopy`. The build refuses a
// bundle where the entry reads one or a lazy import leaves one out
// (scripts/vite-plugins/copyNamespaces.ts).
const NAMESPACE_FILES = import.meta.glob<{ default: ResourceKey }>('./locales/*/*.json');

const NAMESPACES: Record<string, Partial<Record<Locale, Catalog>>> = { translation: CATALOGS };
for (const [file, load] of Object.entries(NAMESPACE_FILES)) {
  const [, ns, lng] = /^\.\/locales\/([^/]+)\/([^/]+)\.json$/.exec(file) ?? [];
  if (ns && isSupported(lng)) (NAMESPACES[ns] ??= {})[lng] = load;
}

const loaderFor = (lng: string, ns: string) => (isSupported(lng) ? NAMESPACES[ns]?.[lng] : undefined);

// The error a namespace's read failed with, so `loadCopy` can hand the import
// failure itself to the lazy boundary instead of rendering raw keys.
const readFailures = new Map<string, unknown>();

const catalogBackend: BackendModule = {
  type: 'backend',
  init() {},
  read(lng, ns, callback) {
    const load = loaderFor(lng, ns);
    // `false` marks a catalog that does not exist, which no retry finds.
    if (!load) {
      return callback(new Error(`no ${ns} catalog for ${lng}`), false);
    }
    // A failed import is marked retryable: with `maxRetries: 0` i18next gives
    // up on it at once rather than retrying for seconds, but reads it again the
    // next time anything asks, so a dropped connection is not the end of that
    // namespace. A deploy that retired the file is lib/staleBuild.tsx's.
    load().then(
      (m) => callback(null, m.default),
      (err: unknown) => {
        readFailures.set(`${lng}|${ns}`, err);
        callback(err instanceof Error ? err : String(err), true);
      },
    );
  },
};

/**
 * Resolves once the active locale's catalog is in, and rejects if it could not
 * be loaded. `resources` bypasses the loader entirely, which is how the unit
 * suite gets both catalogs synchronously.
 */
export async function initI18n(resources?: Resource): Promise<void> {
  const lng = detectLocale();
  await i18n
    .use(initReactI18next)
    .use(catalogBackend)
    .init({
      lng,
      resources,
      // No runtime fallback language: it would be a second catalog on every
      // zh-CN visit, and zh-CN resolves every en-US key, which
      // locales/__tests__/keys.test.ts pins.
      fallbackLng: false,
      // Only the exact code. The default also asks the loader for `zh` and
      // `en`, which have no catalogs.
      load: 'currentOnly',
      interpolation: { escapeValue: false },
      maxRetries: 0,
      // Stated rather than left to the defaults: with neither set, i18next
      // guesses whether a key "looks like natural language" before it splits
      // off a namespace, and a deferred key must always split.
      nsSeparator: ':',
      keySeparator: '.',
    });
  if (!i18n.hasResourceBundle(lng, 'translation')) {
    throw new Error(`[i18n] the ${lng} catalog did not load`);
  }
}

/**
 * Loads namespaces into the active language, rejecting with the read's own
 * error if one did not arrive. i18next settles `loadNamespaces` either way and
 * would render the keys; a rejection reaches the lazy boundary like a failed
 * chunk, which is what it almost always is, and the next ask reads it again.
 */
export async function loadCopy(namespaces: readonly string[]): Promise<void> {
  if (!namespaces.length) return;
  await i18n.loadNamespaces([...namespaces]);
  const lng = i18n.language;
  const missing = namespaces.find((ns) => !i18n.hasResourceBundle(lng, ns));
  if (missing) throw readFailures.get(`${lng}|${missing}`) ?? new Error(`[i18n] the ${missing} catalog did not load`);
}

/**
 * A lazy import that also loads the namespaces its chunk reads, in parallel,
 * so the code never renders before its copy: `lazy(withCopy(() =>
 * import('./Page'), ['setup']))`. List what the chunk reads beyond what the
 * importing chunk already did; the build names any that are missing.
 */
export function withCopy<T>(load: () => Promise<T>, namespaces: readonly string[]): () => Promise<T> {
  return () => Promise.all([load(), loadCopy(namespaces)]).then(([m]) => m);
}

export default i18n;
