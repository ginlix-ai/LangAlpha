import enUSMain from '@/locales/en-US.json';
import zhCNMain from '@/locales/zh-CN.json';

/*
 * Every catalog the app has, loaded up front. `resources` is i18next's shape,
 * which `test/setup.ts` hands to `initI18n`; `enUS` and `zhCN` merge each
 * locale's catalogs into the object a key addresses, every namespace folder
 * under its own name (`setup:steps.title` is `catalog.setup.steps.title`), for
 * tests that check a key resolves without rendering it.
 */

type Catalog = Record<string, unknown>;

export const resources: Record<'en-US' | 'zh-CN', Record<string, Catalog>> = {
  'en-US': { translation: enUSMain },
  'zh-CN': { translation: zhCNMain },
};
const namespaceFiles = import.meta.glob<Catalog>('../locales/*/*.json', { eager: true, import: 'default' });
for (const [file, catalog] of Object.entries(namespaceFiles)) {
  const [, ns, lng] = /\/locales\/([^/]+)\/([^/]+)\.json$/.exec(file) ?? [];
  if (lng === 'en-US' || lng === 'zh-CN') resources[lng][ns] = catalog;
}

const merged = ({ translation, ...namespaces }: Record<string, Catalog>): Catalog => ({ ...translation, ...namespaces });
export const enUS = merged(resources['en-US']);
export const zhCN = merged(resources['zh-CN']);

/** The value at a key, `ns:path` or a bare `path`, or undefined. */
export function lookup(catalog: unknown, key: string): unknown {
  return key.split(/[.:]/).reduce<unknown>(
    (node, part) => (node && typeof node === 'object' ? (node as Catalog)[part] : undefined),
    catalog,
  );
}
