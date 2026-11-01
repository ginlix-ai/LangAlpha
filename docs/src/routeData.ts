import { getCollection } from 'astro:content';
import { defineRouteMiddleware } from '@astrojs/starlight/route-data';

// Search and agent hints per page. An English page points at its Markdown
// twin and only at translations that exist. An untranslated page shown in
// another locale is a copy of the English one, so it defers to it rather
// than competing with it in search results.
export const onRequest = defineRouteMiddleware(async (context) => {
  const route = context.locals.starlightRoute;
  const englishId = route.entry.id;

  if (route.isFallback) {
    const canonical = route.head.find((tag) => tag.tag === 'link' && tag.attrs?.rel === 'canonical');
    if (canonical?.attrs && context.site) {
      canonical.attrs.href = new URL(englishId ? `/${englishId}/` : '/', context.site).href;
    }
    route.head.push({ tag: 'meta', attrs: { name: 'robots', content: 'noindex, follow' } });
    return;
  }
  if (route.locale) return;

  route.head.push({
    tag: 'link',
    attrs: { rel: 'alternate', type: 'text/markdown', href: `/${englishId || 'index'}.md` },
  });
  const translations = await getCollection('docs', (entry) => entry.id.startsWith('zh-cn'));
  const translated = translations.some((entry) => entry.id === (englishId ? `zh-cn/${englishId}` : 'zh-cn'));
  if (!translated) {
    route.head = route.head.filter(
      (tag) => !(tag.tag === 'link' && tag.attrs?.hreflang && !['en', 'x-default'].includes(String(tag.attrs.hreflang))),
    );
  }
});
