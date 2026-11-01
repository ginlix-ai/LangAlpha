import type { APIRoute, GetStaticPaths } from 'astro';
import { getCollection, type CollectionEntry } from 'astro:content';
import { siteUrl, readmeAssetsUrl } from '../../site.config.mjs';

// Each English page as Markdown at its own URL plus ".md", so an agent can
// fetch one page and get the whole answer without the site chrome.
export const getStaticPaths = (async () => {
  const docs = await getCollection('docs', (entry) => !entry.id.startsWith('zh-cn'));
  return docs.map((entry) => ({ params: { slug: entry.id || 'index' }, props: { entry } }));
}) satisfies GetStaticPaths;

export const GET: APIRoute<{ entry: CollectionEntry<'docs'> }> = ({ props }) => {
  const { entry } = props;
  const path = entry.id === 'index' ? '/' : `/${entry.id}/`;
  const body = (entry.body ?? '')
    .replace(/\]\((?:\.\.\/)+assets\/readme\//g, `](${readmeAssetsUrl}/`)
    .replace(/\]\(\/(?!\/)([^)#\s]*)/g, (_m, rest) => `](${new URL('/' + rest, siteUrl).href}`);
  const header = [
    `# ${entry.data.title}`,
    '',
    entry.data.description ? `> ${entry.data.description}` : '',
    '',
    `Source: ${new URL(path, siteUrl).href}`,
    '',
  ].join('\n');
  return new Response(`${header}\n${body.trim()}\n`, {
    headers: { 'Content-Type': 'text/markdown; charset=utf-8' },
  });
};
