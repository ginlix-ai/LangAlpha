import { readFileSync, readdirSync, existsSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const docsDir = fileURLToPath(new URL('./content/docs/', import.meta.url));
// The sidebar, shared with llms.txt so both list pages in the same order.
// A group with `slugs` is ordered by hand; one without lists its directory,
// ordered by each page's `sidebar.order`.
export const sidebarGroups = [
  {
    dir: 'get-started',
    label: 'Get started',
    zh: '开始使用',
    slugs: ['what-is-langalpha', 'hosted-or-self-hosted', 'self-hosting-quickstart'],
  },
  {
    dir: 'concepts',
    label: 'Concepts',
    zh: '核心概念',
    slugs: [
      'overview',
      'computers',
      'sandboxes-and-ssh',
      'workspaces',
      'threads-and-turns',
      'chief-of-staff-and-analysts',
      'files',
      'memory-memos-scratchpad',
      'skills',
      'plugins-mcp-ptc',
      'models-and-keys',
      'brokerages-and-orders',
      'automations-wakeups-commands',
      'chat-apps',
      'sharing',
    ],
  },
  { dir: 'compare', label: 'Compare', zh: '对比' },
  { dir: 'explore', label: 'Explore', zh: '能做什么' },
  { dir: 'best-practices', label: 'Best practices', zh: '最佳实践' },
];

export function starlightSidebar() {
  return sidebarGroups.map(({ dir, label, zh, slugs }) => ({
    label,
    translations: { 'zh-CN': zh },
    items: slugs ? slugs.map((slug) => ({ slug: `${dir}/${slug}` })) : [{ autogenerate: { directory: dir } }],
  }));
}

function frontmatter(file) {
  const match = readFileSync(file, 'utf8').match(/^---\n([\s\S]*?)\n---/);
  const field = (name) => match?.[1].match(new RegExp(`^${name}:\\s*(.+)$`, 'm'))?.[1].trim().replace(/^['"]|['"]$/g, '');
  const order = Number(match?.[1].match(/order:\s*(\d+)/)?.[1] ?? 999);
  return { title: field('title'), description: field('description'), order };
}

function pages({ dir, slugs }) {
  const path = join(docsDir, dir);
  if (!existsSync(path)) return [];
  const names = slugs ?? readdirSync(path).filter((name) => name.endsWith('.md')).map((name) => name.replace(/\.md$/, ''));
  const list = names
    .filter((name) => existsSync(join(path, `${name}.md`)))
    .map((name) => ({ slug: `${dir}/${name}`, ...frontmatter(join(path, `${name}.md`)) }));
  return slugs ? list : list.sort((a, b) => a.order - b.order || a.slug.localeCompare(b.slug));
}

// The per-page index llms.txt carries, one Markdown link per page with its
// description, grouped the way the sidebar groups them.
export function llmsPageIndex(siteUrl) {
  return sidebarGroups
    .map((group) => {
      const lines = pages(group).map(
        (page) => `- [${page.title}](${new URL(`/${page.slug}.md`, siteUrl).href}): ${page.description}`,
      );
      return lines.length ? [`**${group.label}**`, '', ...lines].join('\n') : '';
    })
    .filter(Boolean)
    .join('\n\n');
}

// A locale URL whose page has no translation renders the English text, so it
// stays out of the sitemap until someone translates it.
export function isUntranslatedLocalePage(url) {
  const path = new URL(url).pathname.replace(/^\/|\/$/g, '');
  if (!path.startsWith('zh-cn')) return false;
  const rest = path.replace(/^zh-cn\/?/, '');
  return !existsSync(join(docsDir, 'zh-cn', rest ? `${rest}.md` : 'index.md'));
}
