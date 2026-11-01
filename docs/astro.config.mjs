import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightLlmsTxt from 'starlight-llms-txt';
import starlightLinksValidator from 'starlight-links-validator';
import sitemap from '@astrojs/sitemap';
import { siteUrl, repoUrl } from './site.config.mjs';
import { llmsPageIndex, isUntranslatedLocalePage, starlightSidebar } from './src/page-index.mjs';

export default defineConfig({
  site: siteUrl,
  trailingSlash: 'ignore',
  vite: {
    server: { fs: { allow: ['..'] } },
  },
  integrations: [
    sitemap({
      filter: (page) => !isUntranslatedLocalePage(page),
      i18n: { defaultLocale: 'root', locales: { root: 'en', 'zh-cn': 'zh-CN' } },
    }),
    starlight({
      title: 'LangAlpha Docs',
      description:
        'LangAlpha is an open-source harness for AI agents that research markets, build investment theses and trade your brokerage account within the permissions you set.',
      logo: {
        light: './src/assets/logo-light.svg',
        dark: './src/assets/logo-dark.svg',
        alt: 'LangAlpha',
        replacesTitle: false,
      },
      favicon: '/favicon.svg',
      social: [{ icon: 'github', label: 'GitHub', href: repoUrl }],
      editLink: { baseUrl: `${repoUrl}/edit/main/docs/` },
      lastUpdated: false,
      defaultLocale: 'root',
      locales: {
        root: { label: 'English', lang: 'en' },
        'zh-cn': { label: '简体中文', lang: 'zh-CN' },
      },
      customCss: ['./src/styles/theme.css'],
      routeMiddleware: './src/routeData.ts',
      head: [
        { tag: 'meta', attrs: { property: 'og:image', content: new URL('/og.png', siteUrl).href } },
        { tag: 'meta', attrs: { property: 'og:image:width', content: '1200' } },
        { tag: 'meta', attrs: { property: 'og:image:height', content: '630' } },
        { tag: 'meta', attrs: { name: 'twitter:image', content: new URL('/og.png', siteUrl).href } },
      ],
      sidebar: starlightSidebar(),
      plugins: [
        starlightLinksValidator({
          errorOnLocalLinks: false,
          exclude: ['/llms.txt', '/llms-full.txt', '/llms-small.txt', '/**/*.md'],
        }),
        starlightLlmsTxt({
          projectName: 'LangAlpha',
          details: [
            '- LangAlpha runs as a hosted platform and as a self-hosted open-source stack. Pages mark where the two behave differently with a "Hosted and self-hosted" note.',
            '- The hierarchy is User, Computer, Workspace, Thread, Turn.',
            '- Every page is also available as Markdown by appending .md to its URL, for example /concepts/computers.md.',
            '',
            llmsPageIndex(siteUrl),
          ].join('\n'),
          promote: ['index*', 'get-started/**', 'concepts/overview*'],
        }),
      ],
    }),
  ],
});
