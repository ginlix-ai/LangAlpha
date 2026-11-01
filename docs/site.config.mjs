// The one place the published address lives. Set DOCS_SITE_URL at build time
// (for example https://docs.example.com) once hosting is decided; canonical
// links, the sitemap, Open Graph URLs and llms.txt all derive from it.
export const siteUrl = process.env.DOCS_SITE_URL || 'http://localhost:4321';

export const repoUrl = 'https://github.com/ginlix-ai/langalpha';
export const readmeAssetsUrl = `${repoUrl.replace('github.com', 'raw.githubusercontent.com')}/main/assets/readme`;
