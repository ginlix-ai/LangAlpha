# LangAlpha docs site

The user and agent documentation for LangAlpha, built with [Starlight](https://starlight.astro.build). Pages are plain Markdown under `src/content/docs/`.

```bash
make docs-dev     # live preview at http://localhost:4321
make docs-build   # static site in docs/dist, fails on a broken internal link
```

Both need Node and pnpm, and no account or secret.

- **Site address**: `DOCS_SITE_URL` at build time (see `site.config.mjs`). Canonical links, the sitemap, Open Graph URLs and `llms.txt` derive from it.
- **Agent-readable output**: `/llms.txt`, `/llms-full.txt`, `/llms-small.txt`, and every English page as Markdown at its URL plus `.md` (`src/pages/[...slug].md.ts`).
- **Locales**: English at the root and `zh-cn`. A page with no translation falls back to English with a notice; translations go in `src/content/docs/zh-cn/` under the same path.
- **Screenshots** come from `../assets/readme/`, shared with the README, so a page references them as `../../../../../assets/readme/<file>`.

## Writing rules

- Every behavioral claim must match the code on `main`.
- Front matter needs `title` and a `description` of one sentence.
- Where the hosted platform and a self-hosted install differ, use one note titled exactly `Hosted and self-hosted`:

  ```md
  :::note[Hosted and self-hosted]
  **Hosted:** ...
  **Self-hosted:** ...
  :::
  ```
- No em or en dashes in copy, and no storage terms ("checkpoint", "fork", "branch").
- Link pages root-relative without an extension (`/concepts/computers/`); the build checks every internal link and anchor.
