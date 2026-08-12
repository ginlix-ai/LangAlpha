/**
 * Print-document preamble for react-to-print's iframe.
 *
 * Everything needed for a legible page must live here, and every rule needs
 * `!important`. This is the only stylesheet guaranteed to reach the iframe —
 * react-to-print inlines it, but re-fetches parent <link>s and prints anyway if
 * one 404s, so `ExportPreviewModal.css` (a lazy chunk under VITE_CDN_BASE) and
 * `tokens.css` (main bundle) can each go missing independently. It is also
 * appended *before* the parent's styles, so at equal specificity source order
 * would otherwise hand the win to index.html's unscoped dark rule.
 *
 * The token pins exist because Markdown.tsx sets `color`/`background`/`border`
 * inline from `var(--color-*)`, and the iframe carries no `data-theme` — so
 * those vars resolve to tokens.css's dark `:root` branch. Each pinned pair must
 * stay paired: fixing a foreground without its background just swaps one
 * invisible combination for another (dark ink on `--color-bg-code`'s near-black).
 * `--color-border-muted` is pinned to the tone the stylesheet already gives
 * `hr`/`td`, which also reveals the table wrapper and row rules — those carry
 * only the var, so they were invisible in every export, not just the fallback.
 */
export const PRINT_PAGE_STYLE = `
  @page { size: A4 !important; margin: 15mm !important; }
  html, body { color-scheme: light !important; background: #ffffff !important; }
  html {
    --color-text-primary: #1a1a1a !important;
    --color-text-tertiary: #6b7280 !important;
    --color-bg-code: #f7f6f3 !important;
    --color-bg-input: #f7f6f3 !important;
    --color-border-muted: #d1d5db !important;
  }
`;
