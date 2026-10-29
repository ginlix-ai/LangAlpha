/**
 * Page geometry, alone in a module so that importing it costs nothing else.
 *
 * It used to live in `shellPdf.ts`, which imports `printExport.css` for its side
 * effect. `printPageStyle.ts` reads it from there, and `e2e/export-print-color-
 * scheme.spec.js` reads that in turn under Playwright's own transform, which has
 * no CSS loader: a stylesheet in the import graph took the whole e2e suite down
 * at collection. Keep this file import-free.
 */

/**
 * A4 with 15mm margins: the geometry every export in this app prints at.
 *
 * `!important` carries it. Paged.js writes an unconditioned `@page { margin: 0 }`
 * into the head while a preview is up and it reaches print media too; in the page
 * context an important declaration beats a plain one on either side of it, while
 * two plain declarations fall back to source order, which is how an export once
 * came out full-bleed. Paged.js never writes `!important`, so this holds wherever
 * its styles land.
 */
export const A4_PAGE_RULE = '@page { size: A4 !important; margin: 15mm !important; }';
