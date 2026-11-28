/**
 * A PDF's text as its reader sees it, which is what a comment on it quotes.
 * The text layer holds what the PDF's fonts map their glyphs to, and two
 * mappings show one character but extract another:
 *
 * - ligatures, compatibility spaces and presentation forms, which pdf.js
 *   reads through `normalizeUnicode` when the reader copies (its viewer's
 *   copy handler), so a quote reads as a copy would;
 * - CJK radicals (U+2E80 to U+2FDF), which a CJK font often maps its glyphs
 *   to: the reader sees 长 or 毛 and the layer holds U+2ED3 or U+2F51.
 *   pdf.js's copy leaves these as they are, so they are read here as the
 *   ideograph drawn the same (`radicalIdeographs.ts`).
 */
import { normalizeUnicode } from 'pdfjs-dist';
import { RADICAL_IDEOGRAPHS } from './radicalIdeographs';

let ideographs: Map<string, string> | null = null;

function ideographOf(radical: string): string {
  if (!ideographs) {
    ideographs = new Map();
    for (let i = 0; i < RADICAL_IDEOGRAPHS.length; i += 2) {
      ideographs.set(String.fromCodePoint(RADICAL_IDEOGRAPHS[i]), String.fromCodePoint(RADICAL_IDEOGRAPHS[i + 1]));
    }
  }
  return ideographs.get(radical) ?? radical;
}

const RADICAL = /[\u2e80-\u2fdf]/g;

/**
 * `normalizeUnicode` a character at a time, as the quote search reads the
 * content (`readChars`), so a quote reads the same either way. pdf.js maps a
 * run of U+FB05 as a whole and has a mapping only for one, so "ﬅﬅ" read in
 * one call is "undefined". A text it leaves as it is skips the loop.
 */
function normalized(text: string): string {
  if (normalizeUnicode(text) === text) return text;
  let out = '';
  for (let i = 0; i < text.length; i++) out += normalizeUnicode(text.charAt(i));
  return out;
}

/** `text` as the reader sees it. */
export function seenText(text: string): string {
  return normalized(text).replaceAll('\0', '').replace(RADICAL, ideographOf);
}
