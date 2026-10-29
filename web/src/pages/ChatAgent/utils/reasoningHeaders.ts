/**
 * A thought's phase headers: the whole-line `**bold**` paragraphs of a block
 * that OPENS with one. A model that summarizes its thinking as headers
 * (`**Checking the filing**` … `**Drafting the answer**`) leads with one, and
 * each is a phase the row can be named after. A block that opens with prose
 * is prose: a bold line later in it is emphasis or a sub-section, and lifting
 * it up as the row's label reads as random words in the header's place.
 */
const HEADER_LINE = /^\s*\*\*([^*\n]+?)\*\*[ \t]*$/;
const MAX_HEADER_CHARS = 80;

export function extractReasoningHeaders(content: string | null | undefined): string[] {
  if (!content) return [];
  const out: string[] = [];
  let first = true;
  for (const para of content.split(/\n\s*\n/)) {
    if (!para.trim()) continue;
    const m = para.match(HEADER_LINE);
    if (first) {
      first = false;
      if (!m) return [];
    }
    if (!m) continue;
    const h = m[1].trim();
    if (h && h.length <= MAX_HEADER_CHARS) out.push(h);
  }
  return out;
}

export function extractLeadingBoldHeader(content: string): { title: string | null; body: string } {
  if (!content) return { title: null, body: content };
  const match = content.match(/^\s*\*\*([^*\n]+?)\*\*[ \t]*(?:\n+([\s\S]*))?$/);
  if (!match) return { title: null, body: content };
  const candidate = match[1].trim();
  const body = (match[2] ?? '').trim() ? match[2] : '';
  if (!candidate || candidate.length > 80) {
    return { title: null, body: content };
  }
  return { title: candidate, body };
}
