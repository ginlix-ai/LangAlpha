const MARKDOWN = /\.(md|markdown)$/i;

/** A Markdown file renders with a preview; anything else shows as source. */
export function isMarkdownPath(path: string): boolean {
  return MARKDOWN.test(path);
}
