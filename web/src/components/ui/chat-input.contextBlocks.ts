import type { TFunction } from 'i18next';
import { describeLocatorSize } from '@/pages/ChatAgent/utils/a1';
import type { MentionedFile } from './chat-input.types';

/** One more backtick than the longest run inside, so the body cannot close the fence. */
function fence(snippet: string | undefined): string {
  const body = snippet ?? '';
  const longest = Math.max(2, ...Array.from(body.matchAll(/`+/g), (m) => m[0].length));
  const ticks = '`'.repeat(longest + 1);
  return `${ticks}\n${body}\n${ticks}`;
}

/** A sheet name or a path may carry `<` or `>` and a chart label a newline;
 *  neither survives inside `<summary>` as the text it was. */
const summaryText = (text: string) => text.replace(/[<>]/g, '').replace(/[\r\n]+/g, ' ');

/**
 * The `<details>` block a context pill appends to the message on send. The
 * summary is what the agent reads first, so each source writes the one that
 * tells it how to treat the body: a label for pasted or quoted text, a hint for
 * a live chart, and a reopenable `@path#locator` or line range for a file.
 */
export function formatContextBlock(f: MentionedFile, t: TFunction): string {
  if (f.source === 'chat') {
    return `\n<details>\n<summary>[${t('context.fromAgentResponse')}]</summary>\n\n${fence(f.snippet)}\n</details>`;
  }
  if (f.source === 'paste') {
    return `\n<details>\n<summary>[${t('context.pastedText')}]</summary>\n\n${fence(f.snippet)}\n</details>`;
  }
  // A chart is a live view: the hint says what is on screen and nothing
  // more, so the agent fetches the bars itself rather than reading a copy.
  if (f.source === 'chart') {
    return `\n<details>\n<summary>[${t('context.chartInPanel')}: ${summaryText(f.label ?? '')}]</summary>\n\n${f.snippet}\n</details>`;
  }
  // A locator names the spot in the file's own vocabulary (`Model!B4:D9`),
  // so the summary is the link that reopens it. The block below it may be
  // capped, and says so in its own first line.
  if (f.locator) {
    const described = describeLocatorSize(f.locator);
    const size = described ? ` (${described})` : '';
    return `\n<details>\n<summary>@${summaryText(`${f.path}#${f.locator}`)}${size}</summary>\n\n${fence(f.snippet)}\n</details>`;
  }
  const lineInfo = f.lineStart != null
    ? ` (lines ${f.lineStart}-${f.lineEnd}, ${f.lineCount} line${f.lineCount !== 1 ? 's' : ''})`
    : '';
  return `\n<details>\n<summary>@${summaryText(f.path)}${lineInfo}</summary>\n\n${fence(f.snippet)}\n</details>`;
}
