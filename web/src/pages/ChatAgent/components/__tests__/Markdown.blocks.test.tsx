/**
 * Block-level rendering must be invisible: the HTML for a document rendered
 * block by block has to equal the HTML for the same document rendered whole,
 * at every point of a stream. The splitter is mocked so the same component
 * produces both sides.
 */
import { describe, it, expect, vi } from 'vitest';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { buildReply } from '@e2e/perf/streamFixture';

const mode = vi.hoisted(() => ({ whole: false }));
vi.mock('../../utils/markdownBlocks', async () => {
  const actual = await vi.importActual<typeof import('../../utils/markdownBlocks')>('../../utils/markdownBlocks');
  return {
    splitMarkdownBlocks: (text: string) => (mode.whole ? [text] : actual.splitMarkdownBlocks(text)),
  };
});
vi.mock('@/contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'dark', setTheme: () => {} }),
}));

import Markdown from '../Markdown';
import { splitMarkdownBlocks } from '../../utils/markdownBlocks';

function render(content: string, whole: boolean): string {
  mode.whole = whole;
  try {
    return renderToStaticMarkup(<Markdown variant="chat" content={content} onOpenFile={() => {}} />);
  } finally {
    mode.whole = false;
  }
}

const EDGE_DOCS: Record<string, string> = {
  listAfterParagraph: 'Intro\n- one\n\n- two\n',
  listAfterHeading: '# Title\n- a\n\n- b\n\nAfter\n',
  looseList: 'Intro\n\n- one\n\n- two\n\n  nested paragraph\n\n- three\n\nOutro\n',
  orderedRuns: '1. a\n2. b\n\nBreak\n\n3. c\n4. d\n',
  nestedFence: '- step\n\n  ```bash\n  ls -la\n\n  pwd\n  ```\n\n- next\n',
  mathBlock: 'Given\n\n$$\n\\alpha + \\beta\n\n= \\gamma\n$$\n\nso $x$.\n',
  tableThenQuote: '| a | b |\n|---|---|\n| 1 | 2 |\n\n> quoted\n\n> second quote\n',
  headingAfterFence: '```json\n{"a": 1}\n```\n## Next\ntext\n',
  citations: 'Revenue grew ([10-Q](https://example.com/10q)) and margins ([call](https://example.com/call?price=$100)).\n\nNext para.\n',
  currency: 'Costs $100 and $2,000 rose.\n\n$$E = mc^2$$\n\nInline $a+b$ math.\n',
  html: '<details>\n<summary>More</summary>\n\nHidden **text**\n\n</details>\n\nAfter\n',
  refs: 'See [docs][d].\n\n[d]: https://example.com\n',
  fileRefs: 'Open `__wsref__/ws1/report.md` and [chart](__wsref__/ws1/chart.png).\n\nDone.\n',
  emphasisTail: 'Some **bold and *nested* text',
  crlf: 'a\r\n\r\nb\r\n',
};

describe('Markdown block rendering parity', () => {
  it('matches whole-document rendering for the benchmark reply', () => {
    const doc = buildReply();
    expect(splitMarkdownBlocks(doc).length).toBeGreaterThan(20);
    expect(render(doc, false)).toBe(render(doc, true));
  });

  it('matches whole-document rendering for every streaming prefix of the reply', () => {
    const doc = buildReply();
    for (let end = 37; end <= doc.length; end += 211) {
      const prefix = doc.slice(0, end);
      expect(render(prefix, false), `prefix ${end}`).toBe(render(prefix, true));
    }
  });

  for (const [name, doc] of Object.entries(EDGE_DOCS)) {
    it(`matches whole-document rendering: ${name}`, () => {
      expect(render(doc, false)).toBe(render(doc, true));
      for (let end = 5; end < doc.length; end += 7) {
        const prefix = doc.slice(0, end);
        expect(render(prefix, false), `prefix ${end}`).toBe(render(prefix, true));
      }
    });
  }
});
