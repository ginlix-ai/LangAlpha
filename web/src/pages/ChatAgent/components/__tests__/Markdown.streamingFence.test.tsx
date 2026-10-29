import { act, render } from '@testing-library/react';
import PrismAsyncLight from 'react-syntax-highlighter/dist/esm/prism-async-light';

vi.mock('@/contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'dark', setTheme: () => {} }),
}));

import Markdown from '../Markdown';
import { REVEAL_SETTLE_MS } from '../../utils/revealFade';
import { splitMarkdownBlocks } from '../../utils/markdownBlocks';
import { repairStreamingTail } from '../../utils/streamingTail';

// A block keeps its tree while lines land in a fence still open at its end
// (streamingLineKey), so the DOM a stream builds by growing one mounted block
// has to be the DOM a fresh mount of the same text builds, at every step.
const DOCS: Record<string, string> = {
  fenceUnderIntro: 'Here is the *script*:\n```python\ndef f(x):\n    """Doc\n    string."""\n    return x ** 2  # square\n```\nAnd **after** it, `inline`.\n',
  fenceInList: '- step one\n  ```bash\n  ls -la\n\n  pwd\n  ```\n- step *two*\n',
  fenceLeftByDedent: '- item\n  ```py\n  code = 1\nnext *para* here\n',
  tildeHoldsBackticks: '~~~md\n```js\nlet a = `x`;\n```\n~~~\nDone *now*.\n',
  inlineTripleTicks: '``` inline ` code ``` then *em*\nmore **text**\n',
};

// React leaves `style=""` on an element whose style emptied, where a fresh
// mount writes none. Same pixels, and it happens within a line under any key.
const raw = (el: Element): string => el.innerHTML.replaceAll(' style=""', '');

// What a reveal fades in through (utils/revealFade) depends on how the text
// arrived, so its spans are unwrapped and its marks on whole elements dropped:
// what sits under them has to match.
const markup = (el: Element): string => {
  const copy = el.cloneNode(true) as Element;
  for (const fresh of copy.querySelectorAll('[data-fresh]')) {
    if (fresh.tagName === 'SPAN') fresh.replaceWith(...fresh.childNodes);
    else fresh.removeAttribute('data-fresh');
  }
  copy.normalize();
  return raw(copy);
};

function html(content: string, streaming = false): string {
  const { container, unmount } = render(<Markdown variant="chat" content={content} onOpenFile={() => {}} streaming={streaming} />);
  const out = markup(container);
  unmount();
  return out;
}

function text(content: string, streaming: boolean): string {
  const { container, unmount } = render(<Markdown variant="chat" content={content} onOpenFile={() => {}} streaming={streaming} />);
  const out = container.textContent ?? '';
  unmount();
  return out;
}

// What a streaming render shows: its last block with the tail repaired. These
// docs pass through the prose rewrites unchanged, so the raw text splits as
// the processed text does.
function repaired(content: string): string {
  const blocks = splitMarkdownBlocks(content);
  blocks[blocks.length - 1] = repairStreamingTail(blocks[blocks.length - 1]).text;
  return blocks.join('');
}

type AstGenerator = { highlight: (code: string, language: string) => unknown };
const generator = (): AstGenerator => (PrismAsyncLight as unknown as { astGenerator: AstGenerator }).astGenerator;

describe('Markdown streaming through a fence', () => {
  beforeAll(async () => {
    await (PrismAsyncLight as unknown as { loadAstGenerator: () => Promise<unknown> }).loadAstGenerator();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  for (const [name, doc] of Object.entries(DOCS)) {
    it(`grows to the DOM a fresh mount draws: ${name}`, () => {
      const view = render(<Markdown variant="chat" content="" onOpenFile={() => {}} />);
      for (let end = 1; end <= doc.length; end += 2) {
        const prefix = doc.slice(0, end);
        act(() => view.rerender(<Markdown variant="chat" content={prefix} onOpenFile={() => {}} />));
        expect(markup(view.container), `at ${JSON.stringify(prefix)}`).toBe(html(prefix));
      }
      act(() => view.rerender(<Markdown variant="chat" content={doc} onOpenFile={() => {}} />));
      expect(markup(view.container)).toBe(html(doc));
      view.unmount();
    });
  }

  // While streaming, the fence open at the end highlights its typing line on
  // its own; its text has to stay the text one highlighter draws, and the
  // stream has to settle to exactly the markup above.
  for (const [name, doc] of Object.entries(DOCS)) {
    it(`streams the same text and settles to the same DOM: ${name}`, () => {
      vi.useFakeTimers();
      const view = render(<Markdown variant="chat" content="" onOpenFile={() => {}} streaming />);
      for (let end = 1; end <= doc.length; end += 2) {
        const prefix = doc.slice(0, end);
        act(() => view.rerender(<Markdown variant="chat" content={prefix} onOpenFile={() => {}} streaming />));
        expect(markup(view.container), `at ${JSON.stringify(prefix)}`).toBe(html(prefix, true));
        expect(view.container.textContent, `at ${JSON.stringify(prefix)}`).toBe(text(repaired(prefix), false));
      }
      act(() => view.rerender(<Markdown variant="chat" content={doc} onOpenFile={() => {}} />));
      act(() => {
        vi.advanceTimersByTime(REVEAL_SETTLE_MS);
      });
      // Settled, nothing is left to unwrap.
      expect(raw(view.container)).toBe(html(doc));
      view.unmount();
    });
  }

  it('highlights the finished lines of a streaming fence once per line, not per character', () => {
    const code = 'def f(x):\n    return x ** 2\n\nprint(f(3))\n';
    const spy = vi.spyOn(generator(), 'highlight');
    const view = render(<Markdown variant="chat" content="" onOpenFile={() => {}} streaming />);
    for (let end = 1; end <= code.length; end++) {
      act(() => view.rerender(<Markdown variant="chat" content={'```python\n' + code.slice(0, end)} onOpenFile={() => {}} streaming />));
    }
    const multiLine = spy.mock.calls.filter(([source]) => source.includes('\n')).length;
    spy.mockRestore();
    view.unmount();
    expect(multiLine).toBeLessThanOrEqual(code.split('\n').length);
  });
});
