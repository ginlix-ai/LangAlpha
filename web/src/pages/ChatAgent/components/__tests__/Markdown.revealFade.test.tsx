import { act, render } from '@testing-library/react';
import PrismAsyncLight from 'react-syntax-highlighter/dist/esm/prism-async-light';

vi.mock('@/contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'dark', setTheme: () => {} }),
}));
// Stands in for the image that fetches, so a test reads the alt it was given.
vi.mock('../WorkspaceImage', () => ({ default: ({ alt }: { alt?: string }) => <img alt={alt} /> }));

import Markdown from '../Markdown';
import { MAX_MARKS, REVEAL_SETTLE_MS } from '../../utils/revealFade';

const md = (content: string, streaming = true) => (
  <Markdown variant="chat" content={content} onOpenFile={() => {}} streaming={streaming} />
);

// One mounted bubble taking each typewriter tick in turn.
function stream(ticks: string[], mount = '') {
  const view = render(md(mount));
  for (const tick of ticks) act(() => view.rerender(md(tick)));
  return view;
}

const freshSpans = (el: Element) => [...el.querySelectorAll<HTMLElement>('[data-fresh]')];
const idOf = (el: HTMLElement) => Number(el.dataset.fresh);

// React leaves `style=""` on an element whose style emptied, where a fresh
// mount writes none.
const markup = (el: Element) => el.innerHTML.replaceAll(' style=""', '');

describe('Markdown reveal fade', () => {
  beforeAll(async () => {
    await (PrismAsyncLight as unknown as { loadAstGenerator: () => Promise<unknown> }).loadAstGenerator();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
    document.getSelection()?.removeAllRanges();
  });

  it('wraps what arrives after mount and leaves the text above it bare', () => {
    // Longer than the tail a mid-stream mount fades in, so the first
    // paragraph mounts settled.
    const head = 'Already read.\n\n' + 'Filler words here. '.repeat(20) + '\n\n';
    const view = stream([head + 'Fresh words.'], head);
    const paragraphs = view.container.querySelectorAll('p');
    expect(paragraphs).toHaveLength(3);
    expect(freshSpans(paragraphs[0])).toEqual([]);
    const newest = Math.max(...freshSpans(view.container).map(idOf));
    expect(freshSpans(paragraphs[2]).map((s) => [idOf(s), s.textContent])).toEqual([[newest, 'Fresh words.']]);
  });

  // CodeBlock reads the fence's code as a string and types its own line.
  it('never wraps inside a fence', () => {
    const view = stream(['Intro.\n```python\nx = 1\n', 'Intro.\n```python\nx = 1\ny = 2\n']);
    const pre = view.container.querySelector('pre')!;
    expect(pre.textContent).toContain('x = 1\ny = 2');
    expect(pre.querySelector('[data-fresh]')).toBeNull();
    expect(freshSpans(view.container).map((s) => s.textContent)).toEqual(['Intro.']);
  });

  // Its component reads the children as a string, so it fades as one unit,
  // from the reveal that began it, on its own element.
  it('marks inline code whole, in place', () => {
    const view = stream(['Run `npm', 'Run `npm test` now.']);
    const code = view.container.querySelector('code')!;
    expect(code.textContent).toBe('npm test');
    expect(code.querySelector('[data-fresh]')).toBeNull();
    const [before, unit] = freshSpans(view.container);
    expect(before.textContent).toBe('Run ');
    expect(unit).toBe(code);
    expect(code.parentElement).toBe(view.container.querySelector('p'));
    expect(idOf(code)).toBe(idOf(before));
  });

  // A wrapper would remount the citation when its mark goes, which drops its
  // focus and its hover card.
  it('fades a citation on its own element and keeps it when the mark goes', () => {
    vi.useFakeTimers();
    const doc = 'Revenue rose ([example.com](https://example.com/q)) today.';
    const view = stream(['Revenue rose', doc]);
    const pill = view.container.querySelector<HTMLElement>('a.cite-bubble-pill')!;
    expect(idOf(pill)).toBe(Math.max(...freshSpans(view.container).map(idOf)));

    act(() => view.rerender(md(doc, false)));
    act(() => {
      vi.advanceTimersByTime(REVEAL_SETTLE_MS);
    });
    expect(freshSpans(view.container)).toEqual([]);
    expect(view.container.querySelector('a.cite-bubble-pill')).toBe(pill);
  });

  it('gives a linked image its label as alt while the label is fresh', () => {
    const view = stream(['See ', 'See [Revenue *chart*](charts/revenue.png)']);
    expect(view.container.querySelector('img')?.getAttribute('alt')).toBe('Revenue chart');
  });

  it('marks nothing under reduced motion', () => {
    vi.spyOn(window, 'matchMedia').mockImplementation((query: string) => ({
      matches: query.includes('prefers-reduced-motion'),
      media: query,
      onchange: null,
      addListener: () => {},
      removeListener: () => {},
      addEventListener: () => {},
      removeEventListener: () => {},
      dispatchEvent: () => false,
    }));
    const view = stream(['Hello', 'Hello world']);
    expect(view.container.textContent).toBe('Hello world');
    expect(freshSpans(view.container)).toEqual([]);
  });

  it('leaves a blank reveal bare', () => {
    const view = stream(['Hello', 'Hello ', 'Hello world']);
    const p = view.container.querySelector('p')!;
    expect(p.textContent).toBe('Hello world');
    expect(freshSpans(p).map((s) => s.textContent)).toEqual(['Hello', 'world']);
  });

  // A span edge inside a word shapes the word in two pieces: it breaks the
  // `fi` ligature, and under a link's break-all Chrome drew "filing" as "fiil".
  it.each([
    ['a link', 'See the [10-Q fili', 'See the [10-Q filing](https://example.com/q) now'],
    ['prose', 'The quarterly fili', 'The quarterly filing landed.'],
  ])('never puts a span edge inside a word in %s', (_, head, full) => {
    const view = stream([head, full]);
    const text = view.container.textContent ?? '';
    const letter = (c: string | undefined) => !!c && /\p{L}/u.test(c);
    const spans = freshSpans(view.container);
    for (const span of spans) {
      const range = document.createRange();
      range.setStart(view.container, 0);
      range.setEndBefore(span);
      const start = range.toString().length;
      const end = start + (span.textContent ?? '').length;
      expect(letter(text[start - 1]) && letter(text[start]), `before ${JSON.stringify(span.textContent)}`).toBe(false);
      expect(letter(text[end - 1]) && letter(text[end]), `after ${JSON.stringify(span.textContent)}`).toBe(false);
    }
    // The later reveal still has its own span past the word.
    expect(spans.some((s) => s.textContent?.includes('filing'))).toBe(true);
    expect(new Set(spans.map(idOf)).size).toBe(2);
  });

  // The typewriter can stop inside one drawn character, and each half of a
  // cut is drawn on its own: a lone surrogate, a text-style heart, a flag as
  // two letters, a family as three people.
  it.each([
    ['a surrogate pair', 'Up 📈 now', '📈'],
    ['a variation selector', 'I ❤️ it', '❤️'],
    ['a flag', 'Go 🇺🇸 now', '🇺🇸'],
    ['a skin tone', 'Thumb 👍🏽 ok', '👍🏽'],
    ['a joined emoji', 'A 👨‍👩‍👧 b', '👨‍👩‍👧'],
  ])('never cuts inside %s', (_, full, cluster) => {
    const at = full.indexOf(cluster);
    for (let cut = at + 1; cut < at + cluster.length; cut++) {
      const view = stream([full.slice(0, cut), full]);
      const spans = freshSpans(view.container).map((s) => s.textContent ?? '');
      expect(spans.join(''), `cut at ${cut}`).toBe(full);
      expect(spans.some((s) => s.includes(cluster)), `cut at ${cut}`).toBe(true);
      view.unmount();
    }
  });

  // These merge with a synthetic node in parse5 and come out with no position
  // of their own; read off their neighbours, they still fade.
  it.each([
    ['after a hard break', 'Line one  \n', 'Line one  \nline two', 'line two'],
    ['in a task item', '- [ ] Buy', '- [ ] Buy milk now', 'milk now'],
  ])('fades text %s', (_, head, full, tail) => {
    const view = stream([head, full]);
    const spans = freshSpans(view.container);
    const newest = Math.max(...spans.map(idOf));
    const fresh = spans.find((s) => s.textContent?.includes(tail));
    expect(fresh && idOf(fresh)).toBe(newest);
  });

  // The newline merged in after a hard break has no source character. Counted
  // as one, it read every later character one newer, so a word ending one
  // reveal faded again with the next.
  it('keeps a word after a hard break in the reveal it arrived in', () => {
    const view = stream(['Line one  \nI', 'Line one  \nI am']);
    const spans = freshSpans(view.container);
    const word = spans.find((s) => s.textContent?.includes('I'));
    expect(word && idOf(word)).toBe(idOf(spans.find((s) => s.textContent?.includes('Line one'))!));
    expect(spans.at(-1)?.textContent).toBe(' am');
  });

  it('keeps a list item fading when a nested list arrives under it', () => {
    const view = stream(['- Item text\n', '- Item text\n  - nested']);
    const item = freshSpans(view.container).find((s) => s.textContent?.includes('Item text'));
    expect(item).toBeDefined();
    expect(idOf(item!)).toBe(Math.min(...freshSpans(view.container).map(idOf)));
  });

  // Ideographs are set without spaces, so a run of them would otherwise fade
  // as one block under its oldest reveal.
  it('still splits a CJK run between reveals', () => {
    const view = stream(['收入增长', '收入增长加速']);
    expect(freshSpans(view.container).map((s) => s.textContent)).toEqual(['收入增长', '加速']);
  });

  // Where a word meets punctuation no ligature spans the cut, so a URL, a
  // path or a long number still fades as it types instead of popping in
  // under its first reveal. A run of symbols is shaped as one (-> draws an
  // arrow in the UI font), so it holds together like a word.
  it.each([
    ['splits a URL where a word meets punctuation', ['See example', 'See example.com/docs'], ['See example', '.com/docs']],
    ['keeps a run of symbols whole', ['a -', 'a -> b'], ['a ->', ' b']],
  ])('%s', (_, ticks, expected) => {
    const view = stream(ticks);
    expect(freshSpans(view.container).map((s) => s.textContent)).toEqual(expected);
  });

  // The text node is shorter than its source here, so a character's source
  // offset is not its index. Read by index, the appended text would start
  // inside the previous reveal.
  it.each([
    ['an escape', 'A \\* B', ' and more'],
    ['an entity', 'Tom &amp; Jerry', ' run off'],
  ])('keeps an appended word in the newest reveal after %s', (_, head, tail) => {
    const view = stream([head, head + tail]);
    const spans = freshSpans(view.container);
    const last = spans.at(-1)!;
    expect(idOf(last)).toBe(Math.max(...spans.map(idOf)));
    expect(last.textContent).toBe(tail);
  });

  it('settles to the DOM of a reply that never streamed', () => {
    vi.useFakeTimers();
    const doc = 'Run `npm test`, then **check** it.\n\n- one\n- two &amp; three\n\n```python\nx = 1\n```\n\nDone \\* now.\n';
    const ticks: string[] = [];
    for (let end = 3; end < doc.length; end += 5) ticks.push(doc.slice(0, end));
    const view = stream(ticks);

    act(() => view.rerender(md(doc, false)));
    // The last reveals are still fading when the stream ends.
    expect(freshSpans(view.container)).not.toEqual([]);

    act(() => {
      vi.advanceTimersByTime(REVEAL_SETTLE_MS - 1);
    });
    expect(freshSpans(view.container)).not.toEqual([]);
    act(() => {
      vi.advanceTimersByTime(1);
    });
    expect(freshSpans(view.container)).toEqual([]);
    const settled = render(md(doc, false));
    expect(markup(view.container)).toBe(markup(settled.container));
  });

  // Unwrapping the spans replaces the text nodes a selection is anchored in.
  it('holds the marks mid-stream while a selection reaches into the reply', () => {
    let text = 'Selected paragraph here.\n\nw0';
    const view = stream(['Selected paragraph here.\n\n', text]);
    const first = freshSpans(view.container.querySelector('p')!)[0];
    const anchor = first.firstChild!;
    const selection = document.getSelection()!;
    act(() => {
      selection.selectAllChildren(first);
      document.dispatchEvent(new Event('selectionchange'));
    });
    for (let i = 1; i <= 3 * MAX_MARKS; i++) {
      text += ` w${i}`;
      act(() => view.rerender(md(text)));
    }
    expect(anchor.isConnected).toBe(true);
    // The reveals after the selection still go, so a long reply stays bounded.
    expect(freshSpans(view.container).length).toBeLessThanOrEqual(MAX_MARKS + 2);

    act(() => {
      selection.removeAllRanges();
      document.dispatchEvent(new Event('selectionchange'));
    });
    // Released, it goes with the next batch.
    for (let i = 0; i <= MAX_MARKS && anchor.isConnected; i++) {
      text += ' x';
      act(() => view.rerender(md(text)));
    }
    expect(anchor.isConnected).toBe(false);
  });

  // WebKit reports a selection that runs into a shadow tree as uncollapsed
  // with no range, and getRangeAt(0) throws on it.
  it('reads a selection with no range as none', () => {
    vi.useFakeTimers();
    const view = stream(['Hello', 'Hello world']);
    act(() => view.rerender(md('Hello world', false)));
    act(() => {
      document.getSelection()!.selectAllChildren(view.container.querySelector('p')!);
      document.dispatchEvent(new Event('selectionchange'));
    });
    vi.spyOn(document, 'getSelection').mockReturnValue({
      isCollapsed: false,
      rangeCount: 0,
      getRangeAt: () => {
        throw new DOMException('The index is not in the allowed range.', 'IndexSizeError');
      },
    } as unknown as Selection);
    act(() => {
      document.dispatchEvent(new Event('selectionchange'));
    });
    act(() => {
      vi.advanceTimersByTime(REVEAL_SETTLE_MS);
    });
    expect(freshSpans(view.container)).toEqual([]);
  });

  it('holds the settle while a selection reaches into the reply', () => {
    vi.useFakeTimers();
    const view = stream(['Hello', 'Hello world']);
    act(() => view.rerender(md('Hello world', false)));
    const selection = document.getSelection()!;
    act(() => {
      selection.selectAllChildren(view.container.querySelector('p')!);
      document.dispatchEvent(new Event('selectionchange'));
    });
    act(() => {
      vi.advanceTimersByTime(REVEAL_SETTLE_MS * 4);
    });
    expect(freshSpans(view.container)).not.toEqual([]);

    act(() => {
      selection.removeAllRanges();
      document.dispatchEvent(new Event('selectionchange'));
    });
    act(() => {
      vi.advanceTimersByTime(REVEAL_SETTLE_MS);
    });
    expect(freshSpans(view.container)).toEqual([]);
  });
});

describe('Markdown reveal fade, animated', () => {
  let fades: { el: Element; startTime: number | null; cancel: () => void }[];
  let now: number;

  beforeEach(() => {
    fades = [];
    now = 1000;
    vi.spyOn(performance, 'now').mockImplementation(() => now);
    // jsdom has no Web Animations; record what the hook asks for.
    Element.prototype.animate = function (this: Element) {
      const fade = { el: this, startTime: null, cancel: vi.fn() };
      fades.push(fade);
      return fade as unknown as Animation;
    };
  });

  afterEach(() => {
    delete (Element.prototype as Partial<Element>).animate;
    vi.restoreAllMocks();
  });

  // The live block remounts on each newline, which rebuilds every span in it.
  it('anchors a rebuilt span to the time its text was revealed', () => {
    const view = render(md(''));
    act(() => view.rerender(md('Hello')));
    now = 1100;
    act(() => view.rerender(md('Hello\nworld')));

    const hello = fades.filter((f) => f.el.textContent === 'Hello');
    expect(hello).toHaveLength(2);
    expect(hello[1].el).not.toBe(hello[0].el);
    expect(hello.map((f) => f.startTime)).toEqual([1000, 1000]);
    expect(fades.find((f) => f.el.textContent?.trim() === 'world')?.startTime).toBe(1100);

    // Rebuilt again once its fade is over, it lands settled.
    now = 2000;
    act(() => view.rerender(md('Hello\nworld\nagain')));
    expect(fades.filter((f) => f.el.textContent === 'Hello')).toHaveLength(2);
  });

  // Text applied while the page was hidden was never watched arriving.
  it('starts no fade for text applied while the page is hidden', () => {
    vi.spyOn(document, 'hidden', 'get').mockReturnValue(true);
    stream(['Hello', 'Hello world']);
    expect(fades).toEqual([]);
  });

  it('starts no fade under reduced motion', () => {
    vi.spyOn(window, 'matchMedia').mockImplementation((query: string) => ({
      matches: query.includes('prefers-reduced-motion'),
      media: query,
      onchange: null,
      addListener: () => {},
      removeListener: () => {},
      addEventListener: () => {},
      removeEventListener: () => {},
      dispatchEvent: () => false,
    }));
    stream(['Hello', 'Hello world']);
    expect(fades).toEqual([]);
  });
});
