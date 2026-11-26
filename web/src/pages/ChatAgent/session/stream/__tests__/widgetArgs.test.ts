// @vitest-environment node
/**
 * The widget's markup is read off the ShowWidget arguments as the model
 * writes them, a delta at a time. Wherever a delta boundary falls, inside an
 * escape or between the halves of a surrogate pair, the text read so far must
 * be a prefix of what the whole arguments decode to, and equal to it at the end.
 */
import { describe, expect, it } from 'vitest';
import { EMPTY_WIDGET_ARGS, feedWidgetArgs, type WidgetArgsStream } from '../widgetArgs';

const feedAll = (deltas: string[], from: WidgetArgsStream = EMPTY_WIDGET_ARGS) =>
  deltas.reduce(feedWidgetArgs, from);

/** Every way of cutting `text` into deltas of `size` characters. */
const cut = (text: string, size: number) => {
  const out: string[] = [];
  for (let i = 0; i < text.length; i += size) out.push(text.slice(i, i + size));
  return out;
};

const HTML = '<style>.a{color:red}</style>\n<div class="a">Q3 \u00e9\t"quoted" \\ / \ud83d\udcc8 \u2028</div>\r\n<script>if (a<b) x("\\n")</script>';
const ARGS = JSON.stringify({ title: 'Q3 \ud83d\ude80 "rev"', data_files: ['a.json', { x: '"html"' }], html: HTML, n: 5 });
// The same arguments with every non-ASCII character escaped, as Python's json.dumps writes them.
const ARGS_ASCII = ARGS.replace(/[\u007f-\uffff]/g, (ch) => `\\u${ch.charCodeAt(0).toString(16).padStart(4, '0')}`);

describe('feedWidgetArgs', () => {
  it.each([
    ['raw', ARGS],
    ['ascii-escaped', ARGS_ASCII],
  ])('decodes %s arguments identically at every delta size', (_label, args) => {
    for (const size of [1, 2, 3, 5, 7, 64, args.length]) {
      const state = feedAll(cut(args, size));
      expect(state.html).toBe(HTML);
      expect(state.title).toBe('Q3 \ud83d\ude80 "rev"');
    }
  });

  it('keeps what is read so far a prefix, with no half of a pair or escape showing', () => {
    let state = EMPTY_WIDGET_ARGS;
    for (const delta of cut(ARGS_ASCII, 1)) {
      state = feedWidgetArgs(state, delta);
      if (state.html === null) continue;
      expect(HTML.startsWith(state.html)).toBe(true);
      // A high surrogate is held until its low half arrives.
      const last = state.html.charCodeAt(state.html.length - 1);
      expect(last >= 0xd800 && last <= 0xdbff).toBe(false);
    }
  });

  it('reports no html until its string opens, then an empty one', () => {
    expect(feedAll(['{"title": "x", "da', 'ta_files": ["html"]']).html).toBeNull();
    expect(feedAll(['{"title": "x", "html"', ': "']).html).toBe('');
  });

  it('ignores an "html" key nested below the top level', () => {
    expect(feedAll(['{"data_files": [{"html": "no"}], "meta": {"html": "no"}}']).html).toBeNull();
  });

  it('never changes the state it was given', () => {
    const half = feedAll(['{"html": "<p>a\\u00', '']);
    const frozen = JSON.stringify(half);
    const whole = feedWidgetArgs(half, 'e9</p>"}');
    expect(JSON.stringify(half)).toBe(frozen);
    expect(whole.html).toBe('<p>a\u00e9</p>');
    // Replaying a delta from the same state gives the same answer.
    expect(feedWidgetArgs(half, 'e9</p>"}')).toEqual(whole);
  });

  it('keeps a lone surrogate as written and a bad escape as its text', () => {
    expect(feedAll(['{"html": "a\\ud83d b \\uzz12 c"}']).html).toBe('a\ud83d b uzz12 c');
  });

  it('costs the delta, not the text so far', () => {
    const body = 'x'.repeat(2_000_000);
    let state = feedWidgetArgs(EMPTY_WIDGET_ARGS, '{"html": "');
    const start = performance.now();
    for (let i = 0; i < 2000; i++) state = feedWidgetArgs(state, body.slice(i * 1000, (i + 1) * 1000));
    expect(state.html!.length).toBe(2_000_000);
    expect(performance.now() - start).toBeLessThan(1000);
  });
});
