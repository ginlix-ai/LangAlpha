/**
 * The `html` and `title` of a ShowWidget call read off its arguments while
 * the model is still writing them, so the transcript can draw the widget as
 * it is written. The arguments arrive as raw deltas of one JSON object; this
 * keeps just enough of a JSON scanner to know when it is inside the top-level
 * `"html"` or `"title"` string and decodes those strings as they arrive.
 *
 * Each feed costs the delta, not the arguments so far: the scanner's position
 * is carried in the returned state, which is a new object every time (the
 * stream updaters it runs in may be replayed, so nothing is changed in place).
 */

import type { WidgetArgsStream } from '@/types/chat';

export type { WidgetArgsStream };

export const EMPTY_WIDGET_ARGS: WidgetArgsStream = {
  depth: 0,
  string: null,
  escape: null,
  highSurrogate: '',
  key: '',
  expectValue: false,
  html: null,
  title: null,
};

const SIMPLE_ESCAPES: Record<string, string> = {
  '"': '"', '\\': '\\', '/': '/', b: '\b', f: '\f', n: '\n', r: '\r', t: '\t',
};
const HEX4 = /^[0-9a-fA-F]{4}$/;

const isHigh = (unit: number) => unit >= 0xd800 && unit <= 0xdbff;
const isLow = (unit: number) => unit >= 0xdc00 && unit <= 0xdfff;

/** `state` with `delta`, the next piece of the arguments' JSON text, read. */
export function feedWidgetArgs(state: WidgetArgsStream, delta: string): WidgetArgsStream {
  if (!delta) return state;
  let { depth, string, escape, highSurrogate, key, expectValue, html, title } = state;

  // Decoded text for the string being read, joined once per feed.
  let out = '';
  const emit = (text: string) => {
    if (!text) return;
    if (highSurrogate) {
      // A lone high half stays as it was written; a pair joins as one character.
      out += highSurrogate;
      highSurrogate = '';
    }
    out += text;
  };
  const emitUnit = (unit: number) => {
    if (isHigh(unit)) {
      if (highSurrogate) out += highSurrogate;
      highSurrogate = String.fromCharCode(unit);
    } else if (isLow(unit) && highSurrogate) {
      out += highSurrogate + String.fromCharCode(unit);
      highSurrogate = '';
    } else {
      emit(String.fromCharCode(unit));
    }
  };
  const flush = () => {
    if (!out) return;
    if (string === 'html') html = (html ?? '') + out;
    else if (string === 'title') title = (title ?? '') + out;
    else if (string === 'key') key += out;
    out = '';
  };

  let i = 0;
  const n = delta.length;
  while (i < n) {
    if (string !== null) {
      if (escape !== null) {
        escape += delta[i++];
        if (escape[0] === 'u') {
          if (escape.length < 5) continue;
          const hex = escape.slice(1);
          if (HEX4.test(hex)) emitUnit(parseInt(hex, 16));
          else emit(escape);
        } else {
          emit(SIMPLE_ESCAPES[escape] ?? escape);
        }
        escape = null;
        continue;
      }
      // A run of plain characters up to the next backslash or quote.
      let stop = i;
      while (stop < n && delta[stop] !== '\\' && delta[stop] !== '"') stop++;
      if (stop > i) {
        if (string !== 'skip') {
          const run = delta.slice(i, stop);
          if (highSurrogate && isLow(run.charCodeAt(0))) {
            out += highSurrogate + run;
            highSurrogate = '';
          } else {
            emit(run);
          }
        }
        i = stop;
        continue;
      }
      if (delta[i] === '\\') {
        escape = '';
        i++;
        continue;
      }
      // The closing quote.
      i++;
      if (highSurrogate) {
        out += highSurrogate;
        highSurrogate = '';
      }
      flush();
      string = null;
      continue;
    }

    const c = delta[i++];
    if (c === '"') {
      if (depth === 1 && !expectValue) {
        string = 'key';
        key = '';
      } else if (depth === 1 && expectValue) {
        expectValue = false;
        if (key === 'html') {
          string = 'html';
          html = '';
        } else if (key === 'title') {
          string = 'title';
          title = '';
        } else {
          string = 'skip';
        }
      } else {
        string = 'skip';
      }
    } else if (c === '{' || c === '[') {
      depth++;
      if (depth === 2) expectValue = false;
    } else if (c === '}' || c === ']') {
      depth = Math.max(0, depth - 1);
    } else if (depth === 1 && c === ':') {
      expectValue = true;
    } else if (depth === 1 && c === ',') {
      expectValue = false;
    }
  }
  flush();
  return { depth, string, escape, highSurrogate, key, expectValue, html, title };
}
