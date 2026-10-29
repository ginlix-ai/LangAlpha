/**
 * Excel number-format codes, read far enough to show a cell the way its author
 * saw it: percents, thousands, fixed decimals, a currency symbol, scaled
 * thousands, a negative section, and dates.
 *
 * Deliberately partial. A code this reader cannot follow falls back to the raw
 * value rather than to a guess, because a wrong number in a financial model is
 * worse than an unformatted one. Fraction codes (`# ?/?`) fall back the same
 * way. Date codes are rendered token by token: `y`, `m`, `d`, `h`, `s` runs,
 * weekday names, `AM/PM`, and quoted or escaped literals are honored; month
 * and weekday names follow the UI locale. Elapsed time (`[h]`), fractional
 * seconds (`ss.000`) and era or calendar variants (`e`, `g`, `b`) are not
 * rendered as such: bracket contents are dropped and the rest prints as
 * written.
 */
import { createDateFormatter, createFormatter } from '@/lib/format';

export type ExcelScalar = string | number | boolean | Date | null | undefined;

/** Which day a serial counts from. A workbook picks one for all its sheets. */
export interface DateSystem {
  /** Serial 0 is 1904-01-01, the Mac Excel convention, rather than 1899-12-30. */
  date1904?: boolean;
}

/** 1970-01-01 as a 1900-system serial. A serial is a UTC instant, and stays one
 *  all the way to the formatter, so the calendar date shown is the one the file
 *  stores. */
const EPOCH_OFFSET_DAYS = 25569;
/** Days from the 1900 system's day zero to the 1904 system's. */
const DATE_1904_OFFSET_DAYS = 1462;
const MS_PER_DAY = 86400000;

/**
 * The 1900 system counts 1900-02-29, a day that never happened, as serial 60,
 * so every serial before it lands one day early under the plain subtraction.
 * Serial 60 itself has no date to be; it prints as 1900-03-01, the same as 61.
 */
export function excelSerialToDate(serial: number, system?: DateSystem): Date {
  const days = system?.date1904
    ? serial + DATE_1904_OFFSET_DAYS
    : serial < 61 ? serial + 1 : serial;
  return new Date(Math.round((days - EPOCH_OFFSET_DAYS) * MS_PER_DAY));
}

/** Literals, colour/locale brackets and width placeholders, none of which say
 *  anything about the shape of the number underneath. */
function stripLiterals(fmt: string): string {
  return fmt
    .replace(/"[^"]*"/g, '')
    .replace(/\[[^\]]*\]/g, '')
    .replace(/\\./g, '')
    .replace(/[_*]./g, '');
}

const DATE_LETTERS = /^[ymdhsapge]+$/i;

/**
 * A date code is one whose every letter run is a date token. `General` fails on
 * `n`/`r`/`l`, `0.00E+00` has no `y`/`m`/`d`/`h`/`s`, and `#,##0 "M"` has no
 * letters left once its literal is gone.
 */
export function isDateFormat(fmt: string | undefined | null): boolean {
  if (!fmt) return false;
  const bare = stripLiterals(fmt);
  const runs = bare.match(/[a-z]+/gi);
  if (!runs || !runs.every((r) => DATE_LETTERS.test(r))) return false;
  return /[ymdhs]/i.test(bare);
}

// Names come from the UI locale, but the code's own token order decides the
// layout: `d-mmm` prints `5-Jan` whatever the locale would have put first.
const monthName = {
  short: createDateFormatter({ month: 'short', timeZone: 'UTC' }),
  long: createDateFormatter({ month: 'long', timeZone: 'UTC' }),
};
const weekdayName = {
  short: createDateFormatter({ weekday: 'short', timeZone: 'UTC' }),
  long: createDateFormatter({ weekday: 'long', timeZone: 'UTC' }),
};

type DateToken =
  | { kind: 'literal'; text: string }
  | { kind: 'y' | 'm' | 'd' | 'h' | 's'; len: number }
  | { kind: 'ampm'; short: boolean };

/** The first section of a date code, split into tokens with literals kept. */
function tokenizeDate(fmt: string): DateToken[] {
  const src = splitSections(fmt)[0];
  const out: DateToken[] = [];
  for (let i = 0; i < src.length; ) {
    const ch = src[i];
    if (ch === '"') {
      const end = src.indexOf('"', i + 1);
      out.push({ kind: 'literal', text: src.slice(i + 1, end < 0 ? src.length : end) });
      i = end < 0 ? src.length : end + 1;
    } else if (ch === '\\') {
      out.push({ kind: 'literal', text: src[i + 1] ?? '' });
      i += 2;
    } else if (ch === '_' || ch === '*') {
      i += 2;
    } else if (ch === '[') {
      const end = src.indexOf(']', i);
      i = end < 0 ? src.length : end + 1;
    } else if (/^AM\/PM/i.test(src.slice(i))) {
      out.push({ kind: 'ampm', short: false });
      i += 5;
    } else if (/^A\/P/i.test(src.slice(i))) {
      out.push({ kind: 'ampm', short: true });
      i += 3;
    } else if (/[ymdhs]/i.test(ch)) {
      let j = i;
      while (j < src.length && src[j].toLowerCase() === ch.toLowerCase()) j++;
      out.push({ kind: ch.toLowerCase() as 'y' | 'm' | 'd' | 'h' | 's', len: j - i });
      i = j;
    } else if (ch === 'e' || ch === 'E') {
      // Excel's `e` is the year in the current calendar; the Gregorian one here.
      let j = i;
      while (j < src.length && /e/i.test(src[j])) j++;
      out.push({ kind: 'y', len: 4 });
      i = j;
    } else if (/[gG]/.test(ch)) {
      i++;
    } else {
      out.push({ kind: 'literal', text: ch });
      i++;
    }
  }
  return out;
}

const pad2 = (n: number) => String(n).padStart(2, '0');

function renderDate(date: Date, tokens: DateToken[], locale: string): string {
  const twelveHour = tokens.some((tk) => tk.kind === 'ampm');
  const hours = date.getUTCHours();
  let out = '';
  tokens.forEach((tk, idx) => {
    switch (tk.kind) {
      case 'literal':
        out += tk.text;
        break;
      case 'y':
        out += tk.len <= 2 ? pad2(date.getUTCFullYear() % 100) : String(date.getUTCFullYear());
        break;
      case 'm': {
        // `m` beside an hour or a second is minutes, not a month.
        const prev = tokens[idx - 1];
        const next = tokens[idx + 1];
        const beforeS = next?.kind === 'literal' && tokens[idx + 2]?.kind === 's';
        const afterH = prev?.kind === 'literal' && tokens[idx - 2]?.kind === 'h';
        if (tk.len <= 2 && (afterH || beforeS || prev?.kind === 'h' || next?.kind === 's')) {
          out += tk.len === 1 ? String(date.getUTCMinutes()) : pad2(date.getUTCMinutes());
          break;
        }
        const month = date.getUTCMonth() + 1;
        if (tk.len === 1) out += String(month);
        else if (tk.len === 2) out += pad2(month);
        else if (tk.len === 3) out += monthName.short(date, locale);
        else if (tk.len === 4) out += monthName.long(date, locale);
        else out += monthName.long(date, locale).charAt(0);
        break;
      }
      case 'd': {
        const day = date.getUTCDate();
        if (tk.len === 1) out += String(day);
        else if (tk.len === 2) out += pad2(day);
        else if (tk.len === 3) out += weekdayName.short(date, locale);
        else out += weekdayName.long(date, locale);
        break;
      }
      case 'h': {
        const h = twelveHour ? (hours % 12 || 12) : hours;
        out += tk.len === 1 ? String(h) : pad2(h);
        break;
      }
      case 's':
        out += tk.len === 1 ? String(date.getUTCSeconds()) : pad2(date.getUTCSeconds());
        break;
      case 'ampm': {
        const pm = hours >= 12;
        out += tk.short ? (pm ? 'P' : 'A') : (pm ? 'PM' : 'AM');
        break;
      }
    }
  });
  return out;
}

// One memoised formatter per distinct shape. `createFormatter` already keeps
// one Intl instance per locale inside each closure, so the cache never needs
// clearing.
const numberCache = new Map<string, (n: number, locale: string) => string>();

function numberFormatter(opts: Intl.NumberFormatOptions): (n: number, locale: string) => string {
  const key = JSON.stringify(opts);
  let fmt = numberCache.get(key);
  if (!fmt) {
    fmt = createFormatter(opts);
    numberCache.set(key, fmt);
  }
  return fmt;
}

const generalPlain = createFormatter({ maximumSignificantDigits: 11, useGrouping: false });
const generalInt = createFormatter({ maximumFractionDigits: 0, useGrouping: false });

/**
 * Excel's `General`: eleven significant digits, an exponent below 1e-4 as
 * Excel does, or once an integer would run past its precision. An integer in
 * range keeps every digit: a market cap rounded to eleven reads as a different
 * number. This text reaches the agent in a snippet, where `0` for 1e-12 is a
 * wrong number, not a short one.
 */
function general(value: number, locale: string): string {
  const abs = Math.abs(value);
  if (abs !== 0 && (abs < 1e-4 || abs >= 1e15)) return Number(value.toExponential(10)).toExponential();
  return Number.isInteger(value) ? generalInt(value, locale) : generalPlain(value, locale);
}

interface NumberSection {
  prefix: string;
  suffix: string;
  percent: boolean;
  scientific: boolean;
  /** A trailing comma divides by a thousand: `#,##0,,` reads in millions. */
  scale: number;
  grouping: boolean;
  minFrac: number;
  maxFrac: number;
  minInt: number;
  hasDigits: boolean;
}

/** `;` separates the positive / negative / zero / text sections, except inside
 *  a quoted literal or a bracket. */
export function splitSections(fmt: string): string[] {
  const out: string[] = [];
  let cur = '';
  let quoted = false;
  let bracket = false;
  for (let i = 0; i < fmt.length; i++) {
    const ch = fmt[i];
    if (ch === '\\') {
      cur += ch + (fmt[i + 1] ?? '');
      i++;
      continue;
    }
    if (ch === '"') quoted = !quoted;
    else if (!quoted && ch === '[') bracket = true;
    else if (!quoted && ch === ']') bracket = false;
    else if (ch === ';' && !quoted && !bracket) {
      out.push(cur);
      cur = '';
      continue;
    }
    cur += ch;
  }
  out.push(cur);
  return out;
}

function parseSection(src: string): NumberSection {
  let prefix = '';
  let suffix = '';
  let core = '';
  let seen = false;
  let percent = false;
  let scientific = false;
  const push = (s: string) => {
    if (seen) suffix += s;
    else prefix += s;
  };

  for (let i = 0; i < src.length; ) {
    const ch = src[i];
    if (ch === '"') {
      const end = src.indexOf('"', i + 1);
      push(src.slice(i + 1, end < 0 ? src.length : end));
      i = end < 0 ? src.length : end + 1;
    } else if (ch === '\\') {
      push(src[i + 1] ?? '');
      i += 2;
    } else if (ch === '_' || ch === '*') {
      // Width padding and fill: they change spacing, not the value.
      i += 2;
    } else if (ch === '[') {
      const end = src.indexOf(']', i);
      const inner = src.slice(i + 1, end < 0 ? src.length : end);
      // `[$€-407]` carries a currency symbol; `[Red]` and `[>=100]` carry none.
      const currency = /^\$([^-\]]*)/.exec(inner);
      if (currency?.[1]) push(currency[1]);
      i = end < 0 ? src.length : end + 1;
    } else if (ch === '%') {
      // The percent style draws its own sign, in the place the locale wants it.
      percent = true;
      i++;
    } else if (ch === '#' || ch === '0' || ch === '?' || ch === '.' || ch === ',') {
      seen = true;
      core += ch;
      i++;
    } else if ((ch === 'E' || ch === 'e') && (src[i + 1] === '+' || src[i + 1] === '-')) {
      scientific = true;
      let j = i + 2;
      while (j < src.length && '0#?'.includes(src[j])) j++;
      i = j;
    } else {
      push(ch);
      i++;
    }
  }

  const trailing = /,+$/.exec(core);
  const body = trailing ? core.slice(0, -trailing[0].length) : core;
  const dot = body.indexOf('.');
  const int = dot < 0 ? body : body.slice(0, dot);
  const frac = dot < 0 ? '' : body.slice(dot + 1);
  const count = (s: string, re: RegExp) => (s.match(re) ?? []).length;

  return {
    prefix,
    suffix,
    percent,
    scientific,
    scale: trailing ? 1 / 1000 ** trailing[0].length : 1,
    grouping: body.includes(','),
    minFrac: Math.min(count(frac, /0/g), 20),
    maxFrac: Math.min(count(frac, /[0#?]/g), 20),
    minInt: Math.min(Math.max(count(int, /0/g), 1), 21),
    hasDigits: /[#0?]/.test(body),
  };
}

/** A section chosen by a condition rather than by sign. Not implemented, and
 *  picking by sign anyway prints the wrong section as if it were right. Excel
 *  lets a colour lead the condition, as in `[Red][<0]0.0`, so the leading
 *  bracket directives are stepped over before the condition is looked for. */
const CONDITIONAL = /^(?:\[[^\]]*\])*\[[<>=]/;
/** Elapsed time (`[h]`, `[mm]`, `[ss]`) and fractional seconds (`ss.0`), which
 *  the date renderer has no tokens for and would print as zeros. */
const UNSUPPORTED_TIME = /\[[hms]+\]|ss\.0/i;

function renderSection(value: number, section: NumberSection, locale: string): string {
  const opts: Intl.NumberFormatOptions = {
    style: section.percent ? 'percent' : 'decimal',
    useGrouping: section.grouping,
    minimumFractionDigits: section.minFrac,
    maximumFractionDigits: Math.max(section.maxFrac, section.minFrac),
    minimumIntegerDigits: section.minInt,
  };
  if (section.scientific) opts.notation = 'scientific';
  return section.prefix + numberFormatter(opts)(value * section.scale, locale) + section.suffix;
}

/**
 * A number under its format code. Excel picks the section by sign and, having
 * picked the negative one, formats the magnitude, which is why `#,##0;(#,##0)`
 * shows `(1,240)` and not `(-1,240)`.
 */
export function formatNumber(value: number, fmt: string | undefined | null, locale: string): string {
  if (!Number.isFinite(value)) return String(value);
  if (!fmt || fmt === 'General' || fmt === '@') return general(value, locale);
  try {
    const sections = splitSections(fmt);
    if (sections.some((s) => CONDITIONAL.test(s))) return general(value, locale);
    let src = sections[0];
    let picked = 0;
    let n = value;
    if (value < 0 && sections[1] !== undefined) {
      src = sections[1];
      picked = 1;
      n = Math.abs(value);
    } else if (value === 0 && sections[2] !== undefined) {
      src = sections[2];
      picked = 2;
    }
    // A fraction code (`# ?/?`) reads its digits as a numerator and a
    // denominator, which is a different number entirely. Not supported, so the
    // raw value is the honest answer.
    if (/[#0?]\s*\/\s*[#0?]/.test(src)) return general(value, locale);
    const section = parseSection(src);
    if (section.hasDigits) return renderSection(n, section, locale);
    // A negative or zero section written as a bare literal is Excel's idiom for
    // "print this instead": `#,##0;(#,##0);"-"` puts a dash on zero, and an
    // empty one prints nothing. A *first* section with no digits is far more
    // likely to be a code this reader misread, so that one falls back to raw.
    return picked > 0 ? section.prefix + section.suffix : general(value, locale);
  } catch {
    return general(value, locale);
  }
}

const tokenCache = new Map<string, DateToken[]>();

export function formatDate(date: Date, fmt: string | undefined | null, locale: string): string {
  if (Number.isNaN(date.getTime())) return '';
  const code = fmt && !UNSUPPORTED_TIME.test(fmt) ? fmt : 'yyyy-mm-dd';
  let tokens = tokenCache.get(code);
  if (!tokens) {
    tokens = tokenizeDate(code);
    tokenCache.set(code, tokens);
  }
  return renderDate(date, tokens, locale);
}

/** What a cell shows, given its value and its number format. */
export function formatCellValue(
  value: ExcelScalar,
  fmt: string | null | undefined,
  system: DateSystem | undefined,
  locale: string,
): string {
  if (value === null || value === undefined) return '';
  if (value instanceof Date) return formatDate(value, fmt, locale);
  if (typeof value === 'boolean') return value ? 'TRUE' : 'FALSE';
  if (typeof value === 'number') {
    // A serial under an elapsed-time code is a duration; a date for it is a
    // fabrication, so the raw serial stands.
    if (isDateFormat(fmt) && UNSUPPORTED_TIME.test(fmt!)) return general(value, locale);
    return isDateFormat(fmt) ? formatDate(excelSerialToDate(value, system), fmt, locale) : formatNumber(value, fmt, locale);
  }
  return String(value);
}
