/**
 * Locale-aware number/date formatter factories.
 *
 * Each factory returns a function that memoizes its `Intl.*Format` instance
 * by `i18n.language`, so a per-row formatter on a hot render path costs one
 * Intl construction per locale switch (not per call).
 *
 * Consumers MUST call `useTranslation()` in the component that uses the
 * formatter — without it, the component never re-renders on locale switch
 * and the cell shows stale-locale output until something else triggers a
 * re-render. The formatter itself only re-creates Intl when `i18n.language`
 * changes; React's render cycle is what forces it to actually re-run.
 */
import i18n from '@/i18n';

// Defensive: if `i18n.language` is briefly empty/null/invalid (transient
// changeLanguage state, broken localStorage), the Intl constructor throws.
// Without the catch, the closure would re-throw on every subsequent call —
// every formatted widget on the dashboard would crash at once. The fallback
// uses the host default locale and we still update `lastLocale` so we don't
// retry the bad construction every call.
function safeNumberFormat(lang: string, opts: Intl.NumberFormatOptions): Intl.NumberFormat {
  try {
    return new Intl.NumberFormat(lang, opts);
  } catch {
    return new Intl.NumberFormat(undefined, opts);
  }
}

function safeDateFormat(lang: string, opts: Intl.DateTimeFormatOptions): Intl.DateTimeFormat {
  try {
    return new Intl.DateTimeFormat(lang, opts);
  } catch {
    return new Intl.DateTimeFormat(undefined, opts);
  }
}

export function createFormatter(opts: Intl.NumberFormatOptions): (n: number) => string {
  let lastLocale: string | null = null;
  let fmt: Intl.NumberFormat | null = null;
  return (n: number): string => {
    const lang = i18n.language;
    if (lang !== lastLocale || !fmt) {
      fmt = safeNumberFormat(lang, opts);
      lastLocale = lang;
    }
    return fmt.format(n);
  };
}

export function createDateFormatter(opts: Intl.DateTimeFormatOptions): (d: Date | number) => string {
  let lastLocale: string | null = null;
  let fmt: Intl.DateTimeFormat | null = null;
  return (d: Date | number): string => {
    const lang = i18n.language;
    if (lang !== lastLocale || !fmt) {
      fmt = safeDateFormat(lang, opts);
      lastLocale = lang;
    }
    return fmt.format(d);
  };
}
