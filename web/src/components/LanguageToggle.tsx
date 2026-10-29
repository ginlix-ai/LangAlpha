import * as DropdownMenuPrimitive from '@radix-ui/react-dropdown-menu';
import { Check, Globe } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { DropdownMenu, DropdownMenuContent, DropdownMenuTrigger } from '@/components/ui/dropdown-menu';
import { useLocale } from '@/hooks/useLocale';
import { useChooseLocale } from '@/hooks/useLocaleChoice';
import { DEFAULT_LOCALE } from '@/lib/resolveLocale';
import { SUPPORTED_LOCALES, isSupported, type Locale } from '@/lib/locale';
import { cn } from '@/lib/utils';

/**
 * Each language by its own name in its own script, so it can be found by
 * someone who cannot read the one the page is in: `short` in the bar, `name`
 * in the menu. `lang` is the tag the browser shapes and reads it out by.
 */
const LANGUAGES: Record<Locale, { name: string; short: string; lang: string }> = {
  'en-US': { name: 'English', short: 'English', lang: 'en' },
  'zh-CN': { name: '简体中文', short: '中文', lang: 'zh-CN' },
};

/**
 * The language, picked where a flow starts rather than found later in
 * Settings: a globe and the current language's name, a menu of the others.
 * Quiet, in the bar's text style, since most people keep the language their
 * browser or system already asked for.
 *
 * The names are marked `translate="no"`: a browser translating the page would
 * turn "简体中文" into the page's language and leave nothing to find. Their
 * text sits in spans, so a translator that does run never re-parents a text
 * node React owns.
 */
export function LanguageToggle({ className }: { className?: string }) {
  const { t } = useTranslation();
  const active = useLocale();
  const choose = useChooseLocale();
  const current: Locale = isSupported(active) ? active : DEFAULT_LOCALE;
  const here = LANGUAGES[current];

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <button
          type="button"
          // Named with the language shown, so what is said to it by voice is
          // what is on it; on a phone the globe stands alone.
          aria-label={t('common.languageCurrent', { language: here.short })}
          data-testid="language-toggle"
          className={cn(
            'inline-flex h-8 w-8 shrink-0 items-center justify-center gap-1.5 rounded-md text-sm text-(--color-text-secondary) press hover:bg-(--color-bg-hover) hover:text-(--color-text-primary) data-[state=open]:bg-(--color-bg-hover) data-[state=open]:text-(--color-text-primary) sm:w-auto sm:px-2.5',
            className,
          )}
        >
          <Globe aria-hidden="true" size={16} strokeWidth={1.5} className="shrink-0" />
          <span lang={here.lang} translate="no" className="hidden sm:inline">
            {here.short}
          </span>
        </button>
      </DropdownMenuTrigger>
      <DropdownMenuContent
        align="end"
        // Named for what it lists rather than by the trigger, which Radix
        // would otherwise label it with.
        aria-labelledby={undefined}
        aria-label={t('common.language')}
        // Portaled to the body, so outside the bar's drag region already; this
        // keeps its padding clickable if it ever opens over a titlebar.
        data-no-drag=""
        className="min-w-36"
      >
        <DropdownMenuPrimitive.RadioGroup
          value={current}
          onValueChange={(value) => {
            if (isSupported(value) && value !== current) choose(value);
          }}
        >
          {SUPPORTED_LOCALES.map((locale) => (
            <DropdownMenuPrimitive.RadioItem
              key={locale}
              value={locale}
              lang={LANGUAGES[locale].lang}
              translate="no"
              data-testid={`language-option-${locale}`}
              className="relative flex w-full cursor-default select-none items-center justify-between gap-4 rounded-sm px-2.5 py-1.5 text-sm text-(--color-text-primary) outline-none transition-colors data-highlighted:bg-(--color-bg-hover)"
            >
              <span>{LANGUAGES[locale].name}</span>
              {/* Held open on every row, so the menu keeps its width
                  whichever one carries the check. */}
              <span aria-hidden="true" className="flex h-4 w-4 shrink-0 items-center justify-center">
                <DropdownMenuPrimitive.ItemIndicator>
                  <Check className="h-3.5 w-3.5 text-(--color-text-secondary)" strokeWidth={2} />
                </DropdownMenuPrimitive.ItemIndicator>
              </span>
            </DropdownMenuPrimitive.RadioItem>
          ))}
        </DropdownMenuPrimitive.RadioGroup>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
