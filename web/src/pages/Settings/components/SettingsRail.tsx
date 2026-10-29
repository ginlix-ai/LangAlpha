import { useEffect, useId, useMemo, useRef, useState } from 'react';
import { Link, useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { Search, X } from 'lucide-react';
import { cn } from '@/lib/utils';
import { isDesktopShell } from '@/lib/desktop';
import { isImeComposing } from '@/lib/ime';
import { GROUPS, SECTIONS, searchableSettings, sectionShown, type Section, type SettingEntry } from '../sections';

/**
 * The page's navigation: a search over every setting, then the sections in
 * their groups. While the search has text, its matches take the sections'
 * place, each naming the section it opens; picking one opens that section on
 * the setting itself.
 */
export function SettingsRail({ section }: { section: Section }) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const listId = useId();
  const [query, setQuery] = useState('');
  const [active, setActive] = useState(0);
  const [focused, setFocused] = useState(false);
  // A phone folds the field into an icon beside the pills; this is it opened.
  const [searchOpen, setSearchOpen] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  useEffect(() => {
    if (searchOpen) inputRef.current?.focus();
  }, [searchOpen]);

  // `/` jumps to the search from anywhere on the page but a field being typed in.
  useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key !== '/' || e.metaKey || e.ctrlKey || e.altKey || e.defaultPrevented) return;
      const el = document.activeElement as HTMLElement | null;
      if (el && (el.isContentEditable || ['INPUT', 'TEXTAREA', 'SELECT'].includes(el.tagName))) return;
      e.preventDefault();
      // On a phone the field is folded away until opened; opening it focuses it.
      setSearchOpen(true);
      inputRef.current?.focus();
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, []);

  const entries = useMemo(() => searchableSettings(isDesktopShell()), []);
  const words = query.trim().toLowerCase().split(/\s+/).filter(Boolean);
  const matches = words.length === 0
    ? []
    : entries.filter((entry) => {
        const text = [
          t(entry.labelKey),
          entry.descKey ? t(entry.descKey) : '',
          ...(entry.alsoKeys ?? []).map((key) => t(key)),
          t(SECTIONS[entry.section].labelKey),
        ].join(' ').toLowerCase();
        return words.every((word) => text.includes(word));
      });
  const searching = words.length > 0;
  const shownActive = Math.min(active, Math.max(matches.length - 1, 0));

  /** Opens the setting's section on it. Picked from the keyboard, focus
   *  follows to its control, so the next key acts on what was found. */
  const open = (entry: SettingEntry, fromKeyboard: boolean) => {
    setQuery('');
    setSearchOpen(false);
    navigate(`?tab=${entry.section}#${entry.anchor}`, { state: { focusSetting: fromKeyboard } });
  };

  // On a phone the sections are a row of pills wider than the screen; the one
  // the page is on is brought into view rather than left past the edge.
  const navRef = useRef<HTMLElement>(null);
  useEffect(() => {
    const current = navRef.current?.querySelector<HTMLElement>('[aria-current="page"]');
    current?.scrollIntoView?.({ block: 'nearest', inline: 'nearest' });
  }, [section, searching]);

  return (
    <div className={cn('settings-rail', (searchOpen || searching) && 'is-searching')}>
      <div className="settings-search">
        <Search className="settings-search-icon" aria-hidden />
        <input
          ref={inputRef}
          type="search"
          role="combobox"
          aria-expanded={searching}
          aria-controls={listId}
          aria-autocomplete="list"
          aria-activedescendant={searching && matches.length > 0 ? `${listId}-${shownActive}` : undefined}
          aria-label={t('settings.search.label')}
          placeholder={t('settings.search.placeholder')}
          value={query}
          onChange={(e) => { setQuery(e.target.value); setActive(0); }}
          onFocus={() => setFocused(true)}
          onBlur={() => {
            setFocused(false);
            if (!query) setSearchOpen(false);
          }}
          onKeyDown={(e) => {
            // Keys that commit an IME composition edit the query; they pick nothing.
            if (isImeComposing(e)) return;
            if (e.key === 'Escape') {
              e.preventDefault();
              if (query) setQuery('');
              else e.currentTarget.blur();
            } else if (e.key === 'ArrowDown' && matches.length > 0) {
              e.preventDefault();
              setActive((shownActive + 1) % matches.length);
            } else if (e.key === 'ArrowUp' && matches.length > 0) {
              e.preventDefault();
              setActive((shownActive - 1 + matches.length) % matches.length);
            } else if (e.key === 'Enter' && matches[shownActive]) {
              e.preventDefault();
              open(matches[shownActive], true);
            }
          }}
        />
        {!query && !focused && <kbd className="settings-search-key" aria-hidden>/</kbd>}
        {query && (
          <button
            type="button"
            className="settings-search-clear"
            aria-label={t('settings.search.clear')}
            onMouseDown={(e) => e.preventDefault()}
            onClick={() => setQuery('')}
          >
            <X className="h-3.5 w-3.5" />
          </button>
        )}
      </div>

      {searching ? (
        <div id={listId} role="listbox" aria-label={t('settings.search.results')} className="settings-results">
          {matches.length === 0 ? (
            <p className="settings-results-empty">{t('settings.search.empty')}</p>
          ) : (
            matches.map((entry, index) => {
              const Icon = SECTIONS[entry.section].icon;
              return (
                <div
                  key={entry.anchor}
                  id={`${listId}-${index}`}
                  role="option"
                  aria-selected={index === shownActive}
                  className={cn('settings-result', index === shownActive && 'is-active')}
                  onMouseDown={(e) => e.preventDefault()}
                  onMouseEnter={() => setActive(index)}
                  onClick={() => open(entry, false)}
                >
                  <Icon className="settings-rail-icon" aria-hidden />
                  <span className="min-w-0">
                    <span className="settings-result-name">{t(entry.labelKey)}</span>
                    <span className="settings-result-section">{t(SECTIONS[entry.section].labelKey)}</span>
                  </span>
                </div>
              );
            })
          )}
        </div>
      ) : (
        <nav ref={navRef} className="settings-rail-nav" aria-label={t('settings.railLabel')}>
          <button
            type="button"
            className="settings-search-toggle"
            aria-label={t('settings.search.label')}
            onClick={() => setSearchOpen(true)}
          >
            <Search aria-hidden />
          </button>
          {GROUPS.map((group) => (
            <div key={group.labelKey} className="settings-rail-group">
              <div className="settings-rail-label">{t(group.labelKey)}</div>
              {group.sections.filter(sectionShown).map((s) => {
                const Icon = SECTIONS[s].icon;
                const current = s === section;
                return (
                  <Link
                    key={s}
                    to={`?tab=${s}`}
                    className={cn('settings-rail-link', current && 'is-active')}
                    aria-current={current ? 'page' : undefined}
                  >
                    <Icon className="settings-rail-icon" aria-hidden />
                    <span className="settings-rail-name">{t(SECTIONS[s].labelKey)}</span>
                  </Link>
                );
              })}
            </div>
          ))}
        </nav>
      )}
    </div>
  );
}
