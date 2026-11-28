import { useEffect, useId, useMemo, useRef, useState, type ComponentType } from 'react';
import { Link, useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { Search, X } from 'lucide-react';
import { cn } from '@/lib/utils';
import { isDesktopShell } from '@/lib/desktop';
import { isFeatureEnabled, useFeatures } from '@/hooks/useFeatures';
import { isImeComposing } from '@/lib/ime';
import { matchesAllWords, queryWords } from '@/lib/textMatch';
import { GROUPS, SECTIONS, searchableSettings, sectionOffered, settingSearchText, type SettingEntry, type SettingsView } from '../sections';
import { SidebarReveal } from '@/components/Sidebar/SidebarReveal';
import { usePortalHost } from '@/components/AccountPortal/PortalHost';
import { activePortalPath, portalSettingsPath } from '@/components/AccountPortal/portalPaths';
import type { PortalNavGroup, PortalNavItem } from '@/components/AccountPortal/portalMessages';
import { portalNavIcon, portalSearchPages, type PortalSearchPage } from '@/components/AccountPortal/portalNav';

/** One link in the rail. `id` names what it shows (`section:<name>`,
 *  `portal:<path>`), so the current one is a single comparison. */
interface RailLink {
  id: string;
  to: string;
  Icon: ComponentType<{ className?: string }>;
  label: string;
}

interface RailGroup {
  key: string;
  label: string;
  links: RailLink[];
}

/** Whether the rail draws the section: once the account portal has listed its
 *  pages, the sections that only led out to it give way to them. */
/** A search result: a setting of Settings' own, or a page of the portal. */
type SearchResult = { kind: 'setting'; entry: SettingEntry } | { kind: 'portal'; page: PortalSearchPage };

function resultShown(result: SearchResult, t: (key: string) => string) {
  if (result.kind === 'portal') {
    const { item, group } = result.page;
    return { key: `portal:${item.path}`, Icon: portalNavIcon(item.icon), name: item.label, where: group ?? t('settings:group.account') };
  }
  const { entry } = result;
  return { key: entry.anchor, Icon: SECTIONS[entry.section].icon, name: t(entry.labelKey), where: t(SECTIONS[entry.section].labelKey) };
}

function resultText(result: SearchResult, t: (key: string) => string): string {
  if (result.kind === 'setting') return settingSearchText(result.entry, t);
  const shown = resultShown(result, t);
  return `${shown.name} ${shown.where}`;
}

const portalLink = (item: PortalNavItem): RailLink => ({
  id: `portal:${item.path}`,
  // Absolute, so leaving a portal page drops its `page`.
  to: portalSettingsPath(item.path),
  Icon: portalNavIcon(item.icon),
  label: item.label,
});

/**
 * The rail's groups. Once the account portal has listed its pages they are
 * sections here: the account's own join the Account group, and each
 * organization's follows it as a group of its own.
 */
function railGroups(t: (key: string) => string, nav: PortalNavGroup[] | null): RailGroup[] {
  const personal = nav?.find((group) => group.label === null)?.items.map(portalLink) ?? [];
  const named = nav?.filter((group) => group.label !== null) ?? [];
  return GROUPS.flatMap((group) => {
    const sections = group.sections.filter((s) => sectionOffered(s, nav !== null));
    const links = sections.map((section): RailLink => ({
      id: `section:${section}`,
      to: `/settings?tab=${section}`,
      Icon: SECTIONS[section].icon,
      label: t(SECTIONS[section].labelKey),
    }));
    if (!nav || !group.portalAfter) return [{ key: group.labelKey, label: t(group.labelKey), links }];
    links.splice(sections.indexOf(group.portalAfter) + 1 || links.length, 0, ...personal);
    return [
      { key: group.labelKey, label: t(group.labelKey), links },
      ...named.map((g, i) => ({ key: `portal:${i}:${g.label}`, label: g.label ?? '', links: g.items.map(portalLink) })),
    ];
  });
}

/** The rail link `view` is on: a portal page is on the listed page it sits under. */
function currentLinkId(view: Exclude<SettingsView, { kind: 'away' }>, nav: PortalNavGroup[] | null): string | null {
  if (view.kind === 'section') return `section:${view.section}`;
  const path = activePortalPath(nav?.flatMap((g) => g.items.map((item) => item.path)) ?? [], view.page);
  return path === null ? null : `portal:${path}`;
}

/**
 * The page's navigation: a search over every setting, then the sections in
 * their groups. While the search has text, its matches take the sections'
 * place, each naming the section it opens; picking one opens that section on
 * the setting itself.
 */
export function SettingsRail({ view }: { view: Exclude<SettingsView, { kind: 'away' }> }) {
  const { t } = useTranslation();
  // The portal's pages as it listed them; null where they do not open here or are not listed yet.
  const nav = usePortalHost()?.nav ?? null;
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

  const features = useFeatures().data;
  // The portal's pages are found here too, as they replace Settings' own
  // Plan and Integrations once listed.
  const results = useMemo((): SearchResult[] => [
    ...searchableSettings(isDesktopShell(), (key) => isFeatureEnabled(features, key))
      .filter((entry) => sectionOffered(entry.section, nav !== null))
      .map((entry): SearchResult => ({ kind: 'setting', entry })),
    ...portalSearchPages(nav).map((page): SearchResult => ({ kind: 'portal', page })),
  ], [features, nav]);
  const groups = railGroups(t, nav);
  const currentId = currentLinkId(view, nav);
  const words = queryWords(query);
  const matches = words.length === 0
    ? []
    : results.filter((result) => matchesAllWords(resultText(result, t), words));
  const searching = words.length > 0;
  const shownActive = Math.min(active, Math.max(matches.length - 1, 0));

  /** Opens the setting's section on it. Picked from the keyboard, focus
   *  follows to its control, so the next key acts on what was found. */
  const open = (result: SearchResult, fromKeyboard: boolean) => {
    setQuery('');
    setSearchOpen(false);
    if (result.kind === 'portal') navigate(portalSettingsPath(result.page.item.path));
    else navigate(`/settings?tab=${result.entry.section}#${result.entry.anchor}`, { state: { focusSetting: fromKeyboard } });
  };

  // On a phone the sections are a row of pills wider than the screen; the one
  // the page is on is brought into view rather than left past the edge.
  const navRef = useRef<HTMLElement>(null);
  useEffect(() => {
    const current = navRef.current?.querySelector<HTMLElement>('[aria-current="page"]');
    current?.scrollIntoView?.({ block: 'nearest', inline: 'nearest' });
  }, [currentId, searching]);

  return (
    <div className={cn('page-rail settings-rail', (searchOpen || searching) && 'is-searching')}>
      {/* The search keeps a usable width: beside the hidden sidebar's toggle
          where they fit, under it where they do not (the desktop app's back
          and forward take more room). */}
      <div className="flex flex-wrap items-center gap-2">
        <SidebarReveal titlebar />
        <div className="page-search settings-search min-w-0 flex-[1_1_160px]">
          <Search className="settings-search-icon" aria-hidden />
          <input
            ref={inputRef}
            type="search"
            className="page-search-input"
            role="combobox"
            aria-expanded={searching}
            aria-controls={listId}
            aria-autocomplete="list"
            aria-activedescendant={searching && matches.length > 0 ? `${listId}-${shownActive}` : undefined}
            aria-label={t('settings:search.label')}
            placeholder={t('settings:search.placeholder')}
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
              aria-label={t('settings:search.clear')}
              onMouseDown={(e) => e.preventDefault()}
              onClick={() => setQuery('')}
            >
              <X className="h-3.5 w-3.5" />
            </button>
          )}
        </div>
      </div>

      {searching ? (
        <div id={listId} role="listbox" aria-label={t('settings:search.results')} className="settings-results">
          {matches.length === 0 ? (
            <p className="settings-results-empty">{t('settings:search.empty')}</p>
          ) : (
            matches.map((result, index) => {
              const shown = resultShown(result, t);
              return (
                <div
                  key={shown.key}
                  id={`${listId}-${index}`}
                  role="option"
                  aria-selected={index === shownActive}
                  className={cn('settings-result', index === shownActive && 'is-active')}
                  onMouseDown={(e) => e.preventDefault()}
                  onMouseEnter={() => setActive(index)}
                  onClick={() => open(result, false)}
                >
                  <shown.Icon className="page-rail-icon" aria-hidden />
                  <span className="min-w-0">
                    <span className="settings-result-name">{shown.name}</span>
                    <span className="settings-result-section">{shown.where}</span>
                  </span>
                </div>
              );
            })
          )}
        </div>
      ) : (
        <nav ref={navRef} className="settings-rail-nav" aria-label={t('settings:railLabel')}>
          <button
            type="button"
            className="settings-search-toggle"
            aria-label={t('settings:search.label')}
            onClick={() => setSearchOpen(true)}
          >
            <Search aria-hidden />
          </button>
          {groups.map((group) => (
            <div key={group.key} className="settings-rail-group">
              <div className="page-rail-label open-style:label-voice">{group.label}</div>
              {group.links.map((link) => {
                const current = link.id === currentId;
                return (
                  <Link
                    key={link.id}
                    to={link.to}
                    className={cn('page-rail-link', current && 'is-active')}
                    aria-current={current ? 'page' : undefined}
                  >
                    <link.Icon className="page-rail-icon" aria-hidden />
                    <span className={cn('page-rail-name', current && 'rail-prompt')}>{link.label}</span>
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
