import {
  useEffect, useId, useRef, useState, type CSSProperties, type ReactNode, type Ref, type RefObject,
} from 'react';
import { Check, Settings, Star } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import {
  Button, GridList, GridListHeader, GridListItem, GridListSection, ToggleButton, ToggleButtonGroup,
  useFilter, type Key,
} from 'react-aria-components';
import { SearchField } from './aria-search-field';
import { ComposerPopover, MENU_ROW, usePickToDraft } from './chat-input.composerPopover';
import { getModelDisplayName } from './chat-input.helpers';
import { cn } from '@/lib/utils';
import { llmProviderArt } from '@/lib/brandArt';
import { isImeComposing } from '@/lib/ime';
import type { ModelConnection, ModelGroup } from '@/lib/modelGroups';
import { isSubscription, subscriptionPayer, type Payer } from '@/lib/modelPayer';
import { usePayerLabel } from '@/hooks/usePayerLabel';
import type { ModelMetadataEntry } from '@/hooks/useFilteredModels';
import { ModelMark, MarkedModelName } from '@/components/model/ModelMark';
import { PayerBadge } from '@/components/model/PayerBadge';

export const FAVORITES_TAB = '__favorites__';

type Metadata = Record<string, ModelMetadataEntry>;

/** What a row says about its model: a tag beside the name for one that is a
 *  pick, or, for one that is not, why, under its name where a sentence fits.
 *  A row held back by something the user can fix takes them there instead of
 *  picking, through `fix`. */
export type ModelRowStatus = { tag?: string } | { unavailable: string; fix?: () => void };

interface ListProps {
  selectedModel: string | null;
  metadata?: Metadata;
  payers: Record<string, Payer>;
  groups: ModelGroup[];
  /** The favorites the user can reach now, in their saved order. */
  favorites: string[];
  /** This thread's models and the current one that are not favorites, listed
   *  under the favorites so the current model is always on the first tab. */
  recent: string[];
  onToggleFavorite: (model: string, favorite: boolean) => void;
  rowStatus: (model: string) => ModelRowStatus;
}

/**
 * The composer's model picker: a rail of tabs, favorites first and then one
 * per connection, beside the list of the tab's models. Typing searches every
 * model whatever the tab.
 *
 * A grid rather than a menu because each row holds a second control, the
 * star; a pick closes the picker, a star does not.
 */
export function ModelPicker({
  triggerRef, isOpen, onOpenChange, direction, container, draftRef, onPick, onManage, ...list
}: ListProps & {
  triggerRef: RefObject<HTMLButtonElement | null>;
  isOpen: boolean;
  onOpenChange: (open: boolean) => void;
  direction: 'up' | 'down';
  /** The composer, which a phone portals the picker into. */
  container?: HTMLElement | null;
  /** The message the pick is for, which takes focus after a pick by pointer. */
  draftRef?: RefObject<HTMLElement | null>;
  onPick: (model: string) => void;
  onManage: () => void;
}) {
  const { t } = useTranslation();
  const pickToDraft = usePickToDraft(isOpen, draftRef);
  return (
    <ComposerPopover
      triggerRef={triggerRef}
      isOpen={isOpen}
      onOpenChange={onOpenChange}
      direction={direction}
      align="end"
      container={container}
      label={t('chat.modelSelector.select')}
      className="w-96"
    >
      <ModelList
        {...list}
        onPick={(model) => {
          const status = list.rowStatus(model);
          if ('unavailable' in status) {
            onOpenChange(false);
            status.fix?.();
            return;
          }
          onPick(model);
          pickToDraft();
          onOpenChange(false);
        }}
        onManage={() => {
          onOpenChange(false);
          onManage();
        }}
      />
    </ComposerPopover>
  );
}

/** What the list shows: a search across every model, or one tab. */
type View =
  | { kind: 'search'; rows: string[] }
  | { kind: 'favorites'; rows: string[]; recent: string[] }
  | { kind: 'brand'; group: ModelGroup; connection: ModelConnection; rows: string[] };

function viewOf({ query, tab, connectionTab, groups, favorites, recent, selectedModel, matches }: {
  query: string;
  tab: string | null;
  connectionTab: string | null;
  groups: ModelGroup[];
  favorites: string[];
  recent: string[];
  selectedModel: string | null;
  matches: (model: string) => boolean;
}): View {
  if (query.trim()) {
    const all = new Set(groups.flatMap((g) => g.connections.flatMap((c) => c.models)));
    return { kind: 'search', rows: [...all].filter(matches) };
  }
  // The first tab holds the current model, so it opens there unless it has
  // nothing at all to show. A brand that has since gone falls back to it too.
  const group = tab === null
    ? (favorites.length + recent.length > 0 ? undefined : groups[0])
    : groups.find((g) => g.id === tab);
  if (!group) return { kind: 'favorites', rows: favorites, recent };
  // Within a brand it opens on the way the current model is paid for, else
  // on the user's own before the deployment's.
  const connection = group.connections.find((c) => c.id === connectionTab)
    ?? group.connections.find((c) => !!selectedModel && c.models.includes(selectedModel))
    ?? group.connections[0];
  return { kind: 'brand', group, connection, rows: connection.models };
}

/** The picker's content, mounted while it is open, so the search, the tab and
 *  the brand's payer start over on each opening. */
function ModelList({
  selectedModel, metadata, payers, groups, favorites, recent, onToggleFavorite, rowStatus, onPick, onManage,
}: ListProps & { onPick: (model: string) => void; onManage: () => void }) {
  const { t } = useTranslation();
  const payerLabel = usePayerLabel();
  const [query, setQuery] = useState('');
  const [tab, setTab] = useState<string | null>(null);
  const [connectionTab, setConnectionTab] = useState<string | null>(null);
  const { contains } = useFilter({ sensitivity: 'base' });
  const listId = useId();
  const inputRef = useRef<HTMLInputElement>(null);
  const firstRowRef = useRef<HTMLDivElement | null>(null);
  const currentRowRef = useRef<HTMLDivElement | null>(null);
  const { attach: attachRail, style: railFade } = useFadeAtEnd<HTMLDivElement>();
  const { attach: attachList, style: listFade } = useFadeAtEnd<HTMLDivElement>();

  // A search matches what each row shows, its display name.
  const view = viewOf({
    query, tab, connectionTab, groups, favorites, recent, selectedModel,
    matches: (m) => contains(getModelDisplayName(m, metadata), query),
  });
  const header = view.kind === 'favorites' ? t('chat.modelSelector.favorites')
    : view.kind === 'brand' ? view.group.label
    : null;
  // A connection's tag sits once above its models rather than on each row.
  const headerPayer = view.kind === 'brand' ? view.connection.payer : undefined;
  // The tab's own subscription (Claude Code's) is named by its header already.
  const headerNamesPayer = view.kind === 'brand' && headerPayer === subscriptionPayer(view.group.id);
  const inThread = view.kind === 'favorites' ? view.recent : [];
  const first = [...view.rows, ...inThread].find((m) => !('unavailable' in rowStatus(m)));
  const favoriteSet = new Set(favorites);

  // The current model's check is what the user looks for on opening, and a
  // long first tab can hold it below the fold.
  useEffect(() => {
    const frame = requestAnimationFrame(() => currentRowRef.current?.scrollIntoView({ block: 'nearest' }));
    return () => cancelAnimationFrame(frame);
  }, []);

  const chooseTab = (keys: Set<Key>) => {
    const [next] = keys;
    if (next === undefined) return;
    setTab(String(next));
    setConnectionTab(null);
    setQuery('');
    // The search field holds focus so the arrow keys keep moving through the
    // list, whichever tab was clicked.
    inputRef.current?.focus();
  };

  const row = (m: string) => (
    <ModelRow
      key={m}
      model={m}
      // The first row takes Down from the search; the current one is
      // scrolled to on opening, which a first row never needs.
      rowRef={m === first ? firstRowRef : m === selectedModel ? currentRowRef : undefined}
      metadata={metadata}
      payer={headerPayer ? undefined : payers[m]}
      selected={m === selectedModel}
      favorite={favoriteSet.has(m)}
      status={rowStatus(m)}
      onToggleFavorite={onToggleFavorite}
    />
  );

  return (
    <>
      <div className="flex flex-none items-center border-b border-(--color-border-muted) pr-2">
        {/* A grid takes no virtual focus from a search field, so the field
            hands over itself: Down moves into the list, and Enter picks the
            top row. Any other key goes on, so Escape still closes. */}
        <SearchField
          aria-label={t('chat.modelSelector.search')}
          aria-controls={listId}
          placeholder={t('chat.modelSelector.search')}
          value={query}
          onChange={setQuery}
          inputRef={inputRef}
          onKeyDown={(e) => {
            if (e.key === 'ArrowDown' && firstRowRef.current) {
              e.preventDefault();
              firstRowRef.current.focus();
            } else if (e.key === 'Enter' && !isImeComposing(e) && first) {
              e.preventDefault();
              onPick(first);
            } else {
              e.continuePropagation();
            }
          }}
          autoFocus
          className="min-w-0 flex-1 border-b-0"
        />
        <Button
          aria-label={t('chat.modelSelector.manageModels')}
          onPress={onManage}
          className={cn(
            'flex h-7 w-7 flex-none items-center justify-center rounded-md text-(--color-text-tertiary) outline-hidden',
            'data-hovered:bg-(--color-bg-tag) data-hovered:text-(--color-text-secondary) data-focus-visible:outline-current',
          )}
        >
          <Settings aria-hidden className="h-4 w-4" />
        </Button>
      </div>
      {/* Room for about four and a half rows (2rem each) under the tab
          header: a curated list fits, and a cut row says there is more. */}
      <div className="flex h-[11.5rem] min-h-0">
        {/* Out of the flow, so a long rail scrolls rather than sets the
            height; a curated list keeps the picker short. */}
        <div className="relative w-11 flex-none border-r border-(--color-border-muted)">
          {/* The toggle group takes no ref, so a box of its own scrolls it. */}
          <div
            ref={attachRail}
            style={railFade}
            className="absolute inset-0 overflow-y-auto [scrollbar-width:none]"
          >
            <ToggleButtonGroup
              aria-label={t('chat.modelSelector.groups')}
              orientation="vertical"
              selectionMode="single"
              selectedKeys={view.kind === 'search' ? [] : [view.kind === 'brand' ? view.group.id : FAVORITES_TAB]}
              onSelectionChange={chooseTab}
              className="flex flex-col items-center gap-1 py-1.5"
            >
              <RailTab id={FAVORITES_TAB} label={t('chat.modelSelector.favorites')}>
                <Star aria-hidden className="h-4 w-4 text-(--color-text-tertiary)" />
              </RailTab>
              {groups.map((g) => (
                <RailTab
                  key={g.id}
                  id={g.id}
                  label={g.label}
                  dot={g.connections.some((c) => isSubscription(c.payer) && c.models.some((m) => !('unavailable' in rowStatus(m))))}
                >
                  <RailIcon provider={g.id} label={g.label} />
                </RailTab>
              ))}
            </ToggleButtonGroup>
          </div>
        </div>
        <div className="flex min-w-0 flex-1 flex-col">
          {header && (
            <div className="flex flex-none items-center justify-between gap-2 px-3 pb-1 pt-2 text-xs text-(--color-text-tertiary)">
              <span className="truncate">{header}</span>
              {view.kind === 'brand' && view.group.connections.length > 1 ? (
                <ToggleButtonGroup
                  aria-label={t('chat.modelSelector.paidBy')}
                  selectionMode="single"
                  disallowEmptySelection
                  selectedKeys={[view.connection.id]}
                  onSelectionChange={(keys) => {
                    const [next] = keys;
                    if (next !== undefined) setConnectionTab(String(next));
                  }}
                  className="flex flex-none gap-0.5 rounded-md bg-(--color-bg-tag) p-0.5"
                >
                  {view.group.connections.map((c) => (
                    <ToggleButton
                      key={c.id}
                      id={c.id}
                      className={cn(
                        'rounded px-1.5 py-px text-[0.625rem] font-medium leading-normal text-(--color-text-tertiary) outline-hidden',
                        'data-hovered:text-(--color-text-secondary) data-selected:bg-(--color-bg-elevated) data-selected:text-(--color-text-primary)',
                        'data-focus-visible:outline-current',
                      )}
                    >
                      {c.payer ? payerLabel(c.payer) : t('chat.modelSelector.unbilled')}
                    </ToggleButton>
                  ))}
                </ToggleButtonGroup>
              ) : (
                <PayerBadge payer={headerNamesPayer ? undefined : headerPayer} />
              )}
            </div>
          )}
          <GridList
            id={listId}
            ref={attachList}
            style={listFade}
            aria-label={header ?? t('chat.modelSelector.search')}
            onAction={(key) => onPick(String(key))}
            className="min-h-0 flex-1 overflow-y-auto p-1 outline-hidden"
            renderEmptyState={() => (
              <p className="px-3 py-4 text-center text-[0.8125rem] text-(--color-text-tertiary)">
                {view.kind === 'favorites' ? t('chat.modelSelector.noFavorites') : t('chat.modelSelector.noMatch')}
              </p>
            )}
          >
            {view.rows.map(row)}
            {inThread.length > 0 && (
              <GridListSection>
                <GridListHeader className="px-2 pb-1 pt-2.5 text-xs text-(--color-text-tertiary)">
                  {t('chat.modelSelector.inThread')}
                </GridListHeader>
                {inThread.map(row)}
              </GridListSection>
            )}
          </GridList>
        </div>
      </div>
    </>
  );
}

/**
 * Fades a scrolling list's bottom edge while there is more below it, so a
 * cut row reads as a list going on rather than as a clipped one.
 */
function useFadeAtEnd<T extends HTMLElement>() {
  // A callback ref held as state, so the effect reruns for a new element.
  const [el, attach] = useState<T | null>(null);
  const [more, setMore] = useState(false);
  useEffect(() => {
    if (!el) return;
    const update = () => setMore(el.scrollTop + el.clientHeight < el.scrollHeight - 1);
    update();
    el.addEventListener('scroll', update, { passive: true });
    // A tab switch or a search changes the rows, not the box.
    const rows = new MutationObserver(update);
    rows.observe(el, { childList: true, subtree: true });
    const box = new ResizeObserver(update);
    box.observe(el);
    return () => {
      el.removeEventListener('scroll', update);
      rows.disconnect();
      box.disconnect();
    };
  }, [el]);
  const style: CSSProperties | undefined = more ? { maskImage: FADE, WebkitMaskImage: FADE } : undefined;
  return { attach, style };
}

// A hint, not a curtain: the last row dims rather than disappears.
const FADE = 'linear-gradient(to bottom, #000 calc(100% - 1.5rem), rgb(0 0 0 / 0.35))';

function RailTab({ id, label, dot = false, children }: {
  id: string;
  label: string;
  /** Marks a brand the user reaches through a subscription that can run one
   *  of its models now. */
  dot?: boolean;
  children: ReactNode;
}) {
  return (
    <ToggleButton
      id={id}
      aria-label={label}
      className={cn(
        'relative flex h-8 w-8 flex-none items-center justify-center rounded-lg outline-hidden',
        'data-hovered:bg-(--color-bg-hover) data-selected:bg-(--color-bg-tag)',
        'data-focus-visible:outline-current',
      )}
    >
      <span title={label} className="flex items-center justify-center">{children}</span>
      {dot && (
        <span
          aria-hidden
          className="absolute bottom-1 right-1 h-1.5 w-1.5 rounded-full bg-(--color-success) ring-[1.5px] ring-(--color-bg-elevated)"
        />
      )}
    </ToggleButton>
  );
}

/** A provider's mark, or its initial for one we ship no art for: unlike a
 *  mark beside a name, a rail tab has nothing else to say what it is. */
function RailIcon({ provider, label }: { provider: string; label: string }) {
  if (llmProviderArt(provider)) return <ModelMark provider={provider} className="h-4 w-4" />;
  return (
    <span aria-hidden className="text-xs font-semibold uppercase text-(--color-text-tertiary)">
      {label.charAt(0)}
    </span>
  );
}

function ModelRow({ model, rowRef, metadata, payer, selected, favorite, status, onToggleFavorite }: {
  model: string;
  rowRef?: Ref<HTMLDivElement>;
  metadata?: Metadata;
  payer?: Payer;
  selected: boolean;
  favorite: boolean;
  status: ModelRowStatus;
  onToggleFavorite: (model: string, favorite: boolean) => void;
}) {
  const { t } = useTranslation();
  const provider = metadata?.[model]?.provider;
  const name = getModelDisplayName(model, metadata);
  const reason = 'unavailable' in status ? status.unavailable : undefined;
  const fixable = 'unavailable' in status && !!status.fix;
  const tag = 'tag' in status ? status.tag : undefined;
  return (
    <GridListItem
      ref={rowRef}
      id={model}
      // A row a reason holds back says it as part of its name, which is all a
      // screen reader reads of a row.
      textValue={reason ? `${name}, ${reason}` : name}
      isDisabled={reason !== undefined && !fixable}
      className={cn('group', MENU_ROW, 'data-disabled:text-(--color-text-tertiary)', fixable && 'text-(--color-text-tertiary)')}
    >
      {/* One line holds the name and everything after it, so the check and
          the star sit level with the name whether or not a reason goes under. */}
      <span className="flex min-w-0 flex-1 flex-col">
        <span className="flex min-w-0 items-center gap-2">
          <span className="flex min-w-0 flex-1 items-center gap-2">
            <MarkedModelName provider={provider} name={name} nameClassName="min-w-0 truncate" />
          </span>
          <PayerBadge payer={payer} />
          {tag && <span className="flex-none text-xs text-(--color-text-tertiary)">{tag}</span>}
          <Check aria-hidden className={cn('h-4 w-4 flex-none text-(--color-accent-primary)', !selected && 'invisible')} />
          {selected && <span className="sr-only">{t('chat.modelSelector.current')}</span>}
          <Button
            aria-label={t(favorite ? 'chat.modelSelector.unfavorite' : 'chat.modelSelector.favorite', { model: name })}
            onPress={() => onToggleFavorite(model, !favorite)}
            className={cn(
              'flex h-5 w-5 flex-none items-center justify-center rounded outline-hidden data-focus-visible:outline-current',
              favorite
                ? 'text-(--color-text-secondary)'
                // Shown on the row's hover or focus where there is a pointer to
                // hover with. A touch screen has none, so it stays shown rather
                // than sit there invisible and still take a tap.
                : 'text-(--color-text-tertiary) opacity-0 group-data-focused:opacity-100 group-data-hovered:opacity-100 data-focus-visible:opacity-100 [@media(hover:none)]:opacity-100',
            )}
          >
            <Star aria-hidden className="h-3.5 w-3.5" fill={favorite ? 'currentColor' : 'none'} />
          </Button>
        </span>
        {/* Under the name itself, past the mark and its gap, and clear of the star's column. */}
        {reason && (
          <span className={cn('pe-7 text-xs text-(--color-text-tertiary)', provider && llmProviderArt(provider) && 'ps-[1.375rem]')}>
            {reason}
          </span>
        )}
      </span>
    </GridListItem>
  );
}
