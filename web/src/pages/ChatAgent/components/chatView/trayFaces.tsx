/**
 * What each of the composer tray's chips shows at a density: its class, what
 * it draws, and its words for a hover and a screen reader. Pure, so the probe
 * that measures every density draws them as plain spans, and only the row
 * mounts the controls around them (`trayChips`).
 */
import type { ReactNode } from 'react';
import type { TFunction } from 'i18next';
import { useTranslation } from 'react-i18next';
import { Activity, AlarmClock, Folder, Terminal } from 'lucide-react';
import PulseDot from '@/components/ui/pulse-dot';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { integer, mediumTime } from '@/lib/format';
import { moveTone, signedPercent } from '@/lib/priceMove';
import { cn } from '@/lib/utils';
import type { PlanStep } from '@/types/chat';
import type { CommandRun } from '@/types/commands';
import type { WakeupList } from '@/types/wakeups';
import type { MarketWatchState } from '../../session/marketWatchEvents';
import { sidebarAgentRowLabel } from '../../session/subagents/subagentStatus';
import { DispatchStatusGlyph } from '../dispatchStatusUi';
import { SubagentGlyph } from '../mcp/subagentGlyph';
import { RunGlyph } from '../messageList/runCard';
import { planHeading, planProgress, stepTitle } from '../messageList/requestPlans';
import { awayLabel, awayPercent, chipLabel, chipShortLabel, nearestPriceWakeup } from '../wakeupLabels';
import { lastTrade, quotePrice, useThreadQuotes, type ThreadQuotes } from './threadQuotesContext';
import { MarkedTicker, PlanTicks, TaskGlyph } from './trayGlyphs';
import type { TrayDensity } from './trayDensity';
import type { RosterAnalyst } from './useAnalystRoster';
import type { TrayChipId, TrayContents, TrayTasks } from './useTrayContents';

// A chip is an outline on the page's own color: it reads as unfilled, and it
// is still opaque, since the transcript scrolls under the tray. In the
// composer's own strip (`data-strip`) the chips are items on its surface
// instead: no edge of their own, a wash on hover.
export const CHIP = cn(
  'inline-flex h-5 max-w-full items-center gap-1.5 rounded-full border border-(--color-border-default) bg-(--color-bg-page) px-2 text-[0.71875rem] leading-none text-(--color-text-tertiary)',
  'group-data-strip:rounded-md group-data-strip:border-0 group-data-strip:bg-transparent group-data-strip:px-1.5',
);
const CHIP_BUTTON = cn(
  CHIP,
  'tap-target cursor-pointer outline-hidden transition-colors hover:bg-(--color-bg-elevated) hover:text-(--color-text-secondary) focus-visible:ring-2 focus-visible:ring-ring',
  'data-[state=open]:bg-(--color-bg-elevated) data-[state=open]:text-(--color-text-primary)',
  'group-data-strip:hover:bg-(--color-bg-hover) group-data-strip:data-[state=open]:bg-(--color-bg-hover)',
);

/**
 * The widest a chip's own words run at each density: a lone subagent's or
 * Analyst's name, the plan's title. Below that, a chip with words to give
 * (`min-w-0`) truncates them rather than push the row past its edge, once
 * every chip that can move has.
 */
const LABEL_MAX: Readonly<Record<TrayDensity, string>> = {
  full: 'max-w-64',
  compact: 'max-w-40',
  tight: 'max-w-24',
};

/** What a chip shows at one density. */
export interface ChipFace {
  /**
   * The control the row mounts around it: none for a line that only says
   * what runs (`status`), a button that opens the one thing it names
   * (`button`), or a button opening a list (`list`). Decided here, beside
   * what it draws, so the probe measures the shape the row mounts.
   */
  kind: 'status' | 'button' | 'list';
  /** The chip's whole class, which the row and the probe both draw it with. */
  className: string;
  content: ReactNode;
  title?: string;
  /** Its accessible name, where what it draws does not say it all. */
  name?: string;
}

/** What the faces read beyond a chip's own entry, read once per tray for its row and its probe. */
export interface FaceContext {
  t: TFunction;
  locale: string;
  now: number;
  quotes: ThreadQuotes['bySymbol'];
}

export function useFaceContext(): FaceContext {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useNow();
  const quotes = useThreadQuotes().bySymbol;
  return { t, locale, now, quotes };
}

type FaceOf<K extends TrayChipId> = (value: NonNullable<TrayContents[K]>, density: TrayDensity, ctx: FaceContext) => ChipFace;

const FACES: { [K in TrayChipId]: FaceOf<K> } = {
  plan: planFace,
  watch: watchFace,
  wakeups: wakeupsFace,
  commands: commandsFace,
  tasks: tasksFace,
  analysts: analystsFace,
  reportBack: (label, density) => statusFace(label, density),
};

/** A chip's face at `density`. */
export function chipFace<K extends TrayChipId>(id: K, value: NonNullable<TrayContents[K]>, density: TrayDensity, ctx: FaceContext): ChipFace {
  return FACES[id](value, density, ctx);
}

/** A list chip's face: a spinner while any of it works, its icon, its words. */
function listFace({ label, name, icon, title, working }: {
  label: string;
  /** The whole count when the label is the short one (`2 tasks · 1 running`). */
  name?: string;
  icon?: ReactNode;
  title?: string;
  working: boolean;
}): ChipFace {
  return {
    kind: 'list',
    className: CHIP_BUTTON,
    title: title ?? name,
    name,
    content: (
      <>
        {working && <RunGlyph kind="live" />}
        {icon}
        <span className="tabular-nums">{label}</span>
      </>
    ),
  };
}

/** A list chip that counts what it lists: its words while there is room, an
 *  icon and the bare number when tight, the count in full in its name. */
export function countListFace({ count, label, name, icon, working }: {
  count: number;
  label: string;
  name: string;
  icon: ReactNode;
  working: boolean;
}, density: TrayDensity, locale: string): ChipFace {
  const tight = density === 'tight';
  return listFace({ label: tight ? integer(count, locale) : label, name, icon: tight && icon, working });
}

/** A chip standing for one thing it opens: a glyph and its name, cut short of room. */
function loneRowFace({ glyph, label, title }: { glyph: ReactNode; label: string; title?: string }, density: TrayDensity): ChipFace {
  return {
    kind: 'button',
    className: cn(CHIP_BUTTON, 'min-w-0'),
    title,
    content: (
      <>
        {glyph}
        <span className={cn('min-w-0 truncate', LABEL_MAX[density])}>{label}</span>
      </>
    ),
  };
}

/** A chip that only says what is running, with no control. */
export function statusFace(label: string, density: TrayDensity): ChipFace {
  return {
    kind: 'status',
    className: cn(CHIP, 'min-w-0'),
    title: density === 'full' ? undefined : label,
    content: (
      <>
        <RunGlyph kind="live" />
        <span className={cn('min-w-0 truncate', density !== 'full' && LABEL_MAX[density])}>{label}</span>
      </>
    ),
  };
}

/** The way to the Status panel, counting the chips moved into it. */
export function openStatusFace(more: number, { t, locale }: Pick<FaceContext, 't' | 'locale'>): ChipFace {
  const label = more > 0 ? t('chat.status.openPanelMore', { count: more }) : t('chat.status.openPanel');
  return {
    kind: 'button',
    className: cn(CHIP_BUTTON, 'shrink-0 gap-1 px-1.5'),
    title: label,
    name: label,
    content: (
      <>
        <Activity aria-hidden="true" className="h-3 w-3 shrink-0" />
        {more > 0 && <span aria-hidden="true" className="tabular-nums">+{integer(more, locale)}</span>}
      </>
    ),
  };
}

function planFace(steps: PlanStep[], density: TrayDensity, { t }: FaceContext): ChipFace {
  // The chip stands only while a step is left, so one is current.
  const { current: shown, total } = planProgress(steps);
  const title = stepTitle(steps[shown], shown);
  const tight = density === 'tight';
  return {
    kind: 'list',
    // Sized from 8rem, not its title: short of room the plan gives up its
    // width before any other chip does. Tight, the title is gone and only the
    // ticks can give: a grid whose last column may close, so the glyph and
    // the step count hold and the bars clip after them.
    className: cn(
      CHIP_BUTTON,
      'max-w-max flex-[1_1_8rem]',
      tight ? 'inline-grid grid-cols-[auto_auto_minmax(0,max-content)]' : 'min-w-0',
    ),
    title,
    content: (
      <>
        <RunGlyph kind="live" />
        {/* Tight, the title is only read: the same live region in every
            density, so a change of density announces nothing. */}
        <span
          role="status"
          aria-live="polite"
          className={cn(
            'min-w-0 truncate text-(--color-text-secondary)',
            density === 'full' ? LABEL_MAX.full : density === 'compact' ? 'max-w-32' : 'sr-only',
          )}
        >
          {title}
        </span>
        <span className="sr-only">{t('chat.plan.heading', { progress: planHeading(steps, 'live', t) })}</span>
        {tight && (
          <span aria-hidden="true" className="shrink-0 tabular-nums text-(--color-text-secondary)">
            {shown + 1}/{total}
          </span>
        )}
        <PlanTicks steps={steps} clip={tight} />
      </>
    ),
  };
}

/** Up to this many watched tickers show their prices in the chip; more, and it names them. */
const WATCH_PRICED_MAX = 2;

/** Up to this many tickers show their direction in the tight chip; more are counted. */
const WATCH_MARKED_MAX = 3;

/**
 * The tickers the agent is live-stamping. The label is the polite live
 * region, not the button: a live region should not be the control itself.
 * Short of room, each ticker gives up its price, then its figure for a drawn
 * direction; the title keeps the line in full.
 */
function watchFace(watch: MarketWatchState, density: TrayDensity, { t, locale, quotes }: FaceContext): ChipFace {
  const short = density !== 'full';
  const places = short ? 1 : 2;
  const updated = watch.timestamp
    ? t('chat.marketWatch.chipTitleUpdated', { time: mediumTime(watch.timestamp * 1000, locale) })
    : t('chat.marketWatch.chipTitle');
  const watching = t('chat.marketWatch.watching', { symbols: watch.symbols.join(', ') });
  const priced = watch.symbols.length <= WATCH_PRICED_MAX && watch.symbols.every((symbol) => quotes[symbol]?.price != null);
  const quoted = watch.symbols.some((symbol) => quotes[symbol]?.price != null);
  const line = watch.symbols.map((symbol) => {
    const quote = quotes[symbol];
    return [symbol, quotePrice(quote, locale), quote?.pct != null ? signedPercent(quote.pct) : null].filter(Boolean).join(' ');
  }).join(' · ');
  return {
    kind: 'button',
    className: CHIP_BUTTON,
    title: short && quoted ? `${updated}\n${line}` : updated,
    name: t('chat.marketWatch.chipAction'),
    content: (
      <>
        <PulseDot />
        {density === 'tight' ? (
          <span className="flex items-center gap-1.5">
            <span className="sr-only">{watching}</span>
            {watch.symbols.slice(0, WATCH_MARKED_MAX).map((symbol) => (
              <MarkedTicker key={symbol} symbol={symbol} quote={quotes[symbol]} />
            ))}
            {watch.symbols.length > WATCH_MARKED_MAX && (
              <span aria-hidden="true" className="tabular-nums">+{watch.symbols.length - WATCH_MARKED_MAX}</span>
            )}
          </span>
        ) : priced ? (
          // The prices tick, so they are not a live region; the name says what is watched.
          <span className="flex items-center gap-1.5 tabular-nums">
            <span className="sr-only">{watching}</span>
            {watch.symbols.map((symbol, i) => {
              const quote = quotes[symbol];
              return (
                <span key={symbol} aria-hidden="true" className="flex items-center gap-1">
                  {i > 0 && !short && <span style={{ color: 'var(--color-text-quaternary)' }}>·</span>}
                  <span className="font-medium" style={{ color: 'var(--color-text-secondary)' }}>{symbol}</span>
                  {!short && <span>{quotePrice(quote, locale)}</span>}
                  {quote.pct != null && <span style={{ color: moveTone(quote.pct, places) }}>{signedPercent(quote.pct, places)}</span>}
                </span>
              );
            })}
          </span>
        ) : (
          <span role="status" aria-live="polite" className={cn('tabular-nums', short && 'max-w-40 truncate')}>{watching}</span>
        )}
      </>
    ),
  };
}

/**
 * The wakeups the agent set in this thread: how many are pending and when the
 * next fires. The price wakeup nearest its level says how far it still has to
 * go, at the last trade in the chat's quote of its ticker.
 */
function wakeupsFace(list: WakeupList, density: TrayDensity, { t, locale, now, quotes }: FaceContext): ChipFace {
  const nearest = nearestPriceWakeup(list.items, (symbol) => lastTrade(quotes[symbol]));
  const away = nearest && `${nearest.symbol} ${awayLabel(nearest.distance, t, locale)}`;
  const withDistance = (label: string) => (away ? `${label} · ${away}` : label);
  const short = chipShortLabel(list.items, t, locale, now, list.userTimezone);
  // Short of room the distance stays, being what moves; the count and the next
  // time stay in the name and the title.
  const label = density === 'full' ? withDistance(short)
    : density === 'compact' ? (away ?? short)
      : nearest ? awayPercent(nearest.distance, locale) : integer(list.items.length, locale);
  const name = withDistance(chipLabel(list.items, t, locale, now, list.userTimezone));
  const title = t('chat.wakeups.chipTitle');
  return listFace({
    label,
    name,
    icon: <AlarmClock aria-hidden="true" className="h-3 w-3 shrink-0" />,
    title: density === 'full' ? title : `${name}\n${title}`,
    working: false,
  });
}

/** The commands still running in the background. */
function commandsFace(rows: CommandRun[], density: TrayDensity, { t, locale }: FaceContext): ChipFace {
  return countListFace({
    count: rows.length,
    label: t('chat.tray.commands', { count: rows.length }),
    name: t('chat.commands.chip', { count: rows.length }),
    icon: <Terminal aria-hidden="true" className="h-3 w-3 shrink-0" />,
    working: true,
  }, density, locale);
}

function tasksFace({ rows, unnamedRunning }: TrayTasks, density: TrayDensity, { t, locale }: FaceContext): ChipFace {
  // Running, but nothing the roster names.
  if (rows.length === 0) return statusFace(t('chat.backgroundTasksRunning'), density);
  // A lone task the chip opens, unless it also spins for a task it cannot name.
  if (rows.length === 1 && !unnamedRunning) {
    const [lone] = rows;
    const label = sidebarAgentRowLabel(lone);
    return loneRowFace({ glyph: <TaskGlyph row={lone} />, label, title: label }, density);
  }
  const running = rows.filter((row) => row.running).length;
  const count = t('chat.tray.tasks', { count: rows.length });
  return countListFace({
    count: rows.length,
    label: count,
    name: running > 0 ? t('chat.tray.tasksRunning', { count: rows.length, running }) : count,
    icon: <SubagentGlyph type={undefined} className="h-3 w-3 shrink-0" />,
    working: running > 0 || unnamedRunning,
  }, density, locale);
}

function analystsFace(threads: RosterAnalyst[], density: TrayDensity, { t, locale }: FaceContext): ChipFace {
  if (threads.length === 1) {
    const [thread] = threads;
    const name = thread.workspaceName || thread.title;
    const label = thread.running
      ? t('agents.tray.waitingOn', { name: name ?? t('agents.tray.analysts', { count: 1 }) })
      : (name ?? t('agents.tray.analysts', { count: 1 }));
    const glyph = thread.running ? <RunGlyph kind="live" /> : thread.status && <DispatchStatusGlyph status={thread.status} />;
    return loneRowFace({ glyph, label, title: thread.title ?? undefined }, density);
  }
  const running = threads.filter((thread) => thread.running).length;
  const count = t('agents.tray.analysts', { count: threads.length });
  return countListFace({
    count: threads.length,
    label: count,
    name: running > 0 ? t('agents.tray.analystsRunning', { count: threads.length, running }) : count,
    icon: <Folder aria-hidden="true" className="h-3 w-3 shrink-0" />,
    working: running > 0,
  }, density, locale);
}
