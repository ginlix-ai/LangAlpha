import { useContext, useLayoutEffect, useState, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import type { ClaudeCodePlan, PlanCredential } from '@/api/model';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import { usePlanClock } from '@/hooks/useClaudeCodePlan';
import {
  NEAR_LIMIT, fullerWindow, limitDetail, percentOf, planTime, planWindows, resetsLabel, rowLimit,
  type MeterWindow, type PlanClock, type WindowUse,
} from '@/lib/claudeCodePlan';
import { relativeTime } from '@/lib/format';
import { cn } from '@/lib/utils';
import { CheckNowButton } from './PlanCheck';
import { UsageTableContext } from './usageSteps';
import { usePlanCheck } from './usePlanCheck';

const TRACK = 'color-mix(in srgb, var(--color-text-tertiary) 28%, transparent)';
const tone = (use: WindowUse) => (use.used >= NEAR_LIMIT ? 'var(--color-warning)' : undefined);
const shortName = (t: (key: string) => string, window: MeterWindow) => t(`claudeCodePlan.short.${window}`);

/** Ticks in a window's meter. */
const TICKS = 8;

/** A window's ticks, lit for its share rounded up, so any use lights one and
 *  only a full window lights all. */
function Ticks({ use }: { use: WindowUse }) {
  const lit = use.used > 0 ? Math.min(TICKS, Math.ceil(use.used * TICKS - 1e-9)) : 0;
  return (
    <span aria-hidden="true" className="inline-flex items-center gap-px">
      {Array.from({ length: TICKS }, (_, i) => (
        <span
          key={i}
          className="h-2.5 w-[2px] rounded-[1px]"
          style={{ backgroundColor: i < lit ? (tone(use) ?? 'var(--color-text-secondary)') : TRACK }}
        />
      ))}
    </span>
  );
}

/** Both windows' resets, for a tooltip; null unless both are known. */
function resetsBoth(windows: WindowUse[], clock: PlanClock) {
  const [five, seven] = (['five_hour', 'seven_day'] as const).map((w) => windows.find((u) => u.window === w));
  return five && seven ? { time5: planTime(five.resetsAt, clock), time7: planTime(seven.resetsAt, clock) } : null;
}

/**
 * The plan's use, explained on hover or focus: as of when, and what else
 * counts against it, then when each window resets. One trigger per row
 * takes focus, so a keyboard reaches it once.
 */
function PlanTip({ plan, focusable, children }: { plan: ClaudeCodePlan; focusable: boolean; children: ReactNode }) {
  const { t } = useTranslation();
  const clock = usePlanClock();
  const resets = resetsBoth(planWindows(plan, clock.now), clock);
  const lines = [
    t('claudeCodePlan.asOf', { ago: relativeTime(plan.observed_at, clock.locale, clock.now) }),
    ...(resets ? [t('claudeCodePlan.meter.resets', resets)] : []),
  ];
  return (
    <TooltipProvider delayDuration={300}>
      <Tooltip>
        <TooltipTrigger asChild>
          <span
            tabIndex={focusable ? 0 : -1}
            aria-label={focusable ? lines.join(' ') : undefined}
            className="inline-flex items-center rounded-sm outline-none focus-visible:outline-2 focus-visible:outline-offset-2"
            style={{ outlineColor: 'var(--color-text-primary)' }}
          >
            {children}
          </span>
        </TooltipTrigger>
        <TooltipContent side="top" className="flex max-w-72 flex-col gap-1">
          {lines.map((line) => <p key={line}>{line}</p>)}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

/** A window as the table and the picker draw it: ticks, then its share. */
function WindowMeter({ use, label = false }: { use: WindowUse; label?: boolean }) {
  const { t } = useTranslation();
  return (
    <span data-plan-window={use.window} className="inline-flex items-center gap-1 tabular-nums whitespace-nowrap" style={{ color: tone(use) ?? 'var(--color-text-secondary)' }}>
      {label && shortName(t, use.window)}
      <Ticks use={use} />
      {`${percentOf(use.used)}%`}
    </span>
  );
}

/** A row's use in the table's columns: 5h, 7d, and when the fuller window
 *  resets. Empty cells where a window reported nothing. */
export function UsageCells({ plan }: { plan: ClaudeCodePlan | null | undefined }) {
  const { t } = useTranslation();
  const steps = useContext(UsageTableContext);
  const clock = usePlanClock();
  if (!steps) return null;
  const windows = planWindows(plan, clock.now);
  const fuller = fullerWindow(plan, clock.now);
  const cell = (window: MeterWindow) => {
    const use = windows.find((w) => w.window === window);
    return (
      <span key={window} data-usage-cell className={cn('items-center text-xs', steps.window)}>
        {use && plan && (
          <PlanTip plan={plan} focusable={use === windows[0]}>
            <span className="sr-only">{shortName(t, window)}</span>
            <WindowMeter use={use} />
          </PlanTip>
        )}
      </span>
    );
  };
  return (
    <>
      {cell('five_hour')}
      {cell('seven_day')}
      <span data-usage-cell className={cn('text-xs whitespace-nowrap', steps.resets)} style={{ color: 'var(--color-text-tertiary)' }}>
        {fuller && plan && (
          <PlanTip plan={plan} focusable={false}>
            <span className="sr-only">{t('claudeCodePlan.table.resets')}</span>
            {resetsLabel(fuller.resetsAt, clock)}
          </PlanTip>
        )}
      </span>
    </>
  );
}

/** The quiet header row over a table's card: what the text column holds,
 *  the windows and Resets, each as wide as its widest value so the columns
 *  hold their place with no row to fill them, then over the status and
 *  actions, spanning both so it sets neither's width, `aside` (Check now). No list item: each
 *  cell names its own window to a screen reader. A card none of whose rows
 *  has use to show (`windows` false) names no window, its text spanning
 *  those columns as its rows' does. */
export function UsageHeaderRow({ first, aside, windows = true }: { first: string; aside?: ReactNode; windows?: boolean }) {
  const { t } = useTranslation();
  const steps = useContext(UsageTableContext);
  if (!steps) return null;
  return (
    <li role="presentation" data-usage-header className={cn('col-span-full grid-cols-subgrid items-center px-4 py-2 text-xs whitespace-nowrap', steps.header)} style={{ color: 'var(--color-text-tertiary)' }}>
      <span />
      {windows ? (
        <>
          <span>{first}</span>
          <span className="min-w-14">{shortName(t, 'five_hour')}</span>
          <span className="min-w-14">{shortName(t, 'seven_day')}</span>
          <span className={cn('min-w-12', steps.resets)}>{t('claudeCodePlan.table.resets')}</span>
        </>
      ) : <span className={steps.spanUsage}>{first}</span>}
      <span className="col-span-2 flex justify-end">{aside}</span>
    </li>
  );
}

/** Each window as a short bar on the detail line, its name before it and
 *  its share after, the shares a fixed width so rows line up. */
function InlineBars({ plan }: { plan: ClaudeCodePlan }) {
  const { t } = useTranslation();
  const clock = usePlanClock();
  return (
    <PlanTip plan={plan} focusable>
      <span className="inline-flex items-center gap-x-3 tabular-nums">
        {planWindows(plan, clock.now).map((use) => (
          <span key={use.window} data-plan-window={use.window} className="inline-flex items-center gap-1" style={{ color: tone(use) }}>
            {shortName(t, use.window)}
            <span aria-hidden="true" data-usage-bar className="h-1 w-7 overflow-hidden rounded-full" style={{ backgroundColor: TRACK }}>
              {use.used > 0 && (
                <span
                  className="block h-full rounded-full"
                  style={{ width: `max(3px, ${Math.min(use.used, 1) * 100}%)`, backgroundColor: tone(use) ?? 'var(--color-text-tertiary)' }}
                />
              )}
            </span>
            <span className="inline-block min-w-[2.2em]">{`${percentOf(use.used)}%`}</span>
          </span>
        ))}
      </span>
    </PlanTip>
  );
}

/** What follows a token's detail on its line: the plan's bars once the
 *  table's columns are gone, or S11 where its token reported no use yet. */
export type DetailTail = 'bars' | 'pending' | null;

/**
 * A token's detail on one line in a usage table: what it is, which gives way
 * first, then its `tail`, which never does. A `whole` one (a limit line)
 * never gives way either: the table steps down for it instead. The text is cut in code to the
 * room left, so its ellipsis ends right before the " · "; CSS's own would
 * leave part of a glyph's width as a gap there. Its whole is in a hover title.
 */
export function UsageDetail({ text, plan, tail, whole = false }: {
  text: ReactNode;
  plan: ClaudeCodePlan | null | undefined;
  tail: DetailTail;
  whole?: boolean;
}) {
  const { t } = useTranslation();
  const steps = useContext(UsageTableContext);
  const clock = usePlanClock();
  const [line, setLine] = useState<HTMLSpanElement | null>(null);
  const [full, setFull] = useState<HTMLSpanElement | null>(null);
  const [tailEl, setTailEl] = useState<HTMLSpanElement | null>(null);
  const cut = useFittedText(line, full, tailEl);
  const shown = tail === 'pending' ? t('claudeCodePlan.pending')
    : tail === 'bars' && plan && planWindows(plan, clock.now).length > 0 ? <InlineBars plan={plan} /> : null;
  if (whole) return <span data-usage-whole className="block truncate">{text}</span>;
  return (
    <span ref={setLine} data-usage-line className="flex min-w-0 items-center whitespace-nowrap">
      {/* The whole text, unseen, which the cut is measured from. */}
      <span ref={setFull} hidden>{text}</span>
      {text && <span className="min-w-0 truncate" title={cut?.title}>{cut?.text ?? text}</span>}
      {shown && (
        <span ref={setTailEl} className={cn('flex flex-none items-center', tail === 'bars' && steps?.inline)}>
          {text && <span className="whitespace-pre">{' · '}</span>}
          {shown}
        </span>
      )}
    </span>
  );
}

/** The text in `full` cut, with an ellipsis, to the room its line leaves
 *  beside the tail, with the whole as its title; null where it fits. Measured
 *  again whenever one of the three is mounted anew (a line that was whole,
 *  a tail that came after it), so no cut outlives the elements it was
 *  measured on. */
function useFittedText(
  line: HTMLSpanElement | null, full: HTMLSpanElement | null, tail: HTMLSpanElement | null,
): { text: string; title: string } | null {
  const [cut, setCut] = useState<{ text: string; title: string } | null>(null);
  useLayoutEffect(() => {
    const ctx = document.createElement('canvas').getContext('2d');
    if (!line || !full || !ctx) return;
    const fit = () => {
      const text = full.textContent ?? '';
      const style = getComputedStyle(line);
      ctx.font = style.font;
      ctx.letterSpacing = style.letterSpacing === 'normal' ? '0px' : style.letterSpacing;
      const room = line.getBoundingClientRect().width - (tail?.getBoundingClientRect().width ?? 0) - 0.5;
      const width = (s: string) => ctx.measureText(s).width;
      let next: { text: string; title: string } | null = null;
      if (text && width(text) > room) {
        let lo = 0;
        let hi = text.length;
        while (lo < hi) {
          const mid = (lo + hi + 1) >> 1;
          if (width(text.slice(0, mid) + '…') <= room) lo = mid;
          else hi = mid - 1;
        }
        next = { text: text.slice(0, lo).trimEnd() + '…', title: text };
      }
      setCut((kept) => (kept?.text === next?.text ? kept : next));
    };
    fit();
    const resized = new ResizeObserver(fit);
    resized.observe(line);
    if (tail) resized.observe(tail);
    const changed = new MutationObserver(fit);
    changed.observe(full, { childList: true, subtree: true, characterData: true });
    void document.fonts?.ready.then(fit);
    return () => {
      resized.disconnect();
      changed.disconnect();
    };
  }, [line, full, tail]);
  return cut;
}

/** Whose Claude plan the picker's meter reads, the credential Check now
 *  reads it through, null where the token does not work now, and until when
 *  a check holds it (`CheckTarget`). */
export interface PickerMeter {
  name: string;
  plan: ClaudeCodePlan | null | undefined;
  credential: PlanCredential | null;
  checkAfter: number | null;
}

/**
 * The model picker's Claude Code header. Whose plan its turns run on, then
 * each window's ticks and share as the table draws them, with both resets
 * on hover, or, where no window reported, the limit it is held at or that
 * use shows after the first turn, and Check now as an icon in the stars'
 * column where the token works.
 * Two lines: the picker's width holds the name or the meter, not both.
 */
export function PickerPlanMeter({ meter: { name, plan, credential, checkAfter } }: { meter: PickerMeter }) {
  const { t } = useTranslation();
  const clock = usePlanClock();
  const check = usePlanCheck(credential ? [{ credential, checkAfter }] : []);
  const windows = planWindows(plan, clock.now);
  const limit = rowLimit(plan, clock.now);
  const resets = resetsBoth(windows, clock);
  const ago = plan ? relativeTime(plan.observed_at, clock.locale, clock.now) : '';
  const tip = resets ? t('claudeCodePlan.meter.tip', { ago, ...resets }) : t('claudeCodePlan.asOf', { ago });
  return (
    <span data-plan-meter className="flex min-w-0 flex-1 flex-col gap-0.5 whitespace-nowrap">
      <span className="min-w-0 truncate">{t('claudeCodePlan.meter.title', { name })}</span>
      <span className="flex min-w-0 items-center justify-between gap-2">
        {windows.length > 0 ? (
          <TooltipProvider delayDuration={300}>
            <Tooltip>
              <TooltipTrigger asChild>
                <span
                  tabIndex={0}
                  aria-label={tip}
                  className="flex flex-none items-center gap-2.5 rounded-sm outline-none focus-visible:outline-2 focus-visible:outline-offset-2"
                  style={{ outlineColor: 'var(--color-text-primary)' }}
                >
                  {windows.map((use) => <WindowMeter key={use.window} use={use} label />)}
                </span>
              </TooltipTrigger>
              <TooltipContent side="top" className="max-w-72">{tip}</TooltipContent>
            </Tooltip>
          </TooltipProvider>
        ) : (
          <span className="min-w-0 truncate">{limit ? limitDetail(t, limit, clock) : t('claudeCodePlan.meter.empty')}</span>
        )}
        {credential && <CheckNowButton check={check} icon />}
      </span>
    </span>
  );
}
