import type { CSSProperties, ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { ArrowUpRight, ChevronDown, Pause, Pencil, Plus, Trash2, Zap } from 'lucide-react';
import { useLocale } from '@/hooks/useLocale';
import { fixed2 } from '@/lib/format';
import { cn } from '@/lib/utils';
import { ALERT_LEVEL, BRIEF_EXCERPT, NVDA, on, useBook } from './automationBook';
import { VIEW } from './camera';
import { ease, PAST, useSceneTime } from './clock';

/* The Automations page's parts at scene scale, each a small copy of the
   component named on it, in the page's tokens and labels: the header, the
   price meter, the manage list and its inspector, and the link and excerpt
   a run shows in both the inspector and the feed. The form's parts are in
   automationForm.tsx and the feed's in automationFeed.tsx. Each scene draws
   the page at its own size and a camera (camera.tsx) shows the part a step
   is about. */

/** A section's heading (`.automations-kicker`): small mono capitals, set
    tight in Chinese, with an optional count after it. */
export function Kicker({
  children,
  count,
  className,
}: {
  children: ReactNode;
  count?: number;
  className?: string;
}) {
  const zh = useLocale().startsWith('zh');
  return (
    <div
      className={cn(
        'font-mono text-[5.5px] font-medium uppercase leading-none',
        !zh && 'tracking-[0.08em]',
        className
      )}
      style={{ color: 'var(--color-text-tertiary)' }}
    >
      {children}
      {count != null && (
        <span className="ml-[3px] tabular-nums" style={{ color: 'var(--color-text-quaternary)' }}>
          {count}
        </span>
      )}
    </div>
  );
}

/** A two-way switch (`SegmentedControl`): the chosen side in the accent's
    soft fill and ink. */
export function Segmented({ options, value }: { options: string[]; value: number }) {
  return (
    <span
      className="inline-flex shrink-0 overflow-hidden rounded-[5px] border"
      style={{ borderColor: 'var(--color-border-muted)' }}
    >
      {options.map((label, i) => (
        <span
          key={label}
          className="whitespace-nowrap px-[5px] py-[3px] text-[6.5px] font-medium leading-none"
          style={
            i === value
              ? { background: 'var(--color-accent-soft)', color: 'var(--color-accent-primary)' }
              : { color: 'var(--color-text-secondary)' }
          }
        >
          {label}
        </span>
      ))}
    </span>
  );
}

/** A header action (`HeaderButton`): primary fill, secondary hairline, or a
    ghost with no edge. `pressed` is its transform. */
function Btn({
  variant = 'secondary',
  pressed,
  className,
  children,
}: {
  variant?: 'primary' | 'secondary' | 'ghost';
  pressed?: string;
  className?: string;
  children: ReactNode;
}) {
  const look: CSSProperties =
    variant === 'primary'
      ? {
          background: 'var(--color-btn-primary-bg)',
          color: 'var(--color-btn-primary-text)',
          borderColor: 'transparent',
        }
      : variant === 'secondary'
        ? { color: 'var(--color-text-secondary)', borderColor: 'var(--color-border-muted)' }
        : { color: 'var(--color-text-tertiary)', borderColor: 'transparent' };
  return (
    <span
      className={cn(
        'inline-flex shrink-0 items-center gap-[3px] whitespace-nowrap rounded-[4px] border',
        'px-1.5 py-[3px] text-[6px] leading-none transition-transform duration-press',
        className
      )}
      style={{ ...look, transform: pressed }}
    >
      {children}
    </span>
  );
}

/** The page header (`AutomationsHeader`): the title over the census, or over
    the tagline while nothing is set up; then, once something is, the
    Feed / Manage switch and New automation. */
export function PageHeader({
  census,
  view,
  pressNew,
}: {
  census?: string;
  view?: 'feed' | 'manage';
  pressNew?: string;
}) {
  const { t } = useTranslation();
  return (
    <div className="flex h-[30px] shrink-0 items-end justify-between gap-2">
      <div className="min-w-0">
        <div
          className="text-[12px] font-semibold leading-[1.2] tracking-[-0.01em]"
          style={{ color: 'var(--color-text-primary)' }}
        >
          {t('automation.automations')}
        </div>
        {census ? (
          <div
            className="-mb-[0.75px] mt-[2.25px] truncate font-mono text-[6px] leading-[1.25] tabular-nums"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {census}
          </div>
        ) : (
          <div
            className="mt-[3px] text-[7px] leading-none"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {t('automation.tagline')}
          </div>
        )}
      </div>
      {view && (
        <div className="flex shrink-0 items-center gap-[5px]">
          <Segmented
            options={[t('automation.viewFeed'), t('automation.viewManage')]}
            value={view === 'feed' ? 0 : 1}
          />
          <Btn variant="primary" pressed={pressNew} className="font-medium">
            <Plus className="h-[7px] w-[7px]" />
            {t('automation.newAutomation')}
            <ChevronDown className="h-[7px] w-[7px] opacity-70" />
          </Btn>
        </div>
      )}
    </div>
  );
}

/* ── the price meter ───────────────────────────────────────────────────── */

/** The meter's track (`.automation-meter-track`): the line, the stretch still
    to travel dotted, the level in the accent, and the price as a dot knocked
    out of whatever it sits on. Positions are % of the track; `wash` is a
    tint the host lays over the page, which the knock-out takes too. */
export function Track({
  cur,
  thr,
  gap,
  width,
  wash,
}: {
  cur?: number;
  thr?: number;
  gap?: [number, number];
  width?: number;
  wash?: string;
}) {
  return (
    <span className="relative block h-[6px]" style={{ width }} aria-hidden>
      <span
        className="absolute inset-x-0 top-[2.5px] h-px"
        style={{ background: 'var(--color-border-muted)' }}
      />
      {gap && gap[1] > 0 && (
        <span
          className="absolute top-[2.5px] h-0 border-t border-dotted"
          style={{
            left: `${gap[0]}%`,
            width: `${gap[1]}%`,
            borderColor: 'var(--color-text-tertiary)',
          }}
        />
      )}
      {thr != null && (
        <span
          className="absolute top-0 -ml-[0.5px] h-[6px] w-px"
          style={{ left: `${thr}%`, background: 'var(--color-accent-primary)' }}
        />
      )}
      {cur != null && (
        <span
          className="absolute top-px -ml-0.5 h-1 w-1 rounded-full"
          style={{
            left: `${cur}%`,
            background: 'var(--color-text-primary)',
            boxShadow: `0 0 0 1px ${wash ?? 'transparent'}, 0 0 0 1px var(--color-bg-page)`,
          }}
        />
      )}
    </span>
  );
}

/**
 * The price alert's meter under its distance line, as the trigger readout and
 * the inspector (`PriceReadout`, `Inspector`) both draw it. `at` is when the
 * dotted stretch starts drawing; without one it is already drawn.
 */
export function AlertMeter({ at }: { at?: number }) {
  const B = useBook();
  return (
    <>
      <div
        className="mb-[3px] font-mono text-[5.5px] leading-none tabular-nums"
        style={{ color: 'var(--color-text-tertiary)' }}
      >
        {B.away}
      </div>
      <PriceMeter symbol="NVDA" current={NVDA.price} threshold={ALERT_LEVEL} at={at} />
    </>
  );
}

/**
 * Where a price sits against the level that fires the run (`PriceMeter`),
 * on a scale fitted to the two with room either side, the numbers under each
 * end. The dotted stretch draws out from `at`; without one it is already drawn,
 * as on a page the scene opens on.
 */
export function PriceMeter({
  symbol,
  current,
  threshold,
  at = PAST,
}: {
  symbol: string;
  current: number;
  threshold: number;
  at?: number;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const p = ease(useSceneTime(), at, 700);
  const span = Math.abs(threshold - current);
  const pad = Math.max(span * 0.4, Math.abs(current) * 0.01);
  const lo = Math.min(current, threshold) - pad;
  const hi = Math.max(current, threshold) + pad;
  const pos = (v: number) => ((v - lo) / (hi - lo)) * 100;
  const cur = pos(current);
  const thr = pos(threshold);
  const met = current >= threshold;
  return (
    <div>
      <Track
        cur={cur}
        thr={thr}
        gap={met ? undefined : [Math.min(cur, thr), Math.abs(thr - cur) * p]}
      />
      <div
        className={cn(
          'mt-[2px] flex justify-between gap-1 font-mono text-[5.5px] leading-none',
          'tabular-nums'
        )}
        style={{ color: 'var(--color-text-tertiary)' }}
      >
        <span>
          <span style={{ color: 'var(--color-text-secondary)' }}>{symbol}</span>{' '}
          {fixed2(current, locale)}
        </span>
        <span>{t('automation.firesAt', { value: fixed2(threshold, locale) })}</span>
      </div>
    </div>
  );
}

/* ── the manage list ───────────────────────────────────────────────────── */

/** A row of the list (`AutomationList`): the name over its schedule line,
    what comes next at the end, the hover fill when chosen. */
function ListRow({
  name,
  sub,
  trailing,
  selected = false,
}: {
  name: string;
  sub: string;
  trailing?: string;
  selected?: boolean;
}) {
  return (
    <div
      className="flex items-start gap-1 rounded-[4px] px-1 py-[4px] transition-colors duration-quick"
      style={{ background: selected ? 'var(--color-bg-hover)' : undefined }}
    >
      <span className="min-w-0 flex-1">
        <span
          className="block truncate text-[6.5px] font-medium leading-[1.25]"
          style={{ color: 'var(--color-text-primary)' }}
        >
          {name}
        </span>
        <span
          className="mt-px block truncate text-[5.5px] leading-[1.25]"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {sub}
        </span>
      </span>
      {trailing && (
        <span
          className="shrink-0 whitespace-nowrap pt-px text-[5.5px] leading-[1.25]"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {trailing}
        </span>
      )}
    </div>
  );
}

/* ── the feed ──────────────────────────────────────────────────────────── */

/** A link out of a run (`.automation-link`): Open thread, or Watch live. */
export function LinkOut({ children }: { children: ReactNode }) {
  return (
    <span
      className="inline-flex shrink-0 items-center gap-[2px] text-[6px] leading-none"
      style={{ color: 'var(--color-text-secondary)' }}
    >
      {children}
      <ArrowUpRight className="h-[6px] w-[6px]" />
    </span>
  );
}

/** The start of what a run said (`.automation-run-excerpt`), up to three lines. */
export function Excerpt({ children }: { children: ReactNode }) {
  return (
    <p
      className="mt-[4px] line-clamp-3 text-[7px] leading-[1.5]"
      style={{ color: 'var(--color-text-secondary)' }}
    >
      {children}
    </p>
  );
}

/** Content that opens to its height as `p` goes 0 to 1, pushing what is
    below it down rather than landing on it. */
export function Grow({ p, children }: { p: number; children: ReactNode }) {
  return (
    <div className="grid" style={{ gridTemplateRows: `${p}fr`, opacity: Math.min(1, p * 1.5) }}>
      <div className="min-h-0 overflow-hidden">{children}</div>
    </div>
  );
}

/* ── the page and its manage view ──────────────────────────────────────── */

/** The page at its own size under the camera: the Automations page alone,
    no app around it. `height` runs past the view for a pane the camera
    pans down, as the page's own scroller would. */
export function Page({ height = VIEW.height, children }: { height?: number; children: ReactNode }) {
  return (
    <div className="relative flex flex-col p-[12px]" style={{ width: VIEW.width, height }}>
      {children}
    </div>
  );
}

/** The manage view (`ManageView`): the list on the left, what it lists on
    the right, under a hairline; both columns clip as their scrollers do. */
export function ManageLayout({ list, pane }: { list: ReactNode; pane: ReactNode }) {
  return (
    <div
      className="mt-[10px] grid min-h-0 flex-1 grid-cols-[112px_minmax(0,1fr)] border-t"
      style={{ borderColor: 'var(--color-border-muted)' }}
    >
      <div
        className="min-h-0 overflow-hidden border-r pr-[6px] pt-[6px]"
        style={{ borderColor: 'var(--color-border-muted)' }}
      >
        {list}
      </div>
      <div className="relative min-h-0 overflow-hidden pl-[12px] pt-[10px]">{pane}</div>
    </div>
  );
}

/** The list: the brief and the review under Scheduled and, once it
    exists, the alert under Watching. */
export function AutomationRows({
  chosen,
  alert,
}: {
  chosen: 'brief' | 'alert' | null;
  alert: ReactNode;
}) {
  const { t } = useTranslation();
  const B = useBook();
  return (
    <div className="flex flex-col gap-[9px]">
      <div>
        <Kicker count={2} className="px-1 pb-[4px] pt-[2px]">
          {t('automation.groupScheduled')}
        </Kicker>
        <ListRow
          name={B.brief}
          sub={B.briefLine}
          trailing={B.nextBrief}
          selected={chosen === 'brief'}
        />
        <ListRow name={B.review} sub={B.reviewLine} trailing={B.nextReview} />
      </div>
      {alert}
    </div>
  );
}

export function AlertGroup({ selected }: { selected: boolean }) {
  const { t } = useTranslation();
  const B = useBook();
  return (
    <div>
      <Kicker count={1} className="px-1 pb-[4px] pt-[2px]">
        {t('automation.groupWatching')}
      </Kicker>
      <ListRow name={B.alert} sub={B.alertLine} trailing={B.away} selected={selected} />
    </div>
  );
}

/** One automation in the pane (`AutomationInspector`): the state and place,
    the name, the schedule in a sentence, the meter for a price watch, the
    actions, and the latest run: the brief's report, none yet for the new
    alert. `pressEdit` presses Edit. */
export function Inspector({ which, pressEdit }: { which: 'brief' | 'alert'; pressEdit?: string }) {
  const { t } = useTranslation();
  const B = useBook();
  const brief = which === 'brief';
  const icon = 'h-[6px] w-[6px]';
  return (
    <div>
      <Kicker>
        {[
          t(brief ? 'automation.stateScheduled' : 'automation.stateWatching'),
          t('agents.chiefOfStaff'),
        ].join(' · ')}
      </Kicker>
      <div
        className="mt-[5px] text-[12px] font-semibold leading-[1.2] tracking-[-0.01em]"
        style={{ color: 'var(--color-text-primary)' }}
      >
        {brief ? B.brief : B.alert}
      </div>
      {/* two lines held, so the actions sit level in every language */}
      <p
        className="mt-[4px] min-h-[22px] text-[7px] leading-[1.55]"
        style={{ color: 'var(--color-text-secondary)' }}
      >
        {brief ? B.briefSentence : B.alertSentence}
      </p>
      {/* as wide a share of the pane as the app's meter takes of its own */}
      {!brief && (
        <div className="mt-[7px] max-w-[140px]">
          <AlertMeter />
        </div>
      )}
      <div className="mt-[9px] flex flex-wrap items-center gap-1">
        <Btn>
          <Zap className={icon} />
          {t('automation.runNow')}
        </Btn>
        <Btn>
          <Pause className={icon} />
          {t('automation.pause')}
        </Btn>
        <Btn pressed={pressEdit}>
          <Pencil className={icon} />
          {t('automation.edit')}
        </Btn>
        <Btn variant="ghost" className="ml-auto">
          <Trash2 className={icon} />
          {t('common.delete')}
        </Btn>
      </div>
      <div className="mt-[14px]">
        <Kicker className="mb-[5px]">{t('automation.latestRun')}</Kicker>
        {brief ? (
          <div
            className="rounded-[5px] border px-2 py-[7px]"
            style={{ background: 'var(--color-bg-card)', borderColor: 'var(--color-border-muted)' }}
          >
            <div
              className="font-mono text-[5.5px] leading-none tabular-nums"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              {[t('automation.runCompleted'), B.moment(on(21, 8, 0)), B.took(252_000)].join(' · ')}
            </div>
            <Excerpt>{BRIEF_EXCERPT}</Excerpt>
            <div className="mt-[5px] flex">
              <LinkOut>{t('automation.openThread')}</LinkOut>
            </div>
          </div>
        ) : (
          <p className="text-[7px] leading-[1.55]" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('automation.noRunsForThis')}
          </p>
        )}
      </div>
    </div>
  );
}
