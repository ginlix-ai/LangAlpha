import type { ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { ArrowRight } from 'lucide-react';
import { ease, useSceneTime } from './clock';
import { Loader } from './kit';
import { Excerpt, Grow, Kicker, LinkOut, PriceMeter } from './automationParts';
import { ALERT_LEVEL, BRIEF_EXCERPT, FIRED, NVDA, on, PREVIEW, useBook } from './automationBook';

/* The feed's parts at scene scale (`FeedView`, `FeedRail`): the runs of the
   account in automationBook.ts, a day at a time, and the rail beside them. */

/** One run in the feed (`RunEntry` + `RunReport`): its time, the status
    glyph and name, the meta line, the start of what it said, and its links. */
function RunEntry({
  time,
  glyph,
  name,
  meta,
  excerpt,
  actions,
}: {
  time: string;
  glyph?: ReactNode;
  name: string;
  meta: ReactNode;
  excerpt?: ReactNode;
  actions?: ReactNode;
}) {
  return (
    <div
      className="flex gap-2 border-b py-[7px]"
      style={{ borderColor: 'var(--color-border-muted)' }}
    >
      <div
        className="w-[30px] shrink-0 whitespace-nowrap pt-px font-mono text-[6px] leading-none tabular-nums"
        style={{ color: 'var(--color-text-tertiary)' }}
      >
        {time}
      </div>
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-[4px]">
          {glyph && (
            <span className="inline-flex h-[8px] w-[7px] shrink-0 items-center justify-center">
              {glyph}
            </span>
          )}
          <span
            className="-my-px truncate text-[8px] font-medium leading-[1.25]"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {name}
          </span>
        </div>
        <div
          className="mt-[3px] font-mono text-[5.5px] leading-[1.3] tabular-nums"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {meta}
        </div>
        {excerpt}
        {actions && <div className="mt-[5px] flex flex-wrap items-center gap-1">{actions}</div>}
      </div>
    </div>
  );
}

/** The run the $180 alert started: it lands at `land`, works, and at
    `done` finishes with what it found. */
export function FiredRun({ land = 0, done = 0 }: { land?: number; done?: number }) {
  const { t } = useTranslation();
  const B = useBook();
  const now = useSceneTime();
  const live = now < done;
  const place = t('agents.chiefOfStaff');
  return (
    <Grow p={ease(now, land, 450)}>
      <RunEntry
        time={B.time(10, 12)}
        glyph={
          live ? <Loader color="var(--color-accent-primary)" className="text-[9px]" /> : undefined
        }
        name={FIRED.name}
        meta={
          live
            ? [
                t('automation.runRunning'),
                place,
                t('automation.startedAgo', { when: B.ago(on(21, 10, 13)) }),
              ].join(' · ')
            : [place, B.took(138_000)].join(' · ')
        }
        excerpt={
          !live && (
            <Grow p={ease(now, done, 450)}>
              <Excerpt>{FIRED.excerpt}</Excerpt>
            </Grow>
          )
        }
        actions={<LinkOut>{t(live ? 'automation.watchLive' : 'automation.openThread')}</LinkOut>}
      />
    </Grow>
  );
}

/** A run of the brief; `delivered` adds where it was sent, from `at`. */
export function BriefRun({
  excerpt = BRIEF_EXCERPT,
  delivered,
  at = 0,
}: {
  excerpt?: string;
  delivered?: string;
  at?: number;
}) {
  const { t } = useTranslation();
  const B = useBook();
  const shown = useSceneTime() >= at;
  return (
    <RunEntry
      time={B.time(8, 0)}
      name={B.brief}
      meta={
        <>
          {[t('agents.chiefOfStaff'), B.took(252_000)].join(' · ')}
          {delivered && (
            <span style={{ opacity: shown ? 1 : 0, transition: 'opacity var(--motion-quick) var(--ease-out)' }}>
              {' · '}
              <span style={{ color: 'var(--color-text-secondary)' }}>
                {t('automation.deliveredVia', { method: delivered })}
              </span>
            </span>
          )}
        </>
      }
      excerpt={<Excerpt>{excerpt}</Excerpt>}
      actions={<LinkOut>{t('automation.openThread')}</LinkOut>}
    />
  );
}

/** The earnings preview's run this morning, done, with what it found. */
export function PreviewRun() {
  const { t } = useTranslation();
  const B = useBook();
  return (
    <RunEntry
      time={B.time(6, 30)}
      name={PREVIEW.name}
      meta={[PREVIEW.place, B.took(192_000)].join(' · ')}
      excerpt={<Excerpt>{PREVIEW.excerpt}</Excerpt>}
      actions={<LinkOut>{t('automation.openThread')}</LinkOut>}
    />
  );
}

/** A section of the rail (`FeedRail`): its kicker over a hairline. */
function RailSection({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div className="flex flex-col">
      <Kicker className="border-b pb-[4px]">{title}</Kicker>
      {children}
    </div>
  );
}

/** The rail: Up next, and what is watching. Nothing needs attention, so
    the app leaves that section out. */
export function FeedRail({ now }: { now?: number }) {
  const { t } = useTranslation();
  const B = useBook(now);
  const rowLine = { borderColor: 'var(--color-border-muted)' };
  return (
    <div className="flex min-w-0 flex-col gap-[12px]">
      <RailSection title={t('automation.upNext')}>
        <div className="grid grid-cols-[max-content_minmax(0,1fr)]">
          {B.upNext.map(([when, name]) => (
            <div
              key={name}
              className="col-span-2 grid grid-cols-subgrid gap-x-[8px] border-b py-[5px]"
              style={rowLine}
            >
              <span
                className="whitespace-nowrap font-mono text-[6px] leading-[1.4] tabular-nums"
                style={{ color: 'var(--color-text-secondary)' }}
              >
                {B.upcoming(when)}
              </span>
              <span
                className="text-[7px] leading-[1.25]"
                style={{ color: 'var(--color-text-primary)' }}
              >
                {name}
              </span>
            </div>
          ))}
        </div>
      </RailSection>
      <RailSection title={t('automation.groupWatching')}>
        <div className="border-b py-[6px]" style={rowLine}>
          <div className="flex items-baseline justify-between gap-1">
            <span
              className="truncate text-[7px] leading-[1.25]"
              style={{ color: 'var(--color-text-primary)' }}
            >
              {B.alert}
            </span>
            <span
              className="shrink-0 font-mono text-[5.5px] tabular-nums"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              {B.away}
            </span>
          </div>
          <div className="mt-[5px]">
            <PriceMeter symbol="NVDA" current={NVDA.price} threshold={ALERT_LEVEL} />
          </div>
        </div>
      </RailSection>
      <span
        className="inline-flex items-center gap-[3px] text-[6px] leading-none"
        style={{ color: 'var(--color-text-secondary)' }}
      >
        {t('automation.manageAll', { count: 4 })}
        <ArrowRight className="h-[6px] w-[6px]" />
      </span>
    </div>
  );
}

/** One day of the feed: its heading over a hairline, then its runs. */
export function Day({ label, children }: { label: string; children: ReactNode }) {
  return (
    <section className="[&+&]:mt-[10px]">
      <Kicker className="border-b pb-[4px]">{label}</Kicker>
      {children}
    </section>
  );
}

/** The feed view (`FeedView`): runs by day on the left, the rail on the
    right; the column clips at the window's edge, faded as a page runs on.
    `scroll` (px) scrolls the runs under the header, as the page's scroller
    does, while the rail stays where it is, as its sticky rail does. */
export function FeedLayout({
  runs,
  rail,
  scroll = 0,
}: {
  runs: ReactNode;
  rail: ReactNode;
  scroll?: number;
}) {
  return (
    <div className="mt-[10px] grid min-h-0 flex-1 grid-cols-[minmax(0,1fr)_124px] gap-[14px]">
      <div
        className="min-h-0 overflow-hidden"
        style={{
          WebkitMaskImage: 'linear-gradient(180deg, black 82%, transparent 100%)',
          maskImage: 'linear-gradient(180deg, black 82%, transparent 100%)',
        }}
      >
        <div style={scroll ? { transform: `translateY(${-scroll}px)` } : undefined}>{runs}</div>
      </div>
      {rail}
    </div>
  );
}
