import type { ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import {
  ArrowDownUp,
  ArrowUpRight,
  ChevronDown,
  ChevronRight,
  Cloud,
  Info,
  Pin,
  Plus,
  Search,
  Server,
} from 'lucide-react';
import { HomeRowIcon } from '@/hooks/useHomeWorkspace';
import { useLocale } from '@/hooks/useLocale';
import { relativeTime } from '@/lib/format';
import { cn } from '@/lib/utils';
import { providerLabel } from '@/pages/ChatAgent/components/providerUi';
import { COMPUTER_STATUS_UI } from '@/pages/ChatAgent/utils/computerStatus';
import { VIEW } from './camera';
import { Appear, rv } from './kit';
import type { Icon } from './chatParts';

type Translate = ReturnType<typeof useTranslation>['t'];

/* The workspace gallery as the scenes draw it (`WorkspaceGallery`): its
   header, toolbar, Chief of Staff card and workspace cards, and the two
   computers the cards and the computer menu name. Each part is a small copy
   of the component the app draws it with (named on each), in the same
   tokens and labels. */
/* eslint-disable react-refresh/only-export-components -- scene parts: small
   components plus their data */

/** A computer's status, the few the scenes show. */
type MachineState = 'stopped' | 'running';

/** A machine's state glyph as `ComputerStatusIndicator` draws it, from the
    app's own table (`COMPUTER_STATUS_UI`): a hollow ring or a filled dot in
    the state's colour. */
export function StatusGlyph({ state }: { state: MachineState }) {
  const { shape, color } = COMPUTER_STATUS_UI[state];
  return (
    <span
      className="h-[4px] w-[4px] shrink-0 rounded-full"
      style={shape === 'dot' ? { background: color } : { border: `1px solid ${color}` }}
    />
  );
}

/** The gallery's two computers: the primary, which Home is on, and a second
    one holding one workspace. `tier` is the size the badge names. */
export const MACHINES = {
  primary: { name: 'Cloud computer', state: 'running', tier: 'standard' },
  second: { name: 'Research box', state: 'stopped', tier: 'max' },
} as const;
export type MachineId = keyof typeof MACHINES;

/** A hosted machine's badge words (`ComputerBadge`): its provider, then its size. */
export function badgeWords(t: Translate, id: MachineId): string[] {
  return [providerLabel('daytona', t), t(`workspace.tier.${MACHINES[id].tier}`)];
}

/** The badge as a pill (`ComputerBadge`), the way the computer menu sets it
    beside a name: the kind's glyph, then its words. */
export function BadgePill({ icon: Glyph, words }: { icon: Icon; words: string[] }) {
  return (
    <span
      className="flex shrink-0 items-center gap-[2px] rounded-full px-[3px] py-[1.5px] text-[5px] font-medium leading-none"
      style={{ background: 'var(--color-border-muted)', color: 'var(--color-text-secondary)' }}
    >
      <Glyph className="h-[5px] w-[5px] shrink-0" />
      {words.join(' · ')}
    </span>
  );
}

/** The gallery's header (`WorkspaceGallery`, `GalleryActions`): the title,
    then New workspace. `pressNew` presses New. */
function GalleryHeader({ pressNew }: { pressNew?: string }) {
  const { t } = useTranslation();
  return (
    <div className="intro-rv flex items-center" style={rv(0.08)}>
      <span
        className="title-font mr-auto text-[12px] font-semibold leading-none"
        style={{ color: 'var(--color-text-primary)' }}
      >
        {t('workspace.workspaces')}
      </span>
      <span
        className={cn(
          'flex h-[15px] shrink-0 items-center gap-[3px] rounded-[5px] px-[7px]',
          'transition-transform duration-press'
        )}
        style={{
          background: 'var(--color-btn-primary-bg)',
          color: 'var(--color-btn-primary-text)',
          transform: pressNew,
        }}
      >
        <Plus className="h-[7px] w-[7px]" />
        <span className="text-[6.5px] font-medium leading-none">{t('workspace.newWorkspace')}</span>
      </span>
    </div>
  );
}

/** A toolbar control, as `GalleryToolbar` draws them: outlined, in the
    secondary text colour, a quiet chevron at the end. */
function ToolbarControl({
  icon: Glyph,
  pressed,
  children,
}: {
  icon: Icon;
  pressed?: string;
  children: ReactNode;
}) {
  return (
    <span
      className={cn(
        'flex h-[15px] shrink-0 items-center gap-1 rounded-[5px] border px-[5px]',
        'transition-transform duration-press'
      )}
      style={{
        borderColor: 'var(--color-border-muted)',
        color: 'var(--color-text-secondary)',
        transform: pressed,
      }}
    >
      <Glyph className="h-[7px] w-[7px] shrink-0" />
      <span>{children}</span>
      <ChevronDown className="h-[6px] w-[6px] shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
    </span>
  );
}

/**
 * The toolbar (`GalleryToolbar`): search, then the computer filter, which
 * the gallery shows once there are two computers, and the sort.
 * `pressFilter` presses the filter.
 */
function GalleryToolbar({ pressFilter }: { pressFilter?: string }) {
  const { t } = useTranslation();
  return (
    <div className="intro-rv flex items-center gap-[5px] text-[6.5px] leading-none" style={rv(0.16)}>
      <span
        className="flex h-[15px] min-w-0 max-w-[180px] flex-1 items-center gap-1 rounded-[5px] border px-[6px]"
        style={{ background: 'var(--color-bg-input)', borderColor: 'var(--color-border-muted)' }}
      >
        <Search className="h-[7px] w-[7px] shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
        <span className="truncate" style={{ color: 'var(--color-text-tertiary)' }}>
          {t('workspace.searchWorkspaces')}
        </span>
      </span>
      <ToolbarControl icon={Server} pressed={pressFilter}>
        {t('workspace.allComputers')}
      </ToolbarControl>
      <ToolbarControl icon={ArrowDownUp}>{t('workspace.sortRecent')}</ToolbarControl>
    </div>
  );
}

/** The gallery's small mono label over a section (`Kicker`). */
function Kicker({ children, className }: { children: ReactNode; className?: string }) {
  return (
    <div
      className={cn(
        'font-mono text-[5px] font-medium uppercase leading-none tracking-[0.06em] tabular-nums',
        className
      )}
      style={{ color: 'var(--color-text-tertiary)' }}
    >
      {children}
    </div>
  );
}

const STARTERS = ['workspace.cosStarter1', 'workspace.cosStarter2', 'workspace.cosStarter3'] as const;

/**
 * The Chief of Staff's card over the grid (`ChiefOfStaffCard`): its glyph
 * and name, what it does, New thread and the primary computer it works on
 * (the compact `MachineSummary`, then the hint's info mark); on the right,
 * the questions it offers before it has threads of its own.
 */
function ChiefOfStaffCard() {
  const { t } = useTranslation();
  return (
    <div
      className="grid grid-cols-[1fr_1.15fr] gap-3 rounded-[7px] border px-[9px] py-2"
      style={{ background: 'var(--color-bg-card)', borderColor: 'var(--color-border-default)' }}
    >
      <div className="flex min-w-0 flex-col">
        <div className="flex items-center gap-[5px]">
          <span
            className="grid h-4 w-4 shrink-0 place-items-center rounded-[4px] border"
            style={{
              background: 'var(--color-bg-elevated)',
              borderColor: 'var(--color-border-default)',
            }}
          >
            <HomeRowIcon className="h-2 w-2" style={{ color: 'var(--color-accent-primary)' }} />
          </span>
          <span
            className="title-font text-[8px] font-semibold leading-none"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {t('agents.chiefOfStaff')}
          </span>
        </div>
        <span
          className="mt-[5px] text-[6px] leading-[1.5]"
          style={{ color: 'var(--color-text-secondary)' }}
        >
          {t('agents.widgets.chiefOfStaffDesc')}
        </span>
        <div className="mt-auto flex flex-wrap items-center gap-x-2 gap-y-[5px] pt-2">
          <span
            className="flex h-[13px] items-center gap-[3px] rounded-[5px] px-[6px] text-[6px] font-medium leading-none"
            style={{
              background: 'var(--color-btn-primary-bg)',
              color: 'var(--color-btn-primary-text)',
            }}
          >
            <Plus className="h-[6px] w-[6px]" />
            {t('nav.newThread')}
          </span>
          <span
            className="flex min-w-0 items-center gap-[3px] text-[5.5px] leading-none"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            <span>{t('workspace.cosWorksOn')}</span>
            <StatusGlyph state={MACHINES.primary.state} />
            <Cloud className="h-[6px] w-[6px] shrink-0" />
            <span style={{ color: 'var(--color-text-secondary)' }}>{MACHINES.primary.name}</span>
            <Info className="h-[5px] w-[5px] shrink-0" />
          </span>
        </div>
      </div>
      <div className="flex min-w-0 flex-col">
        <Kicker className="mb-[3px] mt-[3px]">{t('workspace.cosTryAsking')}</Kicker>
        <div className="flex flex-1 flex-col divide-y divide-(--color-border-muted)">
          {STARTERS.map((key) => (
            <span
              key={key}
              className="flex flex-1 items-center gap-1 py-[3px] text-[6px] leading-[1.35]"
              style={{ color: 'var(--color-text-primary)' }}
            >
              <ChevronRight
                className="h-[6px] w-[6px] shrink-0"
                style={{ color: 'var(--color-text-tertiary)' }}
              />
              <span className="min-w-0 flex-1">{t(key)}</span>
              <ArrowUpRight
                className="h-[6px] w-[6px] shrink-0"
                style={{ color: 'var(--color-text-tertiary)' }}
              />
            </span>
          ))}
        </div>
      </div>
    </div>
  );
}

/** One workspace in the gallery: its name, what it is for, how long ago it
    was used, and the computer it lives on when that is not the usual one. */
interface Desk {
  name: string;
  description: string;
  ago: number;
  pinned?: boolean;
  machine?: MachineId;
}

/**
 * A card in the grid (`WorkspaceCard`): the pin, the name and how long ago
 * it was used, then the description held at two lines, and on a card that
 * lives off the usual computer, that computer (`MachineSummary`): its state,
 * kind, name, badge words, and its state's word when it is not running.
 */
function WorkspaceCard({ desk }: { desk: Desk }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const machine = desk.machine && { ...MACHINES[desk.machine], id: desk.machine };
  return (
    <div
      className="flex min-w-0 flex-1 flex-col gap-1 rounded-[7px] border px-2 py-[7px]"
      style={{
        background: 'var(--color-bg-card-gradient)',
        borderColor: 'var(--color-bg-card-border)',
      }}
    >
      <div className="flex min-w-0 items-center gap-[3px]">
        {desk.pinned && (
          <Pin
            className="h-[6px] w-[6px] shrink-0 rotate-45"
            style={{ color: 'var(--color-text-tertiary)' }}
          />
        )}
        <span
          className="min-w-0 flex-1 truncate text-[7.5px] font-medium leading-none"
          style={{ color: 'var(--color-text-primary)' }}
        >
          {desk.name}
        </span>
        <span className="shrink-0 text-[5.5px] leading-none" style={{ color: 'var(--color-text-tertiary)' }}>
          {relativeTime(SCENE_NOW - desk.ago, locale, SCENE_NOW)}
        </span>
      </div>
      <span
        className="line-clamp-2 min-h-[2lh] text-[6px] leading-[1.4]"
        style={{ color: 'var(--color-text-tertiary)' }}
      >
        {desk.description}
      </span>
      {machine && (
        <span
          className="mt-auto flex min-w-0 items-center gap-[3px] pt-[3px] text-[5.5px] leading-none"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          <StatusGlyph state={machine.state} />
          <Cloud className="h-[6px] w-[6px] shrink-0" />
          <span className="truncate" style={{ color: 'var(--color-text-secondary)' }}>
            {machine.name}
          </span>
          {badgeWords(t, machine.id).map((word) => (
            <span key={word} className="shrink-0">
              · {word}
            </span>
          ))}
          {machine.state !== 'running' && (
            <span className="shrink-0">· {t(COMPUTER_STATUS_UI[machine.state].labelKey)}</span>
          )}
        </span>
      )}
    </div>
  );
}

const HOUR = 3_600_000;
const DAY = 24 * HOUR;
const SCENE_NOW = new Date(2026, 9, 21, 16).getTime();

/** The gallery's workspaces in the Recent order: the pinned card, then the
    most recently used. One lives on the second computer. */
const DESKS: Desk[] = [
  {
    name: 'NVDA deep dive',
    description: 'Q3 print vs guidance, data-center demand and a reverse DCF.',
    ago: 9 * HOUR,
    pinned: true,
  },
  {
    name: 'Portfolio monitoring',
    description: 'Daily risk check on current holdings and earnings dates.',
    ago: DAY,
  },
  {
    name: 'AI semis comps',
    description: 'Nine-name comps table on NTM multiples, refreshed weekly.',
    ago: 3 * DAY,
    machine: 'second',
  },
  {
    name: 'Fed path and rates',
    description: 'Rate-path pricing against the dot plot, after each print.',
    ago: 6 * DAY,
  },
  {
    name: 'TSMC supply chain',
    description: 'Supplier map and capex read-through for 2027.',
    ago: 12 * DAY,
  },
  {
    name: 'Oil and OPEC+ supply',
    description: 'Quota compliance, inventories and the 2027 supply balance.',
    ago: 16 * DAY,
  },
];

/**
 * The gallery page (`WorkspaceGallery`) at the view's size: the header, the
 * toolbar, the Chief of Staff's card, then the workspaces under their
 * kicker, two to a row. The Chief of Staff's card and each workspace card
 * come in at their time in `cards`; without them the gallery is there from
 * the start. `pressNew` presses New workspace, `pressFilter` the computer
 * filter.
 */
export function GalleryPage({
  cards,
  pressNew,
  pressFilter,
}: {
  cards?: number[];
  pressNew?: string;
  pressFilter?: string;
}) {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col overflow-hidden px-2.5 pt-2.5" style={VIEW}>
      <GalleryHeader pressNew={pressNew} />
      <div className="mt-[7px]">
        <GalleryToolbar pressFilter={pressFilter} />
      </div>
      <Appear at={cards?.[0] ?? 0} reserve className="mt-[7px]">
        <ChiefOfStaffCard />
      </Appear>
      <Appear at={cards?.[1] ?? 0} reserve>
        <Kicker className="mb-[5px] mt-[11px]">{`${t('workspace.workspaces')} · ${DESKS.length}`}</Kicker>
      </Appear>
      <div className="grid auto-rows-fr grid-cols-2 gap-1.5">
        {DESKS.map((desk, i) => (
          <Appear key={desk.name} at={cards?.[i + 1] ?? 0} reserve className="flex min-w-0">
            <WorkspaceCard desk={desk} />
          </Appear>
        ))}
      </div>
    </div>
  );
}
