import type { ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import {
  ArrowUpDown,
  BookMarked,
  CandlestickChart,
  CheckSquare,
  ChevronDown,
  ChevronRight,
  FilePlus,
  FileSpreadsheet,
  FileText,
  Folder,
  FolderOpen,
  Globe,
  HardDrive,
  RefreshCw,
  ScrollText,
  Search,
  Settings,
  Upload,
  type LucideIcon,
} from 'lucide-react';
import { cn } from '@/lib/utils';
import type { IntroVisualId } from '../../registry';
import { BarChart, type Bar } from './charts';
import { CLOSE, Camera, REST, shot, type Shot } from './camera';
import { PAST, ease, useSceneTime } from './clock';
import { Appear, UserBubble, pointerScript } from './kit';
import { FileCrumbs, HTML_FILE_ACTIONS, PanelTab } from './panelParts';
import { PANEL_W, ThreadPage } from './threadParts';
import { Reply, Timeline, ToolRow } from './transcriptParts';
import type { IntroScene } from './types';

/* The thread intro's first step: the files a turn writes land in the file
   panel's tree, which an empty tab lays across the panel's whole body, and a
   click there opens one in the panel. The thread page is drawn whole and the
   camera crops it to the part each beat is about. */
/* eslint-disable react-refresh/only-export-components -- scene module:
   private scene components, one record export */

/** The Analyst's turn: a line of narration, a read of the names to comp,
    three writes (each landing in the tree as it finishes), and the answer.
    Then the pointer opens the report from the tree. The empty tab takes the
    file, so Back has a step to go to, and the tree folds away over `fold`
    ms: a panel this narrow shows the tree as a sheet over the file rather
    than a column beside it, and a file opened from it closes the sheet. The
    report shows in the HTML viewer. The tree sorts as the app's does: root
    files first, then results/ and data/, names A to Z within. Positions are
    px on the thread page: the report's row sits at y 188 in the tree, its
    name from x 211. */
const FILES = {
  ask: 'Comp NVDA against AI semis',
  lead: 'Pulling NTM estimates and prices for the nine names.',
  leadAt: 250,
  read: { name: 'universe.csv', at: 650, done: 1050 },
  writes: [
    { name: 'prices.csv', at: 1150 },
    { name: 'comps.xlsx', at: 1700 },
    { name: 'report.html', at: 2250 },
  ],
  write: 450,
  answer:
    'Done. NVDA trades at **24.1x** NTM sales against a 9.8x median. ' +
    'The write-up is in `results/report.html`.',
  answerAt: 2800,
  click: 5200,
  open: 5280,
  fold: 200,
  bars: 5900,
  length: 8600,
};

/** Onto the tree, to the report's row and a click on it, then off the
    page's foot. */
const FILES_POINTER = pointerScript(
  { at: 4200, x: 262, y: 248 },
  {
    report: { x: 236, y: 188, arrive: 5000, click: FILES.click, leave: 5400 },
    off: { x: 300, y: 360, arrive: 6000 },
  }
);

/** The camera: on the chat column whole, from its header to the answer,
    with the panel's empty tab beside it, as the turn runs and the answer
    names the report; across to the panel as the pointer goes to the tree
    the files landed in, held there as the report opens in its place; then
    in on the report under its header, its note and figure, as the bars
    draw. */
const FILE_SHOTS: [Shot, ...Shot[]] = [
  shot(0, 160, 132, REST),
  shot(4500, 240, 132, REST, 900),
  shot(7700, 267, 138, CLOSE, 1000),
];

/** EV / NTM sales across the nine names in comps.xlsx, high to low. */
const EV_SALES: Bar[] = [
  { label: 'ARM', value: 30.2, shown: '30.2x' },
  { label: 'NVDA', value: 24.1, shown: '24.1x', mark: true },
  { label: 'AVGO', value: 22.6, shown: '22.6x' },
  { label: 'TSM', value: 10.4, shown: '10.4x' },
  { label: 'AMD', value: 9.8, shown: '9.8x' },
  { label: 'MRVL', value: 9.1, shown: '9.1x' },
  { label: 'QCOM', value: 4.3, shown: '4.3x' },
  { label: 'MU', value: 4.2, shown: '4.2x' },
  { label: 'INTC', value: 2.9, shown: '2.9x' },
];

/** The tree's toolbar (`TreeColumn`): select, upload, back up, refresh and
    sort, then the System chip and the workspace's settings. */
const TREE_TOOLS: LucideIcon[] = [CheckSquare, Upload, HardDrive, RefreshCw, ArrowUpDown];

/** A button in the tree's toolbar, a size down from the strip's. */
function TreeTool({ icon: Icon }: { icon: LucideIcon }) {
  return (
    <span className="flex h-[15px] w-[15px] shrink-0 items-center justify-center">
      <Icon className="h-[9px] w-[9px]" style={{ color: 'var(--color-text-tertiary)' }} />
    </span>
  );
}

/** A chip in the tree's chrome (`file-panel-chip`): the System toggle, or a
    type filter, the one applied on the hover fill. */
function Chip({ active = false, children }: { active?: boolean; children: ReactNode }) {
  return (
    <span
      className="whitespace-nowrap rounded-full border px-[4.5px] py-[1.5px] text-[6px] font-medium leading-none"
      style={
        active
          ? {
              background: 'var(--color-bg-hover)',
              borderColor: 'var(--color-border-default)',
              color: 'var(--color-text-primary)',
            }
          : { borderColor: 'var(--color-border-muted)', color: 'var(--color-text-tertiary)' }
      }
    >
      {children}
    </span>
  );
}

/** A row pinned above the listing (`file-panel-store-row`): Memory, Memo and
    a chart open a tab of their own, as a file does. */
function StoreRow({ icon: Icon, name }: { icon: LucideIcon; name: string }) {
  return (
    <div className="flex h-[15px] min-w-0 items-center gap-[4.5px] px-[5.5px]">
      <Icon className="h-2 w-2 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
      <span
        className="truncate text-[7px] leading-none"
        style={{ color: 'var(--color-text-primary)' }}
      >
        {name}
      </span>
    </div>
  );
}

/**
 * The tree (`TreeColumn`) as an empty tab shows it: the listing is the tab,
 * laid across the panel's whole body. Its toolbar, the search field, the type
 * chips once the files are of more than one kind (`types`, which the app
 * leaves in English), Memory, Memo and Chart pinned above the listing, then
 * the listing. `width` is the column's while it folds away; its content keeps
 * the docked column's width and is clipped, as the app folds it.
 */
function TreeColumn({
  types,
  width,
  children,
}: {
  types: string[];
  width?: number;
  children: ReactNode;
}) {
  const { t } = useTranslation();
  return (
    <div
      className={cn('flex flex-col overflow-hidden', width == null && 'flex-1')}
      style={{ width, background: 'var(--color-bg-page)' }}
    >
      <div className="flex min-w-[148px] flex-1 flex-col">
        <div className="flex items-center gap-px px-[3.5px] pb-[2.5px] pt-[3.5px]">
          {TREE_TOOLS.map((icon, i) => (
            <TreeTool key={i} icon={icon} />
          ))}
          <span className="ml-auto" />
          <Chip>{t('filePanel.systemDirs')}</Chip>
          <TreeTool icon={Settings} />
        </div>
        <div className="px-[4.5px] pb-[3.5px]">
          <div
            className="flex h-[18px] items-center gap-[3.5px] rounded-[3.5px] border px-[4.5px]"
            style={{
              background: 'var(--color-bg-input)',
              borderColor: 'var(--color-border-muted)',
            }}
          >
            <Search className="h-2 w-2 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
            <span
              className="truncate text-[7px] leading-none"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              {t('filePanel.searchFiles')}
            </span>
          </div>
        </div>
        {types.length > 1 && (
          <div className="flex flex-wrap items-center gap-[2.5px] px-[4.5px] pb-[3.5px]">
            <Chip active>{t('filePanel.filterAll')}</Chip>
            {types.map((type) => (
              <Chip key={type}>{type}</Chip>
            ))}
          </div>
        )}
        <div
          className="border-b pb-[2.5px] pt-px"
          style={{ borderColor: 'var(--color-border-muted)' }}
        >
          <StoreRow icon={BookMarked} name={t('filePanel.tabs.memory')} />
          <StoreRow icon={ScrollText} name={t('filePanel.tabs.memo')} />
          <StoreRow icon={CandlestickChart} name={t('filePanel.chartTab')} />
        </div>
        <div className="flex flex-col">{children}</div>
      </div>
    </div>
  );
}

/** A file in the tree, listed from `at`: its kind's glyph (as `fileGlyph`
    picks it), then the name. A file in a folder is indented a step, and
    carries the folder's guide line while the pointer is over the tree
    (`guide`); `hover` is the row under the pointer. */
function TreeFile({
  icon: Icon = FileText,
  name,
  nested = false,
  at = 0,
  guide = false,
  hover = false,
}: {
  icon?: LucideIcon;
  name: string;
  nested?: boolean;
  at?: number;
  guide?: boolean;
  hover?: boolean;
}) {
  return (
    <Appear
      at={at}
      className="relative flex h-[15px] min-w-0 items-center gap-[4.5px] pr-[5.5px]"
      style={{
        paddingLeft: nested ? 10 : 5.5,
        background: hover ? 'var(--color-border-muted)' : undefined,
      }}
    >
      {nested && guide && (
        <span
          className="absolute bottom-0 left-[9px] top-0 w-px"
          style={{ background: 'var(--color-border-muted)' }}
        />
      )}
      <Icon className="h-2 w-2 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
      <span
        className="truncate text-[7px] leading-none"
        style={{ color: 'var(--color-text-primary)' }}
      >
        {name}
      </span>
    </Appear>
  );
}

/** A folder in the tree: chevron, folder, `name/` and how many files it
    holds. The tree is built from the files, so a folder is listed from its
    first file (`at`), open from `openAt`. */
function TreeFolder({
  name,
  count,
  at = 0,
  openAt = at,
}: {
  name: string;
  count: number;
  at?: number;
  openAt?: number;
}) {
  const t = useSceneTime();
  const Chevron = t >= openAt ? ChevronDown : ChevronRight;
  return (
    <Appear at={at} className="flex h-4 min-w-0 items-center gap-[3.5px] px-[9px]">
      <Chevron className="h-2 w-2 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
      <Folder className="h-2 w-2 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
      <span
        className="truncate text-[7px] font-medium leading-none"
        style={{ color: 'var(--color-text-tertiary)' }}
      >
        {name}/
      </span>
      <span className="text-[7px] leading-none" style={{ color: 'var(--color-icon-muted)' }}>
        {count}
      </span>
    </Appear>
  );
}

/** The report the turn wrote, open in the HTML viewer: the page as the
    agent laid it out, a titled note with its figure, the subject's bar in
    the accent. The file's header above it is the panel's, not the
    viewer's. */
function ReportView() {
  return (
    <Appear at={FILES.open} className="flex min-h-0 flex-1 flex-col">
      <div className="flex flex-1 flex-col px-3 pb-2 pt-2.5">
        <span
          className="text-[5px] font-medium uppercase leading-none tracking-[0.08em]"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          AI semis · Oct 21, 2026
        </span>
        <span
          className="mt-1.5 font-serif text-[11px] font-semibold leading-[1.15]"
          style={{ color: 'var(--color-text-primary)' }}
        >
          Where NVDA sits in the AI semis
        </span>
        <p
          className="mt-1.5 text-[7px] leading-[1.55]"
          style={{ color: 'var(--color-text-secondary)' }}
        >
          At $182.42, NVDA trades at 24.1x NTM sales, well above the 9.8x median of the nine names.
          Only ARM screens richer, on a far smaller revenue base.
        </p>
        <span
          className="mt-2.5 text-[6px] font-semibold leading-none"
          style={{ color: 'var(--color-accent-primary)' }}
        >
          Figure 1.{' '}
          <span className="font-normal" style={{ color: 'var(--color-text-secondary)' }}>
            EV / NTM sales, dashed line at the median
          </span>
        </span>
        <BarChart
          bars={EV_SALES}
          max={32}
          median={9.8}
          at={FILES.bars}
          labelWidth={20}
          surface="var(--color-bg-page)"
          className="mt-1.5"
        />
        <span
          className="mt-2 text-[5px] leading-none"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          Source: comps.xlsx, NTM consensus as of Oct 21
        </span>
      </div>
    </Appear>
  );
}

/** The panel's body, its inner width. */
const BODY_W = PANEL_W - 1;

function FilePanel() {
  const t = useSceneTime();
  const { t: tr } = useTranslation();
  const [prices, comps, report] = FILES.writes.map((w) => w.at + FILES.write);
  const opened = t >= FILES.open;
  const fold = ease(t, FILES.open, FILES.fold);
  /* the pointer over the listing shows its guide lines, as hovering the tree does */
  const over = t >= FILES_POINTER.enter && !opened;
  const hover = t >= FILES_POINTER.beats.report.arrive && !opened;
  const types = t >= report ? ['Docs', 'Code', 'Data'] : ['Docs', 'Data'];
  return (
    <Camera shots={FILE_SHOTS} pointer={FILES_POINTER}>
      <ThreadPage
        title="Semis comps"
        tabs={
          opened ? (
            <PanelTab icon={Globe} label="report.html" active onLoan />
          ) : (
            <PanelTab icon={FolderOpen} label={tr('filePanel.openFile')} active />
          )
        }
        back={opened}
        tree={opened ? false : undefined}
        chat={
          <>
            <UserBubble at={PAST}>{FILES.ask}</UserBubble>
            <Reply text={FILES.lead} at={FILES.leadAt} cps={110} />
            <Timeline>
              <ToolRow
                icon={FileText}
                running={{ label: tr('toolArtifact.running.read'), pill: FILES.read.name }}
                settled={{ label: tr('toolArtifact.tool.read'), pill: FILES.read.name }}
                at={FILES.read.at}
                done={FILES.read.done}
              />
              {FILES.writes.map((w) => (
                <ToolRow
                  key={w.name}
                  icon={FilePlus}
                  running={{ label: tr('toolArtifact.running.write'), pill: w.name }}
                  settled={{ label: tr('toolArtifact.tool.write'), pill: w.name }}
                  at={w.at}
                  done={w.at + FILES.write}
                />
              ))}
            </Timeline>
            <Reply text={FILES.answer} at={FILES.answerAt} cps={140} />
          </>
        }
        panel={
          <>
            {opened && (
              <FileCrumbs
                path={['results', 'report.html']}
                meta={tr('filePanel.metaLines', { count: 64 })}
                actions={HTML_FILE_ACTIONS}
              />
            )}
            <div className="flex min-h-0 flex-1">
              {opened && <ReportView />}
              {fold < 1 && (
                <TreeColumn types={types} width={opened ? BODY_W * (1 - fold) : undefined}>
                  <TreeFile name="notes.md" />
                  <TreeFolder name="results" count={t >= report ? 2 : 1} at={comps} />
                  <TreeFile
                    nested
                    icon={FileSpreadsheet}
                    name="comps.xlsx"
                    at={comps}
                    guide={over}
                  />
                  <TreeFile
                    nested
                    icon={Globe}
                    name="report.html"
                    at={report}
                    guide={over}
                    hover={hover}
                  />
                  <TreeFolder name="data" count={t >= prices ? 2 : 1} openAt={prices} />
                  <TreeFile
                    nested
                    icon={FileSpreadsheet}
                    name="prices.csv"
                    at={prices}
                    guide={over}
                  />
                  <TreeFile
                    nested
                    icon={FileSpreadsheet}
                    name="universe.csv"
                    at={prices}
                    guide={over}
                  />
                </TreeColumn>
              )}
            </div>
          </>
        }
      />
    </Camera>
  );
}

export const threadFileScenes = {
  filePanel: { Scene: FilePanel, length: FILES.length, anchor: 'right' },
} satisfies Partial<Record<IntroVisualId, IntroScene>>;
