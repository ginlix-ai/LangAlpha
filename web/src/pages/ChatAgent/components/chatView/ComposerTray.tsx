import { useRef, type ReactNode, type Ref, type RefObject } from 'react';
import { useThreadStatus, type ThreadStatus } from './threadStatus';
import { LiquidRow, TrayLiquid } from './trayLiquid';
import { MOVE_ORDER, TRAY_DENSITIES, type MeasuredId, type TrayLayout } from './trayDensity';
import { chipFace, openStatusFace, useFaceContext, type ChipFace, type FaceContext } from './trayFaces';
import { OpenStatusButton, TrayChip } from './trayChips';
import type { LiquidPhase, TrayFoldPhase } from './useTrayFold';
import { trayEntries, type TrayContents, type TrayEntry } from './useTrayContents';
import { useTrayDensity } from './useTrayDensity';

interface ComposerTrayProps {
  phase: TrayFoldPhase;
  /** The tray's own row, which the fold measures. */
  trayRef: RefObject<HTMLDivElement | null>;
  /** The dock in the composer's toolbar that the chips fold into. */
  dockRef: RefObject<HTMLElement | null>;
  /** The liquid landed (`useTrayFold`'s `settle`). */
  onSettled: (ran: LiquidPhase) => void;
}

/**
 * The tray's chips, above the composer (`trayChips`). The tray stays while a
 * turn runs: what it lists outlives the turn, and work handed off stays a
 * few turns after it went quiet.
 *
 * While the reader is up in the history the chips fold into the composer, and
 * they run there as liquid (`TrayLiquid`).
 */
export function ComposerTray({ phase, trayRef, dockRef, onSettled }: ComposerTrayProps) {
  const chipsRef = useRef<HTMLDivElement>(null);
  const run: LiquidPhase | null = phase === 'folding' || phase === 'unfolding' ? phase : null;
  return (
    // The liquid runs from the chips on screen and to them, so their layout
    // holds while it does.
    <TrayLine rowRef={chipsRef} shown={phase !== 'folded'} hold={!!run}>
      {({ layout, shapes, chips, probe }) => (
        // Inset from the composer's edges, so the row starts inside its rounded corner.
        <div ref={trayRef} className="relative px-4">
          {/* Keyed by the layout: the unfold's first frame may settle the
              layout after the liquid measured it (before paint), and the
              liquid then measures again. The layout holds after that. */}
          {run && (
            <TrayLiquid key={layoutKey(layout)} phase={run} trayRef={trayRef} chipsRef={chipsRef} dockRef={dockRef} onSettled={onSettled} />
          )}
          {/* One line always (`useTrayDensity`); never clipped, so a focus
              ring or a chip past a very narrow edge still shows. */}
          <LiquidRow ref={chipsRef} phase={phase} shapes={shapes} className="flex items-center gap-1.5" {...layoutMark(layout)}>
            {chips}
          </LiquidRow>
          {probe}
        </div>
      )}
    </TrayLine>
  );
}

/**
 * The tray as the composer's own top strip, for a reader who keeps it there
 * (`other_preference.composer_tray: 'inside'`). It is inside the composer's
 * surface, so nothing scrolls under it and it has nothing to fold into.
 */
export function ComposerTrayStrip() {
  const rowRef = useRef<HTMLDivElement>(null);
  return (
    <TrayLine rowRef={rowRef} shown hold={false}>
      {({ layout, chips, probe }) => (
        <div data-strip="" className="group relative border-b border-(--color-border-muted) px-2 py-1">
          <div ref={rowRef} {...layoutMark(layout)} className="flex items-center gap-x-1">
            {chips}
          </div>
          {probe}
        </div>
      )}
    </TrayLine>
  );
}

/** What a tray draws its line from. */
interface LineParts {
  layout: TrayLayout;
  /** The row's chips and the Status button, for inside the row `rowRef` names. */
  chips: ReactNode;
  /** How many things the row draws: its chips and the Status button. */
  shapes: number;
  /** The unseen probe, for after the row so the first button in the tray is still a chip. */
  probe: ReactNode;
}

/**
 * One tray's line, wherever it sits: the chips the layout keeps in the row,
 * the probe that measures them, and the layout that keeps them to one row.
 * Draws nothing while the thread has nothing to show, and keeps its layout
 * through a fold, when the line is not `shown`.
 */
function TrayLine({ rowRef, shown, hold, children }: {
  rowRef: RefObject<HTMLDivElement | null>;
  shown: boolean;
  hold: boolean;
  children: (parts: LineParts) => ReactNode;
}) {
  const probeRef = useRef<HTMLDivElement>(null);
  const status = useThreadStatus();
  const contents = status?.contents ?? null;
  const layout = useTrayDensity(rowRef, probeRef, { shown: shown && !!contents, hold });
  if (!shown || !status || !contents) return null;
  return <ShownLine status={status} contents={contents} layout={layout} probeRef={probeRef} draw={children} />;
}

/** A line on screen, its faces read once for the row and the probe alike. */
function ShownLine({ status, contents, layout, probeRef, draw }: {
  status: ThreadStatus;
  contents: TrayContents;
  layout: TrayLayout;
  probeRef: Ref<HTMLDivElement>;
  draw: (parts: LineParts) => ReactNode;
}) {
  const ctx = useFaceContext();
  const entries = trayEntries(contents);
  const row = entries.filter(({ id }) => !layout.moved.includes(id));
  return draw({
    layout,
    chips: <TrayRow row={row} moved={entries.length - row.length} layout={layout} status={status} ctx={ctx} />,
    shapes: row.length + 1,
    probe: <TrayProbe ref={probeRef} entries={entries} ctx={ctx} />,
  });
}

function layoutKey({ density, moved }: TrayLayout): string {
  return [density, ...moved].join(':');
}

/** The layout in force, on the row for the dev preview's sweep (`__preview__/tray.tsx`) to read; not in a build. */
function layoutMark(layout: TrayLayout): { 'data-tray-layout'?: string } {
  return import.meta.env.DEV ? { 'data-tray-layout': layoutKey(layout) } : {};
}

/** The chips the layout keeps in the row, then the way to the Status panel, counting the rest. */
function TrayRow({ row, moved, layout, status, ctx }: {
  row: TrayEntry[];
  moved: number;
  layout: TrayLayout;
  status: ThreadStatus;
  ctx: FaceContext;
}) {
  return (
    <>
      {row.map((entry) => <TrayChip key={entry.id} entry={entry} density={layout.density} status={status} ctx={ctx} />)}
      <OpenStatusButton more={moved} ctx={ctx} onOpen={() => status.onOpenStatus(null)} />
    </>
  );
}

/**
 * The chips' faces at every density, laid out unseen at their natural widths
 * for `useTrayDensity` to measure, and the Status button counting moved
 * chips. Plain spans in the chips' own classes, so they measure as the row
 * draws them and mount no control. Clipped to nothing, so a wide row adds no
 * scroll to the page; inert and hidden from assistive tech, so only the row
 * is the tray.
 */
function TrayProbe({ ref, entries, ctx }: { ref: Ref<HTMLDivElement>; entries: TrayEntry[]; ctx: FaceContext }) {
  const measured = (id: MeasuredId, face: ChipFace) => (
    <div key={id} data-chip={id} className="shrink-0">
      <span className={face.className}>{face.content}</span>
    </div>
  );
  return (
    <div ref={ref} aria-hidden="true" inert className="pointer-events-none invisible absolute inset-x-0 top-0 h-0 overflow-hidden">
      {TRAY_DENSITIES.map((density) => (
        <div key={density} data-level={density} className="flex w-max">
          {entries.map((entry) => measured(entry.id, chipFace(entry.id, entry.value, density, ctx)))}
          {measured('status', openStatusFace(0, ctx))}
        </div>
      ))}
      <div data-level="more" className="flex w-max">
        {measured('status', openStatusFace(MOVE_ORDER.length, ctx))}
      </div>
    </div>
  );
}
