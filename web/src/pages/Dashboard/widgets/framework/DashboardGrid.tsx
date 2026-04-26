import { Component as ReactComponent, Suspense, useCallback, useEffect, useMemo, useRef, useState, type ErrorInfo, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import i18n from '@/i18n';
import {
  ResponsiveGridLayout,
  useContainerWidth,
  type Layout,
  type LayoutItem,
  type ResponsiveLayouts,
} from 'react-grid-layout';
import 'react-grid-layout/css/styles.css';
import { WidgetFrame } from './WidgetFrame';
import { getWidget } from './WidgetRegistry';
import { reconcileLayouts } from './reconcile';
import { newWidgetId } from './ids';
import {
  BREAKPOINTS_PX,
  BREAKPOINT_KEYS,
  COLS_PER_BP,
  FIT_PADDING_PX,
  MARGIN_X,
  MARGIN_Y,
  ROW_HEIGHT,
  clamp,
} from './gridConstants';
import type { BreakpointKey, DashboardPrefs, WidgetInstance, RGLItem } from '../types';

function pxToRows(px: number): number {
  return Math.max(1, Math.ceil((px + MARGIN_Y) / (ROW_HEIGHT + MARGIN_Y)));
}

function WidgetFallback() {
  const { t } = useTranslation();
  return (
    <div
      className="h-full w-full flex items-center justify-center text-[11px]"
      style={{ color: 'var(--color-text-tertiary)' }}
    >
      {t('dashboard.widgets.common.loading')}
    </div>
  );
}

/** Isolates one widget's render failure so a bug in, say, ChartWidget can't
 *  blank out every other widget on the dashboard. */
class WidgetErrorBoundary extends ReactComponent<
  { widgetType: string; children: ReactNode },
  { error: Error | null }
> {
  state = { error: null as Error | null };
  static getDerivedStateFromError(error: Error) {
    return { error };
  }
  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error(`[widget:${this.props.widgetType}] render error`, error, info.componentStack);
  }
  render() {
    if (this.state.error) {
      return (
        <div
          className="h-full w-full flex flex-col items-center justify-center px-3 py-2 text-center gap-1"
          style={{ color: 'var(--color-text-tertiary)', fontSize: '11px', lineHeight: 1.4 }}
        >
          <span style={{ fontWeight: 500 }}>{i18n.t('dashboard.widgets.frame.errorBoundary')}</span>
          <span style={{ opacity: 0.75 }}>{this.props.widgetType}</span>
        </div>
      );
    }
    return this.props.children;
  }
}

interface DashboardGridProps {
  prefs: DashboardPrefs;
  editMode: boolean;
  onChange: (next: DashboardPrefs) => void;
  onOpenSettings: (id: string) => void;
}

function toLayoutItems(items: RGLItem[]): LayoutItem[] {
  return items.map((l) => ({
    i: l.i,
    x: l.x,
    y: l.y,
    w: l.w,
    h: l.h,
    minW: l.minW,
    minH: l.minH,
    maxW: l.maxW,
    maxH: l.maxH,
    static: l.static,
  }));
}

function fromLayout(layout: Layout): RGLItem[] {
  return layout.map((l) => ({
    i: l.i,
    x: l.x,
    y: l.y,
    w: l.w,
    h: l.h,
    minW: l.minW,
    minH: l.minH,
    maxW: l.maxW,
    maxH: l.maxH,
  }));
}

export function DashboardGrid({ prefs, editMode, onChange, onOpenSettings }: DashboardGridProps) {
  const [currentBp, setCurrentBp] = useState<BreakpointKey>(prefs.lastBreakpoint ?? 'lg');
  const { width, containerRef, mounted } = useContainerWidth();

  const reconciled = useMemo(() => reconcileLayouts(prefs.widgets, prefs.layouts), [prefs.widgets, prefs.layouts]);
  const latestPrefsRef = useRef(prefs);
  const latestReconciledRef = useRef(reconciled);
  const latestOnChangeRef = useRef(onChange);
  latestPrefsRef.current = prefs;
  latestReconciledRef.current = reconciled;
  latestOnChangeRef.current = onChange;

  // If reconcile clamped stored widths/heights to new [min, max] bounds,
  // flush the corrected layouts back to prefs so the DB catches up. Without
  // this, cells that violated the caps keep rendering at the stored size
  // next session because RGL only emits onLayoutChange on user interaction.
  //
  // The diff is against reconciled-vs-stored, not a mount-only latch: if a
  // later registry change tightens bounds for an existing widget, reconcile
  // clamps to the new bounds and we write once more. Writes always match
  // `reconciled`, so the comparison becomes stable after the write and the
  // effect stops firing. This avoids the old "one writeback per mount" trap.
  useEffect(() => {
    const needsWrite = BREAKPOINT_KEYS.some((bp) => {
      const stored = prefs.layouts[bp] ?? [];
      const fixed = reconciled[bp] ?? [];
      if (stored.length !== fixed.length) return true;
      return stored.some((s, i) => {
        const f = fixed[i];
        return (
          !f ||
          f.i !== s.i ||
          f.w !== s.w ||
          f.h !== s.h ||
          f.minW !== s.minW ||
          f.minH !== s.minH ||
          f.maxW !== s.maxW ||
          f.maxH !== s.maxH
        );
      });
    });
    if (needsWrite) {
      // Read onChange via ref so a new onChange identity (parent re-render)
      // doesn't re-fire this effect and risk a loop. The write itself triggers
      // a prefs change, which IS the intended retrigger signal.
      latestOnChangeRef.current({ ...prefs, layouts: reconciled });
    }
  }, [reconciled, prefs]);

  const rglLayouts = useMemo<ResponsiveLayouts<'lg' | 'md'>>(() => {
    return {
      lg: toLayoutItems(reconciled.lg ?? []),
      md: toLayoutItems(reconciled.md ?? []),
    };
  }, [reconciled]);

  // RGL fires onLayoutChange on every drag/resize tick. To keep the heavy work
  // (setState → reconcile → re-render the whole grid) out of the hot path we:
  //   - stash the latest allLayouts during the gesture in a ref
  //   - skip committing while a drag/resize is in progress
  //   - commit once when the gesture stops
  // Programmatic layout changes (add, remove, breakpoint switch) still commit
  // immediately because they pass through onLayoutChange with no in-progress
  // gesture flag set.
  const isGesturingRef = useRef(false);
  const pendingLayoutsRef = useRef<ResponsiveLayouts<'lg' | 'md'> | null>(null);

  const commitLayouts = useCallback(
    (allLayouts: ResponsiveLayouts<'lg' | 'md'>) => {
      const currentPrefs = latestPrefsRef.current;
      const widgetsById = new Map(currentPrefs.widgets.map((w) => [w.id, w]));
      const nextLayouts: Partial<Record<BreakpointKey, RGLItem[]>> = {};
      for (const bp of BREAKPOINT_KEYS) {
        if (!allLayouts[bp]) continue;
        const currentItems = currentPrefs.layouts[bp] ?? latestReconciledRef.current[bp] ?? [];
        const currentById = new Map(currentItems.map((l) => [l.i, l]));
        nextLayouts[bp] = fromLayout(allLayouts[bp] as Layout).map((item) => {
          const widget = widgetsById.get(item.i);
          const def = widget ? getWidget(widget.type) : undefined;
          if (!def?.fitToContent) return item;
          const currentItem = currentById.get(item.i);
          const lockedRows =
            fittedHeightRef.current[item.i] ??
            (typeof currentItem?.h === 'number' ? currentItem.h : undefined);
          if (typeof lockedRows !== 'number') return item;
          return { ...item, h: lockedRows, minH: lockedRows, maxH: lockedRows };
        });
      }
      const nextPrefs = { ...currentPrefs, layouts: nextLayouts, lastBreakpoint: currentBp };
      latestPrefsRef.current = nextPrefs;
      latestOnChangeRef.current(nextPrefs);
    },
    [currentBp]
  );

  const onLayoutChange = useCallback(
    (_current: Layout, allLayouts: ResponsiveLayouts<'lg' | 'md'>) => {
      if (!editMode) return;
      pendingLayoutsRef.current = allLayouts;
      if (isGesturingRef.current) return;
      commitLayouts(allLayouts);
    },
    [editMode, commitLayouts]
  );

  const onDragStart = useCallback(() => {
    isGesturingRef.current = true;
  }, []);
  const onResizeStart = useCallback(() => {
    isGesturingRef.current = true;
  }, []);
  const onDragStop = useCallback(() => {
    isGesturingRef.current = false;
    if (pendingLayoutsRef.current) {
      commitLayouts(pendingLayoutsRef.current);
      pendingLayoutsRef.current = null;
    }
  }, [commitLayouts]);
  const onResizeStop = useCallback(() => {
    isGesturingRef.current = false;
    if (pendingLayoutsRef.current) {
      commitLayouts(pendingLayoutsRef.current);
      pendingLayoutsRef.current = null;
    }
  }, [commitLayouts]);

  const onBreakpointChange = useCallback((bp: string) => {
    if (bp === 'lg' || bp === 'md') setCurrentBp(bp as BreakpointKey);
  }, []);

  // Ref-backed stable wrappers. These read the latest prefs/onChange through
  // refs at call time, so the exposed `handleRemove`/`handleDuplicate`
  // identities never change across renders. That stability is what lets the
  // memoized children array (below) return the same React element refs on
  // layout-only prefs updates, so React's reconciler bails out on widget
  // subtrees during fit-height commits.
  const handleRemoveRef = useRef<(id: string) => void>(() => undefined);
  handleRemoveRef.current = (id: string) => {
    const currentPrefs = latestPrefsRef.current;
    const widgets = currentPrefs.widgets.filter((w) => w.id !== id);
    const layouts: Partial<Record<BreakpointKey, RGLItem[]>> = {};
    for (const bp of BREAKPOINT_KEYS) {
      const items = currentPrefs.layouts[bp];
      if (items) layouts[bp] = items.filter((l) => l.i !== id);
    }
    latestOnChangeRef.current({ ...currentPrefs, widgets, layouts });
  };
  const handleRemove = useCallback((id: string) => handleRemoveRef.current(id), []);

  const handleDuplicateRef = useRef<(id: string) => void>(() => undefined);
  handleDuplicateRef.current = (id: string) => {
    const currentPrefs = latestPrefsRef.current;
    const source = currentPrefs.widgets.find((w) => w.id === id);
    if (!source) return;
    const def = getWidget(source.type);
    if (!def || def.singleton) return;
    const newId = newWidgetId();
    const clone: WidgetInstance = {
      id: newId,
      type: source.type,
      config: { ...(source.config as object) },
    };
    const layouts: Partial<Record<BreakpointKey, RGLItem[]>> = {};
    for (const bp of BREAKPOINT_KEYS) {
      const items = currentPrefs.layouts[bp] ?? [];
      const src = items.find((l) => l.i === id);
      const maxY = items.reduce((m, l) => Math.max(m, l.y + l.h), 0);
      layouts[bp] = [
        ...items,
        {
          i: newId,
          x: 0,
          y: maxY,
          w: src?.w ?? def.defaultSize.w,
          h: src?.h ?? def.defaultSize.h,
          minW: def.minSize.w,
          minH: def.minSize.h,
          maxW: def.maxSize?.w,
          maxH: def.maxSize?.h,
        },
      ];
    }
    latestOnChangeRef.current({ ...currentPrefs, widgets: [...currentPrefs.widgets, clone], layouts });
  };
  const handleDuplicate = useCallback((id: string) => handleDuplicateRef.current(id), []);

  // Map of the last-reported content-driven row count per widget, so we don't
  // write identical layouts on every ResizeObserver tick.
  const fittedHeightRef = useRef<Record<string, number>>({});

  // Per-widget callbacks are built once per widget id and reused across renders,
  // so WidgetFrame and the widget Component don't see fresh function identities
  // every parent render. Internally they forward through refs to the latest
  // fitHeight/updateConfig closures.
  const fitHeightRef = useRef<(id: string, totalCellPx: number) => void>(() => undefined);
  const updateConfigRef = useRef<(id: string, patch: Partial<unknown>) => void>(() => undefined);
  const callbackCacheRef = useRef<
    Map<
      string,
      {
        onFitHeight: (totalCellPx: number) => void;
        updateConfig: (patch: Partial<unknown>) => void;
      }
    >
  >(new Map());

  const getWidgetCallbacks = useCallback((id: string) => {
    const cache = callbackCacheRef.current;
    let entry = cache.get(id);
    if (!entry) {
      entry = {
        onFitHeight: (px: number) => fitHeightRef.current(id, px),
        updateConfig: (patch: Partial<unknown>) => updateConfigRef.current(id, patch),
      };
      cache.set(id, entry);
    }
    return entry;
  }, []);
  // Prune cache entries for widgets that no longer exist.
  useEffect(() => {
    const alive = new Set(prefs.widgets.map((w) => w.id));
    for (const key of callbackCacheRef.current.keys()) {
      if (!alive.has(key)) callbackCacheRef.current.delete(key);
    }
  }, [prefs.widgets]);

  updateConfigRef.current = (id: string, patch: Partial<unknown>) => {
    const currentPrefs = latestPrefsRef.current;
    const widgets = currentPrefs.widgets.map((w) =>
      w.id === id ? { ...w, config: { ...(w.config as object), ...(patch as object) } } : w
    );
    latestOnChangeRef.current({ ...currentPrefs, widgets });
  };

  fitHeightRef.current = (id: string, totalCellPx: number) => {
    const currentPrefs = latestPrefsRef.current;
    const widget = currentPrefs.widgets.find((w) => w.id === id);
    if (!widget) return;
    const def = getWidget(widget.type);
    if (!def) return;
    const targetRows = clamp(
      pxToRows(totalCellPx + FIT_PADDING_PX),
      def.minSize.h,
      def.maxSize?.h
    );
    if (fittedHeightRef.current[id] === targetRows) return;
    fittedHeightRef.current[id] = targetRows;

    // Gesture-gate the commit. Fit widgets reflow their content as the user
    // drags a resize handle (text wraps narrower → taller), so this callback
    // fires many times mid-gesture. Non-fit widgets' layout updates are
    // already deferred via pendingLayoutsRef; mirroring that here keeps the
    // grid from cascading a setState on every row-quantum crossing during a
    // live drag/resize. commitLayouts reads fittedHeightRef on gesture stop,
    // so the latest measurement is applied naturally as part of the flush.
    if (isGesturingRef.current) return;

    const nextLayouts: Partial<Record<BreakpointKey, RGLItem[]>> = {};
    let changed = false;
    for (const bp of BREAKPOINT_KEYS) {
      const items = currentPrefs.layouts[bp] ?? latestReconciledRef.current[bp] ?? [];
      nextLayouts[bp] = items.map((l) => {
        if (l.i !== id) return l;
        // Lock vertical resize to the measured content height. Width stays
        // user-resizable.
        if (l.h === targetRows && l.minH === targetRows && l.maxH === targetRows) return l;
        changed = true;
        return { ...l, h: targetRows, minH: targetRows, maxH: targetRows };
      });
    }
    if (changed) {
      const nextPrefs = { ...currentPrefs, layouts: nextLayouts };
      latestPrefsRef.current = nextPrefs;
      latestOnChangeRef.current(nextPrefs);
    }
  };

  // Two-tier memoization. The widget bodies (Suspense + Component) depend
  // only on prefs.widgets — edit mode is handled entirely at the framework
  // level (WidgetFrame chrome + RGL drag/resize config), not plumbed into
  // widgets, so the heavy subtrees stay mounted/idle across edit-mode
  // toggles. The outer children array wraps each body in a WidgetFrame that
  // does depend on editMode; toggling only recomputes the chrome.
  //
  // On a layout-only prefs update (fit-height commits during drawer
  // expansion), both memos bail — React's reconciler sees the same element
  // refs and skips every widget subtree. Per-commit cost drops from
  // "re-render 20 widget subtrees (~15-40ms)" to RGL's wrapper reposition
  // (~1-3ms), which is what lets the immediate-growth-commit path track
  // content smoothly during expansion.
  const widgetBodies = useMemo(
    () =>
      prefs.widgets.map((instance) => {
        const def = getWidget(instance.type);
        const cb = getWidgetCallbacks(instance.id);
        if (!def) {
          return { id: instance.id, instance, def: null, cb, body: null as ReactNode };
        }
        const Component = def.component;
        return {
          id: instance.id,
          instance,
          def,
          cb,
          body: (
            <WidgetErrorBoundary widgetType={def.type}>
              <Suspense fallback={<WidgetFallback />}>
                <Component
                  instance={instance}
                  updateConfig={cb.updateConfig as (patch: Partial<unknown>) => void}
                />
              </Suspense>
            </WidgetErrorBoundary>
          ) as ReactNode,
        };
      }),
    [prefs.widgets, getWidgetCallbacks]
  );

  const widgetChildren = useMemo(
    () =>
      widgetBodies.map(({ id, instance, def, cb, body }) => {
        if (!def || !body) return <div key={id} />;
        return (
          <div key={id}>
            <WidgetFrame
              definition={def}
              instance={instance}
              editMode={editMode}
              onRemove={handleRemove}
              onDuplicate={handleDuplicate}
              onOpenSettings={onOpenSettings}
              onFitHeight={cb.onFitHeight}
            >
              {body}
            </WidgetFrame>
          </div>
        );
      }),
    [widgetBodies, editMode, handleRemove, handleDuplicate, onOpenSettings]
  );

  return (
    <div ref={containerRef} className="widget-grid-container">
      {mounted && width > 0 && (
        <ResponsiveGridLayout
          width={width}
          className={`widget-grid ${editMode ? 'widget-grid--edit' : ''}`}
          layouts={rglLayouts}
          breakpoints={BREAKPOINTS_PX}
          cols={COLS_PER_BP}
          rowHeight={ROW_HEIGHT}
          margin={[MARGIN_X, MARGIN_Y]}
          containerPadding={[0, 0]}
          dragConfig={{
            enabled: editMode,
            handle: '.widget-drag-handle',
            cancel: '.widget-drag-cancel, .react-resizable-handle',
          }}
          resizeConfig={{ enabled: editMode }}
          onLayoutChange={onLayoutChange}
          onDragStart={onDragStart}
          onDragStop={onDragStop}
          onResizeStart={onResizeStart}
          onResizeStop={onResizeStop}
          onBreakpointChange={onBreakpointChange}
        >
          {widgetChildren}
        </ResponsiveGridLayout>
      )}
    </div>
  );
}
