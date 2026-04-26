import { useState, useRef, useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import { GripVertical, Settings, MoreVertical, X, Copy } from 'lucide-react';
import type { WidgetDefinition, WidgetInstance } from '../types';
import './WidgetFrame.css';

interface WidgetFrameProps {
  definition: WidgetDefinition<unknown>;
  instance: WidgetInstance<unknown>;
  editMode: boolean;
  onRemove: (id: string) => void;
  onDuplicate: (id: string) => void;
  onOpenSettings: (id: string) => void;
  /**
   * When the widget opts in to `fitToContent`, the frame measures its own
   * chrome (header + body padding) plus the child content height and reports
   * the total required cell height here. The grid converts to row count.
   */
  onFitHeight?: (totalPx: number) => void;
  children: React.ReactNode;
}

export function WidgetFrame({
  definition,
  instance,
  editMode,
  onRemove,
  onDuplicate,
  onOpenSettings,
  onFitHeight,
  children,
}: WidgetFrameProps) {
  const { t } = useTranslation();
  const [menuOpen, setMenuOpen] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);
  const headerRef = useRef<HTMLDivElement>(null);
  const bodyRef = useRef<HTMLDivElement>(null);
  const innerRef = useRef<HTMLDivElement>(null);

  // Keep the latest onFitHeight in a ref so the ResizeObserver effect doesn't
  // tear down / reconnect (and fire a synchronous report) on every parent
  // render. Parent passes an inline arrow (new identity each render); without
  // the ref, two fit-to-content widgets can ping-pong parent re-renders.
  const onFitHeightRef = useRef(onFitHeight);
  useEffect(() => {
    onFitHeightRef.current = onFitHeight;
  }, [onFitHeight]);

  useEffect(() => {
    if (!menuOpen) return;
    const handler = (e: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
        setMenuOpen(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [menuOpen]);

  // fit-to-content: report inner content + visible chrome (header) + body
  // padding so the RGL cell hugs the inner card with consistent padding on
  // all four sides. Chrome/padding values come from live DOM so the edit/view
  // mode difference (header hidden + zero padding in view, full chrome + 12px
  // padding in edit) is picked up automatically.
  //
  // Timing: some fit widgets animate their own height (e.g. AIDailyBriefCard
  // uses AnimatePresence to animate height 0→auto over ~300ms when the
  // "earlier insights" drawer opens). We split growth vs. shrink:
  //   - Growth commits immediately on every ResizeObserver tick. The cell
  //     tracks the content in lockstep, so no overflow (and therefore no
  //     scrollbar) can appear during expansion. RGL's height CSS transition
  //     is disabled for fit cells (see WidgetFrame.css), so snapping each
  //     frame is cheap and doesn't fight the content's own animation.
  //   - Shrink is trailing-debounced (~120ms). Collapses are monotonic but
  //     can emit stray intermediate values on layout thrash; debouncing lets
  //     the content settle before the cell commits, which also avoids
  //     oscillation when two fit widgets are in the same grid.
  useEffect(() => {
    if (!definition.fitToContent) return;
    const inner = innerRef.current;
    const body = bodyRef.current;
    const header = headerRef.current;
    if (!inner || !body) return;

    const DEBOUNCE_MS = 120;
    let lastReported = -1;
    let pending = -1;
    let debounceTimer: number | null = null;
    let didInitial = false;

    const measure = () => {
      const bs = window.getComputedStyle(body);
      const padV = (parseFloat(bs.paddingTop) || 0) + (parseFloat(bs.paddingBottom) || 0);
      const headerH = header && header.offsetParent !== null ? header.offsetHeight : 0;
      return inner.offsetHeight + padV + headerH;
    };

    const flush = () => {
      debounceTimer = null;
      if (pending !== lastReported) {
        lastReported = pending;
        onFitHeightRef.current?.(pending);
      }
    };

    const commit = (total: number) => {
      if (debounceTimer !== null) {
        window.clearTimeout(debounceTimer);
        debounceTimer = null;
      }
      lastReported = total;
      pending = total;
      onFitHeightRef.current?.(total);
    };

    const report = () => {
      const total = measure();
      if (total === lastReported) return;
      pending = total;
      if (!didInitial) {
        didInitial = true;
        commit(total);
        return;
      }
      if (total > lastReported) {
        commit(total);
        return;
      }
      if (debounceTimer !== null) window.clearTimeout(debounceTimer);
      debounceTimer = window.setTimeout(flush, DEBOUNCE_MS);
    };

    report();
    const ro = new ResizeObserver(report);
    ro.observe(inner);
    return () => {
      ro.disconnect();
      if (debounceTimer !== null) window.clearTimeout(debounceTimer);
    };
  }, [definition, editMode]);

  const hasSettings = !!definition.settingsComponent;

  // In edit mode the whole frame is the drag handle — users can grab anywhere
  // on the card to rearrange, not just the grip icon. The action buttons
  // (header actions div + its menu children) carry `widget-drag-cancel` so
  // clicking them doesn't initiate a drag. RGL's 3px drag threshold keeps
  // clicks-inside-widget-content working for the same reason — a click under
  // 3px of movement stays a click.
  const classes = [
    'widget-frame',
    editMode ? 'widget-frame--edit' : '',
    editMode ? 'widget-drag-handle' : '',
    definition.fitToContent ? 'widget-frame--fit' : '',
  ]
    .filter(Boolean)
    .join(' ');

  return (
    <div className={classes} data-widget-type={instance.type}>
      <div ref={headerRef} className="widget-frame__header">
        {editMode && (
          <div
            className="widget-drag-handle widget-frame__drag"
            aria-label={t('dashboard.widgets.frame.drag')}
          >
            <GripVertical size={14} />
          </div>
        )}
        <div className="widget-frame__title">{t(definition.titleKey)}</div>
        {editMode && (
          <div className="widget-frame__actions widget-drag-cancel">
            {hasSettings && (
              <button
                type="button"
                className="widget-frame__icon-btn"
                onClick={() => onOpenSettings(instance.id)}
                aria-label={t('dashboard.widgets.frame.settings')}
                title={t('dashboard.widgets.frame.settingsTitle')}
              >
                <Settings size={14} />
              </button>
            )}
            <div className="widget-frame__menu-wrap" ref={menuRef}>
              <button
                type="button"
                className="widget-frame__icon-btn"
                onClick={() => setMenuOpen((p) => !p)}
                aria-label={t('dashboard.widgets.frame.menu')}
              >
                <MoreVertical size={14} />
              </button>
              {menuOpen && (
                <div className="widget-frame__menu">
                  {!definition.singleton && (
                    <button
                      type="button"
                      className="widget-frame__menu-item"
                      onClick={() => {
                        onDuplicate(instance.id);
                        setMenuOpen(false);
                      }}
                    >
                      <Copy size={12} /> {t('dashboard.widgets.frame.duplicate')}
                    </button>
                  )}
                  <button
                    type="button"
                    className="widget-frame__menu-item widget-frame__menu-item--danger"
                    onClick={() => {
                      onRemove(instance.id);
                      setMenuOpen(false);
                    }}
                  >
                    <X size={12} /> {t('dashboard.widgets.frame.remove')}
                  </button>
                </div>
              )}
            </div>
          </div>
        )}
      </div>
      <div ref={bodyRef} className="widget-frame__body">
        <div ref={innerRef} className={definition.fitToContent ? undefined : 'h-full'}>
          {children}
        </div>
      </div>
    </div>
  );
}
