import { useState, useRef, useEffect, useEffectEvent } from 'react';
import { useTranslation } from 'react-i18next';
import { GripVertical, Settings, MoreVertical, X, Copy, Paperclip } from 'lucide-react';
import type { WidgetDefinition, WidgetInstance } from '../types';
import { getWidgetContextSnapshot, useHasWidgetContextExporter } from './contextSnapshot';
import { WidgetCardSlots } from './widgetCardSlots';
import { MARGIN_Y, ROW_HEIGHT } from './gridConstants';
import { ContextBus } from '@/lib/contextBus';
import { useToast } from '@/components/ui/use-toast';
import { cn } from '@/lib/utils';
import { useCardNarrow } from '@/pages/ChatAgent/components/charts/termContext';
// The chat's card stylesheets, so the dashboard's cards are the same cards and
// the Card style setting (data-card-style on <html>) restyles both alike.
import '@/pages/ChatAgent/components/charts/termCard.css';
import '@/pages/ChatAgent/components/charts/termCardStyles.css';
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
   * chrome (the card around the body) plus the child content height and reports
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
  const { toast } = useToast();
  const [menuOpen, setMenuOpen] = useState(false);
  const menuRef = useRef<HTMLSpanElement>(null);
  const cardRef = useRef<HTMLElement>(null);
  const bodyRef = useRef<HTMLDivElement>(null);
  const innerRef = useRef<HTMLDivElement>(null);
  // The edge's sub and meta spans, which the widget fills through a portal.
  const [subSlot, setSubSlot] = useState<HTMLSpanElement | null>(null);
  const [metaSlot, setMetaSlot] = useState<HTMLSpanElement | null>(null);
  // A fit widget with a ceiling (a list that can run long) stops growing at
  // its tallest cell; past it the body scrolls instead of clipping. The grid
  // clamps the cell to the same height, and the phone stack, which sets no
  // height, takes it as the card's max-height.
  const fitMaxPx =
    definition.fitToContent && definition.maxSize
      ? definition.maxSize.h * ROW_HEIGHT + (definition.maxSize.h - 1) * MARGIN_Y
      : undefined;
  const [fitCapped, setFitCapped] = useState(false);
  const narrow = useCardNarrow();
  const bare = definition.frame === 'bare' && !editMode;
  const untitled = definition.frame === 'untitled' && !editMode;

  const exporterRegistered = useHasWidgetContextExporter(instance.id);
  // TradingView widgets render cross-origin iframes — neither pixels nor DOM
  // are reachable, so attach is permanently unavailable. We keep the paperclip
  // visible (parity with native widgets) but disable it and surface the reason
  // in the tooltip so users don't wonder why nothing happens.
  const attachUnsupportedReason =
    definition.source === 'tradingview'
      ? t('dashboard.widgets.frame.contextUnsupportedTradingView', {
          defaultValue:
            "Can't attach this — the content is managed by TradingView, so the agent can't access it.",
        })
      : null;
  const attachDisabled = attachUnsupportedReason !== null || !exporterRegistered;
  const attachTooltip =
    attachUnsupportedReason ??
    t('dashboard.widgets.frame.addToContextTitle', { defaultValue: 'Attach to chat' });

  const handleAddToContext = async (e: React.MouseEvent) => {
    e.stopPropagation();
    if (attachUnsupportedReason) return;
    const snapshot = await getWidgetContextSnapshot(instance.id);
    if (!snapshot) {
      toast({
        variant: 'destructive',
        title: t('dashboard.widgets.frame.contextUnavailable', { defaultValue: 'Widget not available' }),
        description: t('dashboard.widgets.frame.contextUnavailableHint', {
          defaultValue: 'This widget has not registered a context exporter yet.',
        }),
      });
      return;
    }
    ContextBus.attach(snapshot);
    toast({
      title: t('dashboard.widgets.frame.contextAttached', { defaultValue: 'Added to context' }),
      description: snapshot.label,
    });
  };

  // Read the latest onFitHeight through an effect event so the ResizeObserver
  // effect doesn't tear down / reconnect (and fire a synchronous report) on
  // every parent render. Parent passes an inline arrow (new identity each
  // render); as a dep, two fit-to-content widgets can ping-pong parent
  // re-renders.
  const reportFitHeight = useEffectEvent((totalPx: number) => onFitHeight?.(totalPx));

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

  // fit-to-content: report what the cell must hold, the content plus every
  // bit of chrome around it (the card's padding, its title when the style
  // sets the title inside, any body padding), so the cell hugs the card.
  // The chrome is read off the live DOM as the card's height less the body's:
  // the body takes whatever the card leaves it, so this holds in every card
  // style and in edit mode alike. The body is observed as well as the content
  // because a style switch (a CSS attribute on <html>) moves the title in or
  // out of the card without resizing the content; the body is what changes.
  //
  // Timing: some fit widgets animate their own height (the brief's earlier
  // insights fold open). We split growth vs. shrink:
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
    const card = cardRef.current;
    if (!inner || !body || !card) return;

    const DEBOUNCE_MS = 120;
    let lastReported = -1;
    let pending = -1;
    let debounceTimer: number | null = null;
    let didInitial = false;

    const measure = () => {
      const bs = window.getComputedStyle(body);
      const padV = (parseFloat(bs.paddingTop) || 0) + (parseFloat(bs.paddingBottom) || 0);
      const chrome = card.offsetHeight - body.offsetHeight;
      return inner.offsetHeight + padV + chrome;
    };

    const flush = () => {
      debounceTimer = null;
      if (pending !== lastReported) {
        lastReported = pending;
        reportFitHeight(pending);
      }
    };

    const commit = (total: number) => {
      if (debounceTimer !== null) {
        window.clearTimeout(debounceTimer);
        debounceTimer = null;
      }
      lastReported = total;
      pending = total;
      reportFitHeight(total);
    };

    // The hold lifting need not change the content's size, so while one is
    // up the subtree is watched for it. Only then: a fit widget that streams
    // (the conversation) would otherwise measure on every commit.
    let holdWatch: MutationObserver | null = null;

    const report = () => {
      // A placeholder (a skeleton marked `data-fit-hold`) says nothing about
      // the loaded content's height: the cell keeps its saved height, which is
      // the content's from last time, rather than shrinking to the skeleton
      // and growing back when the data lands, moving every cell below twice.
      if (inner.querySelector('[data-fit-hold]')) {
        if (!holdWatch) {
          holdWatch = new MutationObserver(report);
          holdWatch.observe(inner, { subtree: true, childList: true, attributeFilter: ['data-fit-hold'] });
        }
        return;
      }
      holdWatch?.disconnect();
      holdWatch = null;
      const total = measure();
      if (fitMaxPx !== undefined) setFitCapped(total > fitMaxPx);
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
    ro.observe(body);
    return () => {
      ro.disconnect();
      holdWatch?.disconnect();
      if (debounceTimer !== null) window.clearTimeout(debounceTimer);
    };
  }, [definition, editMode, fitMaxPx]);

  const hasSettings = !!definition.settingsComponent;

  // In edit mode the whole frame is the drag handle: users can grab anywhere
  // on the card to rearrange, not just the grip. The action buttons carry
  // `widget-drag-cancel` so clicking them doesn't initiate a drag. RGL's 3px
  // drag threshold keeps clicks inside widget content working for the same
  // reason: a click under 3px of movement stays a click.
  //
  // The card is the chat's term card: its title set into the top edge (inside
  // it, in the Quiet style), the widget's sub and meta beside and opposite it.
  // The actions follow the title rather than the meta, so the paperclip that
  // a hover reveals takes room nothing else is using and the meta never moves.
  return (
    <div
      className={cn(
        'widget-frame',
        editMode && 'widget-frame--edit widget-drag-handle',
        definition.fitToContent && 'widget-frame--fit',
      )}
      data-widget-type={instance.type}
      data-fit-capped={fitCapped ? '' : undefined}
      style={fitMaxPx !== undefined ? ({ '--widget-fit-max': `${fitMaxPx}px` } as React.CSSProperties) : undefined}
    >
      <section
        ref={cardRef}
        className="term-card widget-card"
        data-size={narrow ? 'sm' : undefined}
        data-bare={bare ? '' : undefined}
        data-untitled={untitled ? '' : undefined}
      >
        <header className="term-card-head" data-keep="title">
          {editMode && (
            <span className="widget-card__grip" aria-label={t('dashboard.widgets.frame.drag')}>
              <GripVertical size={12} />
            </span>
          )}
          <span className="term-card-title">
            <span className="term-bracket" aria-hidden="true">[</span>
            <span className="term-card-title-text">{t(definition.cardTitleKey ?? definition.titleKey)}</span>
            <span className="term-bracket" aria-hidden="true">]</span>
          </span>
          <span ref={setSubSlot} className="term-card-sub" />
          <span className="widget-card__actions widget-drag-cancel">
            {editMode && hasSettings && (
              <button
                type="button"
                className="widget-card__icon-btn"
                onClick={() => onOpenSettings(instance.id)}
                aria-label={t('dashboard.widgets.frame.settings')}
                title={t('dashboard.widgets.frame.settingsTitle')}
              >
                <Settings size={12} />
              </button>
            )}
            <button
              type="button"
              className={cn('widget-card__icon-btn widget-card__attach', !editMode && 'widget-card__attach--view')}
              onClick={handleAddToContext}
              disabled={attachDisabled}
              aria-label={attachTooltip}
              title={attachTooltip}
            >
              <Paperclip size={12} />
            </button>
            {editMode && (
              <span className="widget-card__menu-wrap" ref={menuRef}>
                <button
                  type="button"
                  className="widget-card__icon-btn"
                  onClick={() => setMenuOpen((p) => !p)}
                  aria-label={t('dashboard.widgets.frame.menu')}
                >
                  <MoreVertical size={12} />
                </button>
                {menuOpen && (
                  <div className="widget-card__menu">
                    {!definition.singleton && (
                      <button
                        type="button"
                        className="widget-card__menu-item"
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
                      className="widget-card__menu-item widget-card__menu-item--danger"
                      onClick={() => {
                        onRemove(instance.id);
                        setMenuOpen(false);
                      }}
                    >
                      <X size={12} /> {t('dashboard.widgets.frame.remove')}
                    </button>
                  </div>
                )}
              </span>
            )}
          </span>
          <span ref={setMetaSlot} className="term-card-meta" />
        </header>
        <div ref={bodyRef} className="widget-card__body">
          <div ref={innerRef} className={definition.fitToContent ? undefined : 'h-full'}>
            <WidgetCardSlots sub={subSlot} meta={metaSlot}>
              {children}
            </WidgetCardSlots>
          </div>
        </div>
      </section>
    </div>
  );
}
