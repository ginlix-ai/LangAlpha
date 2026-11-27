import { useEffect, useEffectEvent, useMemo, useRef, useState } from 'react';
import { registerAuthReset } from '@/lib/authResets';
import { Loader } from '@/components/ui/loader';
import ImageLightbox from '../ImageLightbox';
import { djb2 } from '@/lib/hash';
import { framePrompts, type WidgetPrompts } from '../../utils/widgetPrompt';
import { buildHtmlSrcDoc } from './html/buildHtmlSrcDoc';
import { useHtmlSandbox } from './html/useHtmlSandbox';
import FrameActionHint from './html/FrameActionHint';
import { useHtmlActions } from './html/useHtmlActions';
import { readWidgetStorage } from './html/widgetStorage';
import { inlineWorkspaceImages } from './html/widgetExport';
import { WIDGET_FRAME } from './html/sandbox';
import HtmlActionBar from './html/HtmlActionBar';
import HtmlFullscreenModal from './html/HtmlFullscreenModal';
import './InlineWidget.css';

interface InlineWidgetProps {
  html: string;
  title?: string;
  /** Inline data file contents — injected directly as __WIDGET_DATA__. */
  data?: Record<string, string>;
  /**
   * The widget's segment id, which its kept localStorage and the data its
   * prompt calls pass are filed under. Absent, the content stands in for it.
   */
  widgetId?: string;
  /** What the host does with the widget's prompt calls. */
  prompts?: WidgetPrompts;
  /**
   * The `<base>` the block decided (`useWidgetBase`), the same one its preview
   * drew under; null for none, undefined while it is still undecided, and the
   * frame waits blank rather than loading once without workspace files and
   * again with them.
   */
  base: string | null | undefined;
  /**
   * The height of the preview this widget replaces. The box holds it, and the
   * widget stays transparent over the preview, until its document reports.
   */
  handoffHeight?: number;
  /** The widget's document reported its first height: it is now on screen. */
  onShown?: () => void;
}

/**
 * Last reported height per widget content, so a revisited thread reserves the
 * real height immediately instead of the 150px guess — the first live report
 * then lands as a small correction rather than a layout jolt. Session-scoped.
 */
const lastKnownHeights = new Map<string, number>();

/** The cache outlives unmounts by design; wiped on sign-out/account switch
 * (module singletons outlive React — web/AGENTS.md). Exported for tests. */
export function resetInlineWidgetHeightCache() {
  lastKnownHeights.clear();
}
registerAuthReset(resetInlineWidgetHeightCache);

function widgetHeightKey(html: string, data?: Record<string, string>): string {
  const h = djb2(html);
  let dataSig = '';
  if (data) {
    for (const k of Object.keys(data)) dataSig += `${k}:${data[k].length};`;
  }
  return `${h}|${html.length}|${dataSig}`;
}

export default function InlineWidget({
  html, title, prompts, data, widgetId, base, handoffHeight, onShown,
}: InlineWidgetProps) {
  const iframeRef = useRef<HTMLIFrameElement>(null);
  const [fullscreen, setFullscreen] = useState(false);
  const [zoom, setZoom] = useState<{ src: string; alt?: string } | null>(null);

  const heightKey = useMemo(() => widgetHeightKey(html, data), [html, data]);
  const storageId = widgetId ?? `content:${heightKey}`;
  // Seeded with what is kept when the document is built, and not rebuilt for
  // a write: that would reload the widget it came from. Once loaded, a frame
  // is handed the writes of the widget's other frame (useHtmlSandbox), so the
  // inline frame sees what was changed in fullscreen and the other way round.
  const srcDoc = useMemo(
    () => (base === undefined
      ? undefined
      : buildHtmlSrcDoc('widget-inline', {
        html, data, base: base ?? undefined, storageSeed: readWidgetStorage(storageId),
      })),
    [html, data, base, storageId],
  );
  // Built when opened, from what the widget has kept by then.
  const fullscreenSrcDoc = useMemo(
    () => (fullscreen && base !== undefined
      ? buildHtmlSrcDoc('widget-fullscreen', {
        html, data, base: base ?? undefined, storageSeed: readWidgetStorage(storageId),
      })
      : undefined),
    [fullscreen, html, data, base, storageId],
  );

  // A call from the dialog closes it first, so the turn, the box or the new
  // thread it leads to is in view.
  const inlinePrompts = useMemo(() => framePrompts(prompts, { widgetId: storageId, title }), [prompts, storageId, title]);
  const dialogPrompts = useMemo(
    () => framePrompts(prompts, { widgetId: storageId, title }, () => setFullscreen(false)),
    [prompts, storageId, title],
  );
  const onZoom = (src: string, alt?: string) => setZoom({ src, alt });

  const { height, onFrameLoad, retryAt } = useHtmlSandbox({
    iframeRef,
    autoHeight: true,
    prompts: inlinePrompts,
    storageId,
    onZoom,
  });

  const lastRecorded = useRef<number | null>(null);
  useEffect(() => {
    // Record only fresh measurements — a key change alone must not re-file the
    // previous document's height under the new content's key.
    if (height != null && height !== lastRecorded.current) {
      lastKnownHeights.set(heightKey, height);
      lastRecorded.current = height;
    }
  }, [height, heightKey]);

  const shown = useEffectEvent(() => onShown?.());
  useEffect(() => {
    if (height != null) shown();
  }, [height]);

  // Live report wins; the preview's height or a cached one bridges the load;
  // 150px is the cold guess.
  const displayHeight = height ?? handoffHeight ?? lastKnownHeights.get(heightKey) ?? null;
  // Over a preview, nothing shows until the widget has drawn: the preview
  // under it stays the picture until then.
  const visible = handoffHeight != null ? height != null : displayHeight != null;

  // One action set for both surfaces, built on the inline document. The
  // fullscreen variant is display markup for the dialog it is shown in and
  // nothing else: opening, downloading or printing it would hand the user a
  // document carrying the dialog's own layout, and on paper the box is 680px
  // wide and neither the column cap nor the magnification means anything.
  // A copy that leaves the app carries no grant: what the markup names under
  // the base is inlined, and the base dropped.
  const exportDoc = () => {
    const build = (markup: string) => buildHtmlSrcDoc('widget-inline', {
      html: markup, data, storageSeed: readWidgetStorage(storageId),
    });
    if (!base) return build(html);
    const inlined = inlineWorkspaceImages(html, base);
    return typeof inlined === 'string' ? build(inlined) : inlined.then(build);
  };
  // Its actions are offered only once `srcDoc` is built, so the '' is never acted on.
  const actions = useHtmlActions({ mode: 'widget', srcDoc: srcDoc ?? '', exportDoc, fileName: title });

  // No max-height cap — widgets span naturally to fit content (charts, tables,
  // dashboards). The agent controls HTML output and the skill doc guides it to
  // keep widgets reasonable. A cap would add scroll-in-scroll UX that's worse
  // than a tall widget pushing chat down.
  return (
    <div className="inline-widget-container">
      {/* Popup tokens: links the agent embeds must open as REAL tabs — a
          sandbox-inheriting tab has no cookie access, so external sites' bot
          checks break. buildHtmlSrcDoc's click handler forces noopener, and a
          noopener'd tab is the reach a chat markdown link already has. */}
      <iframe
        ref={iframeRef}
        srcDoc={srcDoc}
        {...WIDGET_FRAME}
        title={title || 'Widget'}
        className="inline-widget-frame"
        onLoad={onFrameLoad}
        style={{
          height: displayHeight != null ? `${displayHeight}px` : '150px',
          opacity: visible ? 1 : 0,
        }}
      />
      {height == null && handoffHeight == null && (
        // Shown until the document reports its first measurement — even when a
        // cached height already reserved the box, the content is still loading.
        <span className="inline-widget-loading">
          <Loader
            size={14}
            label="Rendering widget"
            className="text-(--color-text-tertiary)"
          />
        </span>
      )}
      <FrameActionHint retryAt={retryAt} />
      {srcDoc !== undefined && (
        <HtmlActionBar
          variant="overlay"
          onFullscreen={() => setFullscreen(true)}
          onOpenInNewTab={actions.openInNewTab}
          onDownload={actions.downloadHtml}
          onExportPdf={actions.exportPdf}
        />
      )}
      {fullscreen && fullscreenSrcDoc && (
        <HtmlFullscreenModal
          open={fullscreen}
          onOpenChange={setFullscreen}
          title={title || 'Widget'}
          srcDoc={fullscreenSrcDoc}
          actions={actions}
          prompts={dialogPrompts}
          storageId={storageId}
        />
      )}
      {zoom && <ImageLightbox src={zoom.src} alt={zoom.alt} open onClose={() => setZoom(null)} />}
    </div>
  );
}
