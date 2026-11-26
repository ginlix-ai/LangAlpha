import { useEffect, useEffectEvent, useRef, useState } from 'react';
import { DURATION } from '@/lib/motion';
import type { WidgetDraft } from '@/types/chat';
import type { WidgetPrompts } from '../../utils/widgetPrompt';
import type { HtmlWidgetData } from '../../hooks/utils/types';
import { PreparingToolCallRow } from '../messageList/InlineActivityRows';
import { buildStreamSrcDoc } from './html/buildHtmlSrcDoc';
import { useHtmlSandbox } from './html/useHtmlSandbox';
import { useWidgetBase } from './html/useWidgetBase';
import { WIDGET_FRAME } from './html/sandbox';
import { postToWidget } from './html/widgetProtocol';
import InlineWidget from './InlineWidget';
import './InlineWidget.css';

/** At most one redraw of the preview this often: each parses all the markup so far. */
const STREAM_POST_MS = 80;

interface WidgetBlockProps {
  /** The widget, once its artifact has landed. */
  widget?: HtmlWidgetData;
  /** The widget drawn from its call's arguments, before the artifact lands. */
  preview?: WidgetDraft;
  widgetId?: string;
  prompts?: WidgetPrompts;
}

/**
 * A ShowWidget call in the transcript, from its first streamed argument to the
 * finished widget, as one element. While the model writes the call, the
 * markup draws as it arrives with none of its scripts (`widget-stream`); when
 * the artifact lands, the widget mounts in the preview's place at the
 * preview's height and stays transparent until it has drawn, then fades in
 * over the preview, which goes once the fade is done. A widget that arrives
 * whole, as in a replayed thread, draws as it always has.
 */
export default function WidgetBlock({ widget, preview, widgetId, prompts }: WidgetBlockProps) {
  const base = useWidgetBase();
  // Whether this block began as a preview; one that began as the widget never draws one.
  const [previewed] = useState(() => preview !== undefined);
  const [previewHeight, setPreviewHeight] = useState<number | null>(null);
  const [handedOver, setHandedOver] = useState(false);
  const [previewGone, setPreviewGone] = useState(false);

  useEffect(() => {
    if (!handedOver) return;
    const timer = setTimeout(() => setPreviewGone(true), DURATION.quick * 1000);
    return () => clearTimeout(timer);
  }, [handedOver]);

  // A preview that never drew anything has nothing to hand over.
  const drawingPreview = previewed && !previewGone && (!widget || previewHeight != null);
  // The preview's height while the widget takes its place over it.
  const handoff = widget && drawingPreview ? previewHeight : null;

  return (
    <div className="widget-block">
      {drawingPreview && base !== undefined && (
        <WidgetStreamFrame
          html={preview?.html ?? null}
          base={base}
          title={widget?.title || preview?.title || undefined}
          overlay={handoff != null}
          onHeight={setPreviewHeight}
        />
      )}
      {!widget && previewHeight == null && (
        <PreparingToolCallRow
          tc={{ pendingKey: widgetId ?? 'widget', toolName: 'ShowWidget', argsLength: preview?.argsLength ?? 0 }}
        />
      )}
      {widget && (
        <InlineWidget
          html={widget.html}
          title={widget.title}
          data={widget.data}
          widgetId={widgetId}
          prompts={prompts}
          base={base}
          handoffHeight={handoff ?? undefined}
          onShown={handoff != null ? () => setHandedOver(true) : undefined}
        />
      )}
    </div>
  );
}

interface WidgetStreamFrameProps {
  /** All the markup written so far; null keeps what is drawn. */
  html: string | null;
  base: string | null;
  title?: string;
  /** Lying under the widget that replaces it, rather than in the flow. */
  overlay: boolean;
  onHeight: (height: number) => void;
}

/** The preview frame: built once, then redrawn by message as the markup grows. */
function WidgetStreamFrame({ html, base, title, overlay, onHeight }: WidgetStreamFrameProps) {
  const iframeRef = useRef<HTMLIFrameElement>(null);
  // Built once: a new document would reload the frame and redraw from nothing.
  const [srcDoc] = useState(() => buildStreamSrcDoc({ base: base ?? undefined }));
  const [loaded, setLoaded] = useState(false);
  const { height } = useHtmlSandbox({ iframeRef, autoHeight: true });

  const reportHeight = useEffectEvent(onHeight);
  useEffect(() => {
    if (height != null) reportHeight(height);
  }, [height]);

  // The last markup posted and when, for the throttle.
  const sent = useRef<{ html: string | null; at: number }>({ html: null, at: -Infinity });
  useEffect(() => {
    if (!loaded || html === null || html === sent.current.html) return;
    const post = () => {
      postToWidget(iframeRef.current?.contentWindow, { type: 'widget:streamHtml', html });
      sent.current = { html, at: performance.now() };
    };
    const wait = sent.current.at + STREAM_POST_MS - performance.now();
    if (wait <= 0) {
      post();
      return;
    }
    // A newer markup cancels this one and waits out the same interval.
    const timer = setTimeout(post, wait);
    return () => clearTimeout(timer);
  }, [html, loaded]);

  return (
    <div
      className="inline-widget-container widget-stream-container"
      data-overlay={overlay ? '' : undefined}
      data-empty={height == null ? '' : undefined}
      aria-hidden={overlay || undefined}
    >
      <iframe
        ref={iframeRef}
        srcDoc={srcDoc}
        {...WIDGET_FRAME}
        title={title || 'Widget'}
        className="inline-widget-frame"
        tabIndex={overlay ? -1 : undefined}
        onLoad={() => setLoaded(true)}
        style={{ height: `${height ?? 0}px` }}
      />
    </div>
  );
}
