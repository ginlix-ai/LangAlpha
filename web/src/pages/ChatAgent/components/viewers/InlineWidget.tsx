import { useEffect, useMemo, useRef, useState } from 'react';
import { registerAuthReset } from '@/lib/authResets';
import { Loader } from '@/components/ui/loader';
import { buildHtmlSrcDoc } from './html/buildHtmlSrcDoc';
import { useHtmlSandbox } from './html/useHtmlSandbox';
import { useHtmlActions } from './html/useHtmlActions';
import HtmlActionBar from './html/HtmlActionBar';
import HtmlFullscreenModal from './html/HtmlFullscreenModal';
import './InlineWidget.css';

interface InlineWidgetProps {
  html: string;
  title?: string;
  onSendPrompt?: (text: string) => void;
  /** Inline data file contents — injected directly as __WIDGET_DATA__. */
  data?: Record<string, string>;
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
  let h = 5381;
  for (let i = 0; i < html.length; i++) h = ((h << 5) + h + html.charCodeAt(i)) | 0;
  let dataSig = '';
  if (data) {
    for (const k of Object.keys(data)) dataSig += `${k}:${data[k].length};`;
  }
  return `${h}|${html.length}|${dataSig}`;
}

export default function InlineWidget({ html, title, onSendPrompt, data }: InlineWidgetProps) {
  const iframeRef = useRef<HTMLIFrameElement>(null);
  const [fullscreen, setFullscreen] = useState(false);

  const srcDoc = useMemo(() => buildHtmlSrcDoc('widget-inline', { html, data }), [html, data]);
  const fullscreenSrcDoc = useMemo(
    () => buildHtmlSrcDoc('widget-fullscreen', { html, data }),
    [html, data],
  );

  const { height } = useHtmlSandbox({ iframeRef, autoHeight: true, onSendPrompt });

  const heightKey = useMemo(() => widgetHeightKey(html, data), [html, data]);
  const lastRecorded = useRef<number | null>(null);
  useEffect(() => {
    // Record only fresh measurements — a key change alone must not re-file the
    // previous document's height under the new content's key.
    if (height != null && height !== lastRecorded.current) {
      lastKnownHeights.set(heightKey, height);
      lastRecorded.current = height;
    }
  }, [height, heightKey]);

  // Live report wins; cached height bridges the load; 150px is the cold guess.
  const displayHeight = height ?? lastKnownHeights.get(heightKey) ?? null;

  const actions = useHtmlActions({ mode: 'widget', srcDoc, fileName: title });
  const fullscreenActions = useHtmlActions({
    mode: 'widget',
    srcDoc: fullscreenSrcDoc,
    fileName: title,
  });

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
        sandbox="allow-scripts allow-popups allow-popups-to-escape-sandbox"
        title={title || 'Widget'}
        className="inline-widget-frame"
        style={{
          height: displayHeight != null ? `${displayHeight}px` : '150px',
          opacity: displayHeight != null ? 1 : 0,
        }}
      />
      {height == null && (
        // Shown until the document reports its first measurement — even when a
        // cached height already reserved the box, the content is still loading.
        <span className="inline-widget-loading">
          <Loader
           
            size={14}
           
            label="Rendering widget"
            className="text-[color:var(--color-text-tertiary)]"
          />
        </span>
      )}
      <HtmlActionBar
        variant="overlay"
        onFullscreen={() => setFullscreen(true)}
        onOpenInNewTab={actions.openInNewTab}
        onDownload={actions.downloadHtml}
        onExportPdf={actions.exportPdf}
      />
      {fullscreen && (
        <HtmlFullscreenModal
          variant="widget"
          open={fullscreen}
          onOpenChange={setFullscreen}
          title={title || 'Widget'}
          srcDoc={fullscreenSrcDoc}
          actions={fullscreenActions}
        />
      )}
    </div>
  );
}
