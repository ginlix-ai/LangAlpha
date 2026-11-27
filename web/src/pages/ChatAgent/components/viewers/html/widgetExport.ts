import { escapeAttr, escapeText, scriptJson } from './docUtils';
import { WIDGET_FRAME } from './sandbox';
import {
  PRINT_HEIGHT_SLACK,
  PRINT_HEIGHT_SLACK_PX,
  PRINT_WIDTH_PX,
  WIDGET_MAX_HEIGHT_PX,
  WIDGET_MAX_WAIT_MS,
  WIDGET_SETTLE_MS,
} from './widgetProtocol';

/**
 * The copies of a widget that leave its frame: a download, a new tab and the
 * browser's print.
 */

/** Past these an image stays a reference: a download is no place for a video-sized blob. */
const MAX_IMAGE_BYTES = 10 * 1024 * 1024;
const MAX_TOTAL_BYTES = 25 * 1024 * 1024;
const IMAGE_WAIT_MS = 15_000;

/** A srcset whose URLs carry no comma (a data: URL does), as `[url, descriptor]` pairs. */
/**
 * The candidates of a `srcset` as HTML splits them: a URL runs to the next
 * ASCII whitespace (a no-break space is part of it), so a data: URL keeps its commas, and a comma ends a candidate
 * only after its URL. Null for one that does not split cleanly.
 */
function srcsetParts(srcset: string): Array<[string, string]> | null {
  const parts: Array<[string, string]> = [];
  const re = /[\t\n\f\r ,]*([^\t\n\f\r ]+)/y;
  let at = 0;
  while (at < srcset.length) {
    re.lastIndex = at;
    const m = re.exec(srcset);
    if (!m) break;
    let url = m[1];
    at = re.lastIndex;
    let descriptor = '';
    if (url.endsWith(',')) {
      url = url.replace(/,+$/, '');
    } else {
      const end = srcset.indexOf(',', at);
      descriptor = srcset.slice(at, end < 0 ? srcset.length : end);
      at = end < 0 ? srcset.length : end + 1;
      if (!/^([\t\n\f\r ]+\d*\.?\d+(e[+-]?\d+)?[wxh])?[\t\n\f\r ]*$/i.test(descriptor)) return null;
      descriptor = descriptor.replace(/[\t\n\f\r ]+$/, '');
    }
    if (!url) return null;
    parts.push([url, descriptor]);
  }
  return parts.length > 0 ? parts : null;
}

async function dataUrl(blob: Blob): Promise<string> {
  const bytes = new Uint8Array(await blob.arrayBuffer());
  let binary = '';
  for (let i = 0; i < bytes.length; i += 0x8000) {
    binary += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  }
  return `data:${blob.type || 'application/octet-stream'};base64,${btoa(binary)}`;
}

/**
 * The widget's markup with every `<img src>` and simple `srcset` that
 * resolves under `base` (the workspace folder under the viewer's grant) turned
 * into a data: URL, so the copy shows its images without carrying the grant.
 *
 * Synchronous, and the markup untouched, when nothing is under the base, so
 * the common open-in-new-tab stays inside its click. A reference under the
 * base that is not inlined (it failed, or is too large) is dropped: written
 * absolute it would carry the grant, and written relative it loads nothing in
 * a copy with no base. So does any path built in script, a stylesheet or a
 * script file: a copy is a snapshot of what the markup names, not a second
 * live view.
 */
export function inlineWorkspaceImages(
  html: string,
  base: string,
  fetchImpl: typeof fetch = (...args) => fetch(...args),
): string | Promise<string> {
  const doc = new DOMParser().parseFromString(
    `<!DOCTYPE html><html><head><base href="${escapeAttr(base)}"></head><body>${html}</body></html>`,
    'text/html',
  );
  const under = (ref: string): string | null => {
    try {
      const url = new URL(ref, base).href;
      return url.startsWith(base) ? url : null;
    } catch {
      return null;
    }
  };

  const basePath = new URL(base).pathname;
  const edits: Array<() => void> = [];
  let dropped = false;
  const wanted = new Set<string>();
  const inlined = new Map<string, string>();
  for (const el of Array.from(doc.body.querySelectorAll('img[src], img[srcset], picture source[srcset]'))) {
    const src = el.getAttribute('src');
    const srcUrl = src ? under(src) : null;
    if (srcUrl) {
      wanted.add(srcUrl);
      edits.push(() => {
        const replaced = inlined.get(srcUrl);
        if (replaced) el.setAttribute('src', replaced);
        else el.removeAttribute('src');
      });
    }
    const srcset = el.getAttribute('srcset');
    const parts = srcset ? srcsetParts(srcset) : null;
    if (parts && parts.some(([u]) => under(u))) {
      for (const [u] of parts) {
        const url = under(u);
        if (url) wanted.add(url);
      }
      edits.push(() => {
        const kept = parts.flatMap(([u, d]) => {
          const url = under(u);
          if (!url) return [`${u}${d}`];
          const replaced = inlined.get(url);
          return replaced ? [`${replaced}${d}`] : [];
        });
        if (kept.length) el.setAttribute('srcset', kept.join(', '));
        else el.removeAttribute('srcset');
      });
    } else if (!parts && srcset?.includes(basePath)) {
      // Not split into candidates, so not inlined, and it names the grant:
      // the src is the copy's image instead.
      el.removeAttribute('srcset');
      dropped = true;
    }
  }
  if (wanted.size === 0) return dropped ? doc.body.innerHTML : html;

  return (async () => {
    let total = 0;
    await Promise.all(
      Array.from(wanted, async (url) => {
        try {
          // The grant is in the URL; nothing of the app's should ride along.
          // Bounded, body included, so one stalled image is dropped rather
          // than holding the copy back.
          const res = await fetchImpl(url, { credentials: 'omit', signal: AbortSignal.timeout(IMAGE_WAIT_MS) });
          if (!res.ok) return;
          const blob = await res.blob();
          if (blob.size > MAX_IMAGE_BYTES || total + blob.size > MAX_TOTAL_BYTES) return;
          total += blob.size;
          inlined.set(url, await dataUrl(blob));
        } catch {
          /* dropped by its edit */
        }
      }),
    );
    edits.forEach((edit) => edit());
    return doc.body.innerHTML;
  })();
}

export interface WidgetShellOptions {
  title: string;
  /** The `:root` declarations the shell wears around the frame (`paletteCss`). */
  themeCss: string;
  /**
   * Print once the widget stops changing height, after pushing `themeCss`
   * into it (the light palette: paper is white whatever the app wears).
   */
  print?: boolean;
}

/**
 * A page that frames a widget document under the widget sandbox, for a copy
 * opened in a tab of its own. A `blob:` the app makes has the app's origin, so
 * the widget document opened directly would run the agent's script with the
 * app's cookies and storage, and with a print handle on the opener, the app
 * window itself. Framed, it keeps the opaque origin it has in the chat; the
 * shell holds no script but this one, which sizes the frame from the
 * widget's own height reports, as the chat does, so the tab scrolls as a page.
 *
 * For print the frame is laid out at paper width (`PRINT_WIDTH_PX`) from the
 * start, so the height it reports is the height it prints at, and the box gets
 * the same slack the desktop's PDF gives it (widgetPdf.ts), once, at print:
 * given to the frame as it reports, a widget sized to its viewport would
 * report the slack back and grow on every round.
 */
export function buildWidgetShell(doc: string, { title, themeCss, print = false }: WidgetShellOptions): string {
  const script = `(function(PRINT,THEME){
  var f=document.getElementById('widget'),printed=false,quiet=0,last=0;
  function done(){if(printed)return;printed=true;if(last)f.style.height=(Math.ceil(last*${PRINT_HEIGHT_SLACK})+${PRINT_HEIGHT_SLACK_PX})+'px';try{window.print();}catch(e){}}
  addEventListener('message',function(e){
    if(e.source!==f.contentWindow||!e.data||e.data.type!=='widget:resize')return;
    var h=e.data.height;
    if(typeof h!=='number'||!isFinite(h)||h<=0)return;
    last=Math.min(Math.ceil(h),${WIDGET_MAX_HEIGHT_PX});
    f.style.height=last+'px';
    if(PRINT){clearTimeout(quiet);quiet=setTimeout(done,${WIDGET_SETTLE_MS});}
  });
  if(PRINT){
    f.addEventListener('load',function(){f.contentWindow.postMessage({type:'widget:themeUpdate',css:THEME},'*');});
    setTimeout(done,${WIDGET_MAX_WAIT_MS});
  }
})(${print},${scriptJson(themeCss)});`;
  return `<!DOCTYPE html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${escapeText(title)}</title>
<style>
:root {
  ${themeCss}
}
html, body { margin: 0; background: var(--color-bg-page); }
body { padding: ${print ? '0' : '24px 16px'}; }
#widget { display: block; width: ${print ? `${PRINT_WIDTH_PX}px` : '100%'}; max-width: ${print ? 'none' : '768px'}; height: 150px; margin: 0 auto; border: 0; }
</style>
</head><body>
<iframe id="widget" title="${escapeAttr(title)}" sandbox="${WIDGET_FRAME.sandbox}" allow="${WIDGET_FRAME.allow}" srcdoc="${escapeAttr(doc)}"></iframe>
<script>
${script}
</script>
</body></html>`;
}
