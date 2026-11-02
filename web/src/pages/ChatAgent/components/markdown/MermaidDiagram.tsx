import { useEffect, useId, useState, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import type { ResolvedTheme } from '@/contexts/ThemeContext';

interface MermaidDiagramProps {
  code: string;
  /** Taken from the caller: an export draws for paper whatever the app's theme. */
  theme: ResolvedTheme;
  /** Shown until the diagram is drawn, and for good if it cannot be. */
  fallback: ReactNode;
}

// `strict` stops script, but a label is still HTML: it can hold a form that posts
// anywhere, media, or an id that lands on `window`, none of which the sanitizer
// around the rest of a message would pass. Parsed inert, so nothing here loads.
function withoutLabelMarkup(svg: string): string {
  const parsed = document.createElement('template');
  parsed.innerHTML = svg;
  parsed.content.querySelectorAll('foreignObject video, foreignObject audio').forEach((el) => el.remove());
  parsed.content.querySelectorAll('foreignObject [id], foreignObject [name]').forEach((el) => {
    el.removeAttribute('id');
    el.removeAttribute('name');
  });
  return parsed.innerHTML;
}

// Mermaid is a large dependency few threads use, so it loads with the first
// fence that asks for it. The result is a picture: what the model wrote can run
// no script, and `inert` takes every link and control in it out of reach, since
// a click there would skip what a link in the message gets.
export default function MermaidDiagram({ code, theme, fallback }: MermaidDiagramProps): ReactNode {
  const { t } = useTranslation();
  const id = `mermaid-${useId().replace(/[^a-zA-Z0-9]/g, '')}`;
  const [drawn, setDrawn] = useState<{ code: string; svg: string } | null>(null);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      const { default: mermaid } = await import('mermaid');
      mermaid.initialize({ startOnLoad: false, securityLevel: 'strict', suppressErrorRendering: true, theme: theme === 'light' ? 'neutral' : 'dark' });
      const { svg } = await mermaid.render(id, code);
      if (!cancelled) setDrawn({ code, svg: withoutLabelMarkup(svg) });
    })().catch(() => undefined);
    return () => { cancelled = true; };
  }, [code, id, theme]);

  if (drawn?.code !== code) return fallback;
  return (
    <div className="markdown-mermaid" role="img" aria-label={t('chat.diagram')}>
      <div inert dangerouslySetInnerHTML={{ __html: drawn.svg }} />
    </div>
  );
}
