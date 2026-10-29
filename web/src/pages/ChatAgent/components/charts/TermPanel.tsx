import React, { useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { ArrowUpRight, Check, Copy } from 'lucide-react';
import { copyText } from '@/lib/clipboard';
import { TermCard } from './TermCard';
import { TermPanelContext, useTermPanel } from './termContext';
import './termPanel.css';

/**
 * A tool's result in the detail panel, in the cards' language a size up:
 * unfilled frames with the title on the edge, figures in mono. Prose the
 * panel holds at length (a snippet, an item's description) stays in the text
 * face, which reads faster at that length.
 */
export function TermPanel({ children }: { children: React.ReactNode }): React.ReactElement {
  return (
    <TermPanelContext value={true}>
      <div className="term-panel">{children}</div>
    </TermPanelContext>
  );
}

/** The panel's first line: `[AMD] ADVANCED MICRO DEVICES ....... [CLOSED]`. */
export function TermPanelHead({
  title,
  sub,
  aside,
}: {
  title: React.ReactNode;
  sub?: React.ReactNode;
  /** At the line's far end: tags and links. */
  aside?: React.ReactNode;
}): React.ReactElement {
  return (
    <header className="term-panel-head">
      <span className="term-panel-title">
        <span className="term-bracket" aria-hidden="true">[</span>
        {title}
        <span className="term-bracket" aria-hidden="true">]</span>
      </span>
      {sub != null && sub !== '' && <span className="term-panel-sub">{sub}</span>}
      {aside && <span className="term-panel-aside">{aside}</span>}
    </header>
  );
}

/** A way out of the panel, `OPEN ON EDGAR ↗`: a link, or a button for an in-app route. */
export function TermLink({
  href,
  onClick,
  children,
}: {
  href?: string;
  onClick?: (e: React.MouseEvent) => void;
  children: React.ReactNode;
}): React.ReactElement {
  const body = (
    <>
      <span>{children}</span>
      <ArrowUpRight className="term-link-mark" aria-hidden="true" />
    </>
  );
  return href ? (
    <a className="term-link" href={href} target="_blank" rel="noopener noreferrer">{body}</a>
  ) : (
    <button type="button" className="term-link" onClick={onClick}>{body}</button>
  );
}

const COPIED_MS = 1500;

/** `COPY` at a section's edge, for a result read as raw text (an order id). */
export function TermCopy({ text }: { text: string }): React.ReactElement {
  const { t } = useTranslation();
  const [copied, setCopied] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => () => {
    if (timer.current) clearTimeout(timer.current);
  }, []);
  // Confirmed once the write lands (it rejects when the document has lost
  // focus); a second copy restarts the one timer rather than racing it.
  const copy = (): void => {
    copyText(text).then(() => {
      setCopied(true);
      if (timer.current) clearTimeout(timer.current);
      timer.current = setTimeout(() => setCopied(false), COPIED_MS);
    }).catch(() => {});
  };
  const Mark = copied ? Check : Copy;
  return (
    <button type="button" className="term-link" onClick={copy}>
      <span>{copied ? t('common.copied') : t('common.copy')}</span>
      <Mark className="term-link-mark" aria-hidden="true" />
    </button>
  );
}

const HEADING: React.CSSProperties = {
  fontSize: '0.8125rem',
  fontWeight: 600,
  color: 'var(--color-text-primary)',
  marginBottom: 8,
};

/** A chart under its title: a section of the panel there, a heading elsewhere. */
export function ChartFrame({ title, children }: { title: React.ReactNode; children: React.ReactNode }): React.ReactElement {
  const term = useTermPanel();
  if (term) {
    return (
      <TermCard title={title}>
        <div className="term-chart">{children}</div>
      </TermCard>
    );
  }
  return (
    <div>
      <h4 style={HEADING}>{title}</h4>
      {children}
    </div>
  );
}
