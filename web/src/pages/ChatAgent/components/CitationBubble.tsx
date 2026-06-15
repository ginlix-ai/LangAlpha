import React from 'react';
import * as HoverCard from '@radix-ui/react-hover-card';
import { useCitationMetadata } from './CitationMetadataContext';
import { Favicon } from './Favicon';
import './CitationBubble.css';

type MarkdownComponentProps = Record<string, any>;

function CitationBubble({ node: _node, label, href, ...props }: MarkdownComponentProps): React.ReactElement {
  const meta = useCitationMetadata(href || '');
  const domain = label || '';
  const displayName = meta?.source || domain.replace(/\.[^.]+$/, '') || domain;
  const url = href || '';

  // Note: Radix HoverCard is mouse-only by design. Keyboard/screen-reader users
  // can still click through via Enter/Space but won't see the preview card.
  // Switch to Popover if keyboard preview becomes a requirement.
  return (
    <HoverCard.Root openDelay={300} closeDelay={100}>
      <HoverCard.Trigger asChild>
        <a
          href={url || undefined}
          target="_blank"
          rel="noopener noreferrer"
          aria-label={`Source: ${displayName}`}
          onClick={(e) => { if (!url || !/^https?:\/\//.test(url)) e.preventDefault(); }}
          className="cite-bubble-pill"
          style={{
            display: 'inline-flex',
            alignItems: 'center',
            gap: 4,
            padding: '1px 8px 1px 6px',
            borderRadius: 9999,
            background: 'var(--color-bg-elevated)',
            fontSize: '0.8125rem',
            fontWeight: 400,
            color: 'var(--color-text-secondary)',
            textDecoration: 'none',
            lineHeight: 1.4,
            verticalAlign: 'baseline',
            cursor: 'pointer',
            transition: 'background 150ms ease',
            marginLeft: 2,
            maxWidth: 200,
            whiteSpace: 'nowrap' as const,
          }}
          {...props}
        >
          <Favicon domain={domain} size={14} />
          <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            {displayName}
          </span>
        </a>
      </HoverCard.Trigger>

      <HoverCard.Portal>
        <HoverCard.Content
          sideOffset={5}
          style={{
            width: 320,
            borderRadius: 10,
            overflow: 'hidden',
            boxShadow: 'var(--shadow-card, 0 4px 16px rgba(0,0,0,0.12))',
            border: '1px solid var(--color-border-muted)',
            background: 'var(--color-bg-card)',
            zIndex: 100,
            animationDuration: '150ms',
          }}
        >
          <div style={{ padding: '12px 14px' }}>
            {/* Source row */}
            <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 8 }}>
              <Favicon domain={domain} size={16} />
              <span style={{
                fontSize: 12,
                fontWeight: 500,
                color: 'var(--color-text-tertiary)',
                letterSpacing: '0.01em',
              }}>
                {displayName}
              </span>
            </div>

            {/* Title or fallback URL */}
            {meta?.title ? (
              <a
                href={url}
                target="_blank"
                rel="noopener noreferrer"
                className="cite-bubble-card-title"
                style={{
                  fontSize: 14,
                  fontWeight: 500,
                  lineHeight: 1.45,
                  color: 'var(--color-text-primary)',
                  textDecoration: 'none',
                  display: '-webkit-box',
                  WebkitLineClamp: 3,
                  WebkitBoxOrient: 'vertical' as const,
                  overflow: 'hidden',
                }}
              >
                {meta.title}
              </a>
            ) : (
              <a
                href={url}
                target="_blank"
                rel="noopener noreferrer"
                style={{
                  fontSize: 12,
                  color: 'var(--color-accent-primary)',
                  textDecoration: 'none',
                  display: 'block',
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap' as const,
                  lineHeight: 1.5,
                }}
              >
                {url}
              </a>
            )}

            {/* Date + Snippet */}
            {(meta?.date || meta?.snippet) && (
              <div style={{
                fontSize: 12,
                color: 'var(--color-text-tertiary)',
                marginTop: 6,
                lineHeight: 1.5,
                display: '-webkit-box',
                WebkitLineClamp: 2,
                WebkitBoxOrient: 'vertical' as const,
                overflow: 'hidden',
              }}>
                {meta.date && meta.snippet
                  ? `${meta.date} – ${meta.snippet}`
                  : meta.date || meta.snippet}
              </div>
            )}
          </div>
        </HoverCard.Content>
      </HoverCard.Portal>
    </HoverCard.Root>
  );
}

export default CitationBubble;
