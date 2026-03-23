import { useState, useEffect, useMemo, useRef, useCallback } from 'react';
import './ChatMinimap.css';

type MessageRecord = Record<string, unknown>;

interface UserEntry {
  id: string;
  preview: string;
}

interface ChatMinimapProps {
  messages: MessageRecord[];
  scrollAreaRef: React.RefObject<HTMLDivElement | null>;
}

function getScrollViewport(scrollAreaRef: React.RefObject<HTMLDivElement | null>): HTMLElement | null {
  if (!scrollAreaRef.current) return null;
  // ScrollArea is a custom wrapper: outer div (overflow-hidden) > inner div (overflow-auto).
  // The inner div is the actual scrollable viewport we need for IntersectionObserver root.
  return (
    scrollAreaRef.current.querySelector<HTMLElement>('[data-radix-scroll-area-viewport]') ??
    scrollAreaRef.current.querySelector<HTMLElement>('.overflow-auto') ??
    scrollAreaRef.current
  );
}

export default function ChatMinimap({ messages, scrollAreaRef }: ChatMinimapProps) {
  const [isHovered, setIsHovered] = useState(false);
  const [visibleMessageId, setVisibleMessageId] = useState<string | null>(null);
  const entriesRef = useRef<HTMLDivElement>(null);

  // Extract user messages with preview text
  const userEntries: UserEntry[] = useMemo(() => {
    return (messages ?? [])
      .filter((m) => (m.role as string) === 'user')
      .map((m) => {
        const content = (m.content as string) || '';
        const preview = content.length > 60 ? content.slice(0, 60) + '...' : content;
        return { id: m.id as string, preview };
      });
  }, [messages]);

  // IntersectionObserver to track which user message is visible
  useEffect(() => {
    const viewport = getScrollViewport(scrollAreaRef);
    if (!viewport || userEntries.length === 0) return;

    const userIds = new Set(userEntries.map((e) => e.id));
    const visibleIds = new Map<string, number>(); // id -> top offset

    const observer = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          const id = (entry.target as HTMLElement).dataset.messageId;
          if (!id || !userIds.has(id)) continue;
          if (entry.isIntersecting) {
            visibleIds.set(id, entry.boundingClientRect.top);
          } else {
            visibleIds.delete(id);
          }
        }
        // Pick topmost visible user message
        let topId: string | null = null;
        let topY = Infinity;
        for (const [id, y] of visibleIds) {
          if (y < topY) {
            topY = y;
            topId = id;
          }
        }
        setVisibleMessageId(topId);
      },
      { root: viewport, threshold: 0.1 }
    );

    const elements = viewport.querySelectorAll<HTMLElement>('[data-message-id]');
    elements.forEach((el) => {
      if (userIds.has(el.dataset.messageId ?? '')) {
        observer.observe(el);
      }
    });

    return () => observer.disconnect();
  }, [scrollAreaRef, userEntries]);

  // Auto-scroll minimap entries to keep active entry visible
  useEffect(() => {
    if (!isHovered || !visibleMessageId || !entriesRef.current) return;
    const active = entriesRef.current.querySelector<HTMLElement>(`[data-minimap-id="${visibleMessageId}"]`);
    if (active) {
      active.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
    }
  }, [visibleMessageId, isHovered]);

  // Track whether user is scrolled to the bottom
  const [isAtBottom, setIsAtBottom] = useState(true);

  useEffect(() => {
    const viewport = getScrollViewport(scrollAreaRef);
    if (!viewport) return;
    const check = () => {
      const atBottom = viewport.scrollHeight - viewport.scrollTop - viewport.clientHeight < 40;
      setIsAtBottom(atBottom);
    };
    check();
    viewport.addEventListener('scroll', check, { passive: true });
    return () => viewport.removeEventListener('scroll', check);
  }, [scrollAreaRef, userEntries]);

  const handleClick = useCallback(
    (id: string) => {
      const viewport = getScrollViewport(scrollAreaRef);
      if (!viewport) return;
      const el = viewport.querySelector<HTMLElement>(`[data-message-id="${id}"]`);
      if (el) {
        el.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }
    },
    [scrollAreaRef],
  );

  const handleScrollToBottom = useCallback(() => {
    const viewport = getScrollViewport(scrollAreaRef);
    if (!viewport) return;
    viewport.scrollTo({ top: viewport.scrollHeight, behavior: 'smooth' });
  }, [scrollAreaRef]);

  if (userEntries.length < 2) return null;

  return (
    <div
      className="chat-minimap"
      /* width controlled by CSS :hover */
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
    >
      {/* Entries */}
      <div
        ref={entriesRef}
        className="chat-minimap-entries"
        style={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: isHovered ? 'stretch' : 'flex-end',
          justifyContent: 'center',
          flex: 1,
          padding: isHovered ? '12px 16px 12px 12px' : '12px 16px',
          gap: 4,
        }}
      >
        {userEntries.map((entry) => {
          const isActive = !isAtBottom && entry.id === visibleMessageId;
          return (
            <button
              key={entry.id}
              data-minimap-id={entry.id}
              onClick={() => handleClick(entry.id)}
              style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: isHovered ? 'flex-start' : 'flex-end',
                gap: 8,
                padding: isHovered ? '4px 6px' : '2px 0',
                borderRadius: 4,
                border: 'none',
                background: isHovered && isActive ? 'var(--color-bg-elevated, rgba(0,0,0,0.04))' : 'transparent',
                cursor: 'pointer',
                width: '100%',
                textAlign: 'left',
                transition: 'background 150ms ease',
              }}
              onMouseEnter={(e) => {
                if (isHovered && !isActive) {
                  e.currentTarget.style.background = 'var(--color-bg-subtle, rgba(0,0,0,0.02))';
                }
              }}
              onMouseLeave={(e) => {
                if (!isActive) {
                  e.currentTarget.style.background = 'transparent';
                }
              }}
            >
              {/* Text preview (only when expanded) */}
              {isHovered && (
                <span
                  style={{
                    flex: 1,
                    minWidth: 0,
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap',
                    fontSize: 12,
                    lineHeight: '18px',
                    color: isActive
                      ? 'var(--color-text-primary, #000)'
                      : 'var(--color-text-tertiary, #888)',
                    fontWeight: isActive ? 500 : 400,
                    transition: 'color 150ms ease',
                    textAlign: 'right',
                  }}
                >
                  {entry.preview}
                </span>
              )}

              {/* Line indicator */}
              <span
                style={{
                  display: 'block',
                  flexShrink: 0,
                  width: isActive ? 28 : 14,
                  height: isActive ? 3 : 2,
                  borderRadius: 2,
                  background: 'var(--color-foreground, #000)',
                  opacity: isActive ? 0.7 : 0.2,
                  transition: 'width 150ms ease, height 150ms ease, opacity 150ms ease',
                }}
              />
            </button>
          );
        })}

        {/* Bottom — always present, scrolls to very end of chat */}
        <button
          onClick={handleScrollToBottom}
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: isHovered ? 'flex-start' : 'flex-end',
            gap: 8,
            padding: isHovered ? '4px 6px' : '2px 0',
            marginTop: 4,
            borderRadius: 4,
            border: 'none',
            background: isHovered && isAtBottom ? 'var(--color-bg-elevated, rgba(0,0,0,0.04))' : 'transparent',
            cursor: 'pointer',
            width: '100%',
            textAlign: 'left',
            transition: 'background 150ms ease',
          }}
          onMouseEnter={(e) => {
            if (isHovered && !isAtBottom) {
              e.currentTarget.style.background = 'var(--color-bg-subtle, rgba(0,0,0,0.02))';
            }
          }}
          onMouseLeave={(e) => {
            if (!isAtBottom) {
              e.currentTarget.style.background = 'transparent';
            }
          }}
        >
          {isHovered && (
            <span
              style={{
                flex: 1,
                minWidth: 0,
                fontSize: 12,
                lineHeight: '18px',
                color: isAtBottom
                  ? 'var(--color-text-primary, #000)'
                  : 'var(--color-text-tertiary, #888)',
                fontWeight: isAtBottom ? 500 : 400,
                transition: 'color 150ms ease',
                textAlign: 'right',
              }}
            >
              Bottom
            </span>
          )}
          <span
            style={{
              display: 'block',
              flexShrink: 0,
              width: isAtBottom ? 28 : 14,
              height: isAtBottom ? 3 : 2,
              borderRadius: 2,
              background: 'var(--color-foreground, #000)',
              opacity: isAtBottom ? 0.7 : 0.2,
              transition: 'width 150ms ease, height 150ms ease, opacity 150ms ease',
            }}
          />
        </button>
      </div>
    </div>
  );
}
