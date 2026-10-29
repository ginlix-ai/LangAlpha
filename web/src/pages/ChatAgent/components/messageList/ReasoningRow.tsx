import React, { memo, useEffect, useMemo, useState, useRef, useId } from 'react';
import { motion } from '@/lib/framer';
import { Brain, ChevronDown } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { TextShimmer } from '@/components/ui/text-shimmer';
import { useAnimatedText } from '@/components/ui/animated-text';
import Markdown from '../Markdown';
import { announceAnchoredToggle } from '../../utils/anchoredToggle';
import { extractLeadingBoldHeader, extractReasoningHeaders } from '../../utils/reasoningHeaders';
import { formatThoughtFor } from '@/lib/elapsed';
import { useNow } from '@/hooks/useNow';
import { SPRING_FOLD } from '@/lib/motion';
import { FoldPanel } from './FoldPanel';
import { RowIcon } from './ActivityRowParts';
import { SwapCell } from '@/components/ui/crossfade';
import type { ReasoningActivityItem } from './activityTypes';

function capitalizeFirst(text: string): string {
  return text.charAt(0).toUpperCase() + text.slice(1);
}

function useThinkingClock(startedAt: number | undefined, active: boolean, t: (k: string, o?: Record<string, unknown>) => string): string | null {
  const now = useNow(1000, active && !!startedAt);
  if (!active) return null;
  // `now` is state, so for up to one tick after mount it can predate `startedAt`.
  const raw = startedAt
    ? t('toolArtifact.thinkingFor', { duration: formatThoughtFor(Math.max(0, now - startedAt), t) })
    : t('toolArtifact.reasoningPending');
  return capitalizeFirst(raw);
}

interface AnimatedReasoningContentProps {
  content: string;
  isStreaming: boolean;
}

/** Module scope, not a literal: a fresh object would defeat Markdown's memo on every tick. */
const REASONING_STYLE = { opacity: 0.8 };

export function AnimatedReasoningContent({ content, isStreaming }: AnimatedReasoningContentProps): React.ReactElement {
  const displayText = useAnimatedText(content || '', { enabled: isStreaming });
  return (
    <Markdown
      variant="compact"
      content={displayText}
      className="text-xs"
      style={REASONING_STYLE}
      streaming={isStreaming || displayText !== content}
    />
  );
}

interface ReasoningRowProps {
  item: ReasoningActivityItem;
  /** Applies until the reader chooses within the current streaming/settled phase. */
  defaultExpanded?: boolean;
  /** The thought is still arriving: the header shimmers and an open body types. */
  isStreaming?: boolean;
}

const HEADER_STEP_MS = 700;

export const ReasoningRow = memo(function ReasoningRow({ item, defaultExpanded = true, isStreaming = false }: ReasoningRowProps): React.ReactElement {
  const { t } = useTranslation();
  const phase = isStreaming ? 'streaming' : 'settled';
  const [choice, setChoice] = useState<{ phase: typeof phase; expanded: boolean } | null>(null);
  const expanded = choice?.phase === phase ? choice.expanded : defaultExpanded;
  const contentId = useId();
  const { title: extractedTitle, body: extractedBody } = useMemo(
    () => extractLeadingBoldHeader(item.content || ''),
    [item.content],
  );
  const headers = useMemo(() => extractReasoningHeaders(item.content || ''), [item.content]);
  // The label is the thought's CURRENT phase: the newest header. When several
  // land at once (a summary that arrives whole), the label walks through them
  // one at a time so the reader sees the phases pass; on a row that was never
  // live (history) it opens on the last one. Only ever one header on screen.
  const wasLiveRef = useRef(isStreaming);
  // Marked from an effect, not render, and declared first so the walk below
  // reads it in the same commit.
  useEffect(() => {
    if (isStreaming) wasLiveRef.current = true;
  }, [isStreaming]);
  const [shownHeader, setShownHeader] = useState(() => Math.max(0, headers.length - 1));
  useEffect(() => {
    const last = headers.length - 1;
    if (shownHeader >= last) return;
    if (!wasLiveRef.current) { setShownHeader(last); return; }
    const id = setTimeout(() => setShownHeader((n) => Math.min(n + 1, last)), HEADER_STEP_MS);
    return () => clearTimeout(id);
  }, [headers.length, shownHeader]);
  const effectiveTitle = headers[Math.min(shownHeader, headers.length - 1)] || extractedTitle;
  // One header with a body under it: the body alone, since the label already
  // says the header. A run of phases: the whole thought, phases included, so
  // the open row reads as the sequence the label only shows one of.
  // `headers` counts phases, which need a blank line to separate them, so a
  // thought written `**Header**\nBody` promotes its header and counts zero.
  // Reading that as "no header" put the promoted line back at the top of the
  // body, and the open row showed it twice.
  const displayContent = headers.length <= 1 && extractedTitle ? extractedBody : item.content;
  const hasContent = !!displayContent;
  // A row with no header of its own is named by its time, not its text: a
  // line of thought copied up as a label is noise down a timeline. Live it
  // ticks, settled it states the duration, and only a thought whose duration
  // nothing recorded falls back to the bare word.
  const clock = useThinkingClock(item.reasoningStartedAt, isStreaming && !effectiveTitle, t);
  const elapsed = item.reasoningElapsedMs;
  const durationTitle = !effectiveTitle && !isStreaming && typeof elapsed === 'number'
    ? capitalizeFirst(t('toolArtifact.thoughtFor', { duration: formatThoughtFor(elapsed, t) }))
    : '';
  const title = effectiveTitle || clock || durationTitle || t('toolArtifact.reasoning');
  const titleKey = clock ? 'thinking' : title;
  // The label crossfades in place, keyed on its text, so a new phase reads as
  // arriving rather than the old words silently changing. The shimmer is an
  // inline-block whose overflow puts its baseline at its bottom edge, so on
  // the baseline it sat over the line's descender space and a thinking row
  // stood 3.4px taller than the thought it settled into.
  const titleNode = (
    <span className="titem-title inline-flex min-w-0" style={{ flexShrink: 1 }}>
      <SwapCell swapKey={titleKey} layerClassName="truncate">
        {isStreaming ? (
          <TextShimmer
            as="span"
            className="truncate align-top [--base-color:var(--color-text-primary)] [--base-gradient-color:var(--Labels-Tertiary)]"
            duration={1.5}
          >
            {title}
          </TextShimmer>
        ) : title}
      </SwapCell>
    </span>
  );

  return (
    <div className="titem">
      <div className="titem-icon">
        <RowIcon running={isStreaming} pulse={false}><Brain className="h-4 w-4" /></RowIcon>
      </div>
      <div className="titem-body" style={{ gap: 0 }}>
        <button
          type="button"
          onClick={(e) => { if (!hasContent) return; announceAnchoredToggle(e.currentTarget); setChoice({ phase, expanded: !expanded }); }}
          aria-expanded={hasContent ? expanded : undefined}
          aria-controls={hasContent ? contentId : undefined}
          className={`titem-line text-left bg-transparent border-0 p-0 ${hasContent ? 'cursor-pointer' : 'cursor-default'}`}
          style={{ color: 'inherit' }}
        >
          {titleNode}
          {hasContent && (
            <motion.div
              animate={{ rotate: expanded ? 90 : 0 }}
              transition={SPRING_FOLD}
              className="shrink-0 inline-flex items-center"
              style={{ opacity: 0.6, alignSelf: 'center' }}
            >
              <ChevronDown className="h-3 w-3 -rotate-90" />
            </motion.div>
          )}
        </button>
        <FoldPanel open={expanded && hasContent}>
          <div id={contentId} className="titem-reasoning-card">
            {isStreaming
              ? <AnimatedReasoningContent content={displayContent} isStreaming />
              : <Markdown variant="compact" content={displayContent} />}
          </div>
        </FoldPanel>
      </div>
    </div>
  );
});
