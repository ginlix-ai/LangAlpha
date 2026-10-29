/**
 * The welcome flow as a conversation: the frame and the turns it is drawn
 * with. It reads as a chat thread does, the agent's lines down the left in
 * the transcript's own type and the user's answers in the user's bubbles, but
 * nothing here talks to the agent: every line is scripted and every answer is
 * picked from a card. The agent is handed the answers as the last question
 * is answered.
 */
import { useCallback, useEffect, useLayoutEffect, useRef, useState, type ReactNode, type Ref } from 'react';
import { Pencil } from 'lucide-react';
import FigureEightLoading from '@/components/ui/figure-eight-loading';
import { WizardTopBar } from '@/components/ui/wizard';
import { useIsMobile } from '@/hooks/useIsMobile';
import { useReducedMotion } from '@/lib/framer';
import { cn } from '@/lib/utils';
import TextMessageContent from '@/pages/ChatAgent/components/TextMessageContent';
import { UserBubble } from '@/pages/ChatAgent/components/messageList/UserBubble';
import { REVEAL_SETTLE_MS } from '@/pages/ChatAgent/utils/revealFade';
import { riseInRows } from './rows';
import './conversation.css';

interface ConversationShellProps {
  /** Right of the top bar: the quiet way out. */
  exit: ReactNode;
  /** The composer under the transcript, or none. */
  composer?: ReactNode;
  /** The composer has just become something to write in, and its edge says
   *  so until it is used. */
  composerCue?: boolean;
  scrollerRef?: Ref<HTMLDivElement>;
  children: ReactNode;
  testId?: string;
}

/**
 * The page: a top bar, the transcript in the chat's column (as wide as a
 * thread's, in its type), and the composer under it, where a thread keeps
 * it. Nothing of the app's chrome renders around it, so the top bar is the
 * window's titlebar on the macOS desktop shell. Its parts are marked for the
 * hand-off to the chat, which moves them into the app (handoff.css).
 */
export function ConversationShell({
  exit,
  composer,
  composerCue = false,
  scrollerRef,
  children,
  testId,
}: ConversationShellProps) {
  return (
    <div
      data-testid={testId}
      data-handoff-scope=""
      className="welcome-chat fixed inset-0 flex flex-col bg-(--color-bg-page)"
    >
      <WizardTopBar exit={exit} />
      <div
        ref={scrollerRef}
        data-handoff="transcript"
        className="welcome-chat__scroller min-h-0 flex-1 overflow-y-auto overflow-x-hidden overscroll-contain"
      >
        <div className="flex justify-center px-3 pb-10 pt-4 sm:px-6 sm:pt-8">
          <div className="font-content flex w-full max-w-3xl flex-col gap-4 sm:gap-6">{children}</div>
        </div>
      </div>
      {composer && (
        <div className="flex shrink-0 justify-center px-3 pb-3 sm:px-4 sm:pb-4">
          <div data-handoff="composer" className={cn('w-full max-w-3xl', composerCue && 'welcome-chat__composer--cue')}>
            {composer}
          </div>
        </div>
      )}
    </div>
  );
}

// A turn the user has just answered into waits this long before it writes, so
// the answer lands above it first and the two do not arrive together.
const LEAD_MS = 200;

interface AgentTurnProps {
  /** The text's id, for the card under it to be labelled by. */
  textId?: string;
  /** A first line set larger, for the greeting. */
  heading?: string;
  /** Markdown, as the agent writes. */
  text: string;
  /** Types in, as the agent's replies do, rather than standing there. A turn
   *  the user is just arriving at types; one they come back to does not. */
  typing?: boolean;
  /** What answers it: shown once the text has settled. */
  children?: ReactNode;
  turnRef?: Ref<HTMLDivElement>;
  className?: string;
  testId?: string;
}

/**
 * One of the agent's turns. The text is drawn by the transcript's own text
 * block, typewriter included, so a scripted line and a written one read the
 * same. What answers it waits for the line to finish and settle, its last
 * word faded all the way in, so the two never arrive together.
 */
export function AgentTurn({
  textId,
  heading,
  text,
  typing = false,
  children,
  turnRef,
  className,
  testId,
}: AgentTurnProps) {
  const still = useReducedMotion() ?? false;
  const animate = typing && !still;
  // The typewriter shows at once whatever it mounts with, so a turn that
  // types mounts empty and is handed its line after the lead; a line changed
  // later (the language switched) goes over on the next frame. The line is
  // handed over ended by a break: a stream's last word is held until
  // something follows it, or for a second of quiet, and nothing follows a
  // scripted line, so without the break its last word, and the card waiting
  // on it, sat out that second.
  const line = `${text}\n`;
  const [content, setContent] = useState(animate ? '' : line);
  // The line is all on screen, and then settled: its last word fades in for
  // a while after it is written.
  const [written, setWritten] = useState(!animate);
  const [settled, setSettled] = useState(!animate);
  useEffect(() => {
    if (content === line) return;
    if (content) {
      const frame = requestAnimationFrame(() => setContent(line));
      return () => cancelAnimationFrame(frame);
    }
    const timer = setTimeout(() => setContent(line), LEAD_MS);
    return () => clearTimeout(timer);
  }, [content, line]);
  const onRevealed = useCallback(
    (done: boolean) => {
      if (done && content) setWritten(true);
    },
    [content],
  );
  useEffect(() => {
    if (!written || settled) return;
    const timer = setTimeout(() => setSettled(true), REVEAL_SETTLE_MS);
    return () => clearTimeout(timer);
  }, [written, settled]);
  // The card comes up a row at a time, once, as it first appears; under
  // reduced motion it fades in whole (conversation.css).
  const hasCard = settled && !!children;
  const cardRef = useRef<HTMLDivElement>(null);
  const risenRef = useRef(false);
  useLayoutEffect(() => {
    const card = cardRef.current;
    if (!hasCard || !card || risenRef.current) return;
    risenRef.current = true;
    if (!still) riseInRows(card);
  }, [hasCard, still, animate]);

  return (
    <div
      ref={turnRef}
      data-testid={testId}
      className={cn('flex scroll-mt-4 items-start justify-start sm:scroll-mt-8', className)}
    >
      <div className="w-full min-w-0">
        {heading && (
          <h1 className="title-font mb-3 text-2xl font-semibold leading-tight text-(--color-text-primary)">
            <span>{heading}</span>
          </h1>
        )}
        <div id={textId} className="welcome-chat__line text-(--color-text-primary)">
          <TextMessageContent content={content} isStreaming={!written} onRevealed={onRevealed} />
        </div>
        {hasCard && (
          <div ref={cardRef} className={cn('mt-4', still && 'welcome-chat__rise')}>
            {children}
          </div>
        )}
      </div>
    </div>
  );
}

/**
 * The user's answer to a turn, in the bubble their messages take. Changing it
 * goes back to the turn that asked, as editing a message does in a thread:
 * what came after is asked again, with its answers kept.
 */
export function UserTurn({
  children,
  onEdit,
  editLabel,
  task = false,
  rise = false,
  testId,
}: {
  children: ReactNode;
  onEdit?: () => void;
  editLabel: string;
  /** A first task the user wrote, which the hand-off carries into the thread
   *  as their message. */
  task?: boolean;
  /** Comes up as it lands, for the answer just given: the card it folds from
   *  goes at once, and a bubble that stood there at once read as a cut. */
  rise?: boolean;
  testId?: string;
}) {
  const isMobile = useIsMobile();
  return (
    <div
      data-testid={testId}
      data-handoff={task ? 'task' : undefined}
      className={cn('group flex items-start justify-end gap-1', rise && 'welcome-chat__rise')}
    >
      {onEdit && (
        <button
          type="button"
          onClick={onEdit}
          aria-label={editLabel}
          title={editLabel}
          className="mt-1.5 inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-md text-(--color-text-tertiary) opacity-0 transition-[opacity,color,background-color] hover:bg-(--color-bg-hover) hover:text-(--color-text-primary) focus-visible:opacity-100 group-hover:opacity-100 [@media(hover:none)]:opacity-100"
        >
          <Pencil className="h-3.5 w-3.5" />
        </button>
      )}
      <UserBubble isMobile={isMobile} className="max-w-[80%]">
        <p className="whitespace-pre-wrap text-base leading-normal">
          <span>{children}</span>
        </p>
      </UserBubble>
    </div>
  );
}

/**
 * The agent at work on the turn it was handed, before the thread opens. It is
 * the mark a thread draws while its agent works, at the same size and in the
 * same color, so the hand-off carries this one into the thread as the same
 * object. What it waits on goes under it, where a thread's status lines go.
 */
export function AgentPending({ label, children, testId }: { label: string; children?: ReactNode; testId?: string }) {
  const isMobile = useIsMobile();
  return (
    <div className="welcome-chat__rise flex flex-col gap-2" aria-live="polite" data-testid={testId}>
      <div role="status" data-handoff="agent" className="relative">
        <FigureEightLoading className={cn(isMobile ? 'h-[20px]' : 'h-[24px]', 'text-(--color-accent-primary)')} />
        <span className="sr-only">{label}</span>
      </div>
      {children && <span className="text-sm text-(--color-text-secondary)">{children}</span>}
    </div>
  );
}

/**
 * What a turn is answered with: its controls under the agent's line, and the
 * way on in a row at their foot. Unframed, since the controls are cards
 * already: a frame around them would be a card in a card.
 */
export function TurnCard({
  children,
  footer,
  className,
  testId,
}: {
  children: ReactNode;
  footer?: ReactNode;
  className?: string;
  testId?: string;
}) {
  return (
    <div data-testid={testId} className={className}>
      {children}
      {footer && <div className="mt-4 flex flex-wrap items-center justify-end gap-2">{footer}</div>}
    </div>
  );
}
