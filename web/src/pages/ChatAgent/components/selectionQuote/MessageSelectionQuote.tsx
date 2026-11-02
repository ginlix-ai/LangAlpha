import type { RefObject } from 'react';
import { useTranslation } from 'react-i18next';
import type { ContextPayload } from '@/components/ui/chat-input.types';
import { ImageReply } from './ImageReply';
import { lineCountOf, makeQuoteAnchor, type SelectionAnchor, type TextAnchor } from './quoteAnchor';
import { placeOnEdge } from './quotePlacement';
import { SelectionQuote, SelectionToolbar } from './SelectionQuote';
import { useHoveredBlock } from './useHoveredBlock';
import { useQuoteSession } from './useQuoteSession';
import { useTextSelectionCapture } from './useTextSelectionCapture';

const userBubbleOf = (node: Node) => (node.nodeType === Node.TEXT_NODE ? node.parentElement : node as Element)
  ?.closest?.('[data-message-role="user"]:not([data-report])');

/** Wholly the user's words: a report sits on their side but is none of theirs, and a drag can cross a reply. */
function sentByUser(anchor: TextAnchor): boolean {
  if (anchor.kind !== 'selection') return false;
  const { range } = anchor;
  if (!userBubbleOf(range.startContainer) || !userBubbleOf(range.endContainer)) return false;
  const within = range.commonAncestorContainer;
  if (within.nodeType === Node.TEXT_NODE) return true;
  return ![...(within as Element).querySelectorAll('[data-message-role="assistant"], [data-report]')].some((other) => range.intersectsNode(other));
}

function messageOf(anchor: TextAnchor): string | undefined {
  if (anchor.kind === 'rect') return undefined;
  const start = anchor.range.startContainer;
  const element = start.nodeType === Node.TEXT_NODE ? start.parentElement : start as Element;
  return element?.closest<HTMLElement>('[data-message-id]')?.dataset.messageId;
}

/** Quoting from the transcript. Keyed on whatever replaces the messages on screen. */
export function MessageSelectionQuote({
  surfaceRef, active, agentName, onAddContext,
}: {
  surfaceRef: RefObject<HTMLElement | null>;
  active: boolean;
  agentName: string;
  onAddContext: (ctx: ContextPayload) => void;
}) {
  const { t } = useTranslation();
  const { anchor, composing, show, beginReply, dismiss } = useQuoteSession<TextAnchor>();

  useTextSelectionCapture({
    surfaceRef,
    enabled: active && !composing,
    onSelect: (text, range, backward) => show(makeQuoteAnchor<SelectionAnchor>({ kind: 'selection', text, range, backward })),
    onClear: dismiss,
  });

  const hovered = useHoveredBlock({ surfaceRef, enabled: active && !anchor });

  const add = (quote: TextAnchor, reply?: string) => {
    onAddContext({
      kind: 'quote',
      snippet: quote.text,
      reply,
      from: {
        kind: 'message',
        speaker: sentByUser(quote) ? t('context.quote.sourceUser') : t('context.quote.sourceAgent', { name: agentName }),
        message: messageOf(quote),
      },
    });
    dismiss();
    window.getSelection()?.removeAllRanges();
  };

  const block = hovered.anchor;
  const lines = anchor ? lineCountOf(anchor.text) : 0;

  return (
    <>
      {/* Stays mounted beside a selection, so as not to lose a reply being written. */}
      <ImageReply surfaceRef={surfaceRef} enabled={active} paused={!!anchor} agentName={agentName} onAddContext={onAddContext} />
      {active && !anchor && block && (
        <SelectionToolbar
          key={block.id}
          anchor={block}
          boundsRef={surfaceRef}
          place={placeOnEdge}
          label={t('context.quote.blockToolbar')}
          addLabel={t('context.addToContext')}
          onReply={() => beginReply(block)}
          onAdd={() => add(block)}
          onLost={hovered.clear}
        />
      )}
      {active && anchor && (
        <SelectionQuote
          key={anchor.id}
          anchor={anchor}
          composing={composing}
          boundsRef={surfaceRef}
          label={t('context.quote.from', { name: sentByUser(anchor) ? t('context.quote.you') : agentName })}
          placeholder={t('context.quote.reply', { name: agentName })}
          addLabel={lines > 1 ? t('context.addNLinesToContext', { count: lines }) : t('context.addToContext')}
          onReply={() => beginReply(anchor)}
          onAdd={(reply) => add(anchor, reply)}
          onDismiss={dismiss}
        />
      )}
    </>
  );
}
