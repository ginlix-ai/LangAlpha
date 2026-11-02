import { useMemo, type ComponentProps } from 'react';
import { ChartCandlestick, Quote } from 'lucide-react';
import { quotePreview } from '@/components/ui/chat-input.quoteTokens';
import { QuoteCard } from '@/components/ui/chat-input.quoteCard';
import { parseSentMessage, type SentPart } from '@/components/ui/chat-input.wire';
import '@/components/ui/chat-input.css';
import TextMessageContent from '../TextMessageContent';
import { markDraftTokens } from './sentMessage';

/** The chip it was in the composer, folded, with the rest on hover. */
function SentContext({ source, body, reply, chart }: { source?: string; body: string; reply?: string; chart?: boolean }) {
  // A chart's body is a hint for the agent, with nothing in it to read here.
  if (chart) {
    return (
      <span className="composer-quote-chip" title={source}>
        <ChartCandlestick className="h-3.5 w-3.5 shrink-0" />
        <span>{source}</span>
      </span>
    );
  }
  return (
    <QuoteCard quote={body} reply={reply} source={source}>
      <button type="button" className="composer-quote-chip text-left">
        <Quote className="h-3.5 w-3.5 shrink-0" />
        <span>{quotePreview(body)}</span>
      </button>
    </QuoteCard>
  );
}

/** A user's message: their words and their chips where they put them, row by row. */
export function SentMessageBody({ content, ...text }: { content: string } & Omit<ComponentProps<typeof TextMessageContent>, 'content'>) {
  const rows = useMemo(() => {
    const split: SentPart[][] = [[]];
    for (const part of parseSentMessage(content)) {
      if (part.kind === 'break') split.push([]);
      else split.at(-1)!.push(part);
    }
    return split;
  }, [content]);
  return (
    <div className="flex flex-col gap-1">
      {rows.map((row, r) => (
        <div key={r} className="flex flex-wrap items-baseline gap-x-2 gap-y-1">
          {row.map((part, i) => (part.kind === 'text'
            ? <div key={i} className="min-w-0"><TextMessageContent {...text} content={markDraftTokens(part.text)} /></div>
            : part.kind === 'context' && <SentContext key={i} {...part} />))}
        </div>
      ))}
    </div>
  );
}
