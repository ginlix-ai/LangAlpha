import { useEffect, useRef, useState, type RefObject } from 'react';
import { createPortal } from 'react-dom';
import { useTranslation } from 'react-i18next';
import { MessageSquare } from 'lucide-react';
import { useToast } from '@/components/ui/use-toast';
import type { ContextPayload } from '@/components/ui/chat-input.types';
import { makeQuoteAnchor, surfaceProps, type ImageAnchor } from './quoteAnchor';
import { placeAtCorner, useAnchoredPosition } from './quotePlacement';
import { QuoteComposer } from './SelectionQuote';
import { useHoverAnchor } from './useHoverAnchor';

/** Below this an image is an icon or an avatar, not something to talk about. */
const MIN_WIDTH = 64;
const MIN_HEIGHT = 48;

function imageName(img: HTMLImageElement, type: string): string {
  const ext = type.split('/')[1]?.split('+')[0] || 'png';
  const name = (img.alt || 'image').trim().replace(/[\\/:*?"<>|]+/g, '-');
  return /\.[a-z0-9]{2,5}$/i.test(name) ? name : `${name}.${ext}`;
}

function quotableImage(target: Element, event: PointerEvent): HTMLImageElement | null {
  const large = target instanceof HTMLImageElement && target.offsetWidth >= MIN_WIDTH && target.offsetHeight >= MIN_HEIGHT;
  return large && event.buttons === 0 ? target : null;
}

const imageAnchor = (image: HTMLImageElement) => makeQuoteAnchor<ImageAnchor>({ kind: 'image', image });

/** The bytes the page already shows. Another site's image may refuse this. */
async function fileFromImage(img: HTMLImageElement): Promise<File | null> {
  try {
    const response = await fetch(img.currentSrc || img.src);
    const blob = await response.blob();
    // A refusal has a body too, and it is no image.
    if (!response.ok || (blob.type && !blob.type.startsWith('image/'))) return null;
    return new File([blob], imageName(img, blob.type), { type: blob.type || 'image/png' });
  } catch {
    return null;
  }
}

function ReplyButton({ anchor, boundsRef, onReply, onLost }: {
  anchor: ImageAnchor;
  boundsRef: RefObject<HTMLElement | null>;
  onReply: () => void;
  onLost: () => void;
}) {
  const { t } = useTranslation();
  const ref = useRef<HTMLDivElement>(null);
  const { position, offscreen } = useAnchoredPosition(anchor, ref, boundsRef, placeAtCorner, onLost);
  const label = t('context.quote.replyToImage');
  return createPortal(
    <div
      ref={ref}
      {...surfaceProps}
      className="selection-quote-toolbar"
      style={{ left: position?.left ?? 0, top: position?.top ?? 0, visibility: position && !offscreen ? 'visible' : 'hidden' }}
    >
      <button type="button" className="selection-quote-icon" title={label} aria-label={label} onClick={onReply}>
        <MessageSquare className="h-4 w-4" />
      </button>
    </div>,
    document.body,
  );
}

/**
 * Replying to an image, wherever one shows on `surfaceRef`. The image goes out
 * as an attachment, so the agent sees what the user sees, and the reply becomes
 * the draft's words.
 */
export function ImageReply({ surfaceRef, enabled, paused = false, agentName, onAddContext }: {
  surfaceRef: RefObject<HTMLElement | null>;
  enabled: boolean;
  /** Something else has the pointer's attention: nothing new opens, what is open stays. */
  paused?: boolean;
  agentName?: string;
  onAddContext?: ((ctx: ContextPayload) => void) | null;
}) {
  const { t } = useTranslation();
  const { toast } = useToast();
  // Held on its own: hovering stops while a reply is written, and takes its anchor with it.
  const [replyingTo, setReplyingTo] = useState<ImageAnchor | null>(null);
  const hovered = useHoverAnchor({ surfaceRef, enabled: enabled && !paused && !replyingTo, resolve: quotableImage, makeAnchor: imageAnchor });

  // `onAddContext` reaches whichever draft the composer holds by then, so a fetch
  // outliving this view must not hand it what was meant for this one.
  const viewRef = useRef(0);
  useEffect(() => () => {
    viewRef.current += 1;
  }, [enabled]);

  if (!replyingTo) {
    if (!enabled || !hovered.anchor) return null;
    const anchor = hovered.anchor;
    return <ReplyButton anchor={anchor} boundsRef={surfaceRef} onReply={() => setReplyingTo(anchor)} onLost={hovered.clear} />;
  }

  const { image } = replyingTo;
  const add = (reply?: string) => {
    setReplyingTo(null);
    const view = viewRef.current;
    void fileFromImage(image).then((file) => {
      if (viewRef.current !== view) {
        toast({ variant: 'destructive', title: t('context.quote.imageFailed') });
      } else if (file) {
        onAddContext?.({ kind: 'attachment', files: [file], text: reply });
      } else if (/^https?:/i.test(image.src)) {
        // Refused its bytes, the address still says which image is meant.
        onAddContext?.({ kind: 'attachment', files: [], text: [image.src, reply].filter(Boolean).join('\n') });
      } else {
        toast({ variant: 'destructive', title: t('context.quote.imageFailed') });
      }
    });
  };

  return (
    <QuoteComposer
      key={replyingTo.id}
      anchor={replyingTo}
      boundsRef={surfaceRef}
      label={t('context.quote.replyToImage')}
      placeholder={agentName ? t('context.quote.reply', { name: agentName }) : t('context.quote.replyGeneric')}
      hidden={!enabled}
      onAdd={add}
      onDismiss={() => setReplyingTo(null)}
    />
  );
}
