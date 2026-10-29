import { useState } from 'react';
import { llmProviderArt, nameBesideMark } from '@/lib/brandArt';
import { cn } from '@/lib/utils';

/**
 * A model's maker, as the mark beside its name. Nothing for a provider we
 * ship no art for (a user's own endpoint), and nothing when the file fails to
 * load: at text size a stand-in monogram reads as noise, not as a brand.
 */
export function ModelMark({ provider, className }: { provider: string | undefined; className?: string }) {
  const art = provider ? llmProviderArt(provider) : undefined;
  // Keyed by src, so other art re-arms the load rather than inheriting a failure.
  const [failedSrc, setFailedSrc] = useState<string | null>(null);
  if (!art || failedSrc === art.src) return null;
  return (
    <img
      src={art.src}
      alt=""
      aria-hidden
      className={cn(
        'h-3.5 w-3.5 flex-none object-contain',
        // A black mark would vanish on the dark theme, so it is turned light
        // there; the half turn of hue keeps an accent near its own color.
        art.ink && 'invert hue-rotate-180 [[data-theme=light]_&]:filter-none',
        className,
      )}
      onError={() => setFailedSrc(art.src)}
    />
  );
}

/**
 * A model's mark and its name. `short` drops the brand word the mark stands
 * for, where room is tight (the composer's pill); a screen reader, which skips
 * the mark, still hears it. A list keeps full names, which is where models of
 * one maker are told apart.
 */
export function MarkedModelName({ provider, name, nameClassName, short = false }: {
  provider: string | undefined;
  name: string;
  nameClassName?: string;
  short?: boolean;
}) {
  const { shown, brand } = short ? nameBesideMark(name, provider) : { shown: name, brand: undefined };
  return (
    <>
      <ModelMark provider={provider} />
      <span className={nameClassName}>
        {brand && <span className="sr-only">{brand} </span>}
        {shown}
      </span>
    </>
  );
}
