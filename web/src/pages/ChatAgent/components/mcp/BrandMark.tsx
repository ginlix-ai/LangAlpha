import { useState } from 'react';
import { cn } from '@/lib/utils';
import { IdentityTile, TILE_SIZES, type TileSize } from './IdentityTile';
import { KindTile, type MarkKind } from './KindTile';
import type { BrandArt } from '@/lib/brandArt';

/**
 * A vendor's own logo where we have one, and a stand-in where we do not: the
 * same box either way, so a row does not resize when art arrives.
 *
 * The fallback is the point rather than a safety net. Art comes from a
 * vendor's live site or a bundled file, and both go missing (a rebrand moves a
 * path, an offline shell has no network, a vendor we do not ship has nothing
 * at all), so a failed load has to land somewhere deliberate rather than on a
 * broken-image glyph.
 *
 * Which stand-in is the caller's to say, because the two answer different
 * questions. `kind` draws the thing's kind, which is what a list of skills or
 * servers wants: they are not brands, and a per-name tint there is decoration
 * pretending to be information. Without it the name's monogram stands in,
 * which is right where every row IS a brand and the only missing piece is our
 * copy of its mark.
 *
 * Decorative in both states: the row's accessible name is its text.
 */
export function BrandMark({
  name,
  art,
  kind,
  size = 'sm',
  className,
}: {
  /** Seeds the monogram fallback and its tint; ignored when `kind` is given. */
  name: string;
  art?: BrandArt;
  /** Draw this kind's glyph instead of a monogram when there is no art. */
  kind?: MarkKind;
  size?: TileSize;
  className?: string;
}) {
  // Keyed by src rather than a boolean, so swapping the art re-arms the load
  // instead of inheriting the previous one's failure.
  const [failedSrc, setFailedSrc] = useState<string | null>(null);

  if (!art || failedSrc === art.src) {
    return kind ? (
      <KindTile kind={kind} size={size} className={className} />
    ) : (
      <IdentityTile name={name} size={size} className={className} />
    );
  }
  return (
    <img
      src={art.src}
      alt=""
      aria-hidden
      className={cn(
        'flex-shrink-0 object-contain',
        TILE_SIZES[size],
        // A logo drawn as a transparent dark glyph disappears on a dark
        // surface; the ones that ship that way say so and get a light bed.
        art.padded && 'bg-white p-0.5',
        className,
      )}
      onError={() => setFailedSrc(art.src)}
    />
  );
}
