import type { ComponentType } from 'react';
import { Blocks, BookOpen, Server } from 'lucide-react';
import { cn } from '@/lib/utils';
import { LangAlphaMark } from '@/components/brand/LangAlphaMark';
import { TILE_SIZES, type TileSize } from './IdentityTile';

/**
 * What a thing with no logo of its own looks like: what kind of thing it is.
 *
 * A monogram answers "which one is this" with a letter and a tint, and that is
 * the right answer for a vendor whose brand we happen not to ship. It is the
 * wrong answer for thirty skills, where every row is a different letter on a
 * different color and none of it carries meaning: the eye reads a palette and
 * learns nothing, while the row's name, the thing that actually identifies it,
 * competes with a colored square. One glyph per kind says the only true thing
 * available and hands identity back to the text.
 *
 * `langalpha` is the exception that proves the shape: it draws our own mark
 * rather than a category glyph, because on our own bundles the kind and the
 * brand are the same fact. It belongs here rather than in `brandArt` because
 * what it needs is a tile's color, not a URL.
 *
 * Same geometry as `IdentityTile` and `BrandMark`, so a row does not resize
 * when a logo arrives for something that had none.
 */
const KIND_GLYPH: Record<MarkKind, ComponentType<{ className?: string }>> = {
  plugin: Blocks,
  skill: BookOpen,
  server: Server,
  langalpha: LangAlphaMark,
};

export type MarkKind = 'plugin' | 'skill' | 'server' | 'langalpha';

const GLYPH_SIZES: Record<TileSize, string> = {
  sm: 'h-3.5 w-3.5',
  lg: 'h-5 w-5',
};

/** Our mark is a logo, not a label for a category, so it fills the tile the way
 *  a vendor's favicon does. Inset at glyph size it collapses into a smudge: the
 *  artwork carries far more line than a 14px box can hold. */
const MARK_SIZES: Record<TileSize, string> = {
  sm: 'h-6 w-6',
  lg: 'h-8 w-8',
};

export function KindTile({
  kind,
  size = 'sm',
  className = '',
}: {
  kind: MarkKind;
  size?: TileSize;
  className?: string;
}) {
  const Glyph = KIND_GLYPH[kind];
  // A category glyph is a quiet label and stays out of the way. Our mark is
  // the identity of the row, so it takes the contrast a fetched logo would
  // have had, which is also what makes it read as white on dark and black on
  // light without either file existing.
  const ours = kind === 'langalpha';
  return (
    <div
      aria-hidden
      className={cn(
        'flex-shrink-0 flex items-center justify-center border',
        TILE_SIZES[size],
        className,
      )}
      style={{
        backgroundColor: 'var(--color-bg-elevated)',
        borderColor: 'var(--color-border-subtle)',
        color: ours ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)',
      }}
    >
      <Glyph className={ours ? MARK_SIZES[size] : GLYPH_SIZES[size]} />
    </div>
  );
}
