import { cn } from '@/lib/utils';

/**
 * The per-item identity mark on Plugins / MCP rows: a rounded tile whose tint
 * derives from the item's name, with the name's initial as a monogram. The
 * same name always lands on the same tint, on every surface — that stability
 * is the whole point, so never salt the hash with the item's kind or origin.
 * Purely decorative: the accessible name of a row stays its text.
 */

// Literal pairs (not computed names) so the tokenRefs guard can see them.
const TINTS: ReadonlyArray<readonly [bg: string, fg: string]> = [
  ['var(--color-identity-0-bg)', 'var(--color-identity-0-fg)'],
  ['var(--color-identity-1-bg)', 'var(--color-identity-1-fg)'],
  ['var(--color-identity-2-bg)', 'var(--color-identity-2-fg)'],
  ['var(--color-identity-3-bg)', 'var(--color-identity-3-fg)'],
  ['var(--color-identity-4-bg)', 'var(--color-identity-4-fg)'],
  ['var(--color-identity-5-bg)', 'var(--color-identity-5-fg)'],
  ['var(--color-identity-6-bg)', 'var(--color-identity-6-fg)'],
  ['var(--color-identity-7-bg)', 'var(--color-identity-7-fg)'],
];

/** djb2 over the name → a stable tint index. Exported for tests. */
export function identityIndex(name: string): number {
  let hash = 5381;
  for (let i = 0; i < name.length; i++) {
    hash = ((hash << 5) + hash + name.charCodeAt(i)) | 0;
  }
  return Math.abs(hash) % TINTS.length;
}

function monogram(name: string): string {
  // First grapheme (handles CJK + astral chars), uppercased for latin.
  const first = [...name.trim()][0] ?? '?';
  return first.toUpperCase();
}

/** Geometry only, shared with `BrandMark` so a real logo and the monogram it
 *  falls back to occupy the same box on the same row. */
export const TILE_SIZES = {
  sm: 'h-7 w-7 rounded-md',
  lg: 'h-10 w-10 rounded-lg',
} as const;

const TEXT_SIZES = {
  sm: 'text-[0.75rem]',
  lg: 'text-base',
} as const;

export type TileSize = keyof typeof TILE_SIZES;

export function IdentityTile({
  name,
  size = 'sm',
  className = '',
}: {
  name: string;
  size?: TileSize;
  /** Extra geometry from the caller (e.g. a round mask on the model picker). */
  className?: string;
}) {
  const [bg, fg] = TINTS[identityIndex(name)];
  return (
    <span
      aria-hidden
      className={cn(
        'inline-flex flex-shrink-0 items-center justify-center font-semibold select-none',
        TILE_SIZES[size],
        TEXT_SIZES[size],
        className,
      )}
      style={{ color: fg, backgroundColor: bg }}
    >
      {monogram(name)}
    </span>
  );
}
