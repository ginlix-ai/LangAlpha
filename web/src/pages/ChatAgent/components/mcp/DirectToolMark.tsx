import { useState } from 'react';
import { Plug } from 'lucide-react';
import { cn } from '@/lib/utils';
import { brokerageArt } from '@/lib/brandArt';
import { BrandMark } from './BrandMark';
import { useDirectToolVendor } from './useDirectToolVendor';

/**
 * Inline vendor mark at icon size: the brokerage's logo when we have one,
 * the plug glyph otherwise. Sized for a timeline row, not a tile.
 */
export function DirectToolRowMark({ server, className, style }: { server: string; className?: string; style?: React.CSSProperties }) {
  const vendor = useDirectToolVendor(server);
  const art = brokerageArt(vendor);
  const [failed, setFailed] = useState<string | null>(null);
  if (!art || failed === art.src) {
    return <Plug className={cn('h-4 w-4', className)} style={style} />;
  }
  return (
    <img
      src={art.src}
      alt=""
      aria-hidden
      className={cn('h-4 w-4 shrink-0 rounded-sm object-contain', className)}
      onError={() => setFailed(art.src)}
    />
  );
}

/** Tile-sized vendor mark for card headers. */
export function DirectToolTileMark({ server, className }: { server: string; className?: string }) {
  const vendor = useDirectToolVendor(server);
  return (
    <BrandMark
      name={vendor?.label || server || 'MCP'}
      art={brokerageArt(vendor)}
      kind={vendor ? undefined : 'server'}
      size="sm"
      className={className}
    />
  );
}
