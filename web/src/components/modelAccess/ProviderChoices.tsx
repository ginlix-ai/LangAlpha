import { ChevronRight } from 'lucide-react';
import { BrandMark } from '@/pages/ChatAgent/components/mcp/BrandMark';
import { llmProviderArt } from '@/lib/brandArt';
import { cn } from '@/lib/utils';
import { StatusLabel } from './parts';

/**
 * One provider to pick: its mark, its name and a note under it, and
 * Connected where it already is. A `list` row leads on to the provider's own
 * step; a `grid` row is a cell of the key providers' columns.
 */
export function ProviderChoice({
  markProvider,
  name,
  note,
  connected,
  onPick,
  layout = 'list',
}: {
  markProvider: string;
  name: string;
  note?: string;
  connected: boolean;
  onPick: () => void;
  layout?: 'list' | 'grid';
}) {
  const list = layout === 'list';
  return (
    <li>
      <button
        type="button"
        onClick={onPick}
        className={cn(
          'flex w-full items-center gap-3 rounded-md px-3 text-left transition-colors hover:bg-(--color-bg-hover)',
          list ? 'py-2.5' : 'h-full py-1.5',
        )}
      >
        <BrandMark name={name} art={llmProviderArt(markProvider)} size="sm" className="rounded-md" />
        <span className="flex min-w-0 flex-1 flex-col">
          <span className="truncate text-sm" style={{ color: 'var(--color-text-primary)' }}>{name}</span>
          {note && <span className={cn('text-xs', !list && 'line-clamp-2')} style={{ color: 'var(--color-text-tertiary)' }}>{note}</span>}
        </span>
        {connected && <StatusLabel status="connected" className="shrink-0" />}
        {list && <ChevronRight aria-hidden="true" className="h-4 w-4 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />}
      </button>
    </li>
  );
}

/** A list of providers to pick from, as rows. */
export function ProviderChoices<P extends string>({
  entries,
  configured,
  onPick,
}: {
  entries: readonly { provider: P; name: string; note?: string }[];
  configured: ReadonlySet<string>;
  onPick: (provider: P) => void;
}) {
  return (
    <ul className="flex flex-col gap-1">
      {entries.map((e) => (
        <ProviderChoice
          key={e.provider}
          markProvider={e.provider}
          name={e.name}
          note={e.note}
          connected={configured.has(e.provider)}
          onPick={() => onPick(e.provider)}
        />
      ))}
    </ul>
  );
}
