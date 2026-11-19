import { useState } from 'react';
import { User } from 'lucide-react';
import { cn } from '@/lib/utils';
import { initialsOf } from '@/lib/initials';

interface PersonAvatarProps {
  name: string | null | undefined;
  email?: string | null;
  avatarUrl?: string | null;
  /** Edge in pixels. */
  size?: number;
  className?: string;
  /** Edged in the page's ground, for a stack where each sits over the last. */
  edged?: boolean;
}

/** A person in a shared workspace: their picture, else their initials, on
 *  the account menu's soft accent disc. Decorative: the name always sits
 *  beside it or in its label. */
export function PersonAvatar({ name, email, avatarUrl, size = 20, className, edged }: PersonAvatarProps) {
  const [failed, setFailed] = useState<string | null>(null);
  // Two letters need more room than a small disc has, more so where the
  // next disc in a stack covers its edge.
  const initials = size < 22 ? (Array.from(initialsOf(name, email))[0] ?? '') : initialsOf(name, email);
  const showImage = !!avatarUrl && failed !== avatarUrl;
  return (
    <span
      aria-hidden="true"
      className={cn('inline-flex shrink-0 items-center justify-center overflow-hidden rounded-full font-semibold leading-none', className)}
      style={{
        width: size,
        height: size,
        fontSize: Math.max(8, Math.round(size * 0.42)),
        // The soft accent over the page's ground, so a disc stacked over
        // another hides it rather than showing through.
        background: 'linear-gradient(var(--color-accent-soft), var(--color-accent-soft)), var(--color-bg-page)',
        color: 'var(--color-accent-light)',
        ...(edged ? { boxShadow: '0 0 0 2px var(--color-bg-page)' } : {}),
      }}
    >
      {showImage ? (
        <img src={avatarUrl!} alt="" className="h-full w-full object-cover" onError={() => setFailed(avatarUrl!)} />
      ) : initials ? (
        initials
      ) : (
        <User style={{ width: size * 0.6, height: size * 0.6 }} />
      )}
    </span>
  );
}

interface AvatarStackProps {
  people: { user_id: string; name: string | null; email?: string | null; avatar_url?: string | null }[];
  max?: number;
  size?: number;
}

/** The first few people of a workspace, overlapped, and how many more. */
export function AvatarStack({ people, max = 4, size = 20 }: AvatarStackProps) {
  const shown = people.slice(0, max);
  const more = people.length - shown.length;
  return (
    <span className="inline-flex items-center" aria-hidden="true">
      {shown.map((p, i) => (
        <PersonAvatar
          key={p.user_id}
          name={p.name}
          email={p.email}
          avatarUrl={p.avatar_url}
          size={size}
          edged
          className={cn(i > 0 && '-ml-1')}
        />
      ))}
      {more > 0 && (
        <span className="ml-1 text-xs tabular-nums" style={{ color: 'var(--color-text-tertiary)' }}>+{more}</span>
      )}
    </span>
  );
}
