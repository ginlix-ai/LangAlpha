/**
 * The computer badge: what kind of machine a computer is, and its size where
 * the app sets one and the row names it. "Daytona · Small", "E2B · Large",
 * "This Mac", "Mac", "SSH".
 *
 * Kept apart from any one surface so the gallery card, the toolbar chip and,
 * later, the thread page and the sidebar all name a machine the same way.
 */
import { Cloud, Laptop, Terminal, type LucideIcon } from 'lucide-react';
import { useTranslation } from 'react-i18next';

import type { Computer } from '@/types/api';

import { providerLabel, providerManagesSpec } from './providerUi';
import { knownEffectiveTier } from './specChangeUi';
import { tierLabel } from './tierUi';

type Translate = ReturnType<typeof useTranslation>['t'];

/**
 * - `thisMac`: the Mac this desktop app runs on, under remote control.
 * - `mac`: a Mac under remote control, seen from anywhere else. The server says
 *   so (`ssh.helper`), the same on every device; only "this" needs the shell.
 * - `ssh`: a machine the user brought, reached over SSH.
 * - `hosted`: a sandbox the deployment runs (Daytona, E2B, Docker, ...).
 */
export type ComputerKind = 'thisMac' | 'mac' | 'ssh' | 'hosted';

export interface ComputerIdentity {
  kind: ComputerKind;
  label: string;
  icon: LucideIcon;
  /** The tier's name, only on a machine the app sizes. Own machines have none,
   *  and nor does a row whose tier is missing or one this build does not know:
   *  a guess would name a size the machine may not be. */
  size: string | null;
}

type BadgeComputer = Pick<Computer, 'kind' | 'ssh' | 'resource_tier' | 'spec_change'>;

export function computerIdentity(t: Translate, computer: BadgeComputer, isThisMac: boolean): ComputerIdentity {
  if (!providerManagesSpec(computer.kind)) {
    if (isThisMac) {
      return { kind: 'thisMac', label: t('computer.remoteControl.thisMacBadge', 'This Mac'), icon: Laptop, size: null };
    }
    if (computer.ssh?.helper) {
      return { kind: 'mac', label: t('computer.badge.mac', 'Mac'), icon: Laptop, size: null };
    }
    return { kind: 'ssh', label: t('computer.badge.ssh', 'SSH'), icon: Terminal, size: null };
  }
  const tier = knownEffectiveTier(computer);
  return {
    kind: 'hosted',
    label: providerLabel(computer.kind, t),
    icon: Cloud,
    size: tier ? tierLabel(t, tier) : null,
  };
}

/** Whether `name` already says `word`, as a whole word in any case. */
function nameSays(name: string, word: string): boolean {
  const escaped = word.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return new RegExp(`(^|[^\\p{L}\\p{N}])${escaped}($|[^\\p{L}\\p{N}])`, 'iu').test(name);
}

/**
 * The badge's words, less any the machine's name already says. A user who
 * calls a Small Daytona box "Daytona Small" has spelled the badge, and the two
 * side by side read as one thing said twice. The server's own defaults ("Cloud
 * computer", "SSH computer") never say a provider or size, so they keep both.
 */
function badgeWords(identity: ComputerIdentity, name?: string | null): string[] {
  const words = identity.size ? [identity.label, identity.size] : [identity.label];
  return name ? words.filter((word) => !nameSays(name, word)) : words;
}

interface ComputerBadgeProps {
  computer: BadgeComputer;
  /** From `useThisMac(...).of(computer)`, which only the desktop app on that Mac can answer. */
  isThisMac?: boolean;
  /**
   * The name it is shown beside, when it is. Words the name already says are
   * left out, and a badge with none left is not drawn at all.
   */
  name?: string | null;
  /**
   * `pill`: the glyph and words on a filled pill, where a machine is named once.
   * `plain`: the words alone, in quiet text after a separator, for a line that
   * repeats on every card and has already drawn the glyph.
   */
  variant?: 'pill' | 'plain';
  className?: string;
}

/** The provider glyph and name, then the size where there is one. */
export function ComputerBadge({ computer, isThisMac = false, name, variant = 'pill', className }: ComputerBadgeProps) {
  const { t } = useTranslation();
  const identity = computerIdentity(t, computer, isThisMac);
  const words = badgeWords(identity, name);
  if (words.length === 0) return null;
  const Icon = identity.icon;
  const dot = (key: number) => (
    <span key={`dot-${key}`} aria-hidden="true" style={{ color: 'var(--color-text-tertiary)' }}>·</span>
  );

  if (variant === 'plain') {
    return (
      <span
        className={`inline-flex shrink-0 items-center gap-1 whitespace-nowrap${className ? ` ${className}` : ''}`}
        style={{ color: 'var(--color-text-tertiary)' }}
        data-testid="computer-badge"
        data-kind={identity.kind}
      >
        {words.flatMap((word, i) => [dot(i), <span key={word}>{word}</span>])}
      </span>
    );
  }

  return (
    <span
      className={`inline-flex shrink-0 items-center gap-1 whitespace-nowrap rounded-full px-2 py-0.5 text-[0.625rem] font-medium${className ? ` ${className}` : ''}`}
      style={{ backgroundColor: 'var(--color-border-muted)', color: 'var(--color-text-secondary)' }}
      data-testid="computer-badge"
      data-kind={identity.kind}
    >
      <Icon className="h-3 w-3 shrink-0" aria-hidden="true" />
      {words.flatMap((word, i) => (i === 0 ? [<span key={word}>{word}</span>] : [dot(i), <span key={word}>{word}</span>]))}
    </span>
  );
}

export default ComputerBadge;
