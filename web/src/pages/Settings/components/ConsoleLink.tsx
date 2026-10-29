import type { ReactNode } from 'react';
import { ArrowUpRight, ChevronRight } from 'lucide-react';
import { buttonClass } from '@/components/ui/button-class';
import { consoleUrl } from '@/config/supportLinks';
import { isDesktopShell } from '@/lib/desktop';

/**
 * A button-shaped link to a page of the hosted account console, which owns
 * plans, billing and the chat apps. In a browser it opens in a new tab so
 * Settings stays where it was; the desktop app shows the console in this
 * window, as the account menu's Usage & Plan does.
 */
export function ConsoleLink({ path, children }: { path: string; children: ReactNode }) {
  const href = consoleUrl(path);
  if (!href) return null;
  const newTab = !isDesktopShell();
  const Icon = newTab ? ArrowUpRight : ChevronRight;
  return (
    <a
      href={href}
      {...(newTab && { target: '_blank', rel: 'noopener noreferrer' })}
      className={buttonClass({ variant: 'outline', size: 'sm', className: 'h-8 shrink-0 gap-1.5' })}
    >
      {children}
      <Icon className="h-3.5 w-3.5 text-(--color-text-tertiary)" aria-hidden />
    </a>
  );
}
