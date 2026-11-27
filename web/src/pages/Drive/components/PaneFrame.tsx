import type React from 'react';
import { ArrowLeft } from 'lucide-react';
import { HeaderButton } from '@/components/mcp/McpPrimitives';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';
import { SidebarReveal } from '@/components/Sidebar/SidebarReveal';

/** The page's small icon buttons (back, more): the ghost button at the list's 28px. */
export const ICON_BUTTON = 'tap-target h-7 w-7 shrink-0 text-(--color-text-tertiary) hover:text-(--color-text-secondary)';

/**
 * An action in a pane's title row: a labelled button, or in a narrow pane
 * (`compact`: beside a file, or on a phone) a 30px square with its icon,
 * the label kept for a screen reader and as its title. A row passes one
 * `compact` to all its actions, so they drop their labels together and never
 * stand at two sizes side by side.
 */
export function PaneAction({ icon, label, compact, className, ...rest }: {
  icon: React.ComponentType<{ className?: string }>;
  label: string;
  compact: boolean;
} & Omit<React.ComponentProps<typeof HeaderButton>, 'icon' | 'children'>) {
  return (
    <HeaderButton
      icon={icon}
      title={compact ? label : undefined}
      className={cn(compact && 'w-[30px] justify-center px-0', className)}
      {...rest}
    >
      {compact ? <span className="sr-only">{label}</span> : label}
    </HeaderButton>
  );
}

export function BackButton({ label, onClick }: { label: string; onClick: () => void }) {
  return (
    <Button type="button" variant="ghost" size="icon" className={ICON_BUTTON} onClick={onClick} aria-label={label}>
      <ArrowLeft className="h-4 w-4" aria-hidden="true" />
    </Button>
  );
}

/**
 * The left pane's frame, shared by a folder and the trash: a title row with
 * its actions, whatever sits under it, the scrolling body, and anything pinned
 * at the foot.
 */
export function PaneFrame({
  title,
  back,
  views,
  actions,
  below,
  children,
  foot,
  className,
  ...rest
}: {
  title: string;
  back?: { label: string; onClick: () => void };
  /** Beside the title: the switch between the Drive's views. */
  views?: React.ReactNode;
  actions?: React.ReactNode;
  /** Under the title row: crumbs, a subtitle, a confirmation. */
  below?: React.ReactNode;
  children: React.ReactNode;
  foot?: React.ReactNode;
} & React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div className={cn('drive-pane', className)} {...rest}>
      <div className="drive-pane-head">
        <div className="drive-pane-title-row">
          <SidebarReveal />
          {back && <BackButton {...back} />}
          <h1 className={cn('drive-title', !!views && 'has-views')}>{title}</h1>
          {views}
          {actions && <div className="drive-pane-actions">{actions}</div>}
        </div>
        {below}
      </div>
      <div className="drive-pane-scroll">{children}</div>
      {foot}
    </div>
  );
}
