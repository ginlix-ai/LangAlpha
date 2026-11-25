import type React from 'react';
import { ArrowLeft } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';

/** The page's small icon buttons (back, more): the ghost button at the list's 28px. */
export const ICON_BUTTON = 'tap-target h-7 w-7 shrink-0 text-(--color-text-tertiary) hover:text-(--color-text-secondary)';

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
  actions,
  below,
  children,
  foot,
  className,
  ...rest
}: {
  title: string;
  back?: { label: string; onClick: () => void };
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
          {back && <BackButton {...back} />}
          <h1 className="drive-title">{title}</h1>
          <div className="drive-pane-actions">{actions}</div>
        </div>
        {below}
      </div>
      <div className="drive-pane-scroll">{children}</div>
      {foot}
    </div>
  );
}
