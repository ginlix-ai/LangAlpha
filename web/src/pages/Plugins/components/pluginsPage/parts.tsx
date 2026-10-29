import type { ButtonHTMLAttributes, ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { EnabledToggle } from '@/components/mcp/McpPrimitives';
import { cn } from '@/lib/utils';
import './pluginsPage.css';

/** The detail header's switch, labelled with the state it is in. */
export function OnOffControl({
  enabled,
  name,
  disabled = false,
  title,
  onToggle,
}: {
  enabled: boolean;
  name: string;
  disabled?: boolean;
  /** Why it cannot be switched, when it cannot. */
  title?: string;
  onToggle: () => void;
}) {
  const { t } = useTranslation();
  return (
    <div className={cn('pp-onoff', disabled && 'is-disabled')} title={title}>
      <span>{enabled ? t('plugins.surface.on') : t('plugins.surface.off')}</span>
      <EnabledToggle enabled={enabled} name={name} disabled={disabled} onToggle={onToggle} />
    </div>
  );
}

/** An outlined (or light primary) button for the detail pages' side column
 *  and callouts. Never amber: amber is annotation only. */
export function PageButton({
  variant = 'outline',
  icon,
  className,
  children,
  ...props
}: {
  variant?: 'outline' | 'primary' | 'danger';
  icon?: ReactNode;
  children: ReactNode;
} & ButtonHTMLAttributes<HTMLButtonElement>) {
  return (
    <button
      type="button"
      className={cn(
        'pp-btn',
        variant === 'primary' && 'is-primary',
        variant === 'danger' && 'is-danger',
        className,
      )}
      {...props}
    >
      {icon}
      {children}
    </button>
  );
}

/** A detail header's meta line: its parts with quiet dots between. Empty
 *  parts drop out, so a missing version or author leaves no stray dot.
 *
 *  Every part carries the dot that leads it, and the row starts one dot to
 *  the left of a box that clips: a part that wraps to the start of a line
 *  takes its dot into the clipped margin. As separate pieces, a narrow
 *  screen broke the line between a dot and its part and left the dot alone. */
export function MetaLine({ parts }: { parts: ReadonlyArray<ReactNode> }) {
  const shown = parts.filter((part) => part !== null && part !== undefined && part !== false && part !== '');
  return (
    <span className="pp-metaline">
      <span className="pp-metaline-row">
        {shown.map((part, i) => (
          <span key={i} className="pp-metaline-part">
            <span className="pp-sep" aria-hidden>
              ·
            </span>
            {part}
          </span>
        ))}
      </span>
    </span>
  );
}
