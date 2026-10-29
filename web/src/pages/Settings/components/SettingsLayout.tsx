import { useId, type ReactNode } from 'react';
import { ToggleSwitch } from '@/components/ui/switch';
import { SegmentedControl, type SegmentedOption } from '@/components/ui/segmented-control';
import { cn } from '@/lib/utils';

/**
 * The shapes every Settings section is drawn in: a group is a titled card,
 * and a row inside it is a name with the line under it on the left and its
 * control on the right, wrapping under the name when the column is narrow.
 * One shape for every setting, so a section reads as a list to scan rather
 * than a page of differently drawn boxes.
 */
export function SettingsGroup({
  title,
  description,
  action,
  children,
}: {
  title?: string;
  description?: ReactNode;
  /** A control for the whole group, level with its title. */
  action?: ReactNode;
  children: ReactNode;
}) {
  const titleId = useId();
  return (
    <section className="settings-group" aria-labelledby={title ? titleId : undefined}>
      {(title || description || action) && (
        <div className="settings-group-head">
          <div className="min-w-0 flex-[1_1_220px]">
            {title && <h2 id={titleId} className="settings-group-title">{title}</h2>}
            {description && <p className="settings-group-desc">{description}</p>}
          </div>
          {action}
        </div>
      )}
      <div className="settings-card">{children}</div>
    </section>
  );
}

export function SettingRow({
  id,
  label,
  description,
  labelId,
  htmlFor,
  wide = false,
  inline = false,
  children,
}: {
  /** The anchor the settings search jumps to. */
  id?: string;
  label: ReactNode;
  description?: ReactNode;
  /** Names the control by the row's label, for a group or switch. */
  labelId?: string;
  /** Ties the label to a form field. */
  htmlFor?: string;
  /** A field rather than a button group: the control takes a fixed width. */
  wide?: boolean;
  /** A switch, which stays beside its name in a narrow card. */
  inline?: boolean;
  children?: ReactNode;
}) {
  return (
    <div id={id} className={cn('settings-row', wide && 'is-wide', inline && 'is-inline')}>
      <div className="settings-row-text">
        {htmlFor ? (
          <label htmlFor={htmlFor} id={labelId} className="settings-row-label">{label}</label>
        ) : (
          <span id={labelId} className="settings-row-label">{label}</span>
        )}
        {description && <p className="settings-row-desc">{description}</p>}
      </div>
      {children && <div className="settings-row-control">{children}</div>}
    </div>
  );
}

/** Content that is not a row (a list of choices, a picker, a summary), padded
 *  to the rows' inset. */
export function SettingsCardBody({ id, className, children }: { id?: string; className?: string; children: ReactNode }) {
  return <div id={id} className={cn('settings-card-body', className)}>{children}</div>;
}

export function SegmentedRow<T extends string>({
  id,
  label,
  description,
  value,
  onChange,
  options,
}: {
  id?: string;
  label: string;
  description?: ReactNode;
  value: T;
  onChange: (value: T) => void;
  options: ReadonlyArray<SegmentedOption<T>>;
}) {
  const labelId = useId();
  return (
    <SettingRow id={id} label={label} description={description} labelId={labelId}>
      <SegmentedControl labelledBy={labelId} value={value} onChange={onChange} options={options} />
    </SettingRow>
  );
}

export function ToggleRow({
  id,
  label,
  description,
  checked,
  onChange,
}: {
  id?: string;
  label: string;
  description?: ReactNode;
  checked: boolean;
  onChange: () => void;
}) {
  // The name toggles it as well as the pill: a switch row is one target.
  const switchId = useId();
  return (
    <SettingRow id={id} label={label} description={description} htmlFor={switchId} inline>
      <ToggleSwitch id={switchId} checked={checked} onChange={onChange} ariaLabel={label} />
    </SettingRow>
  );
}
