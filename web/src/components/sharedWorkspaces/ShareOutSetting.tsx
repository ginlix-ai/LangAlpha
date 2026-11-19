import { useId } from 'react';
import { useTranslation } from 'react-i18next';
import { RadioChoice } from '@/components/ui/choice';
import { cn } from '@/lib/utils';
import type { ShareOut } from '@/types/sharedWorkspaces';

const OPTIONS: ShareOut[] = ['managers', 'members', 'none'];

/**
 * Who may take a restricted shared workspace's work outside it, in its People
 * dialog. A manager picks one of three; a member reads the choice. The host
 * leaves it out while the workspace is open to everyone, where it does not
 * apply.
 */
export function ShareOutSetting({ value, canManage, disabled, organization, onChange }: {
  value: ShareOut;
  canManage: boolean;
  disabled?: boolean;
  organization: string;
  onChange: (value: ShareOut) => void;
}) {
  const { t } = useTranslation();
  const name = useId();
  const labelId = useId();
  const helpId = useId();
  const label = (v: ShareOut) => t(`sharedWorkspace.shareOut.${v}`);
  const body = (v: ShareOut) => t(`sharedWorkspace.shareOut.${v}Body`);

  return (
    <div className="flex min-w-0 flex-col gap-2 border-t pt-4" style={{ borderColor: 'var(--color-border-muted)' }}>
      <div className="flex min-w-0 flex-col gap-0.5 text-sm">
        <span id={labelId} className="font-medium" style={{ color: 'var(--color-text-primary)' }}>
          {t('sharedWorkspace.shareOut.label')}
        </span>
        <span id={helpId} style={{ color: 'var(--color-text-secondary)' }}>
          {t('sharedWorkspace.shareOut.help', { organization })}
        </span>
      </div>
      {canManage ? (
        <div role="radiogroup" aria-labelledby={labelId} aria-describedby={helpId} className="flex flex-col gap-0.5">
          {OPTIONS.map((v) => {
            const checked = v === value;
            return (
              <RadioChoice
                key={v}
                name={name}
                value={v}
                checked={checked}
                disabled={disabled}
                onSelect={onChange}
                aria-labelledby={`${name}-${v}-label`}
                aria-describedby={`${name}-${v}-body`}
                markClassName="mt-[3px]"
                className={cn(
                  'items-start gap-2.5 rounded-md px-2 py-1.5 transition-colors',
                  checked ? 'bg-(--color-bg-hover)' : 'hover:bg-(--color-bg-subtle)',
                )}
              >
                <span className="flex min-w-0 flex-col">
                  <span id={`${name}-${v}-label`} className="text-sm" style={{ color: 'var(--color-text-primary)' }}>{label(v)}</span>
                  <span id={`${name}-${v}-body`} className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{body(v)}</span>
                </span>
              </RadioChoice>
            );
          })}
        </div>
      ) : (
        <div className="flex min-w-0 flex-col rounded-md px-2 py-1.5" style={{ backgroundColor: 'var(--color-bg-hover)' }}>
          <span className="text-sm" style={{ color: 'var(--color-text-primary)' }}>{label(value)}</span>
          <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{body(value)}</span>
        </div>
      )}
    </div>
  );
}
