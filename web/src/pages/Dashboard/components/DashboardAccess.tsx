import React, { useId, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { ShieldCheck } from 'lucide-react';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { ToggleSwitch } from '@/components/ui/switch';
import { ASK_FIRST, type Scope } from '@/lib/appBridge/protocol';
import { useAppScopes } from '@/lib/appBridge/useAppScopes';

interface DashboardAccessProps {
  /** What the dashboard's grants are kept under. */
  grantKey: string;
  /** Every scope the dashboard on screen asked to read. */
  asked: Scope[];
  /** A grant was taken back, and saved. */
  onRevoked: () => void;
  /** Closed after one was: the frame holds what it already read until it loads again. */
  onDone: () => void;
}

/**
 * What the dashboard may read of the user's own (their holdings, their
 * threads), and the way to change it after the consent bar is gone. Shown
 * only for a dashboard that asked for some: the market's data needs no leave.
 */
export function DashboardAccess({ grantKey, asked, onRevoked, onDone }: DashboardAccessProps): React.ReactElement | null {
  const { t } = useTranslation();
  const id = useId();
  const { granted, grant, revoke } = useAppScopes(grantKey);
  // A switch shows where it is going while its save is under way: the save
  // reads the row first, and a switch that sat still would be pressed twice.
  const [saving, setSaving] = useState<Partial<Record<Scope, boolean>>>({});
  const [revoked, setRevoked] = useState(false);
  const scopes = asked.filter((s) => ASK_FIRST.has(s));
  if (!scopes.length) return null;

  const toggle = async (scope: Scope) => {
    if (scope in saving) return;
    const on = !granted.has(scope);
    setSaving((held) => ({ ...held, [scope]: on }));
    const saved = await (on ? grant([scope]) : revoke([scope]));
    setSaving((held) => Object.fromEntries(Object.entries(held).filter(([s]) => s !== scope)));
    if (!saved || on) return;
    setRevoked(true);
    onRevoked();
  };

  // The frame loads again once, however many were taken back, and not under
  // the pointer: its header would lose this popover on the way.
  const onOpenChange = (open: boolean) => {
    if (open || !revoked) return;
    setRevoked(false);
    onDone();
  };

  return (
    <Popover onOpenChange={onOpenChange}>
      <PopoverTrigger asChild>
        <button
          type="button"
          className="tap-target flex h-9 w-9 shrink-0 items-center justify-center rounded-md transition-colors hover:bg-(--color-bg-hover)"
          style={{ color: 'var(--color-text-secondary)' }}
          title={t('customDashboard.access.label')}
          aria-label={t('customDashboard.access.label')}
        >
          <ShieldCheck size={16} />
        </button>
      </PopoverTrigger>
      <PopoverContent align="end" className="w-72 p-3">
        <p className="text-sm font-medium text-(--color-text-primary)">{t('customDashboard.access.title')}</p>
        <p className="mt-0.5 text-xs text-(--color-text-secondary)">{t('customDashboard.access.hint')}</p>
        <ul className="mt-3 flex flex-col gap-2.5">
          {scopes.map((scope) => (
            <li key={scope} className="flex items-center justify-between gap-3">
              <label htmlFor={`${id}-${scope}`} className="text-sm text-(--color-text-primary)">
                {t(`customDashboard.access.scope.${scope}`)}
              </label>
              <ToggleSwitch
                id={`${id}-${scope}`}
                checked={saving[scope] ?? granted.has(scope)}
                onChange={() => void toggle(scope)}
              />
            </li>
          ))}
        </ul>
      </PopoverContent>
    </Popover>
  );
}
