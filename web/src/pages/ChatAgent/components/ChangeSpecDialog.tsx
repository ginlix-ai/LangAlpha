import React, { useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { isPlatformMode } from '@/config/hostMode';
import type { ResourceTier, Workspace, WorkspaceCapacity, WorkspaceQuota } from '@/types/api';

type Translate = ReturnType<typeof useTranslation>['t'];

/** Tier presets, ordered for the change-spec radiogroup. */
export const TIER_ORDER: ResourceTier[] = ['standard', 'performance', 'max'];

/** Coerce an unknown/legacy tier value to a known tier (defaults to standard). */
export function normalizeTier(tier: unknown): ResourceTier {
  return tier === 'performance' || tier === 'max' ? tier : 'standard';
}

/** Localized display name for a tier. */
export function tierLabel(t: Translate, tier: ResourceTier): string {
  return t(`workspace.tier.${tier}`);
}

interface ChangeSpecDialogProps {
  /** Only the tier is read — narrowed so nav-tree rows open this without a cast. */
  target: Pick<Workspace, 'resource_tier'> | null;
  onClose: () => void;
  onSubmit: (tier: ResourceTier) => void;
  busy: boolean;
  /** Per-tier count quotas (platform mode only); null/undefined hides the capacity hint. */
  quota?: WorkspaceQuota | null;
}

/**
 * Change-spec dialog: pick a sandbox resource tier for a workspace. The tier
 * options are a WAI-ARIA radiogroup with roving-tabindex keyboard nav. In
 * platform mode each elevated tier shows its remaining count quota.
 */
function ChangeSpecDialog({ target, onClose, onSubmit, busy, quota }: ChangeSpecDialogProps) {
  const { t } = useTranslation();
  const [tier, setTier] = useState<ResourceTier>('standard');
  const radioRefs = useRef<Array<HTMLButtonElement | null>>([]);

  // Seed the selection from the workspace's current tier each time the dialog opens.
  useEffect(() => {
    if (target) setTier(normalizeTier(target.resource_tier));
  }, [target]);

  const currentTier = normalizeTier(target?.resource_tier);

  // Per-tier capacity + ungrantable flag. In platform mode a tier is ungrantable
  // when the plan excludes it (limit === 0) or its count quota is exhausted
  // (limit > 0 && remaining <= 0). The workspace's current tier is never disabled:
  // re-selecting it is a no-op save that consumes no new slot (mirrors the backend's
  // current_tier count skip).
  const tierRows: Array<{ id: ResourceTier; capacity: WorkspaceCapacity | null; disabled: boolean }> =
    TIER_ORDER.map((id) => {
      // Elevated tiers carry a count quota in platform mode; standard never does.
      const capacity =
        id === 'performance' ? quota?.performance ?? null
        : id === 'max' ? quota?.max ?? null
        : null;
      let disabled = false;
      if (isPlatformMode && capacity && id !== currentTier) {
        const remaining = capacity.limit - capacity.used;
        disabled = capacity.limit === 0 || (capacity.limit > 0 && remaining <= 0);
      }
      return { id, capacity, disabled };
    });

  // Roving-tabindex keyboard nav: arrows move selection + focus, skipping
  // disabled (ungrantable) tiers and wrapping around.
  const handleKeyDown = (e: React.KeyboardEvent, index: number) => {
    const step =
      e.key === 'ArrowDown' || e.key === 'ArrowRight' ? 1
      : e.key === 'ArrowUp' || e.key === 'ArrowLeft' ? -1
      : 0;
    if (step === 0) return;
    e.preventDefault();
    const len = TIER_ORDER.length;
    let nextIndex = index;
    for (let i = 0; i < len; i++) {
      nextIndex = (nextIndex + step + len) % len;
      if (!tierRows[nextIndex].disabled) break;
    }
    if (tierRows[nextIndex].disabled) return; // no enabled sibling to move to
    setTier(TIER_ORDER[nextIndex]);
    radioRefs.current[nextIndex]?.focus();
  };

  return (
    <Dialog open={!!target} onOpenChange={(open) => { if (!open && !busy) onClose(); }}>
      <DialogContent style={{ backgroundColor: 'var(--color-bg-page)', borderColor: 'var(--color-border-muted)' }}>
        <DialogHeader>
          <DialogTitle>{t('workspace.changeSpec', 'Change spec')}</DialogTitle>
          <DialogDescription>
            {t('workspace.changeSpecDesc', 'Pick the sandbox resources for this workspace.')}
          </DialogDescription>
        </DialogHeader>
        <div className="flex flex-col gap-2" role="radiogroup" aria-label={t('workspace.changeSpec', 'Change spec')}>
          {tierRows.map(({ id, capacity, disabled }, index) => {
            const selected = tier === id;
            return (
              <button
                key={id}
                ref={(el) => { radioRefs.current[index] = el; }}
                type="button"
                role="radio"
                aria-checked={selected}
                aria-disabled={disabled}
                disabled={disabled}
                tabIndex={selected ? 0 : -1}
                onKeyDown={(e) => handleKeyDown(e, index)}
                onClick={() => { if (!disabled) setTier(id); }}
                className="flex items-start gap-3 rounded-lg border p-3 text-left transition-colors"
                style={{
                  borderColor: selected ? 'var(--color-border-elevated)' : 'var(--color-border-muted)',
                  backgroundColor: selected ? 'var(--color-bg-elevated)' : 'transparent',
                  opacity: disabled ? 0.5 : 1,
                  cursor: disabled ? 'not-allowed' : 'pointer',
                }}
              >
                <span
                  className="mt-0.5 flex h-4 w-4 flex-shrink-0 items-center justify-center rounded-full border"
                  style={{ borderColor: selected ? 'var(--color-accent-primary)' : 'var(--color-border-default)' }}
                >
                  {selected && (
                    <span className="h-2 w-2 rounded-full" style={{ backgroundColor: 'var(--color-accent-primary)' }} />
                  )}
                </span>
                <span className="flex-1 min-w-0">
                  <span className="flex items-center justify-between gap-2">
                    <span className="font-medium" style={{ color: 'var(--color-text-primary)' }}>{tierLabel(t, id)}</span>
                    {isPlatformMode && capacity && (
                      <span className="text-xs whitespace-nowrap" style={{ color: 'var(--color-text-tertiary)' }}>
                        {capacity.limit < 0
                          ? t('workspace.quotaUnlimited', 'Unlimited')
                          : capacity.limit === 0
                            ? t('workspace.quotaNotOnPlan', 'Not on your plan')
                            : t('workspace.quotaRemaining', '{{remaining}} of {{limit}} left', {
                                remaining: Math.max(0, capacity.limit - capacity.used),
                                limit: capacity.limit,
                              })}
                      </span>
                    )}
                  </span>
                  <span className="block text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
                    {t(`workspace.tierSpec.${id}`)}
                  </span>
                </span>
              </button>
            );
          })}
        </div>
        <DialogFooter>
          <Button variant="ghost" onClick={onClose} disabled={busy}>
            {t('common.cancel')}
          </Button>
          <Button onClick={() => onSubmit(tier)} disabled={busy || tier === currentTier}>
            {busy ? t('common.saving') : t('common.save')}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

export default ChangeSpecDialog;
