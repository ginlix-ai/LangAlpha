import { useEffect, useState } from 'react';
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
import type { ResourceTier, Workspace, WorkspaceQuota } from '@/types/api';
import { TierRadioGroup, normalizeTier } from './tierUi';

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
 * Change-spec dialog: pick a sandbox resource tier for a workspace.
 */
function ChangeSpecDialog({ target, onClose, onSubmit, busy, quota }: ChangeSpecDialogProps) {
  const { t } = useTranslation();
  const [tier, setTier] = useState<ResourceTier>('standard');

  // Seed the selection from the workspace's current tier each time the dialog opens.
  useEffect(() => {
    if (target) setTier(normalizeTier(target.resource_tier));
  }, [target]);

  const currentTier = normalizeTier(target?.resource_tier);

  return (
    <Dialog open={!!target} onOpenChange={(open) => { if (!open && !busy) onClose(); }}>
      <DialogContent style={{ backgroundColor: 'var(--color-bg-page)', borderColor: 'var(--color-border-muted)' }}>
        <DialogHeader>
          <DialogTitle>{t('workspace.changeSpec', 'Change spec')}</DialogTitle>
          <DialogDescription>
            {t('workspace.changeSpecDesc', 'Pick the sandbox resources for this workspace.')}
          </DialogDescription>
        </DialogHeader>
        <TierRadioGroup
          value={tier}
          onChange={setTier}
          label={t('workspace.changeSpec', 'Change spec')}
          quota={quota}
          exemptTier={currentTier}
        />
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
