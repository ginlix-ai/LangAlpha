/**
 * The one control that gets a cloud computer to the size Claude Code runs
 * at (`useMediumAction`), worded by what it does for this user: change the
 * spec, upgrade the plan, or show the change already running. Nothing where
 * no action applies.
 */
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';

import { useMediumAction, type MediumAction } from '../hooks/useMediumAction';
import { specChangeLabel } from './specChangeUi';

type Translate = ReturnType<typeof useTranslation>['t'];

/** What the control reads for `action`. */
function mediumActionLabel(t: Translate, action: MediumAction): string {
  if (action.kind === 'resizing') return specChangeLabel(t, action.change);
  return t(action.kind === 'plan' ? 'computer.spec.upgradePlan' : 'modelAccess.claudeCode.medium.resize');
}

/** As a link in a line of text: a refusal's fix. */
export function MediumActionLink({ computerId }: { computerId: string }) {
  const { t } = useTranslation();
  const action = useMediumAction(computerId);
  if (!action) return null;
  return (
    <button type="button" className="tap-target cursor-pointer" onClick={action.go} style={{ textDecoration: 'underline', fontWeight: 500 }}>
      {mediumActionLabel(t, action)}
    </button>
  );
}

/** As a row's button: Claude Code's view. */
export function MediumActionButton({ action, className }: { action: MediumAction; className?: string }) {
  const { t } = useTranslation();
  return (
    <Button type="button" variant={action.kind === 'resizing' ? 'ghost' : 'outline'} size="sm" className={cn(className)} onClick={action.go}>
      {mediumActionLabel(t, action)}
    </Button>
  );
}
