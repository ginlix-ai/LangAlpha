/**
 * Adding a computer, as the New computer form and the desktop app's first
 * run both do it: the body for a machine the user brings, and a refused
 * create with its way to take a folder over. This Mac's setup is
 * `useAddThisMac`, apart so a page that only words a refusal stays light.
 */
import type { Ref } from 'react';
import { useTranslation } from 'react-i18next';

import type { ComputerCreate, ResourceTier } from '@/types/api';

import { entitlementErrorMessage } from '../utils/entitlementErrors';
import { sshLimitMessage } from '../utils/sshLimit';

type Translate = ReturnType<typeof useTranslation>['t'];

/** The machine's part of the body, as its setup gives it once ready. */
type OwnMachinePart = Required<Pick<ComputerCreate, 'ssh'>> & Pick<ComputerCreate, 'files_backup'>;

/**
 * The create body for a machine the user brings. It always names its kind,
 * since the server takes a target only with it, and its owner sizes it, so
 * its tier is never a pick. `takeOver` is the computer whose mark holds the
 * folder, when the user chose to take it over.
 */
export function ownMachineCreate(
  part: OwnMachinePart,
  { name, takeOver }: { name?: string; takeOver?: string | null } = {},
): ComputerCreate {
  return {
    ...part,
    ...(name ? { name } : {}),
    ...(takeOver ? { ssh: { ...part.ssh, take_over: takeOver } } : {}),
    resource_tier: 'standard',
    kind: 'ssh',
  };
}

/** A refused create, in this app's words where it has some, else the server's. */
export function createRefusalMessage(err: unknown, t: Translate, tier?: ResourceTier): string {
  return sshLimitMessage(err, t) ?? entitlementErrorMessage(err, t, tier);
}

/**
 * A refused create, and where a computer of another account holds the
 * folder, the way to take it over.
 */
export function CreateRefusal({
  message,
  onTakeOver,
  className = '',
  ref,
}: {
  message: string;
  /** Sends the create again, taking the folder over; absent where none holds it. */
  onTakeOver?: () => void;
  className?: string;
  ref?: Ref<HTMLDivElement>;
}) {
  const { t } = useTranslation();
  return (
    <div ref={ref} role="alert" className={`flex flex-col gap-1.5 ${className}`}>
      <p className="text-sm" style={{ color: 'var(--color-icon-danger)' }}>
        {message}
      </p>
      {onTakeOver && (
        <p className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
          {t('computer.takeOverFolderNote', 'The other computer stops working in it. The files in it stay.')}{' '}
          <button
            type="button"
            className="underline font-medium"
            style={{ color: 'var(--color-text-primary)' }}
            onClick={onTakeOver}
          >
            {t('computer.takeOverFolder', 'Take over this folder')}
          </button>
        </p>
      )}
    </div>
  );
}
