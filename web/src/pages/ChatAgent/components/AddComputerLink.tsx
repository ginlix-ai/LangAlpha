/**
 * The ways out of a refusal for want of a computer Home can live on.
 *
 * `needs_computer`: add a computer. In the desktop app on a Mac that can pair
 * itself, that is this Mac, so the link says so and opens the form on it;
 * anywhere else it opens the form as it is. `needs_backup`: the user's
 * computer has backup off, so the way out is that computer's details, where
 * backup is turned on.
 */
import { useTranslation } from 'react-i18next';

import { canAddThisMac } from '@/lib/desktop';
import { useOpenComputers } from '../hooks/useOpenComputers';

const LINK_STYLE = { textDecoration: 'underline', fontWeight: 500 } as const;

export function AddComputerLink() {
  const { t } = useTranslation();
  const openComputers = useOpenComputers();
  const thisMac = canAddThisMac();
  return (
    <button
      type="button"
      className="tap-target cursor-pointer"
      onClick={() => openComputers(thisMac ? { view: 'create', pick: 'thisMac' } : { view: 'create' })}
      style={LINK_STYLE}
    >
      {thisMac ? t('homePlacement.setUpThisMac') : t('homePlacement.addComputer')}
    </button>
  );
}

/** Opens the details of the computer whose backup is off. */
export function ComputerDetailsLink({ computerId, name }: { computerId: string; name: string }) {
  const { t } = useTranslation();
  const openComputers = useOpenComputers();
  return (
    <button
      type="button"
      className="tap-target cursor-pointer"
      onClick={() => openComputers({ view: 'details', computerId })}
      style={LINK_STYLE}
    >
      {t('homePlacement.openComputer', { name })}
    </button>
  );
}
