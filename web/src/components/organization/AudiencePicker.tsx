import { useId } from 'react';
import { Building2, Globe } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import type { ShareAudience } from '@/types/api';
import { useAccount } from '@/hooks/useAccount';

/**
 * Who a link opens to: anyone with it, or only members of the owner's
 * organization, who sign in to open it. Shown only where the server offers
 * the organization (`audiences`), so a personal account never sees it.
 * A rule against public links leaves the members choice open: it is the
 * organization sharing inside itself.
 */
export function AudiencePicker({ value, publicAllowed, disabled, onChange }: {
  value: ShareAudience;
  /** False under a rule against public links: "Anyone" is shown, not chosen. */
  publicAllowed: boolean;
  disabled?: boolean;
  onChange: (audience: ShareAudience) => void;
}) {
  const { t } = useTranslation();
  const { orgT } = useAccount();
  const name = useId();
  const options: { audience: ShareAudience; label: string; Icon: typeof Globe; off: boolean }[] = [
    { audience: 'public', label: t('organization.share.audienceAnyone'), Icon: Globe, off: !publicAllowed },
    {
      audience: 'organization',
      label: orgT('organization.share.audienceMembers'),
      Icon: Building2,
      off: false,
    },
  ];
  return (
    <fieldset className="space-y-1.5" disabled={disabled}>
      <legend className="text-xs mb-1.5" style={{ color: 'var(--color-text-tertiary)' }}>
        {t('organization.share.audienceLabel')}
      </legend>
      {options.map(({ audience, label, Icon, off }) => (
        <label
          key={audience}
          className="flex items-center gap-2 cursor-pointer has-disabled:cursor-default"
          style={{ opacity: off && value !== audience ? 0.5 : 1 }}
        >
          <input
            type="radio"
            name={name}
            value={audience}
            checked={value === audience}
            disabled={off}
            onChange={() => onChange(audience)}
          />
          <Icon aria-hidden="true" className="h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-icon-muted)' }} />
          <span className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>{label}</span>
        </label>
      ))}
    </fieldset>
  );
}
