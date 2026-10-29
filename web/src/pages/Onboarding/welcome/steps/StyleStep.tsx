import { useId } from 'react';
import { useTranslation } from 'react-i18next';
import { ChipGroup } from '@/components/ui/chip-group';
import { WizardSection } from '@/components/ui/wizard';
import {
  APPROACHES,
  PROFILE_FIELDS,
  SECTORS,
  STYLES,
  type Approach,
  type InvestingProfile,
  type InvestingStyle,
} from '../profile';

interface StyleStepProps {
  /** The agent's line that asks it. */
  labelledBy: string;
  profile: InvestingProfile;
  /** Sectors the user has that the list does not name (the agent's, or an
   *  older build's), offered after it so they can be kept or removed. */
  otherSectors: readonly string[];
  onChange: (patch: Partial<InvestingProfile>) => void;
}

/** How the user decides: the kinds of analysis they act on and the styles
 *  that fit them (the two lead the starter tasks between them), and the
 *  sectors they follow if any. */
export function StyleStep({ labelledBy, profile, otherSectors, onChange }: StyleStepProps) {
  const { t } = useTranslation();
  const stylesId = useId();
  const sectorsId = useId();
  return (
    <>
      <ChipGroup<Approach>
        labelledBy={labelledBy}
        values={profile.approaches}
        onChange={(approaches) => onChange({ approaches })}
        options={APPROACHES.map((value) => ({ value, label: PROFILE_FIELDS.approaches.label(t, value) }))}
      />
      <WizardSection title={t('onboarding.welcome.style.stylesTitle')} titleId={stylesId}>
        <ChipGroup<InvestingStyle>
          labelledBy={stylesId}
          values={profile.styles}
          onChange={(styles) => onChange({ styles })}
          options={STYLES.map((value) => ({ value, label: PROFILE_FIELDS.styles.label(t, value) }))}
        />
      </WizardSection>
      <WizardSection
        title={t('onboarding.welcome.style.sectorsTitle')}
        titleId={sectorsId}
        hint={t('onboarding.welcome.optional')}
      >
        <ChipGroup<string>
          labelledBy={sectorsId}
          values={profile.sectors}
          onChange={(sectors) => onChange({ sectors })}
          options={[...SECTORS, ...otherSectors].map((value) => ({
            value,
            label: PROFILE_FIELDS.sectors.label(t, value),
          }))}
        />
      </WizardSection>
    </>
  );
}
