import { useId } from 'react';
import { useTranslation } from 'react-i18next';
import { ChipGroup } from '@/components/ui/chip-group';
import { WizardSection } from '@/components/ui/wizard';
import { HELP_AREAS, MARKETS, PROFILE_FIELDS, type HelpArea, type InvestingProfile, type Market } from '../profile';

interface MarketsStepProps {
  /** The agent's line that asks it. */
  labelledBy: string;
  profile: InvestingProfile;
  onChange: (patch: Partial<InvestingProfile>) => void;
}

/** Where the user invests, and what they want the agent's help with there,
 *  which sets what it offers first. At least one market; the help is optional:
 *  someone just looking around has nothing to name yet. */
export function MarketsStep({ labelledBy, profile, onChange }: MarketsStepProps) {
  const { t } = useTranslation();
  const helpId = useId();
  return (
    <>
      <ChipGroup<Market>
        labelledBy={labelledBy}
        values={profile.markets}
        onChange={(markets) => onChange({ markets })}
        options={MARKETS.map((value) => ({ value, label: PROFILE_FIELDS.markets.label(t, value) }))}
      />
      <WizardSection
        title={t('onboarding.welcome.markets.helpTitle')}
        titleId={helpId}
        hint={t('onboarding.welcome.optional')}
      >
        <ChipGroup<HelpArea>
          labelledBy={helpId}
          values={profile.helpWith}
          onChange={(helpWith) => onChange({ helpWith })}
          options={HELP_AREAS.map((value) => ({ value, label: PROFILE_FIELDS.helpWith.label(t, value) }))}
        />
      </WizardSection>
    </>
  );
}
