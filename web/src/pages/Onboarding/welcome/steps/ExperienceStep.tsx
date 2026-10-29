import { useId, type ComponentType } from 'react';
import { useTranslation } from 'react-i18next';
import { Briefcase, GraduationCap, User } from 'lucide-react';
import { ChipGroup } from '@/components/ui/chip-group';
import { OptionCard, OptionGroup } from '@/components/ui/option-card';
import { WizardSection } from '@/components/ui/wizard';
import {
  EXPERIENCE_LEVELS,
  INSTRUMENTS,
  INVESTOR_TYPES,
  PROFILE_FIELDS,
  type ExperienceLevel,
  type Instrument,
  type InvestingProfile,
  type InvestorType,
} from '../profile';

const TYPE_ICONS: Record<InvestorType, ComponentType<{ className?: string }>> = {
  individual: User,
  professional: Briefcase,
  student: GraduationCap,
};

interface ExperienceStepProps {
  /** The agent's line that asks it. */
  labelledBy: string;
  profile: InvestingProfile;
  onChange: (patch: Partial<InvestingProfile>) => void;
}

/**
 * Who the user is as an investor: what they do and how long they have done
 * it, which set how much the agent explains, and what they want to trade.
 * The last is optional, since an investor who buys funds now and then has no
 * list to give.
 */
export function ExperienceStep({ labelledBy, profile, onChange }: ExperienceStepProps) {
  const { t } = useTranslation();
  const yearsId = useId();
  const instrumentsId = useId();
  return (
    <>
      <OptionGroup<InvestorType>
        value={profile.investorType}
        onChange={(investorType) => onChange({ investorType })}
        labelledBy={labelledBy}
      >
        {INVESTOR_TYPES.map((type) => {
          const Icon = TYPE_ICONS[type];
          return (
            <OptionCard
              key={type}
              value={type}
              icon={<Icon className="h-4 w-4" />}
              title={PROFILE_FIELDS.investorType.label(t, type)}
              description={t(`onboarding.welcome.experience.types.${type}.desc`)}
              testId={`welcome-investor-${type}`}
            />
          );
        })}
      </OptionGroup>
      <WizardSection title={t('onboarding.welcome.experience.yearsTitle')} titleId={yearsId}>
        <OptionGroup<ExperienceLevel>
          value={profile.experience}
          onChange={(experience) => onChange({ experience })}
          labelledBy={yearsId}
          // Title-only cards, short enough to sit two to a row on a phone too.
          className="grid-cols-2"
        >
          {EXPERIENCE_LEVELS.map((level) => (
            <OptionCard
              key={level}
              value={level}
              title={PROFILE_FIELDS.experience.label(t, level)}
              testId={`welcome-experience-${level}`}
            />
          ))}
        </OptionGroup>
      </WizardSection>
      <WizardSection
        title={t('onboarding.welcome.experience.instrumentsTitle')}
        titleId={instrumentsId}
        hint={t('onboarding.welcome.optional')}
      >
        <ChipGroup<Instrument>
          labelledBy={instrumentsId}
          values={profile.instruments}
          onChange={(instruments) => onChange({ instruments })}
          options={INSTRUMENTS.map((value) => ({ value, label: PROFILE_FIELDS.instruments.label(t, value) }))}
        />
      </WizardSection>
    </>
  );
}
