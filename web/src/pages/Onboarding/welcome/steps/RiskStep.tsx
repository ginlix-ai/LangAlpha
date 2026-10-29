import { useId } from 'react';
import { useTranslation } from 'react-i18next';
import { MEDIA_HEIGHT as HEIGHT, MEDIA_WIDTH as WIDTH, OptionCard, OptionGroup } from '@/components/ui/option-card';
import { WizardSection } from '@/components/ui/wizard';
import { HORIZONS, PROFILE_FIELDS, RISK_LEVELS, type Horizon, type InvestingProfile, type RiskLevel } from '../profile';

// A chart per level, of the ride each one signs up for, which is the question,
// in the one picture a reader of charts takes in without reading. One market
// ridden four ways: the same moves on the same days, each level taking more
// of every move and a little more drift, so the calmest ends lowest and the
// wildest first falls below where it began. One scale for all four, so the
// thumbnails compare as charts side by side do.
const MOVES = [0.8, -0.3, 0.5, -1.2, -0.9, 0.4, -0.6, 1.1, 0.7, -0.4, 0.9, -0.2, 0.6, 0.3, -0.5, 0.8];
const RIDES: Record<RiskLevel, { drift: number; swing: number }> = {
  conservative: { drift: 0.22, swing: 0.3 },
  moderate: { drift: 0.28, swing: 1 },
  growth: { drift: 0.34, swing: 1.9 },
  aggressive: { drift: 0.4, swing: 3 },
};
const INSET = 6;

function ride({ drift, swing }: { drift: number; swing: number }): number[] {
  const prices = [0];
  let moved = 0;
  MOVES.forEach((move, day) => {
    moved += move;
    prices.push(drift * (day + 1) + swing * moved);
  });
  return prices;
}

const PRICES = RISK_LEVELS.map((level) => ride(RIDES[level]));
const LOW = Math.min(...PRICES.flat());
const HIGH = Math.max(...PRICES.flat());
const round = (n: number) => Math.round(n * 100) / 100;
const LINES = Object.fromEntries(
  RISK_LEVELS.map((level, i) => [
    level,
    PRICES[i]
      .map((price, day) => {
        const x = INSET + ((WIDTH - 2 * INSET) * day) / MOVES.length;
        const y = INSET + ((HEIGHT - 2 * INSET) * (HIGH - price)) / (HIGH - LOW);
        return `${day ? 'L' : 'M'}${round(x)} ${round(y)}`;
      })
      .join(' '),
  ]),
) as Record<RiskLevel, string>;

/** The ride's chart, as a quote list draws one in the well the card sets it
 *  in: the line over a wash that fades toward the axis. */
function RideChart({ level }: { level: RiskLevel }) {
  const wash = `welcome-ride-${level}`;
  return (
    <>
      <defs>
        <linearGradient id={wash} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0" stopColor="currentColor" stopOpacity="0.22" />
          <stop offset="1" stopColor="currentColor" stopOpacity="0" />
        </linearGradient>
      </defs>
      <path d={`${LINES[level]} L${WIDTH - INSET} ${HEIGHT} L${INSET} ${HEIGHT} Z`} fill={`url(#${wash})`} />
      <path d={LINES[level]} stroke="currentColor" strokeWidth="1.25" strokeLinecap="round" strokeLinejoin="round" />
    </>
  );
}

interface RiskStepProps {
  /** The agent's line that asks it. */
  labelledBy: string;
  profile: InvestingProfile;
  onChange: (patch: Partial<InvestingProfile>) => void;
}

/**
 * The level of risk the user names, taken at their word, and how long they
 * usually hold: together they say how much a position may swing before it is
 * a problem for them.
 */
export function RiskStep({ labelledBy, profile, onChange }: RiskStepProps) {
  const { t } = useTranslation();
  const horizonId = useId();
  return (
    <>
      <OptionGroup<RiskLevel>
        value={profile.riskLevel}
        onChange={(riskLevel) => onChange({ riskLevel })}
        labelledBy={labelledBy}
      >
        {RISK_LEVELS.map((level) => (
          <OptionCard
            key={level}
            value={level}
            media={<RideChart level={level} />}
            title={PROFILE_FIELDS.riskLevel.label(t, level)}
            description={t(`onboarding.welcome.risk.levels.${level}.desc`)}
            testId={`welcome-risk-${level}`}
          />
        ))}
      </OptionGroup>
      <WizardSection title={t('onboarding.welcome.risk.horizonTitle')} titleId={horizonId}>
        <OptionGroup<Horizon>
          value={profile.horizon}
          onChange={(horizon) => onChange({ horizon })}
          labelledBy={horizonId}
          // Title-only cards, short enough to sit two to a row on a phone too.
          className="grid-cols-2"
        >
          {HORIZONS.map((horizon) => (
            <OptionCard
              key={horizon}
              value={horizon}
              title={PROFILE_FIELDS.horizon.label(t, horizon)}
              testId={`welcome-horizon-${horizon}`}
            />
          ))}
        </OptionGroup>
      </WizardSection>
    </>
  );
}
