import { useId, type ComponentType } from 'react';
import { useTranslation } from 'react-i18next';
import { Bell, BellOff, BellRing, FileText, LayoutDashboard, MessageSquareText } from 'lucide-react';
import { OptionCard, OptionGroup } from '@/components/ui/option-card';
import { WizardSection } from '@/components/ui/wizard';
import {
  DELIVERABLES,
  PROACTIVITY,
  PROFILE_FIELDS,
  RESEARCH_DEPTHS,
  type Deliverable,
  type InvestingProfile,
  type Proactivity,
  type ResearchDepth,
} from '../profile';

// The answer each level gives, drawn as a page in the card's well, as the risk
// step draws its rides: the quick one is a conclusion and a line under it, the
// full one the conclusion, the working and the figures beside it, and the
// one that depends on the question the quick page with the rest there to go
// to, faint.
const CONCLUSION: Array<[number, number, number]> = [
  [9, 30, 2],
  [15, 48, 1.25],
];
const WORKING: Array<[number, number, number]> = [
  [20, 48, 1.25],
  [25, 30, 1.25],
  [30, 26, 1.25],
];
const BARS = [5, 9, 7];
/** How much of the working and the figures each page shows. */
const REST: Record<ResearchDepth, number> = { light: 0, adaptive: 0.4, thorough: 1 };

/** The answer as a page, in the card's 56 by 40 well. */
function DepthPage({ depth }: { depth: ResearchDepth }) {
  const rest = REST[depth];
  return (
    <>
      {CONCLUSION.map(([y, end, weight], i) => (
        <path
          key={y}
          d={`M8 ${y} H${end}`}
          stroke="currentColor"
          strokeWidth={weight}
          strokeLinecap="round"
          strokeOpacity={i === 0 ? 1 : 0.55}
        />
      ))}
      {rest > 0 && (
        <g opacity={rest}>
          {WORKING.map(([y, end, weight]) => (
            <path
              key={y}
              d={`M8 ${y} H${end}`}
              stroke="currentColor"
              strokeWidth={weight}
              strokeLinecap="round"
              strokeOpacity={0.55}
            />
          ))}
          {BARS.map((height, i) => (
            <rect key={i} x={35 + i * 5} y={32 - height} width={3} height={height} rx={0.75} fill="currentColor" />
          ))}
        </g>
      )}
    </>
  );
}

const DELIVERABLE_ICONS: Record<Deliverable, ComponentType<{ className?: string }>> = {
  text: MessageSquareText,
  markdown: FileText,
  visual: LayoutDashboard,
};

const PROACTIVITY_ICONS: Record<Proactivity, ComponentType<{ className?: string }>> = {
  proactive: BellRing,
  when_needed: Bell,
  on_request: BellOff,
};

interface AgentStepProps {
  /** The agent's line that asks it. */
  labelledBy: string;
  profile: InvestingProfile;
  onChange: (patch: Partial<InvestingProfile>) => void;
}

/**
 * How the user wants the agent to work with them by default: how much
 * research a question gets, which is most of how long an answer takes and
 * what it costs; what the answer usually is; and whether the agent reaches
 * out unasked. The agent is told all three on every turn, and a question
 * that asks for something else is answered as asked.
 */
export function AgentStep({ labelledBy, profile, onChange }: AgentStepProps) {
  const { t } = useTranslation();
  const deliverableId = useId();
  const proactivityId = useId();
  return (
    <>
      <OptionGroup<ResearchDepth>
        value={profile.depth}
        onChange={(depth) => onChange({ depth })}
        labelledBy={labelledBy}
      >
        {RESEARCH_DEPTHS.map((depth) => (
          <OptionCard
            key={depth}
            value={depth}
            media={<DepthPage depth={depth} />}
            title={PROFILE_FIELDS.depth.label(t, depth)}
            description={t(`onboarding.welcome.agent.depth.${depth}.desc`)}
            testId={`welcome-depth-${depth}`}
          />
        ))}
      </OptionGroup>
      <WizardSection title={t('onboarding.welcome.agent.deliverableTitle')} titleId={deliverableId}>
        <OptionGroup<Deliverable>
          value={profile.deliverable}
          onChange={(deliverable) => onChange({ deliverable })}
          labelledBy={deliverableId}
        >
          {DELIVERABLES.map((deliverable) => {
            const Icon = DELIVERABLE_ICONS[deliverable];
            return (
              <OptionCard
                key={deliverable}
                value={deliverable}
                icon={<Icon className="h-4 w-4" />}
                title={PROFILE_FIELDS.deliverable.label(t, deliverable)}
                description={t(`onboarding.welcome.agent.deliverable.${deliverable}.desc`)}
                testId={`welcome-deliverable-${deliverable}`}
              />
            );
          })}
        </OptionGroup>
      </WizardSection>
      <WizardSection title={t('onboarding.welcome.agent.proactivityTitle')} titleId={proactivityId}>
        <OptionGroup<Proactivity>
          value={profile.proactivity}
          onChange={(proactivity) => onChange({ proactivity })}
          labelledBy={proactivityId}
        >
          {PROACTIVITY.map((level) => {
            const Icon = PROACTIVITY_ICONS[level];
            return (
              <OptionCard
                key={level}
                value={level}
                icon={<Icon className="h-4 w-4" />}
                title={PROFILE_FIELDS.proactivity.label(t, level)}
                description={t(`onboarding.welcome.agent.proactivity.${level}.desc`)}
                testId={`welcome-proactivity-${level}`}
              />
            );
          })}
        </OptionGroup>
      </WizardSection>
    </>
  );
}
