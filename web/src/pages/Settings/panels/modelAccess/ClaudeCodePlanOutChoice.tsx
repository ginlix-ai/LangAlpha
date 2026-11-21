import { useId } from 'react';
import { ExternalLink } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import type { TokenRow } from '@/components/modelAccess/tokenLine';
import { useClaudeCodeComputers, useClaudeCodeStatus } from '@/hooks/useClaudeCode';
import { usePlanOut } from '@/hooks/useClaudeCodePlan';
import { useLocale } from '@/hooks/useLocale';
import { savedTokenInSentence, type PlanOut } from '@/lib/claudeCodePlan';
import { formatList } from '@/lib/format';
import { InfoHint } from '@/components/ui/info-hint';
import { useSaveFailedToast } from '../usePreferenceWrite';
import { ModeCard } from './ModeCard';

/** Where a Claude account turns its usage credits on or off. */
const CLAUDE_USAGE_URL = 'https://claude.ai/settings/usage';

const CHOICES: readonly { value: PlanOut; label: string; detail: string }[] = [
  { value: 'stop', label: 'claudeCodePlan.runsOut.stop', detail: 'claudeCodePlan.runsOut.stopDetail' },
  { value: 'credits', label: 'claudeCodePlan.runsOut.credits', detail: 'claudeCodePlan.runsOut.creditsDetail' },
];

/**
 * What Claude Code turns do once a Claude plan's limit is used up: stop and
 * wait for it to reset, or carry on on the account's usage credits. A pick
 * applies at once, as Settings' other choices do. Where a row of the view
 * (`tokenRows`) reports its account's usage credits on, a note names those
 * rows: Claude bills the first request past the limit to them before
 * LangAlpha can stop, which only the account's own setting prevents. A
 * computer that signs in with the saved token is that token's account, named
 * once on the token's own row where the view shows one. Picking usage
 * credits again saves them again, which asks the account afresh whether
 * they pay: one turned on in claude.ai since a report said off.
 */
export function ClaudeCodePlanOutChoice({ tokenRows }: { tokenRows: TokenRow[] }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const headingId = useId();
  const groupName = useId();
  const detailsId = useId();
  const status = useClaudeCodeStatus();
  const { computers } = useClaudeCodeComputers();
  const { planOut, set } = usePlanOut();
  const toastSaveFailed = useSaveFailedToast();
  const choose = (out: PlanOut) => void set(out).catch(toastSaveFailed);
  const savedShown = tokenRows.some(({ key }) => key === 'saved');
  const creditsOn = [
    ...computers
      .filter((c) => c.plan?.credits === 'on' && tokenRows.some(({ key }) => key === c.computer_id) && !(savedShown && c.source === 'saved'))
      .map((c) => c.name),
    ...(savedShown && status.plan?.credits === 'on'
      ? [savedTokenInSentence(t('modelAccess.claudeCode.setup.savedName'), locale)]
      : []),
  ];

  return (
    <section aria-labelledby={headingId} className="flex flex-col gap-1.5">
      <div className="flex items-center gap-1">
        <h3 id={headingId} className="text-xs font-medium" style={{ color: 'var(--color-text-tertiary)' }}>
          {t('claudeCodePlan.runsOut.title')}
        </h3>
        <InfoHint name={t('claudeCodePlan.runsOut.title')} id={detailsId}>
          {CHOICES.map(({ value, label, detail }) => (
            <span key={value} className="block not-first:mt-1.5">
              <span className="font-medium">{t(label)}</span>: {t(detail)}{' '}
            </span>
          ))}
        </InfoHint>
      </div>
      {/* Two short names side by side, each explained once in the info mark
          rather than under itself; a screen reader hears that on the group. */}
      <div role="radiogroup" aria-labelledby={headingId} aria-describedby={detailsId} className="grid grid-cols-1 gap-2 sm:grid-cols-2">
        {CHOICES.map(({ value, label }) => (
          <ModeCard
            key={value}
            name={groupName}
            mode={value}
            checked={planOut === value}
            label={t(label)}
            onSelect={choose}
            onReselect={value === 'credits' ? choose : undefined}
          />
        ))}
      </div>
      {creditsOn.length > 0 && (
        <p className="text-xs leading-relaxed" style={{ color: 'var(--color-text-secondary)' }}>
          {t('claudeCodePlan.runsOut.note', { names: formatList(creditsOn, locale) })}{' '}
          <a
            href={CLAUDE_USAGE_URL}
            target="_blank"
            rel="noopener noreferrer"
            className="tap-target inline-flex items-center gap-0.5 underline underline-offset-2 hover:opacity-80"
          >
            {t('claudeCodePlan.runsOut.noteLink')}
            <ExternalLink aria-hidden="true" className="h-3 w-3" />
          </a>
        </p>
      )}
    </section>
  );
}
