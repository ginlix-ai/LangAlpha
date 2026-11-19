/**
 * The pieces the answer-driven setup steps share, drawn in the language of
 * the model setup they sit beside (Settings' Model access and the provider
 * steps): the strip naming the two answers, a step's kicker, a row's button,
 * a quiet note under a step, and a step's loading and failed states. Rows
 * are Model access's own (`Group`, `AccessRow`).
 */
import type { ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { Check } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { RadioChoice } from '@/components/ui/choice';
import { Loader } from '@/components/ui/loader';
import { FlowFailure, ROW_BUTTON } from '@/components/modelAccess/parts';
import { isPlatformMode } from '@/config/hostMode';
import { useChangeAnswers } from './flow';
import type { SetupAnswers } from './setupSteps';

/** The answers' tags, in this deployment's words: self-hosted names the
 *  server's models and computers rather than LangAlpha's. */
function answerTags(t: (key: string) => string, answers: SetupAnswers): [string, string] {
  const models = answers.models === 'langalpha_models' && !isPlatformMode ? 'serverModels' : answers.models;
  const runsOn = answers.runsOn === 'cloud' && !isPlatformMode ? 'serverComputer' : answers.runsOn;
  return [t(`setup.answers.tag.${models}`), t(`setup.answers.tag.${runsOn}`)];
}

/**
 * "From your answers", above the first setup step: the two answers as quiet
 * chips and the way back to change them, the hosted onboarding questions on
 * hosted, this app's on self-hosted.
 */
export function AnswersStrip({ answers }: { answers: SetupAnswers }) {
  const { t } = useTranslation();
  const change = useChangeAnswers();
  return (
    <div data-testid="setup-answers" className="shell-box mb-6 flex flex-wrap items-center gap-x-2 gap-y-1.5 px-4 py-2.5 sm:mb-8">
      <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{t('setup.answers.fromYourAnswers')}</span>
      {answerTags(t, answers).map((tag) => (
        <span
          key={tag}
          className="rounded-full px-2 py-0.5 text-xs"
          style={{ background: 'var(--color-bg-tag)', color: 'var(--color-text-secondary)' }}
        >
          {tag}
        </span>
      ))}
      <span className="flex-1" />
      <button
        type="button"
        onClick={change}
        className="tap-target text-xs underline underline-offset-2 transition-colors text-(--color-text-secondary) hover:text-(--color-text-primary)"
      >
        {t('setup.answers.change')}
      </button>
    </div>
  );
}

/** A small label over a step's title, in the voice of a card's section labels. */
export function StepKicker({ children }: { children: ReactNode }) {
  return (
    <p className="open-style:label-text mb-2 text-xs font-medium" style={{ color: 'var(--color-shell-label, var(--color-text-tertiary))' }}>
      {children}
    </p>
  );
}

/** One answer of a few, as a card: what it is, then a check at its end. The
 *  picked card is inked (its border and a filled check), the others an
 *  empty ring at the radio's size. Native radios underneath (`RadioChoice`). */
export function PickCard<V extends string>({ name, value, checked, onSelect, children }: {
  name: string;
  value: V;
  checked: boolean;
  onSelect: (value: V) => void;
  children: ReactNode;
}) {
  return (
    <RadioChoice
      name={name}
      value={value}
      checked={checked}
      onSelect={onSelect}
      size="md"
      markClassName="hidden"
      className="shell-box items-center gap-3 px-4 py-3.5"
      style={checked ? { borderColor: 'var(--color-text-primary)' } : undefined}
    >
      {children}
      <span
        aria-hidden="true"
        className="flex h-4 w-4 shrink-0 items-center justify-center rounded-full border"
        style={checked
          ? { backgroundColor: 'var(--color-btn-primary-bg)', borderColor: 'var(--color-btn-primary-bg)' }
          : { borderColor: 'var(--color-text-quaternary)' }}
      >
        {checked && <Check className="h-2.5 w-2.5" strokeWidth={3.5} style={{ color: 'var(--color-btn-primary-text)' }} />}
      </span>
    </RadioChoice>
  );
}

/** One answer to one of the two questions: its mark, its name over a line
 *  saying what it means, and the check. */
export function AnswerOption<V extends string>({ name, value, checked, onSelect, mark, title, detail }: {
  name: string;
  value: V;
  checked: boolean;
  onSelect: (value: V) => void;
  mark: ReactNode;
  title: string;
  detail: string;
}) {
  return (
    <PickCard name={name} value={value} checked={checked} onSelect={onSelect}>
      <span className="flex min-w-0 flex-1 items-center gap-3">
        <span className="flex w-6 shrink-0 justify-center" style={{ color: 'var(--color-text-secondary)' }}>{mark}</span>
        <span className="flex min-w-0 flex-col gap-0.5">
          <span className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>{title}</span>
          <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{detail}</span>
        </span>
      </span>
    </PickCard>
  );
}

/** A row's button, in Model access's row-button look. */
export function RowButton({ children, onClick, disabled }: { children: ReactNode; onClick: () => void; disabled?: boolean }) {
  return (
    <Button type="button" variant="outline" size="sm" className={ROW_BUTTON} onClick={onClick} disabled={disabled}>
      {children}
    </Button>
  );
}

/** A note under a step, with its icon, in a box of its own. */
export function SetupNote({ icon, children }: { icon?: ReactNode; children: ReactNode }) {
  return (
    <p
      className="mt-3 flex items-start gap-2.5 rounded-md border px-3.5 py-3 text-xs leading-relaxed"
      style={{ backgroundColor: 'var(--color-bg-subtle)', borderColor: 'var(--color-border-muted)', color: 'var(--color-text-secondary)' }}
    >
      {icon && <span aria-hidden="true" className="mt-px flex shrink-0 [&>svg]:h-3.5 [&>svg]:w-3.5" style={{ color: 'var(--color-text-tertiary)' }}>{icon}</span>}
      <span>{children}</span>
    </p>
  );
}

/** A step's write that failed, under the step; `quiet` for one the user
 *  need not act on, since the step goes on without it. */
export function StepError({ children, quiet = false }: { children: ReactNode; quiet?: boolean }) {
  return (
    <p role="alert" className="mt-4 text-sm" style={{ color: quiet ? 'var(--color-text-secondary)' : 'var(--color-loss)' }}>
      {children}
    </p>
  );
}

/** A step still reading what it shows. */
export function StepLoading() {
  const { t } = useTranslation();
  return (
    <div className="flex items-center justify-center py-20 text-(--color-text-tertiary)">
      <Loader size={20} label={t('common.loading')} />
    </div>
  );
}

/** A setup page whose own read (or, with `saving`, its own write) failed,
 *  with the way to try it again. */
export function StepLoadFailed({ onRetry, saving = false }: { onRetry: () => void; saving?: boolean }) {
  const { t } = useTranslation();
  return (
    <div data-testid="setup-load-failed" className="py-10">
      <FlowFailure
        title={t(saving ? 'setup.answers.saveFailed' : 'setup.answers.loadFailed')}
        action={<RowButton onClick={onRetry}>{t('setup.answers.tryAgain')}</RowButton>}
      />
    </div>
  );
}
