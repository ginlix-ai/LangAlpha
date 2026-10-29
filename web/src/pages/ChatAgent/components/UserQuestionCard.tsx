import React, { useState } from 'react';
import { motion, AnimatePresence } from '@/lib/framer';
import { HelpCircle, Check, SkipForward, Send } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { SPRING_INDICATOR, staggerDelay, TWEEN } from '@/lib/motion';
import { Disclosure } from '@/components/ui/Disclosure';
import { isImeComposing } from '@/lib/ime';
import Markdown from './Markdown';
import { ResolveSwap } from './decisionCardMotion';
import { SettledStep } from './SettledStep';

interface OptionCheckboxProps {
  id: string;
  label: string;
  checked: boolean;
  onChange: () => void;
  disabled?: boolean;
}

/**
 * Inline checkbox row adapted from PremiumCheckbox (checkbox-02).
 */
function OptionCheckbox({ id, label, checked, onChange, disabled }: OptionCheckboxProps): React.ReactElement {
  return (
    <label
      htmlFor={id}
      className={`flex items-center gap-3.5 cursor-pointer group py-2.5 px-3 rounded-lg transition-colors ${
        disabled ? 'opacity-50 cursor-not-allowed' : 'hover:bg-(--color-border-muted)'
      }`}
    >
      <div className="relative flex items-center justify-center">
        <input
          id={id}
          type="checkbox"
          checked={checked}
          onChange={onChange}
          disabled={disabled}
          className="sr-only"
        />
        <div
          className={`
            w-[22px] h-[22px] rounded-md border-2 flex items-center justify-center
            transition-colors
            ${
              checked
                ? 'bg-(--color-btn-primary-bg) border-(--color-btn-primary-bg)'
                : 'bg-transparent border-(--color-border-muted) group-hover:border-(--color-border-muted)'
            }
          `}
        >
          <AnimatePresence initial={false}>
            {checked && (
              <motion.div
                initial={{ scale: 0.6, opacity: 0 }}
                animate={{ scale: 1, opacity: 1 }}
                exit={{ scale: 0.6, opacity: 0, transition: TWEEN.exit }}
                transition={SPRING_INDICATOR}
              >
                <Check className="w-3.5 h-3.5 stroke-3" style={{ color: 'var(--color-btn-primary-text)' }} />
              </motion.div>
            )}
          </AnimatePresence>
        </div>
      </div>
      <span
        className="text-sm font-medium tracking-wide transition-colors"
        style={{ color: checked ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)' }}
      >
        {label}
      </span>
    </label>
  );
}

interface OptionRadioProps {
  label: string;
  onClick: () => void;
  disabled?: boolean;
}

/**
 * Single-select option row -- clicking submits immediately.
 */
function OptionRadio({ label, onClick, disabled }: OptionRadioProps): React.ReactElement {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      className={`press flex items-center gap-3.5 w-full text-left py-2.5 px-3 rounded-lg ${
        disabled ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer hover:bg-(--color-border-muted)'
      }`}
    >
      <div
        className="w-[22px] h-[22px] rounded-full border-2 flex items-center justify-center transition-colors"
        style={{ borderColor: 'var(--color-border-muted)' }}
      />
      <span
        className="text-sm font-medium tracking-wide"
        style={{ color: 'var(--color-text-tertiary)' }}
      >
        {label}
      </span>
    </button>
  );
}

interface ResolvedOptionProps {
  label: string;
  isSelected: boolean;
  isMulti: boolean;
}

/**
 * Read-only option row shown in the expanded resolved view.
 * Selected options get a filled check; unselected get a dimmed empty box.
 */
function ResolvedOption({ label, isSelected, isMulti }: ResolvedOptionProps): React.ReactElement {
  return (
    <div className="flex items-center gap-3.5 py-1.5 px-3">
      <div
        className="w-[22px] h-[22px] flex items-center justify-center border-2 transition-colors"
        style={{
          borderRadius: isMulti ? '6px' : '50%',
          backgroundColor: isSelected ? 'var(--color-btn-primary-bg)' : 'transparent',
          borderColor: isSelected ? 'var(--color-btn-primary-bg)' : 'var(--color-border-muted)',
        }}
      >
        {isSelected && <Check className="w-3.5 h-3.5 stroke-3" style={{ color: 'var(--color-btn-primary-text)' }} />}
      </div>
      <span
        className="text-sm tracking-wide"
        style={{ color: isSelected ? 'var(--color-text-primary)' : 'var(--color-icon-muted)' }}
      >
        {label}
      </span>
    </div>
  );
}

interface QuestionData {
  question: string;
  options?: string[];
  allow_multiple?: boolean;
  status: 'pending' | 'answered' | 'skipped';
  answer?: string;
  [key: string]: unknown;
}

interface UserQuestionCardProps {
  questionData: QuestionData | null;
  onAnswer?: (answer: string) => void;
  onSkip?: () => void;
}

/**
 * UserQuestionCard - Inline message segment for AskUserQuestion HITL interrupt.
 *
 * States:
 *   pending   - Full interactive: question + options + "Other" input + Skip
 *   answered  - Collapsed summary, click to expand and see all options with selection highlighted
 *   skipped   - Collapsed summary, click to expand and see the question + options
 */
function UserQuestionCard({ questionData, onAnswer, onSkip }: UserQuestionCardProps): React.ReactElement | null {
  const { t } = useTranslation();
  // Local state
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [otherText, setOtherText] = useState('');

  if (!questionData) return null;

  const { question, options = [], allow_multiple = false, status, answer } = questionData;
  const isAnswered = status === 'answered';
  const isSkipped = status === 'skipped';
  const isResolved = isAnswered || isSkipped;

  // Parse which options were selected from the answer string
  const answeredOptions = isAnswered && answer
    ? new Set(answer.split(', ').map((s) => s.trim()).filter(Boolean))
    : new Set<string>();
  // Whether the answer was a custom "Other" value (not matching any option)
  const isCustomAnswer = isAnswered && answer && answeredOptions.size > 0
    && ![...answeredOptions].some((a) => options.includes(a));

  // --- Resolved (answered / skipped): the shared settled row, opening onto
  // the question and what was picked. A skip is the question declined.
  const settled = (
    <SettledStep
      verdict={isAnswered ? 'approved' : 'rejected'}
      icon={isSkipped ? <SkipForward className="h-4 w-4 shrink-0" style={{ color: 'var(--color-icon-muted)' }} /> : undefined}
      label={isAnswered ? t('chat.userQuestion.answered', { answer: answer || t('chat.userQuestion.noAnswer') }) : t('chat.userQuestion.skipped')}
      testId="user-question-settled"
    >
      {/* Question */}
      <div className="flex items-start gap-2 pb-2">
        <HelpCircle className="h-3.5 w-3.5 shrink-0 mt-1" style={{ color: 'var(--color-icon-muted)' }} />
        <div className="text-sm min-w-0" style={{ color: 'var(--color-text-tertiary)' }}>
          <Markdown variant="compact" content={question} />
        </div>
      </div>

      {/* Options with selection state */}
      {options.length > 0 && (
        <div className="py-0.5">
          {options.map((option) => (
            <ResolvedOption
              key={option}
              label={option}
              isSelected={answeredOptions.has(option)}
              isMulti={allow_multiple}
            />
          ))}
        </div>
      )}

      {/* Show custom answer if it didn't match any option */}
      {isCustomAnswer && (
        <div className="flex items-center gap-2 pt-1 px-3">
          <span className="text-xs" style={{ color: 'var(--color-icon-muted)' }}>{t('chat.userQuestion.custom')}</span>
          <span className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>{answer}</span>
        </div>
      )}
    </SettledStep>
  );

  // --- Pending: full interactive ---
  const handleToggle = (option: string): void => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(option)) next.delete(option);
      else next.add(option);
      return next;
    });
  };

  const handleSubmitMulti = (): void => {
    if (selected.size === 0) return;
    onAnswer?.([...selected].join(', '));
  };

  const handleSubmitOther = (): void => {
    const text = otherText.trim();
    if (!text) return;
    onAnswer?.(text);
  };

  const handleOtherKeyDown = (e: React.KeyboardEvent<HTMLInputElement>): void => {
    if (e.key === 'Enter' && !e.shiftKey && !isImeComposing(e)) {
      e.preventDefault();
      handleSubmitOther();
    }
  };

  return (
    <ResolveSwap resolved={isResolved} settled={settled}>
      {/* Question text */}
      <div className="flex items-start gap-2 pb-2">
        <HelpCircle className="h-4 w-4 shrink-0 mt-1" style={{ color: 'var(--color-accent-light)' }} />
        <div className="text-[0.9375rem] font-medium min-w-0" style={{ color: 'var(--color-text-primary)' }}>
          <Markdown variant="compact" content={question} />
        </div>
      </div>

      {/* Vertical option list. The options arrive with the card, a batch
          read top to bottom, so they stagger in; the card only ever mounts
          them once. */}
      {options.length > 0 && (
        <div className="py-1">
          {options.map((option, index) => (
            <motion.div
              key={option}
              initial={{ opacity: 0, x: -6 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ ...TWEEN.enter, delay: staggerDelay(index) }}
            >
              {allow_multiple ? (
                <OptionCheckbox
                  id={`q-opt-${option}`}
                  label={option}
                  checked={selected.has(option)}
                  onChange={() => handleToggle(option)}
                />
              ) : (
                <OptionRadio
                  label={option}
                  onClick={() => onAnswer?.(option)}
                />
              )}
            </motion.div>
          ))}
        </div>
      )}

      {/* Multi-select submit button, folding in with the first pick so the
          rest of the card is not pushed down in one frame. */}
      <Disclosure open={allow_multiple && selected.size > 0} className="pt-1 pb-2">
        <button
          onClick={handleSubmitMulti}
          className="press flex items-center gap-1.5 text-sm px-4 py-2 rounded-md font-medium hover:brightness-110"
          style={{
            backgroundColor: 'var(--color-btn-primary-bg)',
            color: 'var(--color-btn-primary-text)',
          }}
        >
          <Check className="h-3.5 w-3.5 stroke-[2.5]" />
          {t('chat.userQuestion.submitCount', { n: selected.size })}
        </button>
      </Disclosure>

      {/* "Other" text input */}
      <div className="pt-1 pb-1 flex gap-2 items-center">
        <input
          type="text"
          placeholder={t('chat.userQuestion.customPlaceholder')}
          value={otherText}
          onChange={(e: React.ChangeEvent<HTMLInputElement>) => setOtherText(e.target.value)}
          onKeyDown={handleOtherKeyDown}
          className="flex-1 text-sm px-3 py-2 rounded-md transition-colors focus:border-(--color-accent-overlay)"
          style={{
            backgroundColor: 'var(--color-border-muted)',
            border: '1px solid var(--color-border-muted)',
            color: 'var(--color-text-primary)',
          }}
        />
        <AnimatePresence initial={false}>
          {otherText.trim() && (
            // The scale and fade sit on a wrapper: the button's own `press`
            // transition lists opacity, and would trail framer's writes.
            <motion.span
              className="flex"
              initial={{ opacity: 0, scale: 0.8 }}
              animate={{ opacity: 1, scale: 1 }}
              exit={{ opacity: 0, scale: 0.8, transition: TWEEN.exit }}
              transition={TWEEN.quick}
            >
              <button
                type="button"
                onClick={handleSubmitOther}
                aria-label={t('common.submit')}
                className="press p-2 rounded-md hover:brightness-110"
                style={{
                  backgroundColor: 'var(--color-btn-primary-bg)',
                  color: 'var(--color-btn-primary-text)',
                }}
              >
                <Send className="h-3.5 w-3.5" />
              </button>
            </motion.span>
          )}
        </AnimatePresence>
      </div>

      {/* Skip */}
      <div className="pt-1">
        <button
          onClick={() => onSkip?.()}
          className="press flex items-center gap-1.5 text-sm px-3 py-1.5 rounded-md"
          style={{
            backgroundColor: 'transparent',
            color: 'var(--color-icon-muted)',
          }}
          onMouseEnter={(e: React.MouseEvent<HTMLButtonElement>) => {
            e.currentTarget.style.backgroundColor = 'var(--color-border-muted)';
            e.currentTarget.style.color = 'var(--color-text-tertiary)';
          }}
          onMouseLeave={(e: React.MouseEvent<HTMLButtonElement>) => {
            e.currentTarget.style.backgroundColor = 'transparent';
            e.currentTarget.style.color = 'var(--color-icon-muted)';
          }}
        >
          <SkipForward className="h-3.5 w-3.5" />
          {t('chat.skip')}
        </button>
      </div>
    </ResolveSwap>
  );
}

export default UserQuestionCard;
