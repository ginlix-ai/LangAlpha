import React, { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { HelpCircle, Check, SkipForward, Send, ChevronRight } from 'lucide-react';

/**
 * Inline checkbox row adapted from PremiumCheckbox (checkbox-02).
 */
function OptionCheckbox({ id, label, checked, onChange, disabled }) {
  return (
    <label
      htmlFor={id}
      className={`flex items-center gap-3.5 cursor-pointer group py-2.5 px-3 rounded-lg transition-colors duration-200 ${
        disabled ? 'opacity-50 cursor-not-allowed' : 'hover:bg-[rgba(255,255,255,0.04)]'
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
        <motion.div
          className={`
            w-[22px] h-[22px] rounded-md border-2 flex items-center justify-center
            transition-colors duration-200
            ${
              checked
                ? 'bg-white border-white'
                : 'bg-transparent border-[rgba(255,255,255,0.2)] group-hover:border-[rgba(255,255,255,0.4)]'
            }
          `}
          whileHover={!disabled ? { scale: 1.08 } : {}}
          whileTap={!disabled ? { scale: 0.92 } : {}}
        >
          <AnimatePresence mode="wait">
            {checked && (
              <motion.div
                initial={{ scale: 0, opacity: 0 }}
                animate={{ scale: 1, opacity: 1 }}
                exit={{ scale: 0, opacity: 0 }}
                transition={{ type: 'spring', stiffness: 500, damping: 25 }}
              >
                <Check className="w-3.5 h-3.5 text-black stroke-[3]" />
              </motion.div>
            )}
          </AnimatePresence>
        </motion.div>
      </div>
      <span
        className="text-sm font-medium tracking-wide transition-colors duration-200"
        style={{ color: checked ? 'rgba(255,255,255,0.95)' : 'rgba(255,255,255,0.7)' }}
      >
        {label}
      </span>
    </label>
  );
}

/**
 * Single-select option row — clicking submits immediately.
 */
function OptionRadio({ label, onClick, disabled }) {
  return (
    <motion.button
      onClick={onClick}
      disabled={disabled}
      className={`flex items-center gap-3.5 w-full text-left py-2.5 px-3 rounded-lg transition-colors duration-200 ${
        disabled ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer hover:bg-[rgba(255,255,255,0.04)]'
      }`}
      whileHover={!disabled ? { x: 2 } : {}}
      whileTap={!disabled ? { scale: 0.99 } : {}}
    >
      <div
        className="w-[22px] h-[22px] rounded-full border-2 flex items-center justify-center transition-colors duration-200"
        style={{ borderColor: 'rgba(255,255,255,0.2)' }}
      />
      <span
        className="text-sm font-medium tracking-wide"
        style={{ color: 'rgba(255,255,255,0.7)' }}
      >
        {label}
      </span>
    </motion.button>
  );
}

/**
 * Read-only option row shown in the expanded resolved view.
 * Selected options get a filled check; unselected get a dimmed empty box.
 */
function ResolvedOption({ label, isSelected, isMulti }) {
  return (
    <div className="flex items-center gap-3.5 py-1.5 px-3">
      <div
        className="w-[22px] h-[22px] flex items-center justify-center border-2 transition-colors duration-200"
        style={{
          borderRadius: isMulti ? '6px' : '50%',
          backgroundColor: isSelected ? 'white' : 'transparent',
          borderColor: isSelected ? 'white' : 'rgba(255,255,255,0.12)',
        }}
      >
        {isSelected && <Check className="w-3.5 h-3.5 text-black stroke-[3]" />}
      </div>
      <span
        className="text-sm tracking-wide"
        style={{ color: isSelected ? 'rgba(255,255,255,0.9)' : 'rgba(255,255,255,0.3)' }}
      >
        {label}
      </span>
    </div>
  );
}

/**
 * UserQuestionCard - Inline message segment for AskUserQuestion HITL interrupt.
 *
 * States:
 *   pending   – Full interactive: question + options + "Other" input + Skip
 *   answered  – Collapsed summary, click to expand and see all options with selection highlighted
 *   skipped   – Collapsed summary, click to expand and see the question + options
 */
function UserQuestionCard({ questionData, onAnswer, onSkip }) {
  if (!questionData) return null;

  const { question, options = [], allow_multiple = false, status, answer } = questionData;
  const isAnswered = status === 'answered';
  const isSkipped = status === 'skipped';
  const isResolved = isAnswered || isSkipped;

  // Local state
  const [selected, setSelected] = useState(new Set());
  const [otherText, setOtherText] = useState('');
  const [expanded, setExpanded] = useState(false);

  // Parse which options were selected from the answer string
  const answeredOptions = isAnswered && answer
    ? new Set(answer.split(', ').map((s) => s.trim()).filter(Boolean))
    : new Set();
  // Whether the answer was a custom "Other" value (not matching any option)
  const isCustomAnswer = isAnswered && answer && answeredOptions.size > 0
    && ![...answeredOptions].some((a) => options.includes(a));

  // --- Resolved (answered / skipped): collapsible ---
  if (isResolved) {
    return (
      <div>
        {/* Collapsed summary row */}
        <button
          onClick={() => setExpanded((v) => !v)}
          className="flex items-center gap-2 py-1 group cursor-pointer w-full text-left"
        >
          <motion.div
            animate={{ rotate: expanded ? 90 : 0 }}
            transition={{ duration: 0.2 }}
          >
            <ChevronRight
              className="h-3.5 w-3.5 flex-shrink-0"
              style={{ color: 'rgba(255,255,255,0.25)' }}
            />
          </motion.div>
          {isAnswered ? (
            <Check className="h-4 w-4 flex-shrink-0" style={{ color: '#8B83F0' }} />
          ) : (
            <SkipForward className="h-4 w-4 flex-shrink-0" style={{ color: 'rgba(255, 255, 255, 0.35)' }} />
          )}
          <span
            className="text-sm"
            style={{ color: isAnswered ? 'rgba(255, 255, 255, 0.6)' : 'rgba(255, 255, 255, 0.45)' }}
          >
            {isAnswered ? `Answered: ${answer || '(no answer)'}` : 'Question skipped'}
          </span>
        </button>

        {/* Expanded detail */}
        <AnimatePresence>
          {expanded && (
            <motion.div
              initial={{ height: 0, opacity: 0 }}
              animate={{ height: 'auto', opacity: 1 }}
              exit={{ height: 0, opacity: 0 }}
              transition={{ duration: 0.25, ease: [0.22, 1, 0.36, 1] }}
              className="overflow-hidden"
            >
              <div className="pt-2 pb-1 pl-6">
                {/* Question */}
                <div className="flex items-center gap-2 pb-2">
                  <HelpCircle className="h-3.5 w-3.5 flex-shrink-0" style={{ color: 'rgba(255,255,255,0.3)' }} />
                  <p className="text-sm" style={{ color: 'rgba(255, 255, 255, 0.5)' }}>
                    {question}
                  </p>
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
                    <span className="text-xs" style={{ color: 'rgba(255,255,255,0.35)' }}>Custom:</span>
                    <span className="text-sm" style={{ color: 'rgba(255,255,255,0.7)' }}>{answer}</span>
                  </div>
                )}
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    );
  }

  // --- Pending: full interactive ---
  const handleToggle = (option) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(option)) next.delete(option);
      else next.add(option);
      return next;
    });
  };

  const handleSubmitMulti = () => {
    if (selected.size === 0) return;
    onAnswer?.([...selected].join(', '));
  };

  const handleSubmitOther = () => {
    const text = otherText.trim();
    if (!text) return;
    onAnswer?.(text);
  };

  const handleOtherKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSubmitOther();
    }
  };

  return (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4, ease: [0.22, 1, 0.36, 1] }}
    >
      {/* Question text */}
      <div className="flex items-center gap-2 pb-2">
        <HelpCircle className="h-4 w-4 flex-shrink-0" style={{ color: '#8B83F0' }} />
        <p className="text-[15px] font-medium" style={{ color: 'rgba(255, 255, 255, 0.9)' }}>
          {question}
        </p>
      </div>

      {/* Vertical option list */}
      {options.length > 0 && (
        <div className="py-1">
          {options.map((option, index) => (
            <motion.div
              key={option}
              initial={{ opacity: 0, x: -8 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{
                delay: 0.1 + index * 0.05,
                duration: 0.35,
                ease: [0.22, 1, 0.36, 1],
              }}
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

      {/* Multi-select submit button */}
      {allow_multiple && selected.size > 0 && (
        <div className="pt-1 pb-2">
          <motion.button
            initial={{ opacity: 0, y: 4 }}
            animate={{ opacity: 1, y: 0 }}
            onClick={handleSubmitMulti}
            className="flex items-center gap-1.5 text-sm px-4 py-2 rounded-md font-medium transition-colors hover:brightness-110"
            style={{
              backgroundColor: 'white',
              color: '#1a1b2e',
            }}
            whileHover={{ scale: 1.02 }}
            whileTap={{ scale: 0.98 }}
          >
            <Check className="h-3.5 w-3.5 stroke-[2.5]" />
            Submit ({selected.size})
          </motion.button>
        </div>
      )}

      {/* "Other" text input */}
      <div className="pt-1 pb-1 flex gap-2 items-center">
        <input
          type="text"
          placeholder="Or type a custom answer..."
          value={otherText}
          onChange={(e) => setOtherText(e.target.value)}
          onKeyDown={handleOtherKeyDown}
          className="flex-1 text-sm px-3 py-2 rounded-md outline-none transition-colors duration-200 focus:border-[rgba(139,131,240,0.4)]"
          style={{
            backgroundColor: 'rgba(255, 255, 255, 0.04)',
            border: '1px solid rgba(255, 255, 255, 0.08)',
            color: 'rgba(255, 255, 255, 0.8)',
          }}
        />
        <AnimatePresence>
          {otherText.trim() && (
            <motion.button
              initial={{ opacity: 0, scale: 0.8 }}
              animate={{ opacity: 1, scale: 1 }}
              exit={{ opacity: 0, scale: 0.8 }}
              onClick={handleSubmitOther}
              className="p-2 rounded-md transition-colors hover:brightness-110"
              style={{
                backgroundColor: 'white',
                color: '#1a1b2e',
              }}
              whileTap={{ scale: 0.92 }}
            >
              <Send className="h-3.5 w-3.5" />
            </motion.button>
          )}
        </AnimatePresence>
      </div>

      {/* Skip */}
      <div className="pt-1">
        <button
          onClick={() => onSkip?.()}
          className="flex items-center gap-1.5 text-sm px-3 py-1.5 rounded-md transition-colors"
          style={{
            backgroundColor: 'transparent',
            color: 'rgba(255, 255, 255, 0.35)',
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.backgroundColor = 'rgba(255, 255, 255, 0.06)';
            e.currentTarget.style.color = 'rgba(255, 255, 255, 0.6)';
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.backgroundColor = 'transparent';
            e.currentTarget.style.color = 'rgba(255, 255, 255, 0.35)';
          }}
        >
          <SkipForward className="h-3.5 w-3.5" />
          Skip
        </button>
      </div>
    </motion.div>
  );
}

export default UserQuestionCard;
