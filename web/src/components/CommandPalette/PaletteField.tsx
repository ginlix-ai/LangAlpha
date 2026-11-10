import { useLayoutEffect, useRef, type InputHTMLAttributes, type KeyboardEvent } from 'react';
import { useTranslation } from 'react-i18next';
import { ChevronRight, Search, X } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { cn } from '@/lib/utils';
import './PaletteField.css';

interface FieldProps {
  query: string;
  onText: (text: string) => void;
  onKeyDown: (e: KeyboardEvent<HTMLInputElement>) => void;
  placeholder: string;
  label: string;
  /** The scope searched within, named before the text (a workspace, then the kind that narrows it); none at the top level. */
  chips: readonly string[];
  /** Takes the last chip off, for a pointer: the keyboard has Backspace, Shift+Tab and Escape. */
  onLeave: () => void;
  /** The row whose actions are open, or the drilled-in view, named at the field's end. */
  trailing: string | null;
  busy: boolean;
  /** The field holds a value to edit (a rename): its text is selected as it arrives, so typing replaces it. */
  selected: boolean;
  /** The longest text the field takes, while it edits a value. */
  maxLength?: number;
  /** The combobox's wiring to its list. */
  combobox: Pick<InputHTMLAttributes<HTMLInputElement>, 'aria-describedby' | 'aria-controls' | 'aria-activedescendant'>;
}

/**
 * The palette's one field. The row owns its field's focus: the keyboard ring
 * goes round the row, and the always-focused field draws no edge of its own.
 */
export function PaletteField({ query, onText, onKeyDown, placeholder, label, chips, onLeave, trailing, busy, selected, maxLength, combobox }: FieldProps) {
  const { t } = useTranslation();
  const inputRef = useRef<HTMLInputElement>(null);
  // Once, as the value arrives: later edits leave the caret where it is.
  useLayoutEffect(() => {
    if (selected) inputRef.current?.select();
  }, [selected]);
  return (
    <div className="command-palette-field rings-within owns-its-edge">
      <Search className="command-palette-field-icon" aria-hidden />
      {chips.map((chip, i) => {
        // Only the last comes off: the one before it is where that leaves.
        const last = i === chips.length - 1;
        return (
          // The field's label names the scope for a screen reader; this is for the eye.
          <span key={i} className={cn('command-palette-chip', !last && 'is-plain')} aria-hidden>
            <span className="command-palette-chip-label">{chip}</span>
            {last && (
              <button
                type="button"
                tabIndex={-1}
                title={t('palette.scope.leave')}
                className="command-palette-chip-leave tap-target"
                onMouseDown={(e) => e.preventDefault()}
                onClick={onLeave}
              >
                <X aria-hidden />
              </button>
            )}
          </span>
        );
      })}
      <input
        ref={inputRef}
        value={query}
        onChange={(e) => onText(e.target.value)}
        onKeyDown={onKeyDown}
        placeholder={placeholder}
        maxLength={maxLength}
        aria-label={label}
        role="combobox"
        aria-expanded
        aria-autocomplete="list"
        {...combobox}
        autoComplete="off"
        spellCheck={false}
      />
      {busy && <Loader size={12} className="text-current" />}
      {trailing && (
        <span className="command-palette-scope">
          {trailing}
          <ChevronRight aria-hidden />
        </span>
      )}
    </div>
  );
}
