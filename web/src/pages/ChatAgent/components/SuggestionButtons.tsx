/**
 * Follow-up suggestion buttons rendered below assistant messages.
 *
 * Displays up to 3 clickable chips that, when clicked, send the suggestion
 * text as the next user message. Only visible when the message is not
 * streaming and suggestions are available.
 */

import React from 'react';
import { ArrowRight } from 'lucide-react';
import { useIsMobile } from '@/hooks/useIsMobile';

export interface SuggestionButtonsProps {
  suggestions: string[];
  onSuggestionClick: (text: string) => void;
  disabled?: boolean;
}

function SuggestionButtons({
  suggestions,
  onSuggestionClick,
  disabled = false,
}: SuggestionButtonsProps): React.ReactElement | null {
  const isMobile = useIsMobile();

  if (!suggestions || suggestions.length === 0) return null;

  return (
    <div
      className={`flex flex-wrap ${isMobile ? 'gap-1.5 mt-1.5' : 'gap-2 mt-2'}`}
      role="group"
      aria-label="Follow-up suggestions"
    >
      {suggestions.map((text, idx) => (
        <button
          key={idx}
          type="button"
          disabled={disabled}
          onClick={() => onSuggestionClick(text)}
          className={`inline-flex items-center gap-1 rounded-full border px-3 py-1 text-xs font-medium leading-relaxed transition-all duration-200
            hover:border-[var(--color-accent-primary)] hover:text-[var(--color-accent-primary)]
            disabled:opacity-50 disabled:cursor-not-allowed
            ${isMobile ? 'text-[11px] px-2.5 py-0.5' : ''}`}
          style={{
            borderColor: 'var(--color-border-muted)',
            color: 'var(--color-text-secondary)',
            backgroundColor: 'transparent',
          }}
        >
          <span className="line-clamp-1 max-w-[240px]">{text}</span>
          <ArrowRight
            className={`flex-shrink-0 ${isMobile ? 'h-3 w-3' : 'h-3.5 w-3.5'}`}
            style={{ color: 'var(--color-text-tertiary)' }}
          />
        </button>
      ))}
    </div>
  );
}

export default React.memo(SuggestionButtons);
