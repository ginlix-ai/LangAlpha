import React from 'react';
import { Check, ChevronDown, Pin } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import type { DestinationChoice } from '../../../artifactComments/destinations';

export interface CommentDestinationProps {
  choices: readonly DestinationChoice[];
  /** The key of the choice the comment goes to. */
  selected: string;
  onPick: (choice: DestinationChoice) => void;
  /** Send this file's comments to the thread beside from now on; absent where it already does, or none is beside. */
  onBindBeside?: (() => void) | null;
}

/**
 * The "To:" line of a comment for the agent: the thread it goes to, opening
 * onto the others it could. The default comes first and says so; binding the
 * file to the thread beside makes that thread the default for every comment
 * after this one.
 */
export function CommentDestination({ choices, selected, onPick, onBindBeside }: CommentDestinationProps): React.ReactElement | null {
  const { t } = useTranslation();
  const current = choices.find((c) => c.key === selected) ?? choices[0];
  if (!current) return null;
  const more = choices.length > 1 || !!onBindBeside;

  return (
    <div className="flex min-w-0 items-center gap-1 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
      <span className="shrink-0">{t('pageViewer.comments.to')}</span>
      <DropdownMenu modal={false}>
        <DropdownMenuTrigger asChild>
          <button
            type="button"
            className="page-comment-to inline-flex min-w-0 items-center gap-0.5 rounded-sm px-1 py-0.5"
            disabled={!more}
            aria-label={t('pageViewer.comments.toLabel', { thread: current.label })}
          >
            <span className="truncate font-medium" style={{ color: 'var(--color-text-secondary)' }}>{current.label}</span>
            {more && <ChevronDown className="h-3 w-3 shrink-0" aria-hidden />}
          </button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="start" className="min-w-48 max-w-72">
          <DropdownMenuRadioGroup
            value={current.key}
            onValueChange={(key) => {
              const choice = choices.find((c) => c.key === key);
              if (choice) onPick(choice);
            }}
          >
            {choices.map((choice) => (
              <DropdownMenuRadioItem key={choice.key} value={choice.key}>
                <Check className="h-3.5 w-3.5 shrink-0" style={{ opacity: choice.key === current.key ? 1 : 0 }} aria-hidden />
                <span className="min-w-0 flex-1 truncate">{choice.label}</span>
                {choice.isDefault && (
                  <span className="shrink-0 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                    {t('pageViewer.comments.defaultTag')}
                  </span>
                )}
              </DropdownMenuRadioItem>
            ))}
          </DropdownMenuRadioGroup>
          {onBindBeside && (
            <>
              <DropdownMenuSeparator />
              <DropdownMenuItem onSelect={onBindBeside}>
                <Pin className="h-3.5 w-3.5 shrink-0" aria-hidden />
                {t('pageViewer.comments.bindHere')}
              </DropdownMenuItem>
            </>
          )}
        </DropdownMenuContent>
      </DropdownMenu>
    </div>
  );
}
