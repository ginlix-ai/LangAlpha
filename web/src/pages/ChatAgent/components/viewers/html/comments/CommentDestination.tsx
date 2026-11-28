import React, { useRef, useState } from 'react';
import { Check, ChevronDown, Pin, Search } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Popover, PopoverAnchor, PopoverContent } from '@/components/ui/popover';
import { SkeletonBar } from '@/components/ui/skeleton';
import type { DestinationThread } from '@/types/api';
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
import type { DestinationStatus } from '../../../artifactComments/useCommentDestinations';
import { useFocusWords } from './fieldFocus';
import { DestinationPicker } from './DestinationPicker';

export interface CommentDestinationProps {
  choices: readonly DestinationChoice[];
  status: DestinationStatus;
  onPick: (choice: DestinationChoice) => void;
  /** Send this file's comments to the thread beside from now on; absent where it already does, or none is beside. */
  onBindBeside?: (() => void) | null;
  /** "Another chat…": a search over every chat the record's comments could go to. Absent where the surface does not offer it. */
  search?: { artifactId: string; onPick: (thread: DestinationThread) => void } | null;
}

/**
 * The "To:" line of a comment for the agent: the thread it goes to, opening
 * onto the others it could. The default comes first and says so; binding the
 * file to the thread beside makes that thread the default for every comment
 * after this one.
 */
export function CommentDestination({ choices, status, onPick, onBindBeside, search }: CommentDestinationProps): React.ReactElement | null {
  const { t } = useTranslation();
  const [searching, setSearching] = useState(false);
  const focusWords = useFocusWords();
  // The menu hands focus back to its trigger as it closes; when it closes
  // for the search, the search field keeps it instead.
  const toSearch = useRef(false);
  const toRef = useRef<HTMLButtonElement>(null);
  const searchRef = useRef<HTMLDivElement>(null);
  // A chat picked in the search: the comment's own field takes the focus
  // back, so the words can go on and Enter send them.
  const picked = useRef(false);
  // From the pick of "Another chat…" until the field has the focus, the focus
  // passes through the closing menu and the page, which is not leaving it.
  const arriving = useRef(false);
  // A note needs no chat, so it saves as ever; only sending waits on this.
  if (status.kind === 'loading') return null;
  if (status.kind === 'failed') {
    return (
      <div className="flex min-w-0 items-center gap-1 text-xs" style={{ color: 'var(--color-text-tertiary)' }} role="status">
        <span className="shrink-0">{t('pageViewer:comments.to')}</span>
        <span className="min-w-0 truncate px-1">{t('pageViewer:comments.otherChatFailed')}</span>
        <button
          type="button"
          className="page-comment-to shrink-0 rounded-sm px-1 py-0.5 font-medium"
          style={{ color: 'var(--color-text-secondary)' }}
          onClick={status.retry}
        >
          {t('pageViewer:comments.destinationsRetry')}
        </button>
      </div>
    );
  }
  // While a chat picked outside the menu is checked, the line waits rather
  // than naming the default it may not be.
  const checking = status.kind === 'checking';
  const current = (status.kind === 'ready' && choices.find((c) => c.key === status.selected)) || choices[0];
  const more = choices.length > 1 || !!onBindBeside || !!search;

  return (
    <Popover open={searching && !!search} onOpenChange={setSearching}>
      <PopoverAnchor asChild>
        <div className="flex min-w-0 items-center gap-1 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
          <span className="shrink-0">{t('pageViewer:comments.to')}</span>
          <DropdownMenu modal={false}>
            <DropdownMenuTrigger asChild>
              <button
                ref={toRef}
                type="button"
                className="page-comment-to inline-flex min-w-0 items-center gap-0.5 rounded-sm px-1 py-0.5"
                disabled={!more || checking}
                aria-label={checking ? t('common.loading') : t('pageViewer:comments.toLabel', { thread: current.label })}
                aria-busy={checking || undefined}
              >
                {checking
                  ? <SkeletonBar className="w-28" />
                  : <span className="truncate font-medium" style={{ color: 'var(--color-text-secondary)' }}>{current.label}</span>}
                {more && <ChevronDown className="h-3 w-3 shrink-0" aria-hidden />}
              </button>
            </DropdownMenuTrigger>
            <DropdownMenuContent
              align="start"
              className="min-w-48 max-w-72"
              onCloseAutoFocus={(e) => {
                if (!toSearch.current) return;
                toSearch.current = false;
                e.preventDefault();
                // The field asked for the focus as it mounted, while the menu
                // still held it; the menu going leaves it on nothing.
                requestAnimationFrame(() => {
                  searchRef.current?.querySelector('input')?.focus({ preventScroll: true });
                  arriving.current = false;
                });
              }}
            >
              <DropdownMenuRadioGroup
                value={current.key}
                onValueChange={(key) => {
                  const choice = choices.find((c) => c.key === key);
                  if (choice) onPick(choice);
                }}
              >
                {choices.map((choice) => (
                  <DropdownMenuRadioItem key={choice.key} value={choice.key} title={choice.label}>
                    <Check className="h-3.5 w-3.5 shrink-0" style={{ opacity: choice.key === current.key ? 1 : 0 }} aria-hidden />
                    <span className="min-w-0 truncate">{choice.label}</span>
                    {/* Gives way to the title first: it says why, the title says which. */}
                    {(choice.tag || choice.isDefault) && (
                      <span className="ml-auto min-w-0 shrink-[100] truncate pl-2 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                        {choice.tag ?? t('pageViewer:comments.defaultTag')}
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
                    {t('pageViewer:comments.bindHere')}
                  </DropdownMenuItem>
                </>
              )}
              {search && (
                <>
                  <DropdownMenuSeparator />
                  <DropdownMenuItem
                    onSelect={() => {
                      toSearch.current = true;
                      arriving.current = true;
                      setSearching(true);
                    }}
                  >
                    <Search className="h-3.5 w-3.5 shrink-0" aria-hidden />
                    {t('pageViewer:comments.otherChat')}
                  </DropdownMenuItem>
                </>
              )}
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
      </PopoverAnchor>
      {search && (
        <PopoverContent
          align="start"
          side="bottom"
          className="flex w-72 max-w-[calc(100vw-32px)] flex-col overflow-hidden p-0"
          aria-label={t('pageViewer:comments.otherChat')}
          ref={searchRef}
          onCloseAutoFocus={(e) => {
            e.preventDefault();
            if (picked.current) {
              picked.current = false;
              focusWords?.();
              return;
            }
            // Tab took the focus on to what follows: it stays there.
            const now = document.activeElement;
            if (!now || now === document.body || searchRef.current?.contains(now)) toRef.current?.focus({ preventScroll: true });
          }}
          // Focus leaving closes it, as the composer's other searches close.
          onFocusOutside={(e) => {
            const to = e.target;
            if (arriving.current || !(to instanceof Element) || to === document.body) e.preventDefault();
          }}
          // It sits at the end of the page, so Tab would leave the page from
          // it: either way, Tab closes it onto the "To:" line, as a menu's does.
          onKeyDown={(e) => {
            if (e.key !== 'Tab') return;
            e.preventDefault();
            setSearching(false);
          }}
          // Words typed go first, as in the app's other searches; Esc on an
          // empty field closes it.
          onEscapeKeyDown={(e) => {
            const field = searchRef.current?.querySelector('input');
            if (field?.value) e.preventDefault();
          }}
        >
          <DestinationPicker
            artifactId={search.artifactId}
            onPick={(thread) => {
              picked.current = true;
              setSearching(false);
              search.onPick(thread);
            }}
          />
        </PopoverContent>
      )}
    </Popover>
  );
}
