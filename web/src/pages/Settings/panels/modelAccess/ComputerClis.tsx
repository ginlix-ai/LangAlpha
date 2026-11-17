import { useEffect, useId, useRef } from 'react';
import { useTranslation } from 'react-i18next';
import { Select } from '@/components/ui/select';
import { useSetComputerClis } from '@/hooks/useClaudeCode';
import { useSaveFailedToast } from '../usePreferenceWrite';

/**
 * How many Claude Code agents a computer of the user's own runs at once, on
 * its row of the view: 1 to `max`, applied from its next agent on, saved
 * through `useSetComputerClis`. Compact on desktop, a finger's height under a
 * coarse pointer: a native select draws no `::after`, so `.tap-target`
 * cannot widen its hit area and the select itself grows instead.
 */
export function ComputerClis({ computerId, clis, max, located = false, onLocated }: {
  computerId: string;
  clis: number;
  max: number;
  /** The view was opened for this computer's setting: it is scrolled to and
   *  takes focus a frame on, once the sheet has made its own first focus,
   *  and `onLocated` then drops the link so a remount does not do it again. */
  located?: boolean;
  onLocated?: () => void;
}) {
  const { t } = useTranslation();
  const id = useId();
  const selectRef = useRef<HTMLSelectElement>(null);
  useEffect(() => {
    if (!located) return;
    const frame = requestAnimationFrame(() => {
      const select = selectRef.current;
      if (!select) return;
      select.scrollIntoView({ block: 'center' });
      select.focus({ preventScroll: true, focusVisible: true });
      onLocated?.();
    });
    return () => cancelAnimationFrame(frame);
  }, [located, onLocated]);
  const toastSaveFailed = useSaveFailedToast();
  const save = useSetComputerClis(computerId);
  const shown = save.isPending ? save.variables : clis;

  return (
    <span className="mt-1 flex flex-col gap-1">
      <span className="flex items-center gap-2">
        <label htmlFor={id} className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
          {t('modelAccess.claudeCode.computer.clis')}
        </label>
        <Select
          ref={selectRef}
          id={id}
          className="w-16 shrink-0 [&>select]:h-7 pointer-coarse:[&>select]:h-11"
          aria-describedby={`${id}-help`}
          value={String(shown)}
          // Busy, not disabled, while a save is out: a disabled select drops
          // the focus, and the keyboard user with it, on every change.
          aria-busy={save.isPending}
          onChange={(e) => {
            if (save.isPending) return;
            save.mutate(Number(e.target.value), { onError: toastSaveFailed });
          }}
        >
          {Array.from({ length: max }, (_, i) => i + 1).map((n) => (
            <option key={n} value={n}>{n}</option>
          ))}
        </Select>
      </span>
      <span id={`${id}-help`} className="text-xs text-pretty" style={{ color: 'var(--color-text-tertiary)' }}>
        {t('modelAccess.claudeCode.computer.clisHelp')}
      </span>
    </span>
  );
}
