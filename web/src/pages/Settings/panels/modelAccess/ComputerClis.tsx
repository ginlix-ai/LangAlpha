import { useId } from 'react';
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
export function ComputerClis({ computerId, clis, max }: { computerId: string; clis: number; max: number }) {
  const { t } = useTranslation();
  const id = useId();
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
          id={id}
          className="w-16 shrink-0 [&>select]:h-7 pointer-coarse:[&>select]:h-11"
          aria-describedby={`${id}-help`}
          value={String(shown)}
          disabled={save.isPending}
          onChange={(e) => save.mutate(Number(e.target.value), { onError: toastSaveFailed })}
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
