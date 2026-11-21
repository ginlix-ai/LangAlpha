import { useEffect, useId, useRef } from 'react';
import { useTranslation } from 'react-i18next';
import { InfoHint } from '@/components/ui/info-hint';
import { Select } from '@/components/ui/select';
import { useSetComputerClis } from '@/hooks/useClaudeCode';
import { useSaveFailedToast } from '../usePreferenceWrite';

/** The select's value for "what its memory allows", which saves as null. */
const AUTOMATIC = 'automatic';

/**
 * How many Claude Code agents a computer of the user's own runs at once, on
 * its row of the view: Automatic (what its memory allows, `automatic`) or 1
 * to `max`, applied from its next agent on, saved through
 * `useSetComputerClis`. Compact on desktop, a finger's height under a coarse
 * pointer: a native select draws no `::after`, so `.tap-target` cannot widen
 * its hit area and the select itself grows instead.
 */
export function ComputerClis({ computerId, clis, automatic, chosen = true, max, located = false, onLocated }: {
  computerId: string;
  clis: number;
  /** What it runs while no number is set; absent from an older server,
   *  which offers no Automatic. */
  automatic?: number;
  /** Whether a number was set, else `clis` is `automatic`. */
  chosen?: boolean;
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
  const offersAutomatic = automatic != null;
  const shown = save.isPending ? save.variables : chosen || !offersAutomatic ? clis : null;

  return (
    <span className="mt-1 flex flex-col gap-1">
      <span className="flex items-center gap-2">
        <span className="inline-flex items-center gap-1">
          <label htmlFor={id} className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
            {t('modelAccess.claudeCode.computer.clis')}
          </label>
          <InfoHint name={t('modelAccess.claudeCode.computer.clis')} id={`${id}-help`}>
            {t('modelAccess.claudeCode.computer.clisHelp')}
          </InfoHint>
        </span>
        <Select
          ref={selectRef}
          id={id}
          className="w-fit shrink-0 [&>select]:h-7 pointer-coarse:[&>select]:h-11"
          aria-describedby={`${id}-help`}
          value={shown == null ? AUTOMATIC : String(shown)}
          // Busy, not disabled, while a save is out: a disabled select drops
          // the focus, and the keyboard user with it, on every change.
          aria-busy={save.isPending}
          onChange={(e) => {
            if (save.isPending) return;
            save.mutate(e.target.value === AUTOMATIC ? null : Number(e.target.value), { onError: toastSaveFailed });
          }}
        >
          {offersAutomatic && (
            <option value={AUTOMATIC}>{t('modelAccess.claudeCode.computer.clisAutomatic', { count: automatic })}</option>
          )}
          {Array.from({ length: max }, (_, i) => i + 1).map((n) => (
            <option key={n} value={n}>{n}</option>
          ))}
        </Select>
      </span>
    </span>
  );
}
