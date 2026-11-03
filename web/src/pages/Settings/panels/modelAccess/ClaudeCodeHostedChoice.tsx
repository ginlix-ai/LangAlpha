import { useId } from 'react';
import { useTranslation } from 'react-i18next';
import { RadioChoice } from '@/components/ui/choice';
import type { ClaudeCodeCustodyMode } from '@/api/model';
import { useClaudeCodeCustody } from '@/hooks/useClaudeCodeCustody';
import { CLAUDE_CODE_MODE_LABELS } from '@/lib/claudeCode';
import { cn } from '@/lib/utils';
import { useSaveFailureToast } from '../usePreferenceWrite';

const MODES: readonly { mode: ClaudeCodeCustodyMode; label: string; detail: string }[] = [
  { mode: 'local', label: CLAUDE_CODE_MODE_LABELS.local, detail: 'modelAccess.claudeCode.hosted.localDetail' },
  { mode: 'api_key', label: CLAUDE_CODE_MODE_LABELS.api_key, detail: 'modelAccess.claudeCode.hosted.apiKeyDetail' },
];

/**
 * How the user's Claude Code runs on cloud computers, where the server lets
 * them choose: only on their own computers, or on cloud computers too, with
 * their Anthropic API key. Without that key saved, choosing it opens the
 * key's sheet (`onAddApiKey`), whose save makes the choice; with one
 * it applies as it is picked, as the other choices in Settings do. Where the API key it
 * chose cannot run (a custom Base URL, or no key to use), a line under the
 * choice says why. A pick made while
 * another saves is dropped, so the one shown is the server's last write.
 */
export function ClaudeCodeHostedChoice({ onAddApiKey }: { onAddApiKey: (then: () => void) => void }) {
  const { t } = useTranslation();
  const headingId = useId();
  const groupName = useId();
  const { custody, choose, choosing } = useClaudeCodeCustody();
  const reportFailure = useSaveFailureToast();
  if (!custody) return null;
  const shown = choosing ?? custody.mode;
  const select = (mode: ClaudeCodeCustodyMode) => {
    if (choosing) return;
    if (mode === 'api_key' && !custody.api_key_saved) onAddApiKey(() => void reportFailure(choose(mode)));
    else void reportFailure(choose(mode));
  };

  return (
    <section aria-labelledby={headingId} className="flex flex-col gap-1.5">
      <div className="flex items-baseline justify-between gap-3">
        <h3 id={headingId} className="text-xs font-medium" style={{ color: 'var(--color-text-tertiary)' }}>
          {t('modelAccess.claudeCode.hosted.heading')}
        </h3>
        {choosing && <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{t('common.saving')}</span>}
      </div>
      <div role="radiogroup" aria-labelledby={headingId} className="flex flex-col gap-2">
        {MODES.map(({ mode, label, detail }) => (
          <ModeCard key={mode} name={groupName} mode={mode} checked={shown === mode} label={t(label)} detail={t(detail)} onSelect={select} />
        ))}
      </div>
      {custody.mode === 'api_key' && custody.refusal === 'api_key_endpoint' && <Note>{t('modelAccess.claudeCode.hosted.apiKeyEndpoint')}</Note>}
      {custody.mode === 'api_key' && custody.refusal === 'needs_api_key' && <Note>{t('modelAccess.claudeCode.hosted.needsApiKey')}</Note>}
    </section>
  );
}

function Note({ children }: { children: string }) {
  return <p className="text-xs leading-relaxed" style={{ color: 'var(--color-text-secondary)' }}>{children}</p>;
}

/** One way to run it, drawn as Settings draws a trading level. Named by its
 *  label alone, so its detail is read as a description. */
function ModeCard({ name, mode, checked, label, detail, onSelect }: {
  name: string;
  mode: ClaudeCodeCustodyMode;
  checked: boolean;
  label: string;
  detail: string;
  onSelect: (mode: ClaudeCodeCustodyMode) => void;
}) {
  const labelId = useId();
  const detailId = useId();
  return (
    <RadioChoice
      name={name}
      value={mode}
      checked={checked}
      onSelect={onSelect}
      aria-labelledby={labelId}
      aria-describedby={detailId}
      markClassName="mt-0.5"
      className={cn(
        'items-start gap-3 rounded-lg px-3 py-2.5 transition-colors',
        checked ? 'bg-(--color-bg-tag)' : 'bg-(--color-bg-card) hover:bg-(--color-bg-hover)',
      )}
      style={{ border: `1px solid ${checked ? 'var(--color-text-primary)' : 'var(--color-border-muted)'}` }}
    >
      <span className="min-w-0 flex-1">
        <span id={labelId} className="block text-[0.8125rem] font-medium" style={{ color: 'var(--color-text-primary)' }}>{label}</span>
        <span id={detailId} className="mt-0.5 block text-xs leading-relaxed" style={{ color: 'var(--color-text-tertiary)' }}>{detail}</span>
      </span>
    </RadioChoice>
  );
}
