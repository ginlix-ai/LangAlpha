import { useEffect, useId, useState } from 'react';
import { createPortal } from 'react-dom';
import { ExternalLink } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { RadioChoice } from '@/components/ui/choice';
import { ModalShell } from '@/components/ui/ModalShell';
import type { ClaudeCodeCustodyMode } from '@/api/model';
import { readOAuthError } from '@/api/oauth';
import { useClaudeCodeStatus } from '@/hooks/useClaudeCode';
import { useClaudeCodeCustody } from '@/hooks/useClaudeCodeCustody';
import { CLAUDE_CODE_MODE_LABELS } from '@/lib/claudeCode';
import { cn } from '@/lib/utils';
import { useSaveFailedToast } from '../usePreferenceWrite';

const MODES: readonly { mode: ClaudeCodeCustodyMode; label: string; detail: string }[] = [
  { mode: 'local', label: CLAUDE_CODE_MODE_LABELS.local, detail: 'modelAccess.claudeCode.hosted.localDetail' },
  { mode: 'api_key', label: CLAUDE_CODE_MODE_LABELS.api_key, detail: 'modelAccess.claudeCode.hosted.apiKeyDetail' },
  { mode: 'stored', label: CLAUDE_CODE_MODE_LABELS.stored, detail: 'modelAccess.claudeCode.hosted.storedDetail' },
];

/** Anthropic's consumer terms, which apply to a Claude plan used this way. */
const ANTHROPIC_TERMS_URL = 'https://www.anthropic.com/legal/consumer-terms';

/** What the choice is asking before it switches: consent to store the token,
 *  or leaving the stored token for another mode. */
type Asking = { kind: 'consent' } | { kind: 'leave'; mode: Exclude<ClaudeCodeCustodyMode, 'stored'> };

/**
 * How the user's Claude Code runs on cloud computers, where the server lets
 * them choose: only on their own computers, with their Anthropic API key, or
 * on a setup token they let LangAlpha store. Each is listed while the server
 * offers it or while it is the one chosen; a chosen one the server no longer
 * offers is paused, which a line under the choice says, and the user can
 * still move to another. Where storing is blocked for the user, a line says
 * so in its place.
 *
 * Storing the token needs consent to the version of its text the server
 * names first, then opens the token's sheet (`onAddToken`) when none is
 * saved. Leaving it deletes a token stored there, so it is confirmed first, and
 * a failed switch says the token is still stored, or that it is deleted where
 * only a turn on it did not stop; with none stored it applies
 * at once. The API key without one saved opens
 * the key's sheet (`onAddApiKey`), whose save makes the switch, or, when it
 * leaves the stored token, asks to confirm that next, so a sheet closed
 * unsaved deletes nothing. Any other pick applies at once, as the other
 * choices in Settings do. Where the API key it
 * chose cannot run (a custom Base URL, or no key to use), a line under the
 * choice says why. A pick made while
 * another saves is dropped, so the one shown is the server's last write. Its dialogs open over the sheet it is in
 * (``onAsking``), portaled out of that sheet's panel, which goes inert and
 * fades while covered.
 */
export function ClaudeCodeHostedChoice({ onAddApiKey, onAddToken, onAsking }: {
  onAddApiKey: (then: () => void) => void;
  onAddToken: () => void;
  /** Whether it is asking, in a dialog over its sheet, before it switches. */
  onAsking: (asking: boolean) => void;
}) {
  const { t } = useTranslation();
  const headingId = useId();
  const groupName = useId();
  const status = useClaudeCodeStatus();
  const { custody, choose, choosing } = useClaudeCodeCustody();
  const toastSaveFailed = useSaveFailedToast();
  const [asking, setAsking] = useState<Asking | null>(null);
  const [leaveFailed, setLeaveFailed] = useState<'failed' | 'turnsRunning' | null>(null);
  useEffect(() => onAsking(asking !== null), [asking, onAsking]);
  if (!custody) return null;
  const shown = choosing ?? custody.mode;
  const paused = custody.refusal === 'hosted_paused' || custody.refusal === 'needs_own_computer';
  const modes = MODES.filter(({ mode }) => mode === 'local' || mode === custody.mode || custody.offered.includes(mode));

  const apply = (mode: ClaudeCodeCustodyMode, disclosure?: string) => {
    const leaving = custody.mode === 'stored' && status.saved;
    setLeaveFailed(null);
    return choose({ mode, disclosure }).then(() => true, (error: unknown) => {
      if (leaving) setLeaveFailed(readOAuthError(error)?.code === 'turns_running' ? 'turnsRunning' : 'failed');
      else toastSaveFailed();
      return false;
    });
  };
  const switchTo = async (mode: Exclude<ClaudeCodeCustodyMode, 'stored'>) => {
    if (mode === 'api_key' && !custody.api_key_saved) onAddApiKey(() => void apply(mode));
    else await apply(mode);
  };
  const select = (mode: ClaudeCodeCustodyMode) => {
    if (choosing) return;
    if (mode === 'stored') setAsking({ kind: 'consent' });
    else if (custody.mode !== 'stored' || !status.saved) void switchTo(mode);
    else if (mode === 'api_key' && !custody.api_key_saved) onAddApiKey(() => setAsking({ kind: 'leave', mode }));
    else setAsking({ kind: 'leave', mode });
  };
  const consent = async () => {
    setAsking(null);
    if (await apply('stored', custody.disclosure ?? undefined) && !status.saved) onAddToken();
  };
  const leave = async (mode: Exclude<ClaudeCodeCustodyMode, 'stored'>) => {
    await apply(mode);
    setAsking(null);
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
        {modes.map(({ mode, label, detail }) => (
          <ModeCard key={mode} name={groupName} mode={mode} checked={shown === mode} label={t(label)} detail={t(detail)} onSelect={select} />
        ))}
      </div>
      {custody.blocked && <Note>{t('modelAccess.claudeCode.hosted.blocked')}</Note>}
      {paused && <Note>{t('modelAccess.claudeCode.hosted.paused')}</Note>}
      {custody.mode === 'api_key' && custody.refusal === 'api_key_endpoint' && <Note>{t('modelAccess.claudeCode.hosted.apiKeyEndpoint')}</Note>}
      {custody.mode === 'api_key' && custody.refusal === 'needs_api_key' && <Note>{t('modelAccess.claudeCode.hosted.needsApiKey')}</Note>}
      {leaveFailed && <p role="alert" className="text-xs" style={{ color: 'var(--color-loss)' }}>{t(leaveFailed === 'turnsRunning' ? 'modelAccess.claudeCode.turnsRunning' : 'modelAccess.claudeCode.leaveStored.failed')}</p>}

      {asking?.kind === 'consent' && createPortal(<ConsentDialog onConfirm={consent} onClose={() => setAsking(null)} />, document.body)}
      {asking?.kind === 'leave' && createPortal(
        <LeaveStoredDialog busy={choosing !== null} onConfirm={() => leave(asking.mode)} onClose={() => setAsking(null)} />,
        document.body,
      )}
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

/** What storing the token means, accepted before the server stores one. */
function ConsentDialog({ onConfirm, onClose }: { onConfirm: () => void; onClose: () => void }) {
  const { t } = useTranslation();
  const titleId = useId();
  return (
    <ModalShell
      labelId={titleId}
      title={t('modelAccess.claudeCode.consent.title')}
      onClose={onClose}
      stacked
      footer={
        <div className="flex items-center justify-end gap-2">
          <Button type="button" variant="ghost" onClick={onClose}>{t('common.cancel')}</Button>
          <Button type="button" onClick={onConfirm}>{t('modelAccess.claudeCode.consent.confirm')}</Button>
        </div>
      }
    >
      <div className="flex flex-col gap-3 text-sm" style={{ color: 'var(--color-text-secondary)' }}>
        <p>{t('modelAccess.claudeCode.consent.encrypted')}</p>
        <p>{t('modelAccess.claudeCode.consent.unattended')}</p>
        <p>
          {t('modelAccess.claudeCode.consent.terms')}{' '}
          <a
            href={ANTHROPIC_TERMS_URL}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex items-center gap-1 underline underline-offset-2 hover:opacity-80"
          >
            {t('modelAccess.claudeCode.consent.termsLink')}
            <ExternalLink aria-hidden="true" className="h-3 w-3" />
          </a>
        </p>
        <p>{t('modelAccess.claudeCode.consent.undo')}</p>
      </div>
    </ModalShell>
  );
}

/** Leaving the stored token deletes it, and stops the turns running on it. */
function LeaveStoredDialog({ busy, onConfirm, onClose }: { busy: boolean; onConfirm: () => void; onClose: () => void }) {
  const { t } = useTranslation();
  const titleId = useId();
  return (
    <ModalShell
      labelId={titleId}
      title={t('modelAccess.claudeCode.leaveStored.title')}
      onClose={onClose}
      stacked
      closeDisabled={busy}
      width="narrow"
      footer={
        <div className="flex items-center justify-end gap-2">
          <Button type="button" variant="ghost" onClick={onClose} disabled={busy}>{t('common.cancel')}</Button>
          <Button type="button" variant="destructive" onClick={onConfirm} disabled={busy} aria-busy={busy}>
            {t('modelAccess.claudeCode.leaveStored.confirm')}
          </Button>
        </div>
      }
    >
      <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>{t('modelAccess.claudeCode.leaveStored.body')}</p>
    </ModalShell>
  );
}
