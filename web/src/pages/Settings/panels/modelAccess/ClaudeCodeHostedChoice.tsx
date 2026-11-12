import { useEffect, useId, useState } from 'react';
import { createPortal } from 'react-dom';
import { ExternalLink } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { ModalShell } from '@/components/ui/ModalShell';
import type { ClaudeCodeCustodyMode } from '@/api/model';
import { readOAuthError } from '@/api/oauth';
import { useClaudeCodeStatus } from '@/hooks/useClaudeCode';
import { useClaudeCodeCustody } from '@/hooks/useClaudeCodeCustody';
import { CLAUDE_CODE_DISCLOSURE, CLAUDE_CODE_DOCS, CLAUDE_CODE_MODE_LABELS } from '@/lib/claudeCode';
import { useSaveFailedToast } from '../usePreferenceWrite';
import { ModeCard } from './ModeCard';

const MODES: readonly { mode: ClaudeCodeCustodyMode; label: string; detail: string }[] = [
  { mode: 'local', label: CLAUDE_CODE_MODE_LABELS.local, detail: 'modelAccess.claudeCode.hosted.localDetail' },
  { mode: 'stored', label: CLAUDE_CODE_MODE_LABELS.stored, detail: 'modelAccess.claudeCode.hosted.storedDetail' },
];

/** Anthropic's consumer terms, which apply to a Claude plan used this way. */
const ANTHROPIC_TERMS_URL = 'https://www.anthropic.com/legal/consumer-terms';

/** What the choice is asking before it switches: consent to store the token,
 *  or leaving the stored token for running only on their own computers. */
type Asking = { kind: 'consent' } | { kind: 'leave' };

/**
 * How the user's Claude Code runs on cloud computers, where the server lets
 * them choose: only on their own computers, or on a setup token they let
 * LangAlpha store. Each is listed while the server offers it or while it is
 * the one chosen; where the server runs no turn on a cloud computer, a line
 * under the choice says it is paused, and the user can still move to the
 * other. Where storing is blocked for the user, a line says so in its place.
 *
 * Storing the token needs consent to the version of its text the server
 * names first, then opens the token's sheet (`onAddToken`) when none is
 * saved. Leaving it deletes a token stored there, so it is confirmed first, and
 * a failed switch says the token is still stored, or that it is deleted where
 * only a turn on it did not stop; with none stored it applies
 * at once. A pick made while
 * another saves is dropped, so the one shown is the server's last write. Its dialogs open over the sheet it is in
 * (``onAsking``), portaled out of that sheet's panel, which goes inert and
 * fades while covered.
 */
export function ClaudeCodeHostedChoice({ onAddToken, onAsking }: {
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
  const paused = custody.refusal === 'needs_own_computer';
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
  const select = (mode: ClaudeCodeCustodyMode) => {
    if (choosing) return;
    if (mode === 'stored') setAsking({ kind: 'consent' });
    else if (custody.mode !== 'stored' || !status.saved) void apply(mode);
    else setAsking({ kind: 'leave' });
  };
  const consent = async () => {
    setAsking(null);
    if (await apply('stored', CLAUDE_CODE_DISCLOSURE) && !status.saved) onAddToken();
  };
  const leave = async () => {
    await apply('local');
    setAsking(null);
  };

  return (
    <section aria-labelledby={headingId} className="flex flex-col gap-1.5">
      <div className="flex items-baseline justify-between gap-3">
        <h3 id={headingId} className="text-xs font-medium" style={{ color: 'var(--color-text-tertiary)' }}>
          {t('modelAccess.claudeCode.hosted.heading')}
        </h3>
        {choosing ? <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{t('common.saving')}</span> : (
          <a
            href={CLAUDE_CODE_DOCS.whereItRuns}
            target="_blank"
            rel="noopener noreferrer"
            className="tap-target inline-flex items-center gap-0.5 text-xs underline underline-offset-2 hover:opacity-80"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {t('modelAccess.claudeCode.setup.learnMore')}
            <ExternalLink aria-hidden="true" className="h-3 w-3" />
          </a>
        )}
      </div>
      <div role="radiogroup" aria-labelledby={headingId} className="flex flex-col gap-2">
        {modes.map(({ mode, label, detail }) => (
          <ModeCard key={mode} name={groupName} mode={mode} checked={shown === mode} label={t(label)} detail={t(detail)} onSelect={select} />
        ))}
      </div>
      {custody.blocked && <Note>{t('modelAccess.claudeCode.hosted.blocked')}</Note>}
      {paused && <Note>{t('modelAccess.claudeCode.hosted.paused')}</Note>}
      {leaveFailed && <p role="alert" className="text-xs" style={{ color: 'var(--color-loss)' }}>{t(leaveFailed === 'turnsRunning' ? 'modelAccess.claudeCode.turnsRunning' : 'modelAccess.claudeCode.leaveStored.failed')}</p>}

      {asking?.kind === 'consent' && createPortal(<ConsentDialog onConfirm={consent} onClose={() => setAsking(null)} />, document.body)}
      {asking?.kind === 'leave' && createPortal(
        <LeaveStoredDialog busy={choosing !== null} onConfirm={leave} onClose={() => setAsking(null)} />,
        document.body,
      )}
    </section>
  );
}

function Note({ children }: { children: string }) {
  return <p className="text-xs leading-relaxed" style={{ color: 'var(--color-text-secondary)' }}>{children}</p>;
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
