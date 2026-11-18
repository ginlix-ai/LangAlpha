/**
 * How the composer meets a turn request that failed, read from the error alone
 * so the hook keeps only the effects. The banner is the server's own words,
 * and null for a transport or bare HTTP failure: its raw text ("Failed to
 * fetch", "HTTP error! status: 500") is no message to show, and a failed
 * reply already says the request failed and offers Retry.
 */
import type { AccountStanding } from '@/lib/accountRules';
import { claudeCodeRefusal } from '@/lib/claudeCode';
import { holdsDraft, othersTurnWords, readTurnRefusal } from '@/lib/turnRefusal';
import { buildRateLimitError, type PageLinkSpec, type StructuredError } from '@/utils/rateLimitError';
import { BUSY_REFUSAL_KEYS, isBusyCode } from '../hooks/utils/forkRefusal';
import { isNeedsComputer, needsBackupError, needsBackupOf, needsComputerError } from '../utils/needsComputer';

type Translate = (key: string, options?: Record<string, unknown>) => string;

export interface TurnFailure {
  banner: string | StructuredError | null;
  /** The server's own answer to act on (a quota denial, a provider or access
   *  link), under which a failed reply would read as a second fault. */
  answered: boolean;
  /** Another member's turn is running: nothing failed, the composer holds the
   *  draft until it ends, and the view reloads to show that turn. */
  holdsDraft: boolean;
  /** What the failure says is stale here: the account's rules, the saved
   *  model preferences the server cleaned, or the Claude Code tokens. */
  reread: 'rules' | 'preferences' | 'claude_code' | null;
}

export function readTurnFailure(err: unknown, { t, standing, workspaceId }: {
  t: Translate;
  standing: AccountStanding;
  workspaceId: string | null | undefined;
}): TurnFailure {
  const errObj = err as Record<string, unknown>;
  const message = (err as Error).message;
  const answer = (banner: TurnFailure['banner'], answered: boolean, reread: TurnFailure['reread'] = null): TurnFailure => (
    { banner, answered, holdsDraft: false, reread }
  );
  if (errObj.status === 429) {
    const info = (errObj.rateLimitInfo || {}) as Record<string, unknown>;
    const platformUrl = (import.meta.env.VITE_PLATFORM_URL as string | undefined) || '/account';
    return answer(buildRateLimitError(info, platformUrl), true);
  }
  const errorInfo = errObj.errorInfo as Record<string, unknown> | undefined;
  const refusal = readTurnRefusal(errorInfo);
  switch (refusal?.kind) {
    case 'rule':
      return answer({ message: refusal.refusal.message, ruleStop: refusal }, true, 'rules');
    case 'suspended':
      return answer({ message, ruleStop: refusal }, true);
    // The route corrects the workspace once it has read the thread; the
    // message goes back to the composer.
    case 'other_workspace':
      return answer({ message: t('sharedWorkspace.otherWorkspace'), kind: 'notice' }, false);
    case 'others_turn':
      return { banner: { message: othersTurnWords(t, refusal.member), kind: 'notice' }, answered: false, holdsDraft: holdsDraft(refusal), reread: null };
  }
  // An organization's account whose rules cannot be read is refused every
  // turn until they can; the app-wide banner says the rest.
  if (errObj.status === 503 && errorInfo?.type === 'service_unavailable' && (standing.unknown || standing.organization)) {
    return answer({ message, ruleStop: { kind: 'rules_unknown' } }, true, 'rules');
  }
  // The subscription's token could not be renewed just now, which the next
  // try usually fixes: say so as a notice and leave the failed reply's Retry.
  if (errorInfo?.type === 'oauth_refresh_unavailable') {
    return answer({ message: (errorInfo.message as string) || message, kind: 'notice' }, false);
  }
  if (errorInfo?.link) {
    const link = errorInfo.link as PageLinkSpec;
    return answer(
      {
        message: (errorInfo.message as string) || message || 'An error occurred.',
        // The server words the link in English; these we can translate.
        links: [
          errorInfo.type === 'oauth_reauth_required' ? { ...link, labelKey: 'chat.errorLinkReconnect' }
            : errorInfo.type === 'oauth_plan_no_api_access' ? { ...link, labelKey: 'chat.errorLinkAddApiKey' }
              : link,
        ],
      },
      true,
      // The server scrubbed stale model names from the saved preferences:
      // the cached copy goes, or every retry sends the removed model again.
      errorInfo.type === 'model_removed' ? 'preferences' : null,
    );
  }
  if (errObj.status === 403) {
    return answer({ message: message || 'Access denied.', links: [{ url: '/setup/method', label: 'Configure providers' }] }, true);
  }
  // The transport words an upload too large for the server itself.
  if (errObj.status === 413) return answer(message, false);
  // No computer to run on, and none will be built: the way out is adding one.
  if (isNeedsComputer(err)) return answer(needsComputerError(t), true);
  // A computer Home could live on, but with its backup off: the way out is turning it on.
  const needsBackup = needsBackupOf(err);
  if (needsBackup) return answer(needsBackupError(t, needsBackup), true);
  // A busy chat's refusal, in the view's words: the server's name the CLI's
  // commands.
  if (isBusyCode(errorInfo?.code)) {
    return answer({ message: t(BUSY_REFUSAL_KEYS[errorInfo.code]), kind: 'notice' }, false);
  }
  const worded = typeof errorInfo?.message === 'string' && errorInfo.message ? errorInfo.message : null;
  // A Claude Code turn the server cannot run says what to change, and a
  // retry meets the same refusal. Without a working token, the change is
  // one the user makes in Settings. A Claude plan's limit is worded where it
  // is drawn, in the reader's zone.
  if (errorInfo?.type === 'claude_code_off' && worded) {
    const { reread, link, messageKey, messageValues, planRefusal } = claudeCodeRefusal(errorInfo, workspaceId);
    const stale = reread ? 'claude_code' : null;
    if (planRefusal) return answer({ message: worded, planRefusal }, true, stale);
    const words = messageKey ? t(messageKey, messageValues) : worded;
    return answer(link ? { message: words, links: [link] } : { message: words }, true, stale);
  }
  return answer(worded, false);
}
