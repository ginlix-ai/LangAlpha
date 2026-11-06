import type { ReactNode } from 'react';
import type { TFunction } from 'i18next';
import { Trans, useTranslation } from 'react-i18next';
import { mediumDate } from '@/lib/format';
import type { ClaudeCodeComputer } from '@/api/model';
import { useClaudeCodeComputers, useClaudeCodeStatus } from '@/hooks/useClaudeCode';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import type { RowStatus } from './parts';
import { SAVED_TOKEN, computerTokenTarget, type SheetAction, type TokenTarget } from './tokenTarget';

/** What a token line offers, in the order its buttons show. */
export type TokenAction = 'add' | 'addNew' | 'addOwn' | 'replace' | 'remove';

/** A Claude Code token's line, the same wherever it shows: its status, what
 *  it is, and what can be done about it. */
export interface TokenLine {
  status: RowStatus;
  detail: ReactNode;
  actions: TokenAction[];
}

/** Each button's label and the sheet it opens (none for Remove, which asks first). */
export const TOKEN_ACTIONS: Record<TokenAction, { label: string; sheet: SheetAction | null }> = {
  add: { label: 'modelAccess.claudeCode.add', sheet: 'add' },
  addNew: { label: 'modelAccess.claudeCode.addNew', sheet: 'add' },
  addOwn: { label: 'modelAccess.claudeCode.computer.addOwn', sheet: 'add' },
  replace: { label: 'modelAccess.claudeCode.replace', sheet: 'replace' },
  remove: { label: 'modelAccess.claudeCode.remove', sheet: null },
};

/** The sheet a link to a line opens: the one its first sheet button would. */
export function linkedSheetAction(line: TokenLine): SheetAction | null {
  return line.actions.map((a) => TOKEN_ACTIONS[a].sheet).find((sheet) => sheet !== null) ?? null;
}

/** How long before a saved token lapses its line asks for a new one. */
export const EXPIRES_SOON_MS = 14 * 24 * 60 * 60 * 1000;

const expiry = (expiresAt: string | null) => (expiresAt && !Number.isNaN(Date.parse(expiresAt)) ? new Date(expiresAt) : null);

/** A sentence naming the token's expiry, `key` one of the expiry lines. A
 *  wrap falls around the date, never inside it. */
function dated(key: string, locale: string, at: Date): ReactNode {
  return <Trans i18nKey={key} values={{ date: mediumDate(at, locale) }} components={{ date: <span className="whitespace-nowrap" /> }} />;
}

/** What the token is, then the account and when the token lapses, as a
 *  subscription's row reads. */
function described(locale: string, parts: (string | null | undefined)[], expiresAt: string | null): ReactNode {
  const lead = parts.filter(Boolean).join(' · ');
  const at = expiry(expiresAt);
  if (!at) return lead || null;
  return (
    <>
      {lead && `${lead} · `}
      {dated('modelAccess.claudeCode.expiresBy', locale, at)}
    </>
  );
}

/**
 * The saved token's line. Where this server runs turns on it, every state
 * with a token offers Replace and Remove, whatever its check says; one the
 * check could not reach runs turns, so it reads as connected. One past its
 * expiry reads as refused, saying when it expired, and one within
 * `EXPIRES_SOON_MS` of it asks for a new one before then. Where an
 * organization's policy keeps Claude Code off this server, the line says so
 * and offers only Remove.
 */
function savedTokenLine(t: TFunction, locale: string, status: ReturnType<typeof useClaudeCodeStatus>, now: number): TokenLine | null {
  if (!status.runsHere) return status.saved ? { status: 'off', detail: t('modelAccess.claudeCode.optOut.saved'), actions: ['remove'] } : null;
  const detail = t('modelAccess.claudeCode.detail');
  switch (status.state) {
    case 'no_token':
      return { status: 'not_connected', detail, actions: ['add'] };
    case 'checking':
      return { status: 'working', detail, actions: ['replace', 'remove'] };
    case 'policy_blocked':
      return { status: 'unavailable', detail: t('modelAccess.claudeCode.computer.policyBlocked'), actions: status.saved ? ['remove'] : [] };
  }
  const at = expiry(status.expiresAt);
  if (at && at.getTime() <= now) {
    return { status: 'needs_reauth', detail: dated('modelAccess.claudeCode.expired', locale, at), actions: ['addNew', 'remove'] };
  }
  if (status.state === 'rejected') {
    return { status: 'needs_reauth', detail: t('modelAccess.claudeCode.rejected'), actions: ['addNew', 'remove'] };
  }
  if (at && at.getTime() - now <= EXPIRES_SOON_MS) {
    return { status: 'connected', detail: dated('modelAccess.claudeCode.expiresSoon', locale, at), actions: ['addNew', 'remove'] };
  }
  return { status: 'connected', detail: described(locale, [status.email], status.expiresAt) ?? detail, actions: ['replace', 'remove'] };
}

/**
 * One own computer's line, which also says which token it uses: its own, or
 * the saved one where this server relays it. A computer with its own token
 * keeps Replace and Remove whatever its check says; one on the saved token
 * keeps Remove for a file of its own there that it refuses. With Claude Code off, or
 * where it cannot run (an organization's policy is installed there, no perl,
 * musl), only a token left on the computer shows, to be removed.
 */
export function computerTokenLine(t: TFunction, locale: string, entry: ClaudeCodeComputer, now: number): TokenLine | null {
  const own = entry.source === 'machine';
  const lead = own
    ? t('modelAccess.claudeCode.computer.ownToken')
    : entry.source === 'saved' ? t('modelAccess.claudeCode.computer.savedToken') : null;
  const actions: TokenAction[] = own ? ['replace', 'remove']
    : entry.source === 'saved' ? (entry.own_file ? ['addOwn', 'remove'] : ['addOwn']) : ['add'];
  switch (entry.state) {
    case 'off':
      return own ? { status: 'off', detail: t('modelAccess.claudeCode.optOut.computer'), actions: ['remove'] } : null;
    case 'offline':
      return { status: 'offline', detail: t('modelAccess.claudeCode.computer.offline'), actions: [] };
    case 'policy_blocked':
      return { status: 'unavailable', detail: t('modelAccess.claudeCode.computer.policyBlocked'), actions: own ? ['remove'] : [] };
    case 'needs_perl':
      return { status: 'unavailable', detail: t('modelAccess.claudeCode.fail.needs_perl'), actions: own ? ['remove'] : [] };
    case 'computer_unsupported':
      return { status: 'unavailable', detail: t('modelAccess.claudeCode.fail.computer_unsupported'), actions: own ? ['remove'] : [] };
    case 'no_token':
      return { status: 'not_connected', detail: t('modelAccess.claudeCode.computer.noToken'), actions: ['add'] };
    case 'checking':
      return { status: 'working', detail: lead ?? t('modelAccess.claudeCode.computer.checking'), actions };
    case 'rejected':
      return own
        ? { status: 'needs_reauth', detail: t('modelAccess.claudeCode.rejected'), actions: ['addNew', 'remove'] }
        : { status: 'needs_reauth', detail: t('modelAccess.claudeCode.computer.savedRejected'), actions };
    case 'unverified':
    case 'signed_in': {
      const at = expiry(entry.expires_at);
      if (at && at.getTime() <= now) {
        return own
          ? { status: 'needs_reauth', detail: dated('modelAccess.claudeCode.expired', locale, at), actions: ['addNew', 'remove'] }
          : { status: 'needs_reauth', detail: t('modelAccess.claudeCode.computer.savedRejected'), actions };
      }
      return { status: 'connected', detail: described(locale, [lead, entry.email], entry.expires_at), actions };
    }
  }
}

/** A Claude Code token's row in Settings: whose token, what tags its name,
 *  its line. */
export interface TokenRow {
  key: string;
  target: TokenTarget;
  tag: string | null;
  line: TokenLine;
}

/**
 * Settings' Claude Code rows: the saved token's, then each own computer's,
 * read with Claude Code off too, since a token left behind is still removed
 * there. A line with nothing to say (no token left once Claude Code is off)
 * is no row.
 */
export function useClaudeCodeTokenRows(): TokenRow[] {
  const { t } = useTranslation();
  const locale = useLocale();
  const status = useClaudeCodeStatus({ whenOff: true });
  const now = useNow();
  const { computers } = useClaudeCodeComputers({ whenOff: true });
  const computerRows = computers.flatMap((c) => {
    const line = computerTokenLine(t, locale, c, now);
    return line ? [{ key: c.computer_id, target: computerTokenTarget(c), tag: c.name, line }] : [];
  });
  const savedLine = savedTokenLine(t, locale, status, now);
  return [
    // Beside a row per computer, which name the saved token as theirs.
    ...(savedLine ? [{ key: 'saved', target: SAVED_TOKEN, tag: computerRows.length > 0 ? t('modelAccess.claudeCode.savedTag') : null, line: savedLine }] : []),
    ...computerRows,
  ];
}
