import type { ReactNode } from 'react';
import type { TFunction } from 'i18next';
import { Trans } from 'react-i18next';
import { mediumDate } from '@/lib/format';
import type { ClaudeCodeComputer } from '@/api/model';
import type { useClaudeCodeStatus } from '@/hooks/useClaudeCode';
import type { RowStatus } from './parts';
import type { SheetAction } from './tokenTarget';

/** What a token line offers, in the order its buttons show. */
export type TokenAction = 'add' | 'addNew' | 'addOwn' | 'replace' | 'remove';

/** A Claude Code token's line, the same wherever it shows: its status, what
 *  it is, and what can be done about it. */
export interface TokenLine {
  status: RowStatus;
  detail: ReactNode;
  actions: TokenAction[];
}

/** The sheet a button opens: Replace replaces the token there, the rest add one. */
export const sheetActionOf = (action: TokenAction): SheetAction => (action === 'replace' ? 'replace' : 'add');

/** The sheet a link to a line opens: the one its first sheet button would. */
export function linkedSheetAction(line: TokenLine): SheetAction | null {
  const action = line.actions.find((a) => a !== 'remove');
  return action ? sheetActionOf(action) : null;
}

/** What the token is, then the account and when the token lapses, as a
 *  subscription's row reads. A wrap falls around the date, never inside it. */
function described(locale: string, parts: (string | null | undefined)[], expiresAt: string | null): ReactNode {
  const lead = parts.filter(Boolean).join(' · ');
  const date = expiresAt && !Number.isNaN(Date.parse(expiresAt)) ? mediumDate(new Date(expiresAt), locale) : null;
  if (!date) return lead || null;
  return (
    <>
      {lead && `${lead} · `}
      <Trans i18nKey="modelAccess.claudeCode.expiresBy" values={{ date }} components={{ date: <span className="whitespace-nowrap" /> }} />
    </>
  );
}

/**
 * The saved token's line. Where this server runs turns on it, every state
 * with a token offers Replace and Remove, whatever its check says; one the
 * check could not reach runs turns, so it reads as connected. Anywhere else
 * a saved token shows only to be removed, and none shows nothing.
 */
export function savedTokenLine(t: TFunction, locale: string, status: ReturnType<typeof useClaudeCodeStatus>): TokenLine | null {
  if (!status.runsHere) return status.saved ? { status: 'off', detail: t('modelAccess.claudeCode.optOut.saved'), actions: ['remove'] } : null;
  const detail = t('modelAccess.claudeCode.detail');
  switch (status.state) {
    case 'no_token':
      return { status: 'not_connected', detail, actions: ['add'] };
    case 'checking':
      return { status: 'working', detail, actions: ['replace', 'remove'] };
    case 'rejected':
      return { status: 'not_connected', detail: t('modelAccess.claudeCode.rejected'), actions: ['addNew', 'remove'] };
    case 'unverified':
    case 'signed_in':
    case 'off':
      return { status: 'connected', detail: described(locale, [status.email], status.expiresAt) ?? detail, actions: ['replace', 'remove'] };
  }
}

/**
 * One own computer's line, which also says which token it uses: its own, or
 * the saved one where this server relays it. A computer with its own token
 * keeps Replace and Remove whatever its check says. With Claude Code off,
 * only a token left on the computer shows, to be removed.
 */
export function computerTokenLine(t: TFunction, locale: string, entry: ClaudeCodeComputer): TokenLine | null {
  const own = entry.source === 'machine';
  const lead = own
    ? t('modelAccess.claudeCode.computer.ownToken')
    : entry.source === 'saved' ? t('modelAccess.claudeCode.computer.savedToken') : null;
  const actions: TokenAction[] = own ? ['replace', 'remove'] : entry.source === 'saved' ? ['addOwn'] : ['add'];
  switch (entry.state) {
    case 'off':
      return own ? { status: 'off', detail: t('modelAccess.claudeCode.optOut.computer'), actions: ['remove'] } : null;
    case 'offline':
      return { status: 'offline', detail: t('modelAccess.claudeCode.computer.offline'), actions: [] };
    case 'no_token':
      return { status: 'not_connected', detail: t('modelAccess.claudeCode.computer.noToken'), actions: ['add'] };
    case 'checking':
      return { status: 'working', detail: lead, actions };
    case 'rejected':
      return own
        ? { status: 'not_connected', detail: t('modelAccess.claudeCode.rejected'), actions: ['addNew', 'remove'] }
        : { status: 'not_connected', detail: t('modelAccess.claudeCode.computer.savedRejected'), actions };
    case 'unverified':
    case 'signed_in':
      return { status: 'connected', detail: described(locale, [lead, entry.email], entry.expires_at), actions };
  }
}
