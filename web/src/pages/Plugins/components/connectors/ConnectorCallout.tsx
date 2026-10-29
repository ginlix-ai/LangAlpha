import type { ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { AlertTriangle, CheckCircle2, LogIn } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { verdictDetail, verdictMessageKey } from '@/pages/ChatAgent/components/mcp/mcpProbeVerdict';
import { probeRowState } from '@/pages/ChatAgent/components/mcp/mcpState';
import type { CatalogServer, McpProbeResult } from '@/pages/ChatAgent/utils/api';
import type { Brokerage } from '../../brokerages';
import type { ConnectorState } from '../../utils/connectorState';
import { capitalizeFirst } from '../../utils/sentence';
import { DetailCallout } from '../DetailPage';
import { VendorNotes } from '../OauthRowParts';
import { KeyField } from './KeyField';
import type { KeyFixState } from './useKeyFix';

export type Callout =
  | { kind: 'keyChecking' }
  | { kind: 'keySaved' }
  | { kind: 'keyAccepted'; result: McpProbeResult }
  | { kind: 'keyFailed'; result: McpProbeResult }
  | { kind: 'problem'; problem: Extract<ConnectorState, { kind: 'failing' }> }
  | { kind: 'connect' };

/**
 * The problem at the top of a connector's page, or null when there is none.
 * The answer to a key just saved here outranks what the row's last check said,
 * since the save purged that check; a check's problem outranks the sign-in,
 * whose fix it names first. A brokerage with no row yet has only the sign-in.
 */
export function pickCallout(
  state: ConnectorState | null,
  fix: KeyFixState,
  unadded: boolean,
): Callout | null {
  const { outcome } = fix;
  if (outcome?.phase === 'checking') return { kind: 'keyChecking' };
  if (outcome?.phase === 'saved') return { kind: 'keySaved' };
  if (outcome?.phase === 'checked') {
    return { kind: fix.failed ? 'keyFailed' : 'keyAccepted', result: outcome.result };
  }
  if (state?.kind === 'failing') return { kind: 'problem', problem: state };
  if (state?.kind === 'connect' || unadded) return { kind: 'connect' };
  return null;
}

/** The callout and its fix, in the cell's words. */
export function ConnectorCallout({
  callout,
  catalog,
  state,
  name,
  vendor,
  signInKnown,
  keyNames,
  stored,
  fix,
  connect,
  edit,
}: {
  callout: Callout;
  catalog: CatalogServer | null;
  state: ConnectorState | null;
  name: string;
  vendor: Brokerage | null | undefined;
  /** A sign-in the switch is known to wait for, rather than an unchecked row. */
  signInKnown: boolean;
  /** Every vault key the row reads. */
  keyNames: string[];
  stored: ReadonlySet<string>;
  fix: KeyFixState;
  /** The page's Connect at the emphasis asked, or null where it has none. */
  connect: ((emphasis: 'quiet' | 'loud') => ReactNode) | null;
  /** The primary Edit, or null where the page offers none. */
  edit: ReactNode;
}) {
  const { t } = useTranslation();
  const keyFields = (keys: string[]) =>
    keys.length > 0 && (
      <span className="flex flex-col gap-3 mt-2">
        {keys.map((key) => (
          <KeyField
            key={key}
            name={key}
            stored={stored.has(key)}
            checks={fix.checkable}
            onSaved={() => void fix.recheck()}
          />
        ))}
      </span>
    );
  // The fix leads, as a button here or a key field in the body; a connector
  // still offering Connect keeps it beside the fix, quieter, since none of
  // these checks learned anything about a sign-in.
  const withConnect = (action: ReactNode, fixInBody: boolean) =>
    state?.kind === 'connect' && connect ? (
      <span className="connector-verbs-row">
        {action}
        {connect(action || fixInBody ? 'quiet' : 'loud')}
      </span>
    ) : (
      action
    );
  const warnIcon = <AlertTriangle className="h-4 w-4 mt-px" aria-hidden />;

  switch (callout.kind) {
    case 'keyChecking':
      return (
        <DetailCallout
          tone="quiet"
          icon={<Loader size={14} className="text-current" />}
          title={t('plugins.connectorsPage.callout.keyChecking', { name })}
        />
      );
    case 'keySaved':
      return (
        <DetailCallout
          tone="quiet"
          icon={<CheckCircle2 className="h-4 w-4" aria-hidden />}
          title={t('plugins.connectorsPage.callout.keySaved')}
        >
          {t('plugins.connectorsPage.callout.keySavedBody')}
        </DetailCallout>
      );
    case 'keyAccepted':
      return (
        <DetailCallout
          tone="quiet"
          icon={<CheckCircle2 className="h-4 w-4" style={{ color: 'var(--color-profit)' }} aria-hidden />}
          title={t('plugins.connectorsPage.callout.keyAccepted')}
        >
          <span className="flex flex-col gap-0.5">
            <span data-testid="detail-key-verdict">{verdictDetail(callout.result, t)}</span>
            {!catalog?.enabled && <span>{t('plugins.connectorsPage.callout.switchOnNow')}</span>}
          </span>
        </DetailCallout>
      );
    case 'keyFailed': {
      // The value is stored, it just did not do it. The check's own sentence
      // says why, and the field stays for another try where the verdict still
      // names a key; a verdict that learned the server signs in some other way
      // takes Connect away rather than offering it beside a refusal.
      const { result } = callout;
      const keys =
        result.missing_secrets.length > 0
          ? result.missing_secrets
          : result.verdict === 'credential_rejected'
            ? keyNames
            : [];
      const wire = verdictDetail(result, t);
      const retry =
        keys.length > 0 ? null : (
          <span className="connector-verbs-row">
            <button type="button" className="connector-btn" onClick={() => void fix.recheck()}>
              {t('plugins.connectorsPage.callout.checkAgain')}
            </button>
            {edit}
          </span>
        );
      return (
        <DetailCallout
          align="start"
          icon={warnIcon}
          title={t('plugins.connectorsPage.callout.keyFailed')}
          action={
            probeRowState(result.verdict)?.oauth === 'no' ? retry : withConnect(retry, keys.length > 0)
          }
        >
          <span className="flex flex-col gap-1">
            <span data-testid="detail-key-verdict">
              {t(verdictMessageKey(result), { names: result.missing_secrets.join(', ') })}
            </span>
            {wire && <span className="connector-mono">{wire}</span>}
            {keyFields(keys)}
          </span>
        </DetailCallout>
      );
    }
    case 'problem': {
      // A check found something to fix: refused, unreachable, or short a key.
      // A key it named is filled in right here; anything else is the form's.
      const { keys, noteKey, wire } = callout.problem;
      const verdict = catalog?.probe?.verdict;
      const text =
        verdict === 'missing_secrets'
          ? t('plugins.connectorsPage.callout.missingBody', {
              keys: keys.join(', '),
              count: keys.length,
            })
          : verdict === 'credential_rejected'
            ? t('plugins.connectorsPage.callout.rejectedBody')
            : verdict === 'needs_credential'
              ? t('plugins.connectorsPage.callout.credentialBody')
              : t('plugins.connectorsPage.callout.unreachableBody');
      return (
        <DetailCallout
          align={keys.length > 0 ? 'start' : 'center'}
          icon={keys.length > 0 ? warnIcon : undefined}
          title={capitalizeFirst(t(noteKey))}
          action={withConnect(keys.length > 0 ? null : edit, keys.length > 0)}
        >
          <span className="flex flex-col gap-1">
            <span>{text}</span>
            {wire && <span className="connector-mono">{wire}</span>}
            {keyFields(keys)}
          </span>
        </DetailCallout>
      );
    }
    case 'connect': {
      const status = catalog?.oauth_status ?? null;
      const signIn = signInKnown || !catalog;
      return (
        <DetailCallout
          icon={<LogIn className="h-4 w-4" aria-hidden />}
          title={
            status
              ? t('plugins.connectorsPage.callout.reconnect', { name })
              : signIn
                ? t('plugins.connectorsPage.callout.signIn', { name })
                : t('plugins.connectorsPage.callout.notChecked', { name })
          }
          action={connect?.('loud')}
        >
          <span className="flex flex-col gap-1">
            <span>
              {status
                ? t('plugins.connectorsPage.callout.reconnectBody')
                : signIn
                  ? t('plugins.connectorsPage.callout.signInBody')
                  : t('plugins.connectorsPage.callout.notCheckedBody')}
            </span>
            <VendorNotes vendor={vendor} unconnected rowKey={`detail-${name}`} />
          </span>
        </DetailCallout>
      );
    }
  }
}
