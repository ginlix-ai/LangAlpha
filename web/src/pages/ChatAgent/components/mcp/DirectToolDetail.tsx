import React from 'react';
import { useTranslation } from 'react-i18next';
import { Ban, XCircle, type LucideIcon } from 'lucide-react';
import { TermCard } from '../charts/TermCard';
import { TermCopy, TermPanelHead } from '../charts/TermPanel';
import { ArgsTable } from './ArgsTable';
import { JsonTree } from './JsonTree';
import { DirectToolRowMark } from './DirectToolMark';
import { useDirectToolVendorLabel } from './useDirectToolVendor';
import {
  directToolRejectionReason,
  directToolIdentity,
  maskAccountIdsDeep,
  parseDirectToolResult,
  type DirectToolResult,
} from '../../utils/directTools';
import { humanizeKey } from '../../utils/structuredResult';

/** Why the call has no answer: the state on a line, the reason under it as written. */
function Notice({ icon: Icon, title, body }: { icon: LucideIcon; title: string; body?: string }): React.ReactElement {
  return (
    <div className="term-direct-notice">
      <p className="term-direct-notice-title">
        <Icon className="term-direct-notice-mark" aria-hidden="true" />
        {title}
      </p>
      {body && <p className="term-prose term-direct-notice-body">{body}</p>}
    </div>
  );
}

/**
 * The result body of a direct tool call: refused, rejected, JSON, or raw text.
 *
 * Account ids in a JSON body are masked as the arguments above it are: a
 * brokerage echoes the account back in its answer, and the panel would
 * otherwise print in full the number the Input and the receipt both mask.
 */
export function DirectToolResultView({ result }: { result: DirectToolResult }): React.ReactElement {
  const { t } = useTranslation();
  switch (result.kind) {
    case 'refused':
      return <Notice icon={Ban} title={t('toolArtifact.directTool.refused')} body={result.reason} />;
    case 'rejected':
      return (
        <Notice
          icon={XCircle}
          title={t('toolArtifact.directTool.rejected')}
          body={directToolRejectionReason(result.reason) || undefined}
        />
      );
    case 'blocks':
      return (
        <div className="term-direct-blocks">
          {result.blocks.map((b, i) =>
            b.json !== undefined ? (
              <div key={i} className="term-direct-json">
                <JsonTree value={maskAccountIdsDeep(b.json)} />
              </div>
            ) : (
              <pre key={i} className="term-direct-text">{b.text}</pre>
            ),
          )}
        </div>
      );
    case 'json':
      return (
        <div className="term-direct-json">
          <JsonTree value={maskAccountIdsDeep(result.json)} />
        </div>
      );
    case 'text':
      return <pre className="term-direct-text">{result.text}</pre>;
    case 'empty':
      return <div className="term-t3">{t('toolArtifact.noResultContent')}</div>;
  }
}

/** The answer as text to copy, masked as it is shown; nothing for a call with no answer. */
function resultText(result: DirectToolResult): string | null {
  switch (result.kind) {
    case 'json':
      return JSON.stringify(maskAccountIdsDeep(result.json), null, 2);
    case 'text':
      return result.text;
    case 'blocks':
      return result.blocks
        .map((b) => (b.json !== undefined ? JSON.stringify(maskAccountIdsDeep(b.json), null, 2) : b.text))
        .join('\n\n');
    default:
      return null;
  }
}

/**
 * Detail-panel body for a `mcp__<server>__<tool>` call: the tool and its
 * vendor, the arguments sent, and the answer. It is what an order's receipt
 * opens, so the vendor reads as it does on the receipt's edge.
 */
export function DirectToolDetail({
  toolName,
  args,
  content,
  artifact,
  isFailed,
}: {
  toolName: string;
  args: Record<string, unknown> | undefined;
  content: unknown;
  artifact?: unknown;
  isFailed?: boolean;
}): React.ReactElement {
  const { t } = useTranslation();
  const parsed = directToolIdentity(toolName, artifact) || { server: '', tool: toolName };
  const vendorLabel = useDirectToolVendorLabel(parsed.server);
  const result = parseDirectToolResult(content, isFailed);
  const copyText = resultText(result);

  return (
    <>
      <TermPanelHead
        title={humanizeKey(parsed.tool)}
        sub={
          <>
            {vendorLabel && (
              <>
                <DirectToolRowMark server={parsed.server} className="term-direct-mark h-3 w-3" />
                {vendorLabel}
                <span aria-hidden="true"> · </span>
              </>
            )}
            {/* The vendor's own name for it, as its docs spell it. */}
            <span className="term-direct-id">{parsed.tool}</span>
          </>
        }
      />

      {/* ArgsTable and JsonTree are the approval cards' too; here they take
          the panel's inks. */}
      <TermCard title={t('toolArtifact.input')}>
        <div className="term-direct-body">
          <ArgsTable args={args || {}} emptyLabel={t('toolArtifact.directTool.noArguments')} />
        </div>
      </TermCard>

      <TermCard title={t('toolArtifact.output')} meta={copyText ? <TermCopy text={copyText} /> : undefined}>
        <div className="term-direct-body">
          <DirectToolResultView result={result} />
        </div>
      </TermCard>
    </>
  );
}
