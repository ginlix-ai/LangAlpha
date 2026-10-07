import React from 'react';
import { useTranslation } from 'react-i18next';
import { Ban, XCircle } from 'lucide-react';
import { CodeBlock } from '../Markdown';
import { ArgsTable } from './ArgsTable';
import { JsonTree } from './JsonTree';
import { DirectToolTileMark } from './DirectToolMark';
import { useDirectToolVendorLabel } from './useDirectToolVendor';
import {
  directToolRejectionReason,
  directToolIdentity,
  maskAccountIdsDeep,
  parseDirectToolResult,
  type DirectToolResult,
} from '../../utils/directTools';
import { humanizeKey } from '../../utils/structuredResult';

function SectionLabel({ children }: { children: React.ReactNode }): React.ReactElement {
  return (
    <div className="text-xs font-medium uppercase tracking-wider mb-2 px-1" style={{ color: 'var(--color-text-tertiary)' }}>
      {children}
    </div>
  );
}

function Notice({ icon, title, body }: { icon: React.ReactNode; title: string; body?: string }): React.ReactElement {
  return (
    <div
      className="rounded-lg px-4 py-3 flex items-start gap-3"
      style={{ backgroundColor: 'var(--color-bg-elevated)', border: '1px solid var(--color-border-default)' }}
    >
      {icon}
      <div className="min-w-0 space-y-1">
        <p className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>{title}</p>
        {body && <p className="text-xs wrap-break-word" style={{ color: 'var(--color-text-tertiary)' }}>{body}</p>}
      </div>
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
      return (
        <Notice
          icon={<Ban className="h-4 w-4 shrink-0 mt-0.5" style={{ color: 'var(--color-text-tertiary)' }} />}
          title={t('toolArtifact.directTool.refused')}
          body={result.reason}
        />
      );
    case 'rejected':
      return (
        <Notice
          icon={<XCircle className="h-4 w-4 shrink-0 mt-0.5" style={{ color: 'var(--color-text-tertiary)' }} />}
          title={t('toolArtifact.directTool.rejected')}
          body={directToolRejectionReason(result.reason) || undefined}
        />
      );
    case 'blocks':
      return (
        // sibling-space-y, not space-y: CodeBlock's inline margin would
        // otherwise win over the gap on one side of it.
        <div className="sibling-space-y-3">
          {result.blocks.map((b, i) =>
            b.json !== undefined ? (
              <JsonTree key={i} value={maskAccountIdsDeep(b.json)} />
            ) : (
              <CodeBlock key={i} language="text" code={b.text} />
            ),
          )}
        </div>
      );
    case 'json':
      return <JsonTree value={maskAccountIdsDeep(result.json)} />;
    case 'text':
      return <CodeBlock language="text" code={result.text} />;
    case 'empty':
      return <div className="text-xs" style={{ color: 'var(--color-text-quaternary)' }}>{t('toolArtifact.noResultContent')}</div>;
  }
}

/** Detail-panel body for a `mcp__<server>__<tool>` call: vendor, input, output. */
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

  return (
    <div className="space-y-5">
      <div className="flex items-center gap-3">
        <DirectToolTileMark server={parsed.server} />
        <div className="min-w-0">
          <div className="text-sm font-medium truncate" style={{ color: 'var(--color-text-primary)' }}>
            {humanizeKey(parsed.tool)}
          </div>
          <div className="text-xs truncate font-mono" style={{ color: 'var(--color-text-tertiary)' }}>
            {vendorLabel} · {parsed.tool}
          </div>
        </div>
      </div>

      <div>
        <SectionLabel>{t('toolArtifact.input')}</SectionLabel>
        <div className="px-1">
          <ArgsTable args={args || {}} emptyLabel={t('toolArtifact.directTool.noArguments')} />
        </div>
      </div>

      <div>
        <SectionLabel>{t('toolArtifact.output')}</SectionLabel>
        <div className="px-1">
          <DirectToolResultView result={result} />
        </div>
      </div>
    </div>
  );
}
