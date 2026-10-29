import { useId, useState, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { ChevronRight } from 'lucide-react';
import { cn } from '@/lib/utils';
import type { McpToolSummary } from '@/pages/ChatAgent/utils/api';
import { readToolDoc, type ToolDoc, type ToolParam } from './toolDoc';

/**
 * One tool: its name and the sentence that says what it is for, opened in
 * place for the rest. Closed, a list of them scans like a table of contents;
 * open, a row answers what the tool takes and what it gives back, which is
 * what someone deciding whether to keep a connector on came to find out.
 */
export function ToolRow({
  tool,
  checkbox,
  control,
  defaultOpen = false,
}: {
  tool: McpToolSummary;
  checkbox?: ReactNode;
  control?: ReactNode;
  defaultOpen?: boolean;
}) {
  const doc = readToolDoc(tool);
  const [open, setOpen] = useState(defaultOpen);
  const bodyId = useId();
  const more = doc.body.length > 0 || doc.params.length > 0 || doc.returns !== null;

  const text = (
    <span className="tool-row-text">
      <span className="tool-row-name">{tool.name}</span>
      {doc.summary && <span className="tool-row-summary">{doc.summary}</span>}
    </span>
  );

  return (
    <div className={cn('tool-row', open && more && 'is-open', !!checkbox && 'has-check')}>
      <div className="tool-row-head">
        {checkbox && <span className="tool-row-check">{checkbox}</span>}
        {more ? (
          <button
            type="button"
            className="tool-row-toggle"
            aria-expanded={open}
            aria-controls={bodyId}
            onClick={() => setOpen((o) => !o)}
          >
            <ChevronRight className="tool-row-chevron" aria-hidden />
            {text}
          </button>
        ) : (
          <div className="tool-row-toggle is-static">
            <span className="tool-row-chevron" aria-hidden />
            {text}
          </div>
        )}
        {control && <div className="tool-row-control">{control}</div>}
      </div>
      {open && more && <ToolDocBody id={bodyId} doc={doc} />}
    </div>
  );
}

function ToolDocBody({ id, doc }: { id: string; doc: ToolDoc }) {
  const { t } = useTranslation();
  return (
    <div id={id} className="tool-doc">
      {doc.body.map((paragraph, i) => (
        <p key={i} className="tool-doc-prose">
          {paragraph}
        </p>
      ))}
      {doc.params.length > 0 && (
        <div className="tool-doc-block">
          <div className="tool-doc-label">{t('plugins.detail.toolParams')}</div>
          <dl className="tool-params">
            {doc.params.map((param) => (
              <ToolParamRow key={param.name} param={param} />
            ))}
          </dl>
        </div>
      )}
      {doc.returns && (
        <div className="tool-doc-block">
          <div className="tool-doc-label">{t('plugins.detail.toolReturns')}</div>
          <p className="tool-doc-returns">{doc.returns}</p>
        </div>
      )}
    </div>
  );
}

function ToolParamRow({ param }: { param: ToolParam }) {
  const { t } = useTranslation();
  // A sentence that already names every choice says it better than chips
  // repeating them underneath.
  const choices =
    param.choices.length > 0 && !param.choices.every((c) => param.description?.includes(c))
      ? param.choices
      : [];
  return (
    <div className="tool-param">
      <dt>
        <code>{param.name}</code>
      </dt>
      <dd>
        <span className="tool-param-meta">
          {param.type && <span>{param.type}</span>}
          {param.required ? (
            <span className="is-required">{t('plugins.detail.toolRequired')}</span>
          ) : (
            param.defaultValue !== null && (
              <span>
                {t('plugins.detail.toolDefault')} <code>{param.defaultValue}</code>
              </span>
            )
          )}
        </span>
        {param.description && <span className="tool-param-desc">{param.description}</span>}
        {choices.length > 0 && (
          <span className="tool-param-choices">
            {choices.map((choice) => (
              <code key={choice}>{choice}</code>
            ))}
          </span>
        )}
      </dd>
    </div>
  );
}
