import React, { useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { AlertTriangle, ChevronRight } from 'lucide-react';
import Markdown from '../Markdown';
import {
  humanizeKey,
  type StructuredResult,
  type StructuredValue,
} from '../../utils/structuredResult';

/**
 * Past this depth a value is shown as JSON instead of fields: deeper nesting
 * stops reading as a document and starts reading as a data structure, which the
 * code block already presents well.
 */
const MAX_DEPTH = 3;

/**
 * Past this many siblings the remainder becomes one JSON block. A wide payload
 * would otherwise mount a markdown renderer per item, and the cost is in the
 * mounting, so bounding the rendered height does not pay it back.
 */
const MAX_BREADTH = 50;

const LABEL_STYLE: React.CSSProperties = {
  fontSize: '0.6875rem',
  color: 'var(--color-text-quaternary)',
  letterSpacing: '0.05em',
  textTransform: 'uppercase',
};

type OpenFile = (path: string, workspaceId?: string) => void;

function Empty(): React.ReactElement {
  return <span style={{ color: 'var(--color-text-quaternary)' }}>—</span>;
}

function JsonDump({ value }: { value: StructuredValue }): React.ReactElement {
  return (
    <Markdown
      variant="chat"
      content={`\`\`\`json\n${JSON.stringify(value, null, 2)}\n\`\`\``}
    />
  );
}

function Overflow({
  value,
  count,
}: {
  value: StructuredValue;
  count: number;
}): React.ReactElement {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col gap-1" data-testid="structured-result-overflow">
      <div style={LABEL_STYLE}>{t('chat.resultMoreItems', { n: count })}</div>
      <JsonDump value={value} />
    </div>
  );
}

interface ValueProps {
  value: StructuredValue;
  depth: number;
  onOpenFile?: OpenFile;
}

function Value({ value, depth, onOpenFile }: ValueProps): React.ReactElement {
  if (value === null) return <Empty />;

  // Strings carry the prose — through Markdown so the child's inline citations
  // become real pills rather than the literal tags a code block would show.
  if (typeof value === 'string') {
    return value.trim() ? (
      <Markdown
        variant="chat"
        content={value}
        className="text-base"
        onOpenFile={onOpenFile}
      />
    ) : (
      <Empty />
    );
  }

  if (typeof value === 'number' || typeof value === 'boolean') {
    return <span style={{ color: 'var(--color-text-primary)' }}>{String(value)}</span>;
  }

  if (depth >= MAX_DEPTH) return <JsonDump value={value} />;

  if (Array.isArray(value)) {
    if (value.length === 0) return <Empty />;
    const rest = value.slice(MAX_BREADTH);
    return (
      <div className="flex flex-col gap-2">
        <ul
          className="list-disc pl-5 flex flex-col gap-1.5"
          style={{ color: 'var(--color-text-primary)' }}
        >
          {value.slice(0, MAX_BREADTH).map((item, index) => (
            <li key={index}>
              <Value value={item} depth={depth + 1} onOpenFile={onOpenFile} />
            </li>
          ))}
        </ul>
        {rest.length > 0 && <Overflow value={rest} count={rest.length} />}
      </div>
    );
  }

  const entries = Object.entries(value);
  if (entries.length === 0) return <Empty />;
  const rest = entries.slice(MAX_BREADTH);
  return (
    <div
      className="flex flex-col gap-2 pl-3"
      style={{ borderLeft: '1px solid var(--color-border-subtle)' }}
    >
      {entries.slice(0, MAX_BREADTH).map(([key, child]) => (
        <Field
          key={key}
          label={key}
          value={child}
          depth={depth + 1}
          onOpenFile={onOpenFile}
        />
      ))}
      {rest.length > 0 && (
        <Overflow value={Object.fromEntries(rest)} count={rest.length} />
      )}
    </div>
  );
}

interface FieldProps extends ValueProps {
  label: string;
}

function Field({ label, value, depth, onOpenFile }: FieldProps): React.ReactElement {
  return (
    <div className="flex flex-col gap-1">
      <div style={LABEL_STYLE}>{humanizeKey(label)}</div>
      <Value value={value} depth={depth} onOpenFile={onOpenFile} />
    </div>
  );
}

const CONTROL_STYLE: React.CSSProperties = {
  color: 'var(--color-text-quaternary)',
  background: 'none',
  border: 'none',
  cursor: 'pointer',
  padding: 0,
};

/** Fades the content itself, so it works on whatever background the host has. */
const FADE = 'linear-gradient(to bottom, black 70%, transparent)';

interface StructuredResultBlockProps {
  result: StructuredResult;
  onOpenFile?: OpenFile;
  /** Bound the fields to this many pixels behind an expand control. */
  collapsedMaxHeight?: number;
}

/**
 * Renders a JSON answer as labeled fields, with the original object one click
 * away.
 */
function StructuredResultBlock({
  result,
  onOpenFile,
  collapsedMaxHeight,
}: StructuredResultBlockProps): React.ReactElement {
  const { t } = useTranslation();
  const [showRaw, setShowRaw] = useState(false);
  const [expanded, setExpanded] = useState(false);
  const [overflows, setOverflows] = useState(false);
  // Measured on the inner element, which is never clipped, so the reading stays
  // valid once expanded — measuring the clipped wrapper would report no overflow
  // and drop the control that collapses it again.
  const contentRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (collapsedMaxHeight == null) return;
    const content = contentRef.current;
    if (!content) return;
    setOverflows(content.scrollHeight > collapsedMaxHeight);
  }, [collapsedMaxHeight, result]);

  const bounded = overflows && !expanded;
  const rootOverflow = result.entries.slice(MAX_BREADTH);

  return (
    <div className="flex flex-col gap-4" data-testid="structured-result">
      <div
        style={
          bounded
            ? {
                maxHeight: collapsedMaxHeight,
                overflow: 'hidden',
                maskImage: FADE,
                WebkitMaskImage: FADE,
              }
            : undefined
        }
      >
        <div ref={contentRef} className="flex flex-col gap-4">
          {result.entries.slice(0, MAX_BREADTH).map(([key, value]) => (
            <Field key={key} label={key} value={value} depth={0} onOpenFile={onOpenFile} />
          ))}
          {rootOverflow.length > 0 && (
            <Overflow
              value={Object.fromEntries(rootOverflow)}
              count={rootOverflow.length}
            />
          )}
        </div>
      </div>

      {result.truncated && (
        <div
          className="flex items-center gap-1.5 text-xs"
          data-testid="structured-result-truncated"
          style={{ color: 'var(--color-warning)' }}
        >
          <AlertTriangle className="h-3 w-3 flex-shrink-0" />
          {t('chat.resultTruncated')}
        </div>
      )}

      <div className="flex items-center gap-4">
        {overflows && (
          <button
            type="button"
            onClick={() => setExpanded((open) => !open)}
            className="text-xs transition-opacity hover:opacity-80"
            style={CONTROL_STYLE}
          >
            {expanded ? t('chat.collapseResult') : t('chat.expandResult')}
          </button>
        )}
        <button
          type="button"
          onClick={() => setShowRaw((shown) => !shown)}
          className="flex items-center gap-1 text-xs transition-opacity hover:opacity-80"
          style={CONTROL_STYLE}
        >
          <ChevronRight
            className="h-3 w-3 transition-transform"
            style={{ transform: showRaw ? 'rotate(90deg)' : undefined }}
          />
          {showRaw ? t('chat.hideRawJson') : t('chat.showRawJson')}
        </button>
      </div>
      {showRaw && <Markdown variant="chat" content={`\`\`\`json\n${result.raw}\n\`\`\``} />}
    </div>
  );
}

export default React.memo(StructuredResultBlock);
