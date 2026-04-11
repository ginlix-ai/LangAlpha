import React, { useState, useMemo } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import remarkCjkFriendly from 'remark-cjk-friendly';
import remarkMath from 'remark-math';
import rehypeRaw from 'rehype-raw';
import rehypeKatex from 'rehype-katex';
import rehypeSanitize, { defaultSchema } from 'rehype-sanitize';
import 'katex/dist/katex.min.css';
import SyntaxHighlighter, { oneDark, oneLight } from './SyntaxHighlighter';
import { Copy, Check } from 'lucide-react';
import { useTheme } from '@/contexts/ThemeContext';
import WorkspaceImage from './WorkspaceImage';
import { isFilePath, isImagePath, normalizeFilePath, parseWsPath } from './FileCard';
import { normalizeFileRefs } from '../utils/normalizeFileRefs';
import CitationBubble from './CitationBubble';

// Sanitize schema: extends GitHub-style defaults to allow KaTeX output,
// cite-bubble custom element, and MathML/SVG for accessibility.
const sanitizeSchema = {
  ...defaultSchema,
  tagNames: [
    ...(defaultSchema.tagNames ?? []),
    // Custom citation element
    'cite-bubble',
    // MathML (KaTeX accessibility tree inside .katex-mathml)
    'math', 'annotation', 'semantics',
    'mi', 'mn', 'mo', 'mrow', 'mfrac', 'msqrt', 'mroot', 'mstyle',
    'msub', 'msup', 'msubsup', 'munder', 'mover', 'munderover',
    'mtable', 'mtr', 'mtd', 'mtext', 'mspace', 'mpadded', 'mphantom',
    // SVG (KaTeX stretchy symbols: radicals, braces, arrows)
    'svg', 'path', 'line',
  ],
  attributes: {
    ...defaultSchema.attributes,
    'cite-bubble': ['label', 'href'],
    span: [
      ...(defaultSchema.attributes?.span ?? []),
      'className', 'style', 'ariaHidden',
    ],
    div: [
      ...(defaultSchema.attributes?.div ?? []),
      'className', 'style',
    ],
    math: ['xmlns', 'display'],
    annotation: ['encoding'],
    svg: ['xmlns', 'width', 'height', 'viewBox', 'preserveAspectRatio', 'style'],
    path: ['d'],
    line: ['x1', 'x2', 'y1', 'y2'],
  },
};

interface CodeBlockProps {
  language: string | null;
  code: string;
  compact?: boolean;
  codeTheme?: 'light' | 'dark';
}

// --- CodeBlock component ---
function CodeBlock({ language, code, compact = false, codeTheme }: CodeBlockProps): React.ReactElement {
  const { theme } = useTheme();
  const effectiveTheme = codeTheme ?? theme;
  const [copied, setCopied] = useState(false);

  const handleCopy = (): void => {
    navigator.clipboard.writeText(code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  // When codeTheme is set, use explicit colors instead of CSS vars.
  // CSS vars resolve to the current theme at render time and get baked into
  // innerHTML clones (Paged.js) and react-to-print iframes.
  const isForceLight = codeTheme === 'light';
  const bgColor = isForceLight ? '#f8f9fa' : 'var(--color-bg-code)';
  const borderColor = isForceLight ? '#e0e0e0' : 'var(--color-border-muted)';
  const labelColor = isForceLight ? '#6b7280' : 'var(--color-text-tertiary)';

  // Export/print mode: Notion-style clean code block — no header chrome,
  // just code on a light gray background. Page-break-inside:avoid keeps
  // the block together across pages.
  if (isForceLight) {
    return (
      <div style={{ margin: compact ? '4px 0' : '6px 0' }}>
        <div className="rounded overflow-hidden"
          style={{ backgroundColor: '#f7f6f3', pageBreakInside: 'avoid', breakInside: 'avoid' }}>
          <SyntaxHighlighter
            language={language || 'text'}
            style={oneLight}
            customStyle={{
              margin: 0,
              padding: '0.8rem 1rem',
              backgroundColor: 'transparent',
              fontSize: compact ? '0.75rem' : '0.8rem',
              lineHeight: '1.6',
            }}
            codeTagProps={{ style: { backgroundColor: 'transparent' } }}
            wrapLongLines
          >
            {code}
          </SyntaxHighlighter>
        </div>
      </div>
    );
  }

  return (
    <div style={{ margin: compact ? '4px 0' : '6px 0' }}>
      <div className="rounded-lg overflow-hidden"
        style={{ backgroundColor: bgColor, border: `1px solid ${borderColor}` }}>
        {!compact && (
          <div className="flex items-center justify-between px-3 py-1.5"
            style={{ borderBottom: `1px solid ${borderColor}` }}>
            <span className="text-xs font-mono" style={{ color: labelColor }}>
              {language || 'text'}
            </span>
            <button onClick={handleCopy}
              className="flex items-center gap-1 text-xs hover:opacity-100 transition-opacity"
              style={{ color: labelColor, background: 'none', border: 'none', cursor: 'pointer' }}>
              {copied ? <><Check className="h-3 w-3" /> Copied</> : <><Copy className="h-3 w-3" /> Copy</>}
            </button>
          </div>
        )}
        <SyntaxHighlighter
          language={language || 'text'}
          style={effectiveTheme === 'light' ? oneLight : oneDark}
          customStyle={{
            margin: 0,
            padding: compact ? '0.6rem' : '1rem',
            backgroundColor: 'transparent',
            fontSize: compact ? '0.75rem' : '0.875rem',
            lineHeight: '1.5',
          }}
          codeTagProps={{ style: { backgroundColor: 'transparent' } }}
          wrapLongLines
        >
          {code}
        </SyntaxHighlighter>
      </div>
    </div>
  );
}

// --- JSON auto-detection helper ---
function tryFormatJson(code: string): { formatted: string; language: string } | null {
  const trimmed = code.trim();
  if (!(trimmed.startsWith('{') || trimmed.startsWith('['))) return null;
  try {
    return { formatted: JSON.stringify(JSON.parse(trimmed), null, 2), language: 'json' };
  } catch {
    return null;
  }
}

// --- Helper to extract code info from a <pre> element ---
function extractCodeFromPre(children: React.ReactNode): { language: string | null; code: string } {
  const codeEl = (children as any)?.props ? (children as any) : null;
  const className = (codeEl?.props?.className || '') as string;
  const match = /language-(\w+)/.exec(className);
  const raw = String(codeEl?.props?.children ?? children ?? '').replace(/\n$/, '');
  const json = !match ? tryFormatJson(raw) : null;
  const language = match?.[1] || json?.language || null;
  const code = json?.formatted || raw;
  return { language, code };
}

// react-markdown passes a `node` prop to all component overrides.
// We strip it out via destructuring to avoid passing it to DOM elements.
// Using a loose props type for these overrides since react-markdown's
// component typing is complex and varies by element.
type MarkdownComponentProps = Record<string, any>;

// --- Shared overrides (used by all variants) ---
const strong = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <strong style={{ color: 'var(--color-text-primary)', fontWeight: 700 }} {...props} />
);
const em = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <em className="italic" style={{ color: 'var(--color-text-primary)' }} {...props} />
);
const del = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <del style={{ color: 'var(--color-text-tertiary)', textDecoration: 'line-through' }} {...props} />
);
const input = ({ node: _node, type, checked, ...props }: MarkdownComponentProps) => {
  if (type === 'checkbox') {
    return (
      <input type="checkbox" checked={checked} readOnly
        style={{ marginRight: '6px', accentColor: 'var(--color-accent-primary)' }} />
    );
  }
  return <input {...props} />;
};
const img = ({ node: _node, ...props }: MarkdownComponentProps) => <WorkspaceImage {...props} />;
const ul = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <ul className="list-disc ml-4 my-1" style={{ color: 'var(--color-text-primary)' }} {...props} />
);
const ol = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <ol className="list-decimal ml-4 my-1" style={{ color: 'var(--color-text-primary)' }} {...props} />
);
const li = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <li className="break-words" style={{ color: 'var(--color-text-primary)' }} {...props} />
);

// ===================== CHAT variant =====================
const chatUl = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <ul className="list-disc ml-6 my-2" style={{ color: 'var(--color-text-primary)' }} {...props} />
);
const chatOl = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <ol className="list-decimal ml-6 my-2" style={{ color: 'var(--color-text-primary)' }} {...props} />
);
const chatLi = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <li className="ps-[2px] break-words" style={{ color: 'var(--color-text-primary)' }} {...props} />
);
const chatP = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <p className="my-[1px] py-[3px] whitespace-pre-wrap break-words first:mt-0 last:mb-0" style={{ color: 'var(--color-text-primary)' }} {...props} />
);
const chatH1 = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <h1 className="first:mt-0" style={{ color: 'var(--color-text-primary)', fontSize: '1.75em', fontWeight: 700, lineHeight: '1.3', marginTop: '1.5em', marginBottom: '0.5em' }} {...props} />
);
const chatH2 = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <h2 className="first:mt-0" style={{ color: 'var(--color-text-primary)', fontSize: '1.4em', fontWeight: 700, lineHeight: '1.3', marginTop: '1.4em', marginBottom: '0.4em' }} {...props} />
);
const chatH3 = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <h3 className="first:mt-0" style={{ color: 'var(--color-text-primary)', fontSize: '1.2em', fontWeight: 600, lineHeight: '1.3', marginTop: '1.2em', marginBottom: '0.3em' }} {...props} />
);
const chatH4 = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <h4 className="first:mt-0" style={{ color: 'var(--color-text-primary)', fontSize: '1.05em', fontWeight: 600, lineHeight: '1.4', marginTop: '1em', marginBottom: '0.25em' }} {...props} />
);
const chatCode = ({ node: _node, className, children, ...props }: MarkdownComponentProps) => {
  const isBlock = /language-/.test(className || '');
  if (!isBlock) {
    return (
      <code className="font-mono rounded px-1.5 py-0.5"
        style={{ backgroundColor: 'var(--color-bg-code)', color: 'var(--color-text-primary)', fontSize: '0.85em' }}
        {...props}>
        {children}
      </code>
    );
  }
  return <code className={className} {...props}>{children}</code>;
};
const chatPre = ({ node: _node, children, ..._props }: MarkdownComponentProps) => {
  const { language, code } = extractCodeFromPre(children);
  return <CodeBlock language={language} code={code} />;
};
const chatBlockquote = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <blockquote
    className="border-l-4 pl-4 my-2 italic"
    style={{ borderColor: 'var(--color-accent-primary)', color: 'var(--color-text-primary)', opacity: 0.8 }}
    {...props}
  />
);
const chatA = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <a className="underline hover:opacity-80 transition-opacity break-all" style={{ color: 'var(--color-accent-primary)' }} target="_blank" rel="noopener noreferrer" {...props} />
);
const chatHr = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <hr className="my-4 border-0" style={{ borderTop: '1px solid var(--color-border-muted)' }} {...props} />
);
const chatTable = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <div className="pt-[8px] pb-[18px]">
    <div className="overflow-x-auto rounded-lg" style={{ border: '1px solid var(--color-border-muted)' }}>
      <table className="m-0 w-full border-collapse" {...props} />
    </div>
  </div>
);
const chatThead = ({ node: _node, ...props }: MarkdownComponentProps) => <thead style={{ backgroundColor: 'var(--color-bg-input)' }} {...props} />;
const chatTbody = ({ node: _node, ...props }: MarkdownComponentProps) => <tbody {...props} />;
const chatTr = ({ node: _node, ...props }: MarkdownComponentProps) => <tr {...props} />;
const chatTh = ({ node: _node, style, ...props }: MarkdownComponentProps) => (
  <th
    className="align-top [&:not(:first-child)]:border-l"
    style={{
      textAlign: 'left',
      borderBottom: '1px solid var(--color-border-muted)',
      borderColor: 'var(--color-border-muted)',
      color: 'var(--color-text-primary)',
      fontSize: '0.875rem',
      fontWeight: 600,
      padding: '8px 14px',
      ...style,
    }}
    {...props}
  />
);
const chatTd = ({ node: _node, style, ...props }: MarkdownComponentProps) => (
  <td
    className="align-top [&:not(:first-child)]:border-l"
    style={{
      textAlign: 'left',
      borderTop: '1px solid var(--color-border-muted)',
      borderColor: 'var(--color-border-muted)',
      color: 'var(--color-text-primary)',
      fontSize: '0.875rem',
      padding: '8px 14px',
      ...style,
    }}
    {...props}
  />
);

// ===================== PANEL variant =====================
const panelP = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <p className="my-1 whitespace-pre-wrap break-words" style={{ color: 'var(--color-text-primary)' }} {...props} />
);
const panelH1 = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <h1 className="first:mt-0" style={{ color: 'var(--color-text-primary)', fontSize: '1.5em', fontWeight: 700, lineHeight: '1.3', marginTop: '1.2em', marginBottom: '0.4em' }} {...props} />
);
const panelH2 = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <h2 className="first:mt-0" style={{ color: 'var(--color-text-primary)', fontSize: '1.25em', fontWeight: 700, lineHeight: '1.3', marginTop: '1.1em', marginBottom: '0.35em' }} {...props} />
);
const panelH3 = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <h3 className="first:mt-0" style={{ color: 'var(--color-text-primary)', fontSize: '1.1em', fontWeight: 600, lineHeight: '1.3', marginTop: '1em', marginBottom: '0.3em' }} {...props} />
);
const panelH4 = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <h4 className="first:mt-0" style={{ color: 'var(--color-text-primary)', fontSize: '1em', fontWeight: 600, lineHeight: '1.4', marginTop: '0.8em', marginBottom: '0.2em' }} {...props} />
);
const panelCode = ({ node: _node, className, children, ...props }: MarkdownComponentProps) => {
  const isBlock = /language-/.test(className || '');
  if (!isBlock) {
    return (
      <code className="font-mono rounded px-1.5 py-0.5"
        style={{ backgroundColor: 'var(--color-bg-code)', color: 'var(--color-text-primary)', fontSize: 'inherit' }}
        {...props}>
        {children}
      </code>
    );
  }
  return <code className={className} {...props}>{children}</code>;
};
const panelPre = ({ node: _node, children, ..._props }: MarkdownComponentProps) => {
  const { language, code } = extractCodeFromPre(children);
  return <CodeBlock language={language} code={code} />;
};
const panelA = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <a className="underline" style={{ color: 'var(--color-accent-primary)' }} target="_blank" rel="noopener noreferrer" {...props} />
);
const panelBlockquote = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <blockquote
    className="pl-3 my-2"
    style={{ borderLeft: '3px solid var(--color-accent-overlay)', color: 'var(--color-text-primary)' }}
    {...props}
  />
);
const panelHr = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <hr className="my-3 border-0" style={{ borderTop: '1px solid var(--color-border-muted)' }} {...props} />
);
const panelTable = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <div className="my-2 overflow-x-auto rounded" style={{ border: '1px solid var(--color-border-muted)' }}>
    <table className="w-full border-collapse text-left" style={{ minWidth: '100%' }} {...props} />
  </div>
);
const panelThead = ({ node: _node, ...props }: MarkdownComponentProps) => <thead style={{ backgroundColor: 'var(--color-bg-input)' }} {...props} />;
const panelTr = ({ node: _node, ...props }: MarkdownComponentProps) => <tr className="last:border-b-0" style={{ borderBottom: '1px solid var(--color-border-muted)' }} {...props} />;
const panelTh = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <th className="px-3 py-2 whitespace-nowrap" style={{ color: 'var(--color-text-primary)', fontWeight: 600, borderBottom: '1px solid var(--color-border-muted)' }} {...props} />
);
const panelTd = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <td className="px-3 py-2 break-words align-top" style={{ color: 'var(--color-text-primary)' }} {...props} />
);

// ===================== COMPACT variant =====================
const compactP = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <p className="my-[1px] py-[3px] whitespace-pre-wrap break-words first:mt-0 last:mb-0" style={{ color: 'var(--color-text-primary)' }} {...props} />
);
const compactH1 = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <h1 className="first:mt-0" style={{ color: 'var(--color-text-primary)', fontSize: '1.25em', fontWeight: 700, lineHeight: '1.3', marginTop: '0.8em', marginBottom: '0.2em' }} {...props} />
);
const compactH2 = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <h2 className="first:mt-0" style={{ color: 'var(--color-text-primary)', fontSize: '1.15em', fontWeight: 700, lineHeight: '1.3', marginTop: '0.7em', marginBottom: '0.15em' }} {...props} />
);
const compactH3 = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <h3 className="first:mt-0" style={{ color: 'var(--color-text-primary)', fontSize: '1.05em', fontWeight: 600, lineHeight: '1.3', marginTop: '0.6em', marginBottom: '0.1em' }} {...props} />
);
const compactCode = ({ node: _node, className, children, ...props }: MarkdownComponentProps) => {
  const isBlock = /language-/.test(className || '');
  if (!isBlock) {
    return (
      <code className="font-mono rounded px-1.5 py-0.5"
        style={{ backgroundColor: 'var(--color-bg-code)', color: 'var(--color-text-primary)', fontSize: 'inherit' }}
        {...props}>
        {children}
      </code>
    );
  }
  return <code className={className} {...props}>{children}</code>;
};
const compactPre = ({ node: _node, children, ..._props }: MarkdownComponentProps) => {
  const { language, code } = extractCodeFromPre(children);
  return <CodeBlock language={language} code={code} compact />;
};

// ===================== Variant component maps =====================
const CHAT_COMPONENTS = {
  strong, em, del, input, img,
  ul: chatUl, ol: chatOl, li: chatLi,
  p: chatP, h1: chatH1, h2: chatH2, h3: chatH3, h4: chatH4,
  code: chatCode, pre: chatPre,
  blockquote: chatBlockquote, a: chatA, hr: chatHr,
  table: chatTable, thead: chatThead, tbody: chatTbody, tr: chatTr, th: chatTh, td: chatTd,
  'cite-bubble': CitationBubble,
};

const PANEL_COMPONENTS = {
  strong, em, del, input, img, ul, ol, li,
  p: panelP, h1: panelH1, h2: panelH2, h3: panelH3, h4: panelH4,
  code: panelCode, pre: panelPre,
  a: panelA, blockquote: panelBlockquote, hr: panelHr,
  table: panelTable, thead: panelThead, tr: panelTr, th: panelTh, td: panelTd,
  'cite-bubble': CitationBubble,
};

// Compact table components -- reuse panel styles for consistency
const compactTable = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <div className="my-1 overflow-x-auto rounded" style={{ border: '1px solid var(--color-border-muted)' }}>
    <table className="w-full border-collapse text-left" style={{ minWidth: '100%', fontSize: '0.85em' }} {...props} />
  </div>
);
const compactThead = ({ node: _node, ...props }: MarkdownComponentProps) => <thead style={{ backgroundColor: 'var(--color-bg-input)' }} {...props} />;
const compactTr = ({ node: _node, ...props }: MarkdownComponentProps) => <tr className="last:border-b-0" style={{ borderBottom: '1px solid var(--color-border-muted)' }} {...props} />;
const compactTh = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <th className="px-2 py-1.5 whitespace-nowrap" style={{ color: 'var(--color-text-primary)', fontWeight: 600, borderBottom: '1px solid var(--color-border-muted)' }} {...props} />
);
const compactTd = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <td className="px-2 py-1.5 break-words align-top" style={{ color: 'var(--color-text-primary)' }} {...props} />
);

const COMPACT_COMPONENTS = {
  strong, em, del, ul, ol, li,
  p: compactP, h1: compactH1, h2: compactH2, h3: compactH3,
  code: compactCode, pre: compactPre,
  a: panelA, blockquote: panelBlockquote, hr: panelHr,
  table: compactTable, thead: compactThead, tr: compactTr, th: compactTh, td: compactTd,
  'cite-bubble': CitationBubble,
};

interface VariantConfig {
  className: string;
  style: React.CSSProperties;
  components: Record<string, React.ComponentType<MarkdownComponentProps>>;
}

const VARIANTS: Record<string, VariantConfig> = {
  chat: {
    className: 'leading-[1.5] break-words max-w-none overflow-hidden',
    style: { color: 'var(--color-text-primary)' },
    components: CHAT_COMPONENTS,
  },
  panel: {
    className: '',
    style: { color: 'var(--color-text-primary)', opacity: 0.9 },
    components: PANEL_COMPONENTS,
  },
  compact: {
    className: '',
    style: { color: 'var(--color-text-primary)', opacity: 0.9 },
    components: COMPACT_COMPONENTS,
  },
};

export { CodeBlock };

/**
 * Fix malformed GFM tables so remark-gfm can parse them.
 *
 * Common LLM mistakes:
 *  - Separator row has fewer columns than the header row
 *  - Data rows have fewer/more columns than the header
 *  - Merged/mangled cells like "|---|------ 1 | ..."
 *
 * Strategy: detect table blocks (consecutive lines starting/ending with |),
 * count header columns, then rebuild the separator and pad/trim data rows.
 */
function fixMarkdownTables(content: string): string {
  if (!content || typeof content !== 'string') return content;

  const lines = content.split('\n');
  const result: string[] = [];
  let i = 0;

  while (i < lines.length) {
    const line = lines[i];
    const trimmed = line.trim();

    // Detect a potential table header: must have at least 2 pipes and look like "| ... | ... |"
    if (trimmed.startsWith('|') && trimmed.endsWith('|') && (trimmed.match(/\|/g) || []).length >= 3) {
      const headerCols = trimmed.split('|').slice(1, -1); // cells between outer pipes
      const colCount = headerCols.length;

      // Check if next line is a separator (pipes + dashes/colons/spaces only)
      if (i + 1 < lines.length && /^\|[\s\-:|]+\|$/.test(lines[i + 1].trim())) {
        const sepCols = lines[i + 1].trim().split('|').slice(1, -1).length;

        // If separator column count doesn't match header, rebuild it
        if (sepCols !== colCount) {
          result.push(line);
          result.push('|' + Array(colCount).fill('---').join('|') + '|');
          i += 2;
        } else {
          // Separator is fine, push both
          result.push(line);
          result.push(lines[i + 1]);
          i += 2;
        }

        // Now process data rows: pad or trim cells to match colCount
        while (i < lines.length) {
          const row = lines[i].trim();
          if (!row.startsWith('|')) break; // end of table

          const cells = row.split('|');
          // Split gives ['', cell1, cell2, ..., ''] for "|a|b|"
          // Handle malformed rows that don't end with |
          const hasTrailingPipe = row.endsWith('|');
          const inner = hasTrailingPipe ? cells.slice(1, -1) : cells.slice(1);

          if (inner.length === colCount) {
            result.push(lines[i]); // row is fine
          } else if (inner.length < colCount) {
            // Pad with empty cells
            const padded = [...inner, ...Array(colCount - inner.length).fill(' ')];
            result.push('|' + padded.join('|') + '|');
          } else {
            // Too many cells -- trim to colCount
            result.push('|' + inner.slice(0, colCount).join('|') + '|');
          }
          i++;
        }
        continue;
      }
    }

    result.push(line);
    i++;
  }

  return result.join('\n');
}

/**
 * Strip YAML front matter (--- delimited block at start of content).
 * Without this, `---` renders as <hr> and YAML fields as plain text.
 */
function stripFrontMatter(content: string): string {
  if (!content || typeof content !== 'string') return content;
  if (!content.startsWith('---\n') && !content.startsWith('---\r\n')) return content;
  const end = content.indexOf('\n---', 3);
  if (end === -1) return content;
  // Skip past closing "---" and optional newline
  const rest = content.slice(end + 4);
  return rest.startsWith('\n') ? rest.slice(1) : rest.startsWith('\r\n') ? rest.slice(2) : rest;
}

/**
 * Escape dollar signs used as currency (e.g. $129.82) so remark-math
 * does not treat them as inline-math delimiters.
 * Matches a lone $ followed by an optional sign and a digit.
 * Leaves $$ (display math) untouched via negative lookbehind/lookahead.
 */
function escapeCurrencyDollars(content: string): string {
  if (!content || typeof content !== 'string') return content;
  return content.replace(/(?<!\$)\$(?!\$)(?=[-+]?\d)/g, '\\$');
}

/**
 * Normalize LaTeX delimiters for remark-math compatibility.
 *
 * LLMs often emit \[...\] (display) and \(...\) (inline) notation,
 * but remark-math only recognizes $$...$$ and $...$ delimiters.
 */
function normalizeLatexDelimiters(content: string): string {
  if (!content || typeof content !== 'string') return content;

  // Convert display math: \[...\] -> $$...$$
  // Match \[ ... \] allowing newlines in between
  content = content.replace(/\\\[([\s\S]*?)\\\]/g, (_, math) => `$$${math}$$`);

  // Convert inline math: \(...\) -> $...$
  content = content.replace(/\\\((.*?)\\\)/g, (_, math) => `$${math}$`);

  return content;
}

/**
 * Convert inline citation patterns ([label](url)) into <cite-bubble> HTML tags.
 * rehype-raw will parse these into the AST and the CitationBubble component renders them.
 */
function escapeHtmlAttr(s: string): string {
  return s.replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;');
}

function transformCitationBubbles(content: string): string {
  if (!content || typeof content !== 'string') return content;
  return content.replace(
    /\(\[([^\]]+)\]\((https?:\/\/[^)]+)\)\)/g,
    (_, label, url) => {
      // Encode $ as %24 so escapeCurrencyDollars won't mangle URLs (e.g. ?price=$100)
      const safeUrl = url.replace(/\$/g, '%24');
      return `<cite-bubble label="${escapeHtmlAttr(label)}" href="${escapeHtmlAttr(safeUrl)}"></cite-bubble>`;
    }
  );
}

type MarkdownVariant = 'chat' | 'panel' | 'compact';

interface MarkdownProps {
  content: string;
  variant?: MarkdownVariant;
  className?: string;
  style?: React.CSSProperties;
  /** Called when a file link is clicked. workspaceId is set for cross-workspace (ws://) references. */
  onOpenFile?: (path: string, workspaceId?: string) => void;
  /** Force code blocks to use a specific syntax theme regardless of app theme */
  codeTheme?: 'light' | 'dark';
}

function Markdown({ content, variant = 'panel', className = '', style, onOpenFile, codeTheme }: MarkdownProps): React.ReactElement {
  const config = VARIANTS[variant];
  const processed = useMemo(
    () => normalizeLatexDelimiters(escapeCurrencyDollars(transformCitationBubbles(fixMarkdownTables(normalizeFileRefs(stripFrontMatter(content)))))),
    [content]
  );

  const lineKey = useMemo(() => (processed.match(/\n/g) || []).length, [processed]);

  const components = useMemo(() => {
    let result = config.components;

    // Override pre to pass codeTheme to CodeBlock when specified
    if (codeTheme) {
      const themedPre = ({ node: _node, children, ..._props }: MarkdownComponentProps) => {
        const { language, code } = extractCodeFromPre(children);
        return <CodeBlock language={language} code={code} compact={variant === 'compact'} codeTheme={codeTheme} />;
      };
      result = { ...result, pre: themedPre };
    }

    if (!onOpenFile && variant !== 'chat') return result;

    // Fallback: detect __wsref__ links inside inline code spans that survived
    // normalizeFileRefs' backtick unwrapping (e.g., nested backticks, extra whitespace).
    // The content-level normalization handles the common case; this catches edge cases.
    if (onOpenFile) {
      const BaseCode = result.code;
      const wsrefLinkRe = /^!?\[([^\]]*)\]\((__wsref__\/[^)]+)\)$/;
      const fileAwareCode = (props: MarkdownComponentProps) => {
        const { className, children } = props;
        const isBlock = /language-/.test(className || '');
        if (!isBlock) {
          const text = String(children ?? '');
          const match = wsrefLinkRe.exec(text);
          if (match) {
            const [full, linkText, href] = match;
            if (full.startsWith('!') && isImagePath(href)) {
              return <WorkspaceImage src={href} alt={linkText} />;
            }
            const wsRef = parseWsPath(href);
            return (
              <a
                className="underline hover:opacity-80 transition-opacity cursor-pointer"
                style={{ color: 'var(--color-accent-primary)' }}
                onClick={(e: React.MouseEvent) => { e.preventDefault(); onOpenFile(normalizeFilePath(href), wsRef?.workspaceId); }}
              >{linkText}</a>
            );
          }
        }
        return <BaseCode {...props} />;
      };
      result = { ...result, code: fileAwareCode };
    }

    const fileAwareA = ({ node: _node, href, children, ...props }: MarkdownComponentProps) => {
      if (isFilePath(href)) {
        // Image file linked as [name](path.png) -- render as embedded image
        // For ws:// paths, pass the full href so WorkspaceImage can extract the workspace
        if (isImagePath(href)) {
          const wsRef = parseWsPath(href);
          return <WorkspaceImage src={wsRef ? href : normalizeFilePath(href)} alt={typeof children === 'string' ? children : ''} />;
        }
        if (onOpenFile) {
          const wsRef = parseWsPath(href);
          return (
            <a
              className="underline hover:opacity-80 transition-opacity cursor-pointer"
              style={{ color: 'var(--color-accent-primary)' }}
              onClick={(e: React.MouseEvent) => { e.preventDefault(); onOpenFile(normalizeFilePath(href), wsRef?.workspaceId); }}
              {...props}
            >{children}</a>
          );
        }
        // No onOpenFile handler -- render as non-clickable text
        return <span {...props}>{children}</span>;
      }
      // External URL -- default behavior
      const DefaultA = result.a;
      return <DefaultA node={_node} href={href} {...props}>{children}</DefaultA>;
    };
    return { ...result, a: fileAwareA };
  }, [onOpenFile, variant, config.components, codeTheme]);

  return (
    <div
      className={`${config.className} ${className}`.trim()}
      style={{ ...config.style, ...style }}
    >
      <ReactMarkdown key={lineKey} remarkPlugins={[[remarkGfm, { singleTilde: false }], remarkCjkFriendly, remarkMath]} rehypePlugins={[[rehypeKatex, { strict: false }], rehypeRaw, [rehypeSanitize, sanitizeSchema]]} components={components}>
        {processed}
      </ReactMarkdown>
    </div>
  );
}

export { transformCitationBubbles, escapeHtmlAttr };
export default Markdown;
