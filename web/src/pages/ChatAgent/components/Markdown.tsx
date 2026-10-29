import React, { createContext, useContext, useEffect, useState, useMemo, useRef } from 'react';
import { useTranslation } from 'react-i18next';
import ReactMarkdown from 'react-markdown';
import { useInRouterContext } from 'react-router';
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
import { GuardedLink } from '@/components/GuardedLink';
import { appRoutePath } from '@/lib/appRoutes';
import { copyText } from '@/lib/clipboard';
import WorkspaceImage from './WorkspaceImage';
import { isFilePath, isImagePath, normalizeFilePath, parseFolderHref, parseWsPath } from '../utils/filePaths';
import { useWorkspaceFolders } from '../contexts/WorkspaceContext';
import { useShareLinkOpener } from '../contexts/ShareLinkOpenerContext';
import { onOtherComputer, parseAgentHref, parseAgentPath } from '../utils/agentPaths';
import { normalizeFileRefs } from '../utils/normalizeFileRefs';
import { splitFileLocation, type OpenFileHandler } from '../utils/fileLocation';
import { mapOutsideCode, mapOutsideMultilineCode } from '../utils/markdownSegments';
import { splitMarkdownBlocks, scanStreamingBlock } from '../utils/markdownBlocks';
import { repairStreamingTail } from '../utils/streamingTail';
import { blockFreshKeys, rehypeFreshText, useRevealFade } from '../utils/revealFade';
import CitationBubble from './CitationBubble';
import { InPlaceSwap } from '@/components/ui/crossfade';

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
  /** The fence is still streaming in. */
  live?: boolean;
}

const CODE_STYLE: React.CSSProperties = { margin: 0, padding: '1rem', backgroundColor: 'transparent', fontSize: '0.875rem', lineHeight: '1.5' };
const COMPACT_CODE_STYLE: React.CSSProperties = { ...CODE_STYLE, padding: '0.6rem', fontSize: '0.75rem' };

/** How long a code block's Copy reads Copied before it turns back. */
const CODE_COPIED_MS = 1500;

// The highlighted line being typed (null between lines), handed past the
// memoized highlighter of the lines above it into the same <code>. No value
// while the fence is settled.
const TypingLine = createContext<{ line: React.ReactNode } | null>(null);

const DONE_LINES_STYLE: React.CSSProperties = { display: 'block' };

function LiveCode({ children, ...props }: React.ComponentProps<'code'>): React.ReactElement {
  const typing = useContext(TypingLine);
  if (!typing) return <code {...props}>{children}</code>;
  // The finished lines are a block of their own, so a keystroke lays out and
  // shapes the typing line alone rather than every line above it. Their
  // trailing newline draws no extra line, so the height is the one a single
  // highlighter gives at every prefix; only a line too long to wrap scrolls
  // 1rem less far until the fence settles. Each part in its own element also
  // keeps a finished line an append: landing in front of the typing line, it
  // restyled the whole <code>, since the sibling-space rules (`.x > * ~ *`)
  // have a universal right side.
  return <code {...props}><span style={DONE_LINES_STYLE}>{children}</span><span>{typing.line}</span></code>;
}

function Bare({ children }: { children?: React.ReactNode }): React.ReactElement {
  return <>{children}</>;
}

// --- CodeBlock component ---
// Memoized on its primitive props: prism re-tokenizes and rebuilds an element
// per token on every render, which was the single largest cost of a streaming
// reply once it contained a fence. The parent re-renders on every typewriter
// tick; the code itself only changes while its own fence is still open.
//
// While it is open only its last line changes, so a live fence highlights the
// lines above it once per newline and the line being typed on its own, into
// the same <code>: the text is the one a single highlighter draws. A line
// alone cannot see what an earlier line opened (a string, a comment), so it
// can color differently until it is done, and the fence settles to one
// highlighter over the whole code once it closes.
const CodeBlock = React.memo(function CodeBlock({ language, code, compact = false, codeTheme, live = false }: CodeBlockProps): React.ReactElement {
  const { t } = useTranslation();
  const { theme } = useTheme();
  const effectiveTheme = codeTheme ?? theme;
  const [copied, setCopied] = useState(false);
  const copiedTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => () => {
    if (copiedTimer.current) clearTimeout(copiedTimer.current);
  }, []);
  const lang = language || 'text';
  const prismStyle = effectiveTheme === 'light' ? oneLight : oneDark;
  const cut = live ? code.lastIndexOf('\n') + 1 : code.length;
  const done = code.slice(0, cut);
  const typing = code.slice(cut);
  // Memoized by hand: this is the whole point of the split, so it must not
  // depend on the compiler (REACT_COMPILER=off) to hold.
  const highlighted = useMemo(() => (
    <SyntaxHighlighter
      language={lang}
      style={prismStyle}
      customStyle={compact ? COMPACT_CODE_STYLE : CODE_STYLE}
      codeTagProps={{ style: { backgroundColor: 'transparent' } }}
      CodeTag={LiveCode}
      wrapLongLines
    >
      {done}
    </SyntaxHighlighter>
  ), [lang, prismStyle, compact, done]);
  const typingLine = typing
    ? <SyntaxHighlighter language={lang} style={prismStyle} PreTag={Bare} CodeTag={Bare} wrapLongLines>{typing}</SyntaxHighlighter>
    : null;

  // Confirmed once the write lands (it rejects when the document has lost
  // focus); a second copy restarts the one timer rather than racing it.
  const handleCopy = (): void => {
    copyText(code).then(() => {
      setCopied(true);
      if (copiedTimer.current) clearTimeout(copiedTimer.current);
      copiedTimer.current = setTimeout(() => setCopied(false), CODE_COPIED_MS);
    }).catch(() => {});
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
            {/* Both labels hold the one cell, so the button is as wide as the
                wider of them in every language and the header never shifts. */}
            <button onClick={handleCopy}
              className="press text-xs text-(--color-text-tertiary) hover:text-(--color-text-secondary)"
              style={{ background: 'none', border: 'none', cursor: 'pointer' }}>
              <InPlaceSwap
                swapped={copied}
                className="justify-items-end"
                from={<><Copy className="h-3 w-3" /> {t('common.copy')}</>}
                to={<><Check className="h-3 w-3" /> {t('common.copied')}</>}
              />
            </button>
          </div>
        )}
        <TypingLine.Provider value={live ? { line: typingLine } : null}>{highlighted}</TypingLine.Provider>
      </div>
    </div>
  );
});

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

// The text of rendered children, for an attribute. A link label is split into
// fade spans while it is fresh (utils/revealFade), and can hold emphasis.
function textOf(node: React.ReactNode): string {
  if (typeof node === 'string' || typeof node === 'number') return String(node);
  if (Array.isArray(node)) return node.map(textOf).join('');
  if (React.isValidElement<{ children?: React.ReactNode }>(node)) return textOf(node.props.children);
  return '';
}

// --- Helper to extract code info from a <pre> element ---
function extractCodeFromPre(children: React.ReactNode): { language: string | null; code: string } {
  const codeEl = (children as any)?.props ? (children as any) : null;
  const className = (codeEl?.props?.className || '') as string;
  const match = /language-(\w+)/.exec(className);
  // An empty fence's <code> has no children; falling back to `children` there
  // would stringify the element itself.
  const raw = String((codeEl ? codeEl.props.children : children) ?? '').replace(/\n$/, '');
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
// Where the fence still open at the end of a streaming block starts, from
// MarkdownBlock; -1 when there is none. Nothing follows an open fence in its
// block, so the <pre> starting at or after that line is the fence.
const LiveFence = createContext(-1);

function useLiveFence(node: MarkdownComponentProps['node']): boolean {
  const from = useContext(LiveFence);
  return from >= 0 && (node?.position?.start?.offset ?? -1) >= from;
}
function Pre({ node, children }: MarkdownComponentProps): React.ReactElement {
  const { language, code } = extractCodeFromPre(children);
  return <CodeBlock language={language} code={code} live={useLiveFence(node)} />;
}
function CompactPre({ node, children }: MarkdownComponentProps): React.ReactElement {
  const { language, code } = extractCodeFromPre(children);
  return <CodeBlock language={language} code={code} compact live={useLiveFence(node)} />;
}
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
  <li className="wrap-break-word" style={{ color: 'var(--color-text-primary)' }} {...props} />
);

// ===================== CHAT variant =====================
const chatUl = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <ul className="list-disc ml-6 my-2" style={{ color: 'var(--color-text-primary)' }} {...props} />
);
const chatOl = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <ol className="list-decimal ml-6 my-2" style={{ color: 'var(--color-text-primary)' }} {...props} />
);
// A loose list wraps each item's text in a paragraph and a tight one does not,
// so a list turning loose as its second item streams in would pad every item.
// An item of one paragraph sits the same either way; one of several keeps the
// paragraph spacing between them.
const chatLi = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <li className="ps-[2px] wrap-break-word [&>p:only-of-type]:py-0" style={{ color: 'var(--color-text-primary)' }} {...props} />
);
const chatP = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <p className="my-px py-[3px] whitespace-pre-wrap wrap-break-word first:mt-0 last:mb-0" style={{ color: 'var(--color-text-primary)' }} {...props} />
);
const chatH1 = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <h1 className="mt-[1.5em] mb-[0.5em] first:mt-0" style={{ color: 'var(--color-text-primary)', fontSize: '1.75em', fontWeight: 700, lineHeight: '1.3' }} {...props} />
);
const chatH2 = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <h2 className="mt-[1.4em] mb-[0.4em] first:mt-0" style={{ color: 'var(--color-text-primary)', fontSize: '1.4em', fontWeight: 700, lineHeight: '1.3' }} {...props} />
);
const chatH3 = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <h3 className="mt-[1.2em] mb-[0.3em] first:mt-0" style={{ color: 'var(--color-text-primary)', fontSize: '1.2em', fontWeight: 600, lineHeight: '1.3' }} {...props} />
);
const chatH4 = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <h4 className="mt-[1em] mb-[0.25em] first:mt-0" style={{ color: 'var(--color-text-primary)', fontSize: '1.05em', fontWeight: 600, lineHeight: '1.4' }} {...props} />
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
const chatBlockquote = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <blockquote
    className="border-l-2 pl-4 my-2 italic"
    style={{ borderColor: 'var(--color-border-elevated)', color: 'var(--color-text-primary)', opacity: 0.8 }}
    {...props}
  />
);
// A page of this app opens in place, through the router and past the host's
// leave guard; anything else opens in a new tab so the conversation survives
// the detour, unless the chat around it takes the click (an app or file link
// it opens as a panel tab). A modified click is always the new tab.
function MarkdownLink({ href, ...props }: MarkdownComponentProps) {
  const route = useInRouterContext() ? appRoutePath(href) : null;
  const openShareLink = useShareLinkOpener();
  if (route !== null) return <GuardedLink to={route} {...props} />;
  const onClick = openShareLink && href
    ? (e: React.MouseEvent) => {
        if (e.metaKey || e.ctrlKey || e.shiftKey || e.altKey || e.button !== 0) return;
        if (openShareLink(href)) e.preventDefault();
      }
    : undefined;
  return <a href={href} target="_blank" rel="noopener noreferrer" onClick={onClick} {...props} />;
}
const chatA = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <MarkdownLink className="underline hover:opacity-80 transition-opacity [overflow-wrap:anywhere] [word-break:normal]" style={{ color: 'var(--color-accent-primary)' }} {...props} />
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
    className="align-top not-first:border-l"
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
    className="align-top not-first:border-l"
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
  <p className="my-1 whitespace-pre-wrap wrap-break-word" style={{ color: 'var(--color-text-primary)' }} {...props} />
);
const panelH1 = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <h1 className="mt-[1.2em] mb-[0.4em] first:mt-0" style={{ color: 'var(--color-text-primary)', fontSize: '1.5em', fontWeight: 700, lineHeight: '1.3' }} {...props} />
);
const panelH2 = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <h2 className="mt-[1.1em] mb-[0.35em] first:mt-0" style={{ color: 'var(--color-text-primary)', fontSize: '1.25em', fontWeight: 700, lineHeight: '1.3' }} {...props} />
);
const panelH3 = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <h3 className="mt-[1em] mb-[0.3em] first:mt-0" style={{ color: 'var(--color-text-primary)', fontSize: '1.1em', fontWeight: 600, lineHeight: '1.3' }} {...props} />
);
const panelH4 = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <h4 className="mt-[0.8em] mb-[0.2em] first:mt-0" style={{ color: 'var(--color-text-primary)', fontSize: '1em', fontWeight: 600, lineHeight: '1.4' }} {...props} />
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
const panelA = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <MarkdownLink className="underline" style={{ color: 'var(--color-accent-primary)' }} {...props} />
);
const panelBlockquote = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <blockquote
    className="pl-3 my-2"
    style={{ borderLeft: '2px solid var(--color-border-elevated)', color: 'var(--color-text-primary)' }}
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
  <td className="px-3 py-2 wrap-break-word align-top" style={{ color: 'var(--color-text-primary)' }} {...props} />
);

// ===================== COMPACT variant =====================
const compactP = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <p className="my-px py-[3px] whitespace-pre-wrap wrap-break-word first:mt-0 last:mb-0" style={{ color: 'var(--color-text-primary)' }} {...props} />
);
const compactH1 = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <h1 className="mt-[0.8em] mb-[0.2em] first:mt-0" style={{ color: 'var(--color-text-primary)', fontSize: '1.25em', fontWeight: 700, lineHeight: '1.3' }} {...props} />
);
const compactH2 = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <h2 className="mt-[0.7em] mb-[0.15em] first:mt-0" style={{ color: 'var(--color-text-primary)', fontSize: '1.15em', fontWeight: 700, lineHeight: '1.3' }} {...props} />
);
const compactH3 = ({ node: _node, ...props }: MarkdownComponentProps) => (
  <h3 className="mt-[0.6em] mb-[0.1em] first:mt-0" style={{ color: 'var(--color-text-primary)', fontSize: '1.05em', fontWeight: 600, lineHeight: '1.3' }} {...props} />
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

// ===================== Variant component maps =====================
const CHAT_COMPONENTS = {
  strong, em, del, input, img,
  ul: chatUl, ol: chatOl, li: chatLi,
  p: chatP, h1: chatH1, h2: chatH2, h3: chatH3, h4: chatH4,
  code: chatCode, pre: Pre,
  blockquote: chatBlockquote, a: chatA, hr: chatHr,
  table: chatTable, thead: chatThead, tbody: chatTbody, tr: chatTr, th: chatTh, td: chatTd,
  'cite-bubble': CitationBubble,
};

const PANEL_COMPONENTS = {
  strong, em, del, input, img, ul, ol, li,
  p: panelP, h1: panelH1, h2: panelH2, h3: panelH3, h4: panelH4,
  code: panelCode, pre: Pre,
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
  <td className="px-2 py-1.5 wrap-break-word align-top" style={{ color: 'var(--color-text-primary)' }} {...props} />
);

const COMPACT_COMPONENTS = {
  strong, em, del, ul, ol, li,
  p: compactP, h1: compactH1, h2: compactH2, h3: compactH3,
  code: compactCode, pre: CompactPre,
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
    className: 'leading-normal wrap-break-word max-w-none overflow-hidden',
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

// A cashtag: `$AAPL`, `$BRK.B`. Followed by a dollar, `^`, `_`, `{`, `(` or a
// backslash, it is the start of a formula (`$X$`, `$P(A)$`, `$X_t$`) instead.
const TICKER = String.raw`[A-Z]{1,5}(?:\.[A-Z]{1,2})?(?![\p{L}\p{N}$^_{(\\])`;
const TICKER_DOLLAR_RE = new RegExp(String.raw`(?<![\\$])\$(?=${TICKER})`, 'gu');
const OPENS_TICKER_RE = new RegExp(String.raw`^\$${TICKER}`, 'u');

/**
 * Escape dollar signs used as currency ($129.82, $/bbl, in millions ($)) or in
 * front of a ticker ($AAPL, $BRK.B) so remark-math does not treat them as
 * inline-math delimiters. remark-math pairs any two dollars, so `$AAPL and
 * $MSFT` would set "AAPL and " as a formula. $$ (display math) is left alone.
 *
 * Capitals after a dollar also start formulas (`$N = 5$`), so a ticker-shaped
 * one stays math when the next dollar in its paragraph closes it: touching
 * the text before it, not just after an opening bracket or quote (the dollar
 * in `($x$)` opens a formula of its own), and not itself opening a ticker.
 */
function escapeCurrencyDollars(content: string): string {
  if (!content || typeof content !== 'string') return content;
  // A dollar standing for the unit (`85 $/bbl`, `($)`) opens nothing either;
  // after a word it closes a formula, as in `($x$)`.
  const currency = content
    .replace(/(?<!\$)\$(?!\$)(?=[-+]?\d)/g, '\\$')
    .replace(/(?<=^|[\s(])\$(?=[/)])/gm, '\\$');
  return currency.replace(TICKER_DOLLAR_RE, (dollar, at: number) => (closesInlineMath(currency, at + 1) ? dollar : '\\$'));
}

// The next dollar is found first and only the text up to it searched for a
// blank line, so a list with a cashtag on every row stays one pass.
function closesInlineMath(content: string, from: number): boolean {
  for (let i = content.indexOf('$', from); i >= 0; i = content.indexOf('$', i + 1)) {
    if (content[i - 1] === '\\') continue;
    if (content[i + 1] === '$') { i++; continue; }
    if (/\n[ \t]*\n/.test(content.slice(from, i))) return false;
    return !/[\s([{"“]/.test(content[i - 1]) && !OPENS_TICKER_RE.test(content.slice(i, i + 12));
  }
  return false;
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

// A whole line holding one `$$...$$` equation. The body has no unescaped `$`,
// so two equations on one line, or `$$` closing mid-line, never match.
const ONE_LINE_DISPLAY_MATH_RE = /^( {0,3})\$\$((?:[^$\\\n]|\\.)+)\$\$[ \t]*$/gm;

/**
 * Give display math written on a single line a block of its own.
 *
 * remark-math only opens a display block on a `$$` line by itself, so
 * `$$x$$` (and `\[x\]`, which normalizeLatexDelimiters has already turned into
 * it) parses as inline math even when it is the whole line. The equation moves
 * onto its own line between fences; `$$x$$` inside a sentence stays inline,
 * and an equation still streaming in has no closing `$$` to match yet.
 */
function blockOneLineDisplayMath(content: string): string {
  if (!content || typeof content !== 'string') return content;
  return content.replace(ONE_LINE_DISPLAY_MATH_RE, (line, indent: string, body: string) =>
    body.trim() ? `${indent}$$\n${indent}${body.trim()}\n${indent}$$` : line,
  );
}

/**
 * Drop the row break LLMs leave after the last row of a numbered environment.
 *
 * LaTeX, and KaTeX since 0.18, read that `\\` as opening one more row and
 * number it, so the block grows an empty line tagged with the next equation
 * number. The unnumbered forms (`aligned`, `align*`, matrices) draw nothing
 * for it, so only the three numbered ones are touched.
 */
function dropTrailingRowBreaks(content: string): string {
  if (!content || typeof content !== 'string') return content;
  return content.replace(/\\\\\s*(?=\\end\{(?:align|gather|alignat)\})/g, '');
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

const REMARK_PLUGINS: React.ComponentProps<typeof ReactMarkdown>['remarkPlugins'] = [
  [remarkGfm, { singleTilde: false }], remarkCjkFriendly, remarkMath,
];
type RehypePlugins = NonNullable<React.ComponentProps<typeof ReactMarkdown>['rehypePlugins']>;
const REHYPE_PLUGINS: RehypePlugins = [
  [rehypeKatex, { strict: false }], rehypeRaw, [rehypeSanitize, sanitizeSchema],
];

// The fade pass runs after the sanitizer: it only wraps text the sanitizer
// already passed, in spans carrying a mark id.
const withFreshText = (fresh: string): RehypePlugins =>
  fresh ? [...REHYPE_PLUGINS, [rehypeFreshText, fresh] as RehypePlugins[number]] : REHYPE_PLUGINS;

interface MarkdownBlockProps {
  source: string;
  components: React.ComponentProps<typeof ReactMarkdown>['components'];
  /** The block text is still arriving. */
  live: boolean;
  /** The reveals still fading in over this block (`blockFreshKeys`), '' for none. */
  fresh: string;
}

// One parsed block. Memoized on its source and its fresh key, so a streaming
// reply only re-parses the block still receiving text, and the one before it
// once more as its last reveals settle; the blocks above keep their DOM, and a
// fence in them is highlighted once. The tree is keyed on the block's line
// count so the block being streamed remounts on each newline, which clears a
// stale inline-emphasis node React otherwise leaves behind mid-stream.
//
// A fence needs no such clearing, so lines added to a fence still open at the
// block's end do not count (`scanStreamingBlock`): a long fence would otherwise
// rebuild every highlighted token on each new line. Closing the fence counts
// its lines at once, one remount per fence. While the block is live, that
// fence highlights only its typing line per tick (CodeBlock).
const MarkdownBlock = React.memo(function MarkdownBlock({ source, components, live, fresh }: MarkdownBlockProps) {
  const { lineKey, openFence } = useMemo(() => scanStreamingBlock(source), [source]);
  const rehypePlugins = useMemo(() => withFreshText(fresh), [fresh]);
  return (
    <LiveFence.Provider value={live ? openFence : -1}>
      <ReactMarkdown key={lineKey} remarkPlugins={REMARK_PLUGINS} rehypePlugins={rehypePlugins} components={components}>
        {source}
      </ReactMarkdown>
    </LiveFence.Provider>
  );
});

interface MarkdownProps {
  content: string;
  variant?: MarkdownVariant;
  className?: string;
  style?: React.CSSProperties;
  /** Called when a file link is clicked. workspaceId is set for cross-workspace (ws://) references. */
  onOpenFile?: OpenFileHandler;
  /** Called with the fragment of a same-document link (`[Valuation](#valuation)`). */
  onAnchorLink?: (fragment: string) => void;
  /** Force code blocks to use a specific syntax theme regardless of app theme */
  codeTheme?: 'light' | 'dark';
  /** The content is still arriving, so a fence open at its end is still being written. */
  streaming?: boolean;
}

function Markdown({ content, variant = 'panel', className = '', style, onOpenFile, onAnchorLink, codeTheme, streaming = false }: MarkdownProps): React.ReactElement {
  const config = VARIANTS[variant];
  const folders = useWorkspaceFolders();
  const rootDir = folders?.rootDir;
  const otherRoots = folders?.otherRoots;
  // Every pass below rewrites prose before the markdown parser sees it, so each
  // one has to say how much of the string it may touch. Inside code, markdown
  // stops interpreting escapes and raw HTML — a rewrite that lands there is
  // visible corruption and makes the Copy button return unparseable text.
  const processed = useMemo(() => {
    // Whole-string by design: front matter is anchored at the start, and
    // normalizeFileRefs deliberately unwraps backticks around file refs.
    const base = normalizeFileRefs(stripFrontMatter(content), rootDir, otherRoots);
    // Table repair is line-structural, so it reads whole lines and has to stay
    // out of the code spans that own one — fenced, or inline across a break.
    const tables = mapOutsideMultilineCode(base, fixMarkdownTables);
    // These inject characters and tags, which is corruption inside any code.
    const prose = mapOutsideCode(tables, (text) =>
      dropTrailingRowBreaks(normalizeLatexDelimiters(escapeCurrencyDollars(transformCitationBubbles(text))))
    );
    // Line-structural again, and after the delimiter pass so `\[x\]` arrives
    // already spelled `$$x$$`.
    return mapOutsideMultilineCode(prose, blockOneLineDisplayMath);
  }, [content, rootDir, otherRoots]);

  const split = useMemo(() => splitMarkdownBlocks(processed), [processed]);
  // The block still being written renders with its tail held to the shape it
  // will settle in (utils/streamingTail): a lone `-` under a line is not a
  // heading, a link being typed is not a run of brackets.
  const tail = useMemo(() => (streaming ? repairStreamingTail(split[split.length - 1]) : null), [split, streaming]);
  const blocks = useMemo(() => (tail ? [...split.slice(0, -1), tail.text] : split), [split, tail]);
  // The repair keeps every character it shows at its offset in the source, so
  // what it holds back is the end of the source, and fades in when it shows.
  const shown = tail ? processed.length - split[split.length - 1].length + tail.shown : processed.length;

  // What each typewriter tick added fades in (utils/revealFade). Tracked in
  // the processed source, the text the parser positions its nodes in.
  const rootRef = useRef<HTMLDivElement>(null);
  const marks = useRevealFade(rootRef, processed.slice(0, shown), streaming);
  const freshKeys = useMemo(() => blockFreshKeys(blocks, marks), [blocks, marks]);

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

    if (!onOpenFile && !onAnchorLink && variant !== 'chat') return result;

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
            const { path, location } = splitFileLocation(href);
            return (
              <a
                className="underline hover:opacity-80 transition-opacity cursor-pointer"
                style={{ color: 'var(--color-accent-primary)' }}
                onClick={(e: React.MouseEvent) => { e.preventDefault(); onOpenFile(normalizeFilePath(path), wsRef?.workspaceId, location ?? undefined, { rooted: parseAgentPath(path).absolute || !!wsRef }); }}
              >{linkText}</a>
            );
          }
        }
        return <BaseCode {...props} />;
      };
      result = { ...result, code: fileAwareCode };
    }

    const fileAwareA = ({ node: _node, href, children, ...props }: MarkdownComponentProps) => {
      if (onAnchorLink && href?.startsWith('#') && href.length > 1) {
        return (
          <a
            className="underline hover:opacity-80 transition-opacity cursor-pointer"
            style={{ color: 'var(--color-accent-primary)' }}
            href={href}
            onClick={(e: React.MouseEvent) => { e.preventDefault(); onAnchorLink(href.slice(1)); }}
            {...props}
          >{children}</a>
        );
      }
      if (isFilePath(href, folders)) {
        // Image file linked as [name](path.png) -- render as embedded image.
        // The destination goes over raw: WorkspaceImage reads it the same way
        // this link does, and normalizing here would decode it twice.
        if (isImagePath(href)) {
          return <WorkspaceImage src={href} alt={textOf(children)} />;
        }
        if (onOpenFile) {
          const wsRef = parseWsPath(href);
          const { path, location } = splitFileLocation(href!);
          // Another workspace's folder names that workspace as a `__wsref__`
          // does, so the link opens there with the path inside it.
          const other = parseFolderHref(path, folders);
          // A rooted path through this workspace's own folder is a path in it,
          // and stripping the root alone would leave the folder as a
          // subdirectory. A relative one keeps its climb, which the handler
          // reads against the file it was written in.
          const rooted = parseAgentPath(path, folders?.rootDir).absolute;
          const opened = other?.path ?? normalizeFilePath(path, rooted ? folders : null);
          // The rest of another computer is on no machine this view reads, so
          // the link opens nothing rather than a namesake here.
          if (!other && !wsRef && onOtherComputer(parseAgentHref(path, folders?.rootDir), folders)) return <span {...props}>{children}</span>;
          return (
            <a
              className="underline hover:opacity-80 transition-opacity cursor-pointer"
              style={{ color: 'var(--color-accent-primary)' }}
              onClick={(e: React.MouseEvent) => { e.preventDefault(); onOpenFile(opened, wsRef?.workspaceId ?? other?.workspaceId, location ?? undefined, { rooted: rooted || !!wsRef || !!other }); }}
              {...props}
            >{children}</a>
          );
        }
        // No onOpenFile handler -- render as non-clickable text
        return <span {...props}>{children}</span>;
      }
      // A web link, or a page of this app -- default behavior
      const DefaultA = result.a;
      return <DefaultA node={_node} href={href} {...props}>{children}</DefaultA>;
    };
    return { ...result, a: fileAwareA };
  }, [onOpenFile, onAnchorLink, variant, config.components, codeTheme, folders]);

  // Rendered markdown is always long-form reading content — every call site
  // (transcript, detail panels, memos) gets the content face here.
  return (
    <div
      ref={rootRef}
      className={`font-content ${config.className} ${className}`.trim()}
      style={{ ...config.style, ...style }}
    >
      {blocks.map((block, i) => (
        <React.Fragment key={i}>
          {/* mdast-to-hast puts a newline text node between top-level siblings;
              keep the DOM identical to a whole-document render. */}
          {i > 0 && '\n'}
          <MarkdownBlock source={block} components={components} live={streaming && i === blocks.length - 1} fresh={freshKeys[i]} />
        </React.Fragment>
      ))}
    </div>
  );
}

export { transformCitationBubbles, escapeHtmlAttr };
// Shallow compare: a caller that varies `style` must hoist the object, or the
// memo never hits and every tick re-parses the document.
export default React.memo(Markdown);
