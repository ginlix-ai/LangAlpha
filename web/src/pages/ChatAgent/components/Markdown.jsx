import { useState } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import rehypeRaw from 'rehype-raw';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { oneDark } from 'react-syntax-highlighter/dist/esm/styles/prism';
import { Copy, Check } from 'lucide-react';

// --- CodeBlock component ---
function CodeBlock({ language, code, compact = false }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = () => {
    navigator.clipboard.writeText(code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div style={{ margin: compact ? '4px 0' : '6px 0' }}>
      <div className="rounded-lg overflow-hidden"
        style={{ backgroundColor: '#282c34', border: '1px solid rgba(255,255,255,0.1)' }}>
        {!compact && (
          <div className="flex items-center justify-between px-3 py-1.5"
            style={{ borderBottom: '1px solid rgba(255,255,255,0.08)' }}>
            <span className="text-xs font-mono" style={{ color: 'rgba(255,255,255,0.4)' }}>
              {language || 'text'}
            </span>
            <button onClick={handleCopy}
              className="flex items-center gap-1 text-xs hover:opacity-100 transition-opacity"
              style={{ color: 'rgba(255,255,255,0.4)', background: 'none', border: 'none', cursor: 'pointer' }}>
              {copied ? <><Check className="h-3 w-3" /> Copied</> : <><Copy className="h-3 w-3" /> Copy</>}
            </button>
          </div>
        )}
        <SyntaxHighlighter
          language={language || 'text'}
          style={oneDark}
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
function tryFormatJson(code) {
  const trimmed = code.trim();
  if (!(trimmed.startsWith('{') || trimmed.startsWith('['))) return null;
  try {
    return { formatted: JSON.stringify(JSON.parse(trimmed), null, 2), language: 'json' };
  } catch {
    return null;
  }
}

// --- Helper to extract code info from a <pre> element ---
function extractCodeFromPre(children) {
  const codeEl = children?.props ? children : null;
  const className = codeEl?.props?.className || '';
  const match = /language-(\w+)/.exec(className);
  const raw = String(codeEl?.props?.children ?? children ?? '').replace(/\n$/, '');
  const json = !match ? tryFormatJson(raw) : null;
  const language = match?.[1] || json?.language || null;
  const code = json?.formatted || raw;
  return { language, code };
}

// --- Shared overrides (used by all variants) ---
const strong = ({ node, ...props }) => (
  <strong style={{ color: '#FFFFFF', fontWeight: 700 }} {...props} />
);
const em = ({ node, ...props }) => (
  <em className="italic" style={{ color: '#FFFFFF' }} {...props} />
);
const del = ({ node, ...props }) => (
  <del style={{ color: 'rgba(255,255,255,0.5)', textDecoration: 'line-through' }} {...props} />
);
const input = ({ node, type, checked, ...props }) => {
  if (type === 'checkbox') {
    return (
      <input type="checkbox" checked={checked} readOnly
        style={{ marginRight: '6px', accentColor: '#6155F5' }} />
    );
  }
  return <input {...props} />;
};
const img = ({ node, ...props }) => (
  <img className="rounded-lg my-2" style={{ maxWidth: '100%', height: 'auto' }} {...props} />
);
const ul = ({ node, ...props }) => (
  <ul className="list-disc ml-4 my-1" style={{ color: '#FFFFFF' }} {...props} />
);
const ol = ({ node, ...props }) => (
  <ol className="list-decimal ml-4 my-1" style={{ color: '#FFFFFF' }} {...props} />
);
const li = ({ node, ...props }) => (
  <li className="break-words" style={{ color: '#FFFFFF' }} {...props} />
);

// ===================== CHAT variant =====================
const chatUl = ({ node, ...props }) => (
  <ul className="list-disc ml-6 my-2" style={{ color: '#FFFFFF' }} {...props} />
);
const chatOl = ({ node, ...props }) => (
  <ol className="list-decimal ml-6 my-2" style={{ color: '#FFFFFF' }} {...props} />
);
const chatLi = ({ node, ...props }) => (
  <li className="ps-[2px] break-words" style={{ color: '#FFFFFF' }} {...props} />
);
const chatP = ({ node, ...props }) => (
  <p className="my-[1px] py-[3px] whitespace-pre-wrap break-words first:mt-0 last:mb-0" style={{ color: '#FFFFFF' }} {...props} />
);
const chatH1 = ({ node, ...props }) => (
  <h1 className="first:mt-0" style={{ color: '#FFFFFF', fontSize: '1.75em', fontWeight: 700, lineHeight: '1.3', marginTop: '1.5em', marginBottom: '0.5em' }} {...props} />
);
const chatH2 = ({ node, ...props }) => (
  <h2 className="first:mt-0" style={{ color: '#FFFFFF', fontSize: '1.4em', fontWeight: 700, lineHeight: '1.3', marginTop: '1.4em', marginBottom: '0.4em' }} {...props} />
);
const chatH3 = ({ node, ...props }) => (
  <h3 className="first:mt-0" style={{ color: '#FFFFFF', fontSize: '1.2em', fontWeight: 600, lineHeight: '1.3', marginTop: '1.2em', marginBottom: '0.3em' }} {...props} />
);
const chatH4 = ({ node, ...props }) => (
  <h4 className="first:mt-0" style={{ color: '#FFFFFF', fontSize: '1.05em', fontWeight: 600, lineHeight: '1.4', marginTop: '1em', marginBottom: '0.25em' }} {...props} />
);
const chatCode = ({ node, className, children, ...props }) => {
  const isBlock = /language-/.test(className || '');
  if (!isBlock) {
    return (
      <code className="font-mono rounded px-1.5 py-0.5"
        style={{ backgroundColor: 'rgba(255,255,255,0.1)', color: '#abb2bf', fontSize: '0.85em' }}
        {...props}>
        {children}
      </code>
    );
  }
  return <code className={className} {...props}>{children}</code>;
};
const chatPre = ({ node, children, ...props }) => {
  const { language, code } = extractCodeFromPre(children);
  return <CodeBlock language={language} code={code} />;
};
const chatBlockquote = ({ node, ...props }) => (
  <blockquote
    className="border-l-4 pl-4 my-2 italic"
    style={{ borderColor: '#6155F5', color: '#FFFFFF', opacity: 0.8 }}
    {...props}
  />
);
const chatA = ({ node, ...props }) => (
  <a className="underline hover:opacity-80 transition-opacity" style={{ color: '#6155F5' }} target="_blank" rel="noopener noreferrer" {...props} />
);
const chatHr = ({ node, ...props }) => (
  <hr className="my-4 border-0" style={{ borderTop: '1px solid rgba(255, 255, 255, 0.15)' }} {...props} />
);
const chatTable = ({ node, ...props }) => (
  <div className="pt-[8px] pb-[18px]">
    <div className="overflow-x-auto inline-block border rounded-lg" style={{ borderColor: 'rgba(255, 255, 255, 0.1)', maxWidth: '100%' }}>
      <table className="m-0 table-auto border-collapse" {...props} />
    </div>
  </div>
);
const chatThead = ({ node, ...props }) => <thead {...props} />;
const chatTbody = ({ node, ...props }) => <tbody {...props} />;
const chatTr = ({ node, ...props }) => <tr {...props} />;
const chatTh = ({ node, ...props }) => (
  <th
    className="text-left align-top first:border-s-0 last:border-e-0"
    style={{
      backgroundColor: 'rgba(255, 255, 255, 0.05)',
      borderBottom: '1px solid rgba(255, 255, 255, 0.1)',
      borderLeft: '1px solid rgba(255, 255, 255, 0.1)',
      color: '#FFFFFF',
      fontSize: '0.875rem',
      fontWeight: 600,
      padding: '7px 9px',
    }}
    {...props}
  />
);
const chatTd = ({ node, ...props }) => (
  <td
    className="text-left first:border-s-0 last:border-e-0"
    style={{
      borderTop: '1px solid rgba(255, 255, 255, 0.1)',
      borderLeft: '1px solid rgba(255, 255, 255, 0.1)',
      color: '#FFFFFF',
      fontSize: '0.875rem',
      padding: '8px 14px',
    }}
    {...props}
  />
);

// ===================== PANEL variant =====================
const panelP = ({ node, ...props }) => (
  <p className="my-1 whitespace-pre-wrap break-words" style={{ color: '#FFFFFF' }} {...props} />
);
const panelH1 = ({ node, ...props }) => (
  <h1 className="first:mt-0" style={{ color: '#FFFFFF', fontSize: '1.5em', fontWeight: 700, lineHeight: '1.3', marginTop: '1.2em', marginBottom: '0.4em' }} {...props} />
);
const panelH2 = ({ node, ...props }) => (
  <h2 className="first:mt-0" style={{ color: '#FFFFFF', fontSize: '1.25em', fontWeight: 700, lineHeight: '1.3', marginTop: '1.1em', marginBottom: '0.35em' }} {...props} />
);
const panelH3 = ({ node, ...props }) => (
  <h3 className="first:mt-0" style={{ color: '#FFFFFF', fontSize: '1.1em', fontWeight: 600, lineHeight: '1.3', marginTop: '1em', marginBottom: '0.3em' }} {...props} />
);
const panelH4 = ({ node, ...props }) => (
  <h4 className="first:mt-0" style={{ color: '#FFFFFF', fontSize: '1em', fontWeight: 600, lineHeight: '1.4', marginTop: '0.8em', marginBottom: '0.2em' }} {...props} />
);
const panelCode = ({ node, className, children, ...props }) => {
  const isBlock = /language-/.test(className || '');
  if (!isBlock) {
    return (
      <code className="font-mono rounded px-1.5 py-0.5"
        style={{ backgroundColor: 'rgba(255,255,255,0.1)', color: '#abb2bf', fontSize: 'inherit' }}
        {...props}>
        {children}
      </code>
    );
  }
  return <code className={className} {...props}>{children}</code>;
};
const panelPre = ({ node, children, ...props }) => {
  const { language, code } = extractCodeFromPre(children);
  return <CodeBlock language={language} code={code} />;
};
const panelA = ({ node, ...props }) => (
  <a className="underline" style={{ color: '#6155F5' }} target="_blank" rel="noopener noreferrer" {...props} />
);
const panelBlockquote = ({ node, ...props }) => (
  <blockquote
    className="pl-3 my-2"
    style={{ borderLeft: '3px solid rgba(97, 85, 245, 0.5)', color: 'rgba(255,255,255,0.8)' }}
    {...props}
  />
);
const panelHr = ({ node, ...props }) => (
  <hr className="my-3 border-0" style={{ borderTop: '1px solid rgba(255, 255, 255, 0.1)' }} {...props} />
);
const panelTable = ({ node, ...props }) => (
  <div className="my-2 overflow-x-auto rounded" style={{ border: '1px solid rgba(255,255,255,0.2)' }}>
    <table className="w-full border-collapse text-left" style={{ minWidth: '100%' }} {...props} />
  </div>
);
const panelThead = ({ node, ...props }) => <thead style={{ backgroundColor: 'rgba(0,0,0,0.25)' }} {...props} />;
const panelTr = ({ node, ...props }) => <tr className="border-b border-white/10 last:border-b-0" {...props} />;
const panelTh = ({ node, ...props }) => (
  <th className="px-3 py-2 whitespace-nowrap" style={{ color: '#FFFFFF', fontWeight: 600, borderBottom: '1px solid rgba(255,255,255,0.2)' }} {...props} />
);
const panelTd = ({ node, ...props }) => (
  <td className="px-3 py-2 break-words align-top" style={{ color: '#FFFFFF' }} {...props} />
);

// ===================== COMPACT variant =====================
const compactP = ({ node, ...props }) => (
  <p className="my-[1px] py-[3px] whitespace-pre-wrap break-words first:mt-0 last:mb-0" style={{ color: '#FFFFFF' }} {...props} />
);
const compactH1 = ({ node, ...props }) => (
  <h1 className="first:mt-0" style={{ color: '#FFFFFF', fontSize: '1.25em', fontWeight: 700, lineHeight: '1.3', marginTop: '0.8em', marginBottom: '0.2em' }} {...props} />
);
const compactH2 = ({ node, ...props }) => (
  <h2 className="first:mt-0" style={{ color: '#FFFFFF', fontSize: '1.15em', fontWeight: 700, lineHeight: '1.3', marginTop: '0.7em', marginBottom: '0.15em' }} {...props} />
);
const compactH3 = ({ node, ...props }) => (
  <h3 className="first:mt-0" style={{ color: '#FFFFFF', fontSize: '1.05em', fontWeight: 600, lineHeight: '1.3', marginTop: '0.6em', marginBottom: '0.1em' }} {...props} />
);
const compactCode = ({ node, className, children, ...props }) => {
  const isBlock = /language-/.test(className || '');
  if (!isBlock) {
    return (
      <code className="font-mono rounded px-1.5 py-0.5"
        style={{ backgroundColor: 'rgba(255,255,255,0.1)', color: '#abb2bf', fontSize: 'inherit' }}
        {...props}>
        {children}
      </code>
    );
  }
  return <code className={className} {...props}>{children}</code>;
};
const compactPre = ({ node, children, ...props }) => {
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
};

const PANEL_COMPONENTS = {
  strong, em, del, input, img, ul, ol, li,
  p: panelP, h1: panelH1, h2: panelH2, h3: panelH3, h4: panelH4,
  code: panelCode, pre: panelPre,
  a: panelA, blockquote: panelBlockquote, hr: panelHr,
  table: panelTable, thead: panelThead, tr: panelTr, th: panelTh, td: panelTd,
};

// Compact table components — reuse panel styles for consistency
const compactTable = ({ node, ...props }) => (
  <div className="my-1 overflow-x-auto rounded" style={{ border: '1px solid rgba(255,255,255,0.15)' }}>
    <table className="w-full border-collapse text-left" style={{ minWidth: '100%', fontSize: '0.85em' }} {...props} />
  </div>
);
const compactThead = ({ node, ...props }) => <thead style={{ backgroundColor: 'rgba(0,0,0,0.25)' }} {...props} />;
const compactTr = ({ node, ...props }) => <tr className="border-b border-white/10 last:border-b-0" {...props} />;
const compactTh = ({ node, ...props }) => (
  <th className="px-2 py-1.5 whitespace-nowrap" style={{ color: '#FFFFFF', fontWeight: 600, borderBottom: '1px solid rgba(255,255,255,0.15)' }} {...props} />
);
const compactTd = ({ node, ...props }) => (
  <td className="px-2 py-1.5 break-words align-top" style={{ color: '#FFFFFF' }} {...props} />
);

const COMPACT_COMPONENTS = {
  strong, em, del, ul, ol, li,
  p: compactP, h1: compactH1, h2: compactH2, h3: compactH3,
  code: compactCode, pre: compactPre,
  a: panelA, blockquote: panelBlockquote, hr: panelHr,
  table: compactTable, thead: compactThead, tr: compactTr, th: compactTh, td: compactTd,
};

const VARIANTS = {
  chat: {
    className: 'leading-[1.5] break-words max-w-none overflow-hidden',
    style: { color: '#FFFFFF' },
    components: CHAT_COMPONENTS,
  },
  panel: {
    className: '',
    style: { color: '#FFFFFF', opacity: 0.9 },
    components: PANEL_COMPONENTS,
  },
  compact: {
    className: '',
    style: { color: '#FFFFFF', opacity: 0.9 },
    components: COMPACT_COMPONENTS,
  },
};

export { CodeBlock };

function Markdown({ content, variant = 'panel', className = '', style }) {
  const config = VARIANTS[variant];
  return (
    <div
      className={`${config.className} ${className}`.trim()}
      style={{ ...config.style, ...style }}
    >
      <ReactMarkdown remarkPlugins={[remarkGfm]} rehypePlugins={[rehypeRaw]} components={config.components}>
        {content}
      </ReactMarkdown>
    </div>
  );
}

export default Markdown;
