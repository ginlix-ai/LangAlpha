import { useId, useState, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import ReactMarkdown, { type Components } from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { FileCode, FileText } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { useLocale } from '@/hooks/useLocale';
import { formatBytes } from '@/lib/format';
import { scrollMotion } from '@/lib/reducedMotion';
import { cn } from '@/lib/utils';
import { isMarkdownPath } from './skillPaths';

/**
 * A skill's file as its page reads it: SKILL.md (or any Markdown file beside
 * it) rendered by default with the raw file one click away, and on a wide page
 * an outline of its headings beside it; a script as its source. The skill is
 * its instructions, so they are the page's centerpiece.
 *
 * Rendered plainly (GFM, no raw HTML, no math): this is a file somebody else
 * wrote, shown to the user, and the chat's renderer brings workspace links and
 * file cards that mean nothing here.
 */

interface Heading {
  level: number;
  text: string;
  /** Step number pulled out of "Step 3: …", drawn beside the entry. */
  step: string | null;
  line: number;
  id: string;
}

const FRONTMATTER = /^---\r?\n[\s\S]*?\r?\n---\r?\n?/;

/** The body without its YAML frontmatter. The outline reads headings from
 *  the same string the renderer parses, so their line numbers agree. */
function splitFrontmatter(content: string): string {
  const match = FRONTMATTER.exec(content);
  return match ? content.slice(match[0].length) : content;
}

function plainText(md: string): string {
  return md
    .replace(/`([^`]*)`/g, '$1')
    .replace(/\*\*([^*]*)\*\*/g, '$1')
    .replace(/\*([^*]*)\*/g, '$1')
    .replace(/\[([^\]]*)\]\([^)]*\)/g, '$1')
    .trim();
}

function slug(text: string): string {
  return (
    text
      .toLowerCase()
      .replace(/[^\p{L}\p{N}]+/gu, '-')
      .replace(/^-+|-+$/g, '') || 'section'
  );
}

/** h1–h3 outside code fences, with the 1-based line each starts on. */
function readHeadings(body: string, base: string): Heading[] {
  const out: Heading[] = [];
  const seen = new Map<string, number>();
  let fence: string | null = null;
  body.split('\n').forEach((raw, index) => {
    const line = raw.trimEnd();
    const fenceMatch = /^\s*(```|~~~)/.exec(line);
    if (fenceMatch) {
      if (!fence) fence = fenceMatch[1];
      else if (fence === fenceMatch[1]) fence = null;
      return;
    }
    if (fence) return;
    const match = /^(#{1,3})\s+(.+?)\s*#*$/.exec(line);
    if (!match) return;
    const text = plainText(match[2]);
    const stepMatch = /^step\s+(\d+)\s*[:.]\s*(.+)$/i.exec(text);
    const key = slug(text);
    const n = seen.get(key) ?? 0;
    seen.set(key, n + 1);
    out.push({
      level: match[1].length,
      text: stepMatch ? stepMatch[2] : text,
      step: stepMatch ? stepMatch[1] : null,
      line: index + 1,
      id: `${base}-${key}${n ? `-${n}` : ''}`,
    });
  });
  return out;
}

export function SkillDocument({
  path = 'SKILL.md',
  content,
  size = 0,
  loading,
  failed,
  preview,
}: {
  path?: string;
  /** Null: the file is not text, or too large to show. */
  content: string | null | undefined;
  size?: number;
  loading: boolean;
  failed: boolean;
  /** What Preview renders in place of the text, which Source keeps exact. */
  preview?: (body: string) => string;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const base = useId();
  const [chosenMode, setMode] = useState<'preview' | 'source'>('preview');
  // Only Markdown has a preview; a script is its source.
  const markdown = isMarkdownPath(path);
  const mode = markdown ? chosenMode : 'source';

  const text = content ?? '';
  const body = preview ? preview(splitFrontmatter(text)) : splitFrontmatter(text);
  const headings = readHeadings(body, base);
  const idByLine = new Map(headings.map((h) => [h.line, h.id]));
  const lineCount = text ? text.replace(/\n$/, '').split('\n').length : 0;
  // A document with a heading or two has nothing to navigate.
  const showOutline = mode === 'preview' && headings.length >= 3;

  const heading =
    (Tag: 'h1' | 'h2' | 'h3' | 'h4'): Components['h1'] =>
    ({ node, children }) => {
      const id = idByLine.get(node?.position?.start.line ?? -1);
      return <Tag id={id}>{children}</Tag>;
    };
  const components: Components = {
    h1: heading('h1'),
    h2: heading('h2'),
    h3: heading('h3'),
    h4: heading('h4'),
    a: ({ href, children }) => (
      <a href={href} target="_blank" rel="noopener noreferrer">
        {children}
      </a>
    ),
  };

  let doc: ReactNode;
  if (loading) {
    doc = (
      <div className="pp-doc-status flex items-center gap-2">
        <Loader size={14} className="text-current" />
        {t('common.loading')}
      </div>
    );
  } else if (failed) {
    doc = <div className="pp-doc-status">{t('plugins.detail.sourceLoadFailed')}</div>;
  } else if (content === null) {
    doc = (
      <div className="pp-doc-status">
        {t('plugins.skillsPage.fileNotShown', { size: formatBytes(size, locale) })}
      </div>
    );
  } else if (mode === 'source') {
    doc = <pre className={cn('pp-doc-source', !markdown && 'is-code')}>{text}</pre>;
  } else {
    doc = (
      <div className="pp-md">
        <ReactMarkdown remarkPlugins={[remarkGfm]} components={components}>
          {body}
        </ReactMarkdown>
      </div>
    );
  }

  function jump(id: string) {
    document.getElementById(id)?.scrollIntoView({ behavior: scrollMotion(), block: 'start' });
  }

  return (
    <div className={cn('pp-skill-body', showOutline && 'has-outline')}>
      {showOutline && (
        <nav className="pp-outline" aria-label={t('plugins.skillsPage.onThisPage')}>
          <h3>{t('plugins.skillsPage.onThisPage')}</h3>
          {headings.map((h) => (
            <button
              key={h.id}
              type="button"
              className={h.level >= 3 ? 'is-sub' : undefined}
              onClick={() => jump(h.id)}
              title={h.text}
            >
              {h.step && <span className="pp-outline-step">{h.step}</span>}
              {h.text}
            </button>
          ))}
        </nav>
      )}
      <div className="pp-doc">
        <div className="pp-doc-bar">
          {markdown ? (
            <FileText className="h-3.5 w-3.5" aria-hidden />
          ) : (
            <FileCode className="h-3.5 w-3.5" aria-hidden />
          )}
          <span className="pp-doc-path">{path}</span>
          {lineCount > 0 && (
            <span className="pp-doc-lines">{t('plugins.skillsPage.lines', { count: lineCount })}</span>
          )}
          {markdown && (
            <div className="pp-seg" role="group">
              <button type="button" aria-pressed={mode === 'preview'} onClick={() => setMode('preview')}>
                {t('plugins.skillsPage.preview')}
              </button>
              <button type="button" aria-pressed={mode === 'source'} onClick={() => setMode('source')}>
                {t('plugins.skillsPage.source')}
              </button>
            </div>
          )}
        </div>
        {doc}
      </div>
    </div>
  );
}
