import React, { useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useTheme } from '@/contexts/ThemeContext';
import SyntaxHighlighter, { oneDark, oneLight } from '../../../SyntaxHighlighter';
import { ViewerComments, type ViewerCommentsTarget } from '../ViewerComments';
import { scrollParent } from '../scrollParent';
import { codeSpots } from './codeSpots';

const CODE_STYLE: React.CSSProperties = { margin: 0, padding: 0, backgroundColor: 'transparent', fontSize: '0.75rem', lineHeight: '1.6' };
const CODE_TAG_PROPS = { style: { backgroundColor: 'transparent' } };
const LINE_NUMBER_STYLE: React.CSSProperties = {
  minWidth: '2.5em', paddingRight: '1em', color: 'var(--color-text-tertiary)', userSelect: 'none', fontSize: '0.6875rem', opacity: 0.5,
};

/**
 * A code or text file in the panel, read only, each line numbered and
 * carrying its number (`data-line`), which a reference into the file lands
 * on and the owner's comments (`comments`) pick lines by.
 */
export function CodeFile({ content, language, lineRange, comments }: {
  content: string;
  language: string;
  /** The lines a reference into the file points at, marked. */
  lineRange: [number, number] | null;
  comments: ViewerCommentsTarget | null;
}) {
  const { t } = useTranslation();
  const { theme } = useTheme();
  const [root, setRoot] = useState<HTMLDivElement | null>(null);
  const [scroller, setScroller] = useState<HTMLElement | null>(null);
  const placed = useCallback((el: HTMLDivElement | null) => {
    setRoot(el);
    setScroller(el && scrollParent(el));
  }, []);
  const strategy = useMemo(() => (root ? codeSpots(root) : null), [root]);

  // The highlighter tokenizes the whole file on every render it gets, so it
  // is drawn again only when what it draws changes.
  const from = lineRange?.[0];
  const to = lineRange?.[1];
  const code = useMemo(() => (
    <SyntaxHighlighter
      language={language}
      style={theme === 'light' ? oneLight : oneDark}
      customStyle={CODE_STYLE}
      codeTagProps={CODE_TAG_PROPS}
      showLineNumbers
      lineNumberStyle={LINE_NUMBER_STYLE}
      wrapLines
      lineProps={(lineNumber: number) => {
        const focused = from != null && to != null && lineNumber >= from && lineNumber <= to;
        return { 'data-line': lineNumber, ...(focused ? { className: 'file-focus-line' } : {}) } as React.HTMLProps<HTMLElement>;
      }}
      wrapLongLines
    >
      {content}
    </SyntaxHighlighter>
  ), [content, language, theme, from, to]);

  return (
    <>
      <div ref={placed}>{code}</div>
      {comments && (
        <ViewerComments
          scroller={scroller}
          content={root}
          strategy={strategy}
          target={comments}
          modeHint={t('pageViewer.comments.modeHintCode')}
        />
      )}
    </>
  );
}
