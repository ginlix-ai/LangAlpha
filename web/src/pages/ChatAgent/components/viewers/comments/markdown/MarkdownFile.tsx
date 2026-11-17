import { useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import Markdown, { type RehypePlugins } from '../../../Markdown';
import type { OpenFileHandler } from '../../../../utils/fileLocation';
import { ViewerComments, type ViewerCommentsTarget } from '../ViewerComments';
import { scrollParent } from '../scrollParent';
import { markdownSpots, markdownTitle } from './markdownSpots';
import { rehypeSourceLines } from './sourceLines';

const SOURCE_LINES = (fileLine: (line: number) => number): RehypePlugins[number] => [rehypeSourceLines, fileLine];

/**
 * A Markdown file in the panel. The owner's view takes comments on its
 * blocks and words (`comments`), which marks each block with its lines in
 * the file; anyone else's shows the file alone.
 */
export function MarkdownFile({ content, comments, onOpenFile, onAnchorLink }: {
  content: string;
  comments: ViewerCommentsTarget | null;
  onOpenFile?: OpenFileHandler;
  onAnchorLink?: (fragment: string) => void;
}) {
  const { t } = useTranslation();
  const [root, setRoot] = useState<HTMLDivElement | null>(null);
  const [scroller, setScroller] = useState<HTMLElement | null>(null);
  const placed = useCallback((el: HTMLDivElement | null) => {
    setRoot(el);
    setScroller(el && scrollParent(el));
  }, []);
  const strategy = useMemo(() => (root ? markdownSpots(root) : null), [root]);
  return (
    <>
      <div ref={placed} className="markdown-print-content">
        <Markdown
          variant="panel"
          content={content}
          className="text-sm"
          onOpenFile={onOpenFile}
          onAnchorLink={onAnchorLink}
          rehypeForBlock={comments ? SOURCE_LINES : undefined}
        />
      </div>
      {comments && (
        <ViewerComments
          scroller={scroller}
          content={root}
          strategy={strategy}
          target={comments}
          modeHint={t('pageViewer.comments.modeHintMarkdown')}
          title={markdownTitle}
        />
      )}
    </>
  );
}
