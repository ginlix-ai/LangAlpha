import { useMemo } from 'react';
import Markdown, { type RehypePlugins } from '@/pages/ChatAgent/components/Markdown';
import { rehypeMarkWords } from '@/pages/ChatAgent/utils/rehypeMarkWords';
import { rehypePreview } from './rehypePreview';
import { useShown, type TurnBodyProps } from './turnBody';

/**
 * One side of a turn in the preview pane, rendered as the chat renders it.
 * Its own chunk, loaded from the pane, so the palette opens without the
 * Markdown renderer and its highlighter.
 *
 * The matches are marked by a rehype pass rather than by the CSS Custom
 * Highlight API: the marks are elements React renders, so nothing edits the
 * DOM behind its back, the first one is found and scrolled to as the plain
 * fallback's is, and every browser draws them (Firefox below 140, inside
 * the app's floor, has no `CSS.highlights`). What it cannot reach is a fenced
 * code block, which is drawn from its text.
 */
export default function PreviewMarkdown(props: TurnBodyProps) {
  const { text, words } = props;
  // The links, images and sources stand in as text first, so their text is marked too.
  const rehypeAfter = useMemo<RehypePlugins>(() => [rehypePreview, [rehypeMarkWords, words]], [words]);
  useShown(props);
  return <Markdown content={text} variant="compact" rehypeAfter={rehypeAfter} className="command-palette-pane-md" />;
}
