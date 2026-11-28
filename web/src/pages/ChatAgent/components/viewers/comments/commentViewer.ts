import type { FocusViewer } from '../../filePanel/useFileFocus';

/** A viewer in the app that draws the comment layer, through `ViewerComments`. */
export type CommentViewer = 'markdown' | 'code' | 'pdf' | 'sheet' | 'image';

const LAYERS: Partial<Record<FocusViewer, 'html' | CommentViewer>> = {
  html: 'html', markdown: 'markdown', code: 'code', pdf: 'pdf', excel: 'sheet', csv: 'sheet', image: 'image',
};

/**
 * The comment layer the viewer showing a file draws for its owner, or null
 * where it draws none: a large tool result, an edited file. An
 * HTML file is known by its name, any other once its bytes are `read`.
 */
export function commentViewer(viewer: FocusViewer, read: boolean): 'html' | CommentViewer | null {
  const layer = LAYERS[viewer] ?? null;
  return layer === 'html' || read ? layer : null;
}
