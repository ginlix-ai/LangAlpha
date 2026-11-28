import { viewerFor } from '@/pages/ChatAgent/components/filePanel/fileMeta';
import { commentViewer } from '@/pages/ChatAgent/components/viewers/comments/commentViewer';
import type { BodyView } from './components/FileBody';

/**
 * Whether the pane shows a body its comment layer draws over, so the header's
 * comment tools have something to act on: an HTML page or its source, or a
 * document whose viewer draws the layer, as the file panel's does
 * (`commentViewer`). Not a file with no preview or too large for one, a read
 * that failed, a viewer that failed on the bytes, a page framed with no
 * bridge, or a first read still on its way. The file's comments still count
 * in the lists either way.
 *
 * Unlike the panel, which frames an HTML file by its address, a document here
 * is drawn from its bytes alone, so it is `read` only once it holds them.
 */
export function bodyTakesComments(view: BodyView, viewerFailed: boolean): boolean {
  if (view.kind === 'html') return !viewerFailed;
  if (view.kind !== 'document' || view.loading || view.error || viewerFailed) return false;
  const { body, entry } = view;
  if (!body || (body.content == null && body.buffer == null && body.ranged == null)) return false;
  return commentViewer(viewerFor(entry.name, body.mime), true) !== null;
}
