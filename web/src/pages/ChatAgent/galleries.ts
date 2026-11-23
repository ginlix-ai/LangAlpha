import { preloadableLazy } from '@/lib/lazyWithMotion';
import { withCopy } from '@/i18n';

// Imported by the shell (routePrefetch), so a chat route's gallery downloads
// beside the chat page rather than behind it and its copy. Being loaded from
// the entry, each lists every namespace it reads, though the chat page loads
// most of them too.
export const WorkspaceGallery = preloadableLazy(withCopy(() => import('./components/WorkspaceGallery'), ['filePanel']));
export const ThreadGallery = preloadableLazy(withCopy(() => import('./components/ThreadGallery'), [
  'customDashboard', 'dashboard', 'filePanel', 'marketView', 'mcp', 'pageViewer', 'plugins', 'publish', 'settings', 'toolArtifact', 'vault',
]));
