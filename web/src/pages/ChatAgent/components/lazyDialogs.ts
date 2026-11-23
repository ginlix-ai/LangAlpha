import { lazy } from 'react';

// The sidebar's confirm dialogs, loaded the first time one opens: the hooks
// that host them mount on every page, and each opens only on its action. Held
// here rather than beside the hooks so those files export no component, which
// fast refresh needs, and so both hosts share one load of the delete modal.
export const DeleteConfirmModal = lazy(() => import('./DeleteConfirmModal'));
export const DuplicateWorkspaceDialog = lazy(() => import('./DuplicateWorkspaceDialog'));
export const ArchiveThreadConfirmDialog = lazy(() => import('./ArchiveThreadConfirmDialog'));
