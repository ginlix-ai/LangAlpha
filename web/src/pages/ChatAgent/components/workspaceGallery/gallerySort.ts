import { safeLocalStorage } from '@/lib/utils';

export type GallerySort = 'activity' | 'name' | 'custom';

/** The sort menu's options, in its order. */
export const GALLERY_SORTS: readonly GallerySort[] = ['activity', 'name', 'custom'];

/**
 * The order the user last picked, a per-browser convenience. Nothing stored,
 * or a value this build does not know, reads as Recent; blocked storage just
 * forgets on reload. Apart from the gallery so the route's prefetch reads the
 * list under the key the gallery will.
 */
export const GALLERY_SORT_KEY = 'workspaceGallery.sort';

export const readGallerySort = (): GallerySort => {
  const stored = safeLocalStorage.getItem(GALLERY_SORT_KEY);
  return GALLERY_SORTS.find((sort) => sort === stored) ?? 'activity';
};
