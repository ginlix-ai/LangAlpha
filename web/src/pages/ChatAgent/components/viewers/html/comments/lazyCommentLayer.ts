import { lazyWithMotion } from '@/lib/lazyWithMotion';

/**
 * The comment layer over an owner's HTML file, loaded apart from the viewer.
 * One seam for the viewer that renders it and the panel that warms it: a
 * preload counts only on the lazy component that then renders. Its own
 * seam, so the layer's motion reaches framer with the reader's
 * reduced-motion setting.
 */
export const LazyCommentLayer = lazyWithMotion(() => import('./CommentLayer'));
