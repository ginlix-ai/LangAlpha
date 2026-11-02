import type { ComponentProps, MouseEvent, ReactElement } from 'react';
import { cn } from '@/lib/utils';

export function isFootnoteLink(props: object): boolean {
  return 'data-footnote-ref' in props || 'data-footnote-backref' in props;
}

// The sanitizer prefixes the ids a footnote renders and not the hrefs that point
// at them, so the address alone reaches nothing. Scrolling here also leaves the
// route's URL alone, and stays inside this document when a thread holds several
// with the same ids.
function jumpToFootnote(e: MouseEvent<HTMLAnchorElement>): void {
  e.preventDefault();
  const fragment = e.currentTarget.getAttribute('href')?.slice(1) ?? '';
  const target = e.currentTarget.closest('.markdown-root')?.querySelector<HTMLElement>(`[id="user-content-${CSS.escape(fragment)}"]`);
  if (!target) return;
  target.scrollIntoView({ block: 'center' });
  // A list item takes focus only this way; the reference a back link returns to
  // already can, and has to stay in the tab order.
  if (target.tabIndex < 0) target.tabIndex = -1;
  target.focus({ preventScroll: true });
}

export function FootnoteLink({ className, ...props }: ComponentProps<'a'>): ReactElement {
  return <a onClick={jumpToFootnote} {...props} className={cn('tap-target', className)} />;
}
