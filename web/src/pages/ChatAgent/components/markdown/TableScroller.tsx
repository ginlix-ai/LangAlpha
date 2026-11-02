import { useEffect, useRef, useState, type ReactElement, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { cn } from '@/lib/utils';

interface TableScrollerProps {
  children: ReactNode;
  className: string;
}

// A table wider than its column scrolls, and the cut often lands between two
// columns, where it reads as the table's real edge. The side with more to see
// fades out, and the scroller takes focus and a name so a keyboard can move it.
export default function TableScroller({ children, className }: TableScrollerProps): ReactElement {
  const { t } = useTranslation();
  const ref = useRef<HTMLDivElement>(null);
  const [more, setMore] = useState('');
  useEffect(() => {
    const el = ref.current;
    if (!el) return undefined;
    const measure = (): void => {
      const left = el.scrollLeft > 1;
      const right = el.scrollWidth - el.clientWidth - el.scrollLeft > 1;
      setMore(left && right ? 'both' : left ? 'left' : right ? 'right' : '');
    };
    measure();
    el.addEventListener('scroll', measure, { passive: true });
    const resize = new ResizeObserver(measure);
    resize.observe(el);
    if (el.firstElementChild) resize.observe(el.firstElementChild);
    return () => {
      el.removeEventListener('scroll', measure);
      resize.disconnect();
    };
  }, []);
  return (
    <div className={cn('overflow-hidden', className)} style={{ border: '1px solid var(--color-border-muted)' }}>
      <div
        ref={ref}
        className="markdown-table-scroller overflow-x-auto"
        data-more={more || undefined}
        {...(more ? { tabIndex: 0, role: 'region', 'aria-label': t('chat.scrollableTable') } : {})}
      >
        {children}
      </div>
    </div>
  );
}
