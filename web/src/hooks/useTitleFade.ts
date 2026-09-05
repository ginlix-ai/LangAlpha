import { useEffect, useRef, useState } from 'react';

/**
 * True for a beat after `title` changes post-mount — drives a soft fade on
 * live thread-title rewrites (auto-title replacing the raw first query,
 * renames landing from another surface). Never fires on first paint, so
 * list mounts stay static.
 */
export function useTitleFade(title: string): boolean {
  const prevRef = useRef(title);
  const [fading, setFading] = useState(false);
  useEffect(() => {
    if (prevRef.current === title) return;
    prevRef.current = title;
    setFading(true);
    const t = setTimeout(() => setFading(false), 300);
    return () => clearTimeout(t);
  }, [title]);
  return fading;
}
