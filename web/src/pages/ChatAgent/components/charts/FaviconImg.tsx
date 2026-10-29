import React, { useState } from 'react';

/** Google's favicon service, the icon a site with none of its own named falls back to. */
function googleFaviconUrl(domain: string): string {
  return domain ? `https://www.google.com/s2/favicons?domain=${encodeURIComponent(domain)}&sz=32` : '';
}

/**
 * A site's icon: the page's own when it names one, else the one served for
 * its domain, and a monogram of `name` (the domain when absent) when neither
 * loads.
 */
export function FaviconImg({
  domain,
  icon,
  name,
  size = 14,
}: {
  domain: string;
  icon?: string;
  name?: string;
  size?: number;
}): React.ReactElement {
  const [failed, setFailed] = useState(false);
  const src = icon || googleFaviconUrl(domain);

  if (failed || !src) {
    return (
      <span
        style={{
          width: size,
          height: size,
          borderRadius: 2,
          backgroundColor: 'var(--color-bg-surface)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          fontSize: Math.round(size * 0.64),
          fontWeight: 600,
          color: 'var(--color-text-tertiary)',
          flexShrink: 0,
        }}
      >
        {((name ?? domain) || '?')[0].toUpperCase()}
      </span>
    );
  }

  return (
    <img
      src={src}
      alt=""
      width={size}
      height={size}
      style={{ borderRadius: 2, flexShrink: 0 }}
      onError={() => setFailed(true)}
    />
  );
}
