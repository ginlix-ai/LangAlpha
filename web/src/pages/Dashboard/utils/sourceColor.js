/**
 * Derive a deterministic hue-based color from a source/publisher name.
 * Used for source badge backgrounds in news feed cards.
 */
export function getSourceColor(name) {
  if (!name) return { bg: 'var(--color-bg-tag)', color: 'var(--color-text-secondary)' };
  let hash = 0;
  for (let i = 0; i < name.length; i++) hash = name.charCodeAt(i) + ((hash << 5) - hash);
  const hue = Math.abs(hash) % 360;
  return {
    bg: `hsla(${hue}, 45%, 50%, 0.15)`,
    color: `hsla(${hue}, 55%, 55%, 1)`,
  };
}
