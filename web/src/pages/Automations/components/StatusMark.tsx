import React from 'react';
import { Loader } from '@/components/ui/loader';
import type { StatusUi } from '../utils/status';

/** The glyph for a status: the ascii loader for live work, the table's icon
 *  for an exceptional state, nothing for a quiet one. */
export function StatusGlyph({ ui, label, size = 14 }: { ui: StatusUi; label?: string; size?: number }) {
  if (ui.live) {
    return <Loader size={size - 1} label={label} style={{ color: ui.color }} />;
  }
  if (!ui.Icon) return null;
  const Icon = ui.Icon;
  return (
    <Icon
      aria-label={label}
      aria-hidden={label ? undefined : true}
      className="shrink-0"
      style={{ width: size, height: size, color: ui.color }}
      strokeWidth={2}
    />
  );
}
