import type { ComponentType } from 'react';
import { ChartCandlestick, Database, FileText, Shapes, Telescope } from 'lucide-react';
import { SubagentsRings } from '@/components/ui/subagents-rings';

type Glyph = ComponentType<{ className?: string }>;

/** The built-in roles; any other subagent draws the kind's rings. */
const SUBAGENT_GLYPH: Record<string, Glyph> = {
  // Any kind of task, drawn as any shape.
  'general-purpose': Shapes,
  research: Telescope,
  'data-prep': Database,
  'equity-analyst': ChartCandlestick,
  'report-builder': FileText,
};

/**
 * A subagent's role icon. Shared by the Plugins tiles and the chat's subagent
 * rows, so a role reads as the same picture wherever the user meets it.
 */
export function SubagentGlyph({ type, className }: { type: string | undefined; className?: string }) {
  // Own keys only: a user-defined type named `constructor` or `toString` would
  // otherwise resolve to an Object.prototype function and render it.
  const Glyph = (type && Object.hasOwn(SUBAGENT_GLYPH, type) ? SUBAGENT_GLYPH[type] : undefined) || SubagentsRings;
  return <Glyph className={className} />;
}
