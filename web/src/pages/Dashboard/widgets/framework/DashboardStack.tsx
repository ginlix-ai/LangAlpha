import { useMemo } from 'react';
import { WidgetFrame } from './WidgetFrame';
import { WidgetBody } from './WidgetBody';
import { getWidget } from './WidgetRegistry';
import { reconcileLayouts } from './reconcile';
import { MARGIN_Y, ROW_HEIGHT } from './gridConstants';
import type { DashboardPrefs } from '../types';

const noop = () => undefined;

/**
 * The dashboard on a phone: the same widgets in one column, in the order the
 * grid reads (top to bottom, then left to right). A widget keeps the height
 * its grid cell gives it, and one that fits its content takes the content's.
 * Layout editing is a desktop affordance; a phone only reads the layout.
 */
export function DashboardStack({
  prefs,
  onConfigChange,
}: {
  prefs: DashboardPrefs;
  onConfigChange: (id: string, patch: Partial<unknown>) => void;
}) {
  const cells = useMemo(() => {
    const placed = new Map((reconcileLayouts(prefs.widgets, prefs.layouts).lg ?? []).map((l) => [l.i, l]));
    return prefs.widgets
      .flatMap((instance) => {
        const definition = getWidget(instance.type);
        const cell = placed.get(instance.id);
        return definition && cell ? [{ instance, definition, cell }] : [];
      })
      .sort((a, b) => a.cell.y - b.cell.y || a.cell.x - b.cell.x);
  }, [prefs.widgets, prefs.layouts]);

  return (
    <div className="flex flex-col" style={{ gap: MARGIN_Y }}>
      {cells.map(({ instance, definition, cell }) => (
        <div
          key={instance.id}
          style={{ height: definition.fitToContent ? undefined : cell.h * ROW_HEIGHT + (cell.h - 1) * MARGIN_Y }}
        >
          <WidgetFrame
            definition={definition}
            instance={instance}
            editMode={false}
            onRemove={noop}
            onDuplicate={noop}
            onOpenSettings={noop}
          >
            <WidgetBody definition={definition} instance={instance} updateConfig={(patch) => onConfigChange(instance.id, patch)} />
          </WidgetFrame>
        </div>
      ))}
    </div>
  );
}
