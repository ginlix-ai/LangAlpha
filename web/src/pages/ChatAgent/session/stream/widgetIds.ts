/**
 * A ShowWidget widget's segment id is its call's id under a prefix: the
 * artifact names the call it came from, so the widget, its preview and its
 * call are found by one id. The segment id also files the widget's kept
 * localStorage (`widgetStorage.ts`), so its format is stored and must stay.
 */
const PREFIX = 'widget-';

/** The `html_widget` segment id, and `htmlWidgetProcesses` key, of the widget call `callId` drew. */
export function widgetSegmentId(callId: string): string {
  return `${PREFIX}${callId}`;
}

/** The call a widget segment id names; null for an id this module did not mint. */
export function callIdOfWidgetSegment(segmentId: string | undefined | null): string | null {
  return segmentId?.startsWith(PREFIX) && segmentId.length > PREFIX.length ? segmentId.slice(PREFIX.length) : null;
}
