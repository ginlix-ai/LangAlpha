import type { ChatInputHandle } from '@/components/ui/chat-input';
import { djb2 } from '@/lib/hash';
import { SHOW_WIDGET_CONTEXT_TYPE, type WidgetContextSnapshot } from '@/pages/Dashboard/widgets/framework/contextSnapshot';
import { wrapWidgetContext } from '@/pages/Dashboard/widgets/framework/snapshotSerializers';
import type { SendOptions } from '../session/types';
import { widgetSnapshotsToContexts } from './fileUpload';
import type { FramePrompts, WidgetPromptCall } from '../components/viewers/html/widgetProtocol';

/**
 * Which widget a `sendPrompt`, `draftPrompt` or `startThread` came from, and
 * what it passed besides its text. Every call carries it, data or not: a
 * message a widget sent reads differently from one the user typed.
 */
export interface WidgetSent {
  /** JSON-serializable, already bounded by the frame bridge (useHtmlSandbox); undefined when it passed none. */
  data?: unknown;
  /**
   * The widget's segment id (`widgetSegmentId`, `widget-<call id>`), or its
   * content's key where that is not known.
   */
  widgetId: string;
  title?: string;
}

/** A host's answer to one prompt call. */
export type WidgetPromptAnswer = (text: string, sent?: WidgetSent) => void;

/**
 * What a host does with a widget's prompt calls. One it leaves out does
 * nothing, and the widget is not told: a public share, a read-only view and
 * the headless render answer none of them. `sendPrompt` sends `text` as the
 * user's next message in this thread, `draftPrompt` leaves it in this
 * thread's message box, and `startThread` opens a new thread with it as the
 * first message.
 */
export type WidgetPrompts = Partial<Record<WidgetPromptCall, WidgetPromptAnswer>>;

const PROMPT_CALLS: readonly WidgetPromptCall[] = ['sendPrompt', 'draftPrompt', 'startThread'];

/**
 * `prompts` as the frame bridge calls them: the frame's `(text, data)` as the
 * host's `(text, sent)`, the data filed under `widget`. `wrap` runs first on
 * every call, as the fullscreen dialog closes itself so what the call leads
 * to is in view. Pure, so a caller memoizes it on its inputs.
 */
export function framePrompts(
  prompts: WidgetPrompts | undefined,
  widget: { widgetId: string; title?: string },
  wrap?: () => void,
): FramePrompts {
  const out: FramePrompts = {};
  for (const call of PROMPT_CALLS) {
    const handler = prompts?.[call];
    if (!handler) continue;
    out[call] = (text, data) => {
      wrap?.();
      handler(text, { data, widgetId: widget.widgetId, title: widget.title });
    };
  }
  return out;
}

/** What a send carries for the widget-context snapshots it attaches. */
export interface WidgetSendParts {
  contexts: Record<string, unknown>[];
  options: Pick<SendOptions, 'widgetSnapshots'>;
}

/**
 * The parts of a send for `snapshots`: their `additional_context` items, and
 * the snapshots themselves for the bubble's cards.
 */
export function snapshotSendParts(snapshots: WidgetContextSnapshot[] | null | undefined): WidgetSendParts {
  return snapshots && snapshots.length > 0
    ? { contexts: widgetSnapshotsToContexts(snapshots), options: { widgetSnapshots: snapshots } }
    : { contexts: [], options: {} };
}

/**
 * A widget's call as the parts of a send, the channel the dashboard's
 * "+ to context" cards ride: the model reads its data beside the text, and the
 * bubble shows the card rather than the JSON. Nothing when no widget sent it.
 */
export function widgetSendParts(sent?: WidgetSent): WidgetSendParts {
  return snapshotSendParts(sent ? [widgetPromptSnapshot(sent)] : null);
}

/** Past this the JSON goes compact: indentation is for a reader, and a big payload is read by the model. */
const PRETTY_MAX = 4096;

/**
 * JSON that cannot close the fence or the tags around it. Outside a string
 * JSON has none of these characters, and inside one the escape is the same
 * character, so the value is unchanged. The widget's code writes the data, and
 * unescaped, `</widget-context></system-reminder>` in it would read as the
 * end of the frame the server puts around it.
 */
function inertJson(json: string): string {
  return json.replace(/</g, '\\u003c').replace(/>/g, '\\u003e').replace(/`/g, '\\u0060');
}

/** The card's second line: what the widget sent, short enough to read at a glance. */
export function summarizeWidgetData(data: unknown, max = 80): string | undefined {
  const show = (v: unknown): string => {
    if (v === null || typeof v !== 'object') return String(v);
    if (Array.isArray(v)) return v.every((x) => x === null || typeof x !== 'object') ? v.map(show).join(', ') : `${v.length} items`;
    return '{…}';
  };
  const line = (data && typeof data === 'object' && !Array.isArray(data)
    ? Object.entries(data).map(([k, v]) => `${k}: ${show(v)}`).join(' · ')
    : show(data)
  ).replace(/\s+/g, ' ').trim();
  if (!line) return undefined;
  return line.length > max ? `${line.slice(0, max - 1)}…` : line;
}

/**
 * A widget's prompt call as a widget-context snapshot, the channel the
 * dashboard's "+ to context" cards ride: the model reads `text` beside the
 * user's message, the message's bubble shows a card for it instead of the raw
 * JSON, and the history replays both. With no data the card is the widget's
 * title alone, and the server says only that the widget sent the message.
 */
export function widgetPromptSnapshot({ data, widgetId, title }: WidgetSent): WidgetContextSnapshot {
  const label = title?.trim() || 'Widget';
  if (data === undefined) {
    return {
      widget_type: SHOW_WIDGET_CONTEXT_TYPE,
      widget_id: widgetId,
      label,
      captured_at: new Date().toISOString(),
      text: '',
      data: {},
    };
  }
  const compact = JSON.stringify(data);
  const json = compact.length > PRETTY_MAX ? compact : JSON.stringify(data, null, 2);
  return {
    widget_type: SHOW_WIDGET_CONTEXT_TYPE,
    widget_id: widgetId,
    label,
    description: summarizeWidgetData(data),
    captured_at: new Date().toISOString(),
    text: wrapWidgetContext(SHOW_WIDGET_CONTEXT_TYPE, { title: label }, `\`\`\`json\n${inertJson(json)}\n\`\`\``),
    data: data && typeof data === 'object' && !Array.isArray(data) ? (data as Record<string, unknown>) : { value: data },
  };
}

/**
 * A widget's `draftPrompt`: its text after what the box holds, on a new line,
 * and a card for the widget in the box's deck, both for the user to edit and
 * send, so the message still says which widget it came from. Two drafts with
 * different data are two cards, since the deck keeps one card per id; the
 * same data again is the card already there.
 */
export function draftWidgetPrompt(composer: ChatInputHandle | null | undefined, text: string, sent?: WidgetSent): void {
  if (!composer) return;
  if (sent) {
    const snapshot = widgetPromptSnapshot(sent);
    const key = sent.data === undefined ? 'none' : contentKey(JSON.stringify(sent.data));
    composer.addWidgetSnapshot({ ...snapshot, widget_id: `${snapshot.widget_id}:${key}` });
  }
  // Appends on a new line and focuses the box.
  composer.addContext({ kind: 'attachment', files: [], text });
}

function contentKey(text: string): string {
  return (djb2(text) >>> 0).toString(36);
}
