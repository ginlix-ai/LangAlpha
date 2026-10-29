import type { ReactNode } from 'react';
import type { LucideIcon } from 'lucide-react';
import { useAgentNames } from '../introAgents';
import { VIEW } from './camera';
import { useSceneTime } from './clock';
import { InputDock, Typed } from './kit';
import { ChatHeaderButtons, PanelStrip } from './panelParts';
import { HEADER_H, ThreadHeader, Transcript } from './transcriptParts';

/* The thread page the thread intro's scenes play on: the chat column beside
   the file panel, the panel a full-height column with its own tab strip.
   Each part is a small copy of the component the app draws it with; the
   transcript's own parts are in transcriptParts.tsx. */

/** The file panel's width in the 400px scene; the chat column takes the rest. */
export const PANEL_W = 212;

/** A glyph in the chrome of a tab's own body (the Memory and Memo tabs). */
export function ChromeIcon({ icon: Icon }: { icon: LucideIcon }) {
  return <Icon className="h-2 w-2 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />;
}

/**
 * The thread page alone, drawn at the view's size (`VIEW`) for a scene's
 * `Camera` to crop: the chat column with its header (the thread's `title`)
 * beside the file panel, whose tab strip (`tabs`, with `back` and `tree` as
 * `PanelStrip` reads them) sits level with the chat header and `panel` fills
 * the rest. `stack` places the transcript: from the top for a new thread, or
 * settled on the composer for one with history above, the oldest lines
 * running off the top as the newest come in. `ask` is typed into the
 * composer from `at`, `cps` characters a second, and sent at `send`.
 */
export function ThreadPage({
  title,
  tabs,
  back,
  tree,
  chat,
  panel,
  ask,
  stack = 'top',
}: {
  title: string;
  tabs: ReactNode;
  back?: boolean;
  tree?: boolean;
  chat: ReactNode;
  panel: ReactNode;
  ask?: { text: string; at: number; send: number; cps: number };
  stack?: 'top' | 'bottom';
}) {
  const { worker } = useAgentNames();
  const t = useSceneTime();
  const typing = ask && t >= ask.at && t < ask.send;
  return (
    <div className="relative flex flex-col" style={VIEW}>
      {/* the header row: the chat header, with share (the panel being open,
          its toggle is in the panel's corner), beside the panel's tab strip */}
      <div className="flex shrink-0">
        <ThreadHeader title={title} agent={worker} className="min-w-0 flex-1">
          <ChatHeaderButtons panelOpen />
        </ThreadHeader>
        <PanelStrip
          tabs={tabs}
          back={back}
          tree={tree}
          className="border-b border-l"
          style={{ width: PANEL_W, height: HEADER_H, borderColor: 'var(--color-border-muted)' }}
        />
      </div>
      <div className="flex min-h-0 flex-1">
        <div className="flex min-w-0 flex-1 flex-col gap-1.5 p-2.5">
          {stack === 'bottom' ? <Transcript settled>{chat}</Transcript> : chat}
          <InputDock
            text={typing ? <Typed text={ask.text} at={ask.at} cps={ask.cps} /> : undefined}
            sendAt={ask && [ask.send]}
          />
        </div>
        <div
          className="flex shrink-0 flex-col border-l"
          style={{ width: PANEL_W, borderColor: 'var(--color-border-muted)' }}
        >
          {panel}
        </div>
      </div>
    </div>
  );
}
