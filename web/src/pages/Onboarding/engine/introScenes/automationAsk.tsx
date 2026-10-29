import { useTranslation } from 'react-i18next';
import { Clock, Globe } from 'lucide-react';
import { useAgentNames } from '../introAgents';
import { CLOSE, Camera, REST, type Shot, VIEW, shot } from './camera';
import { PAST, useSceneTime } from './clock';
import { InputDock, Typed, UserBubble } from './kit';
import {
  HEADER_H,
  PanelToggle,
  Reply,
  ThreadHeader,
  Timeline,
  ToolRow,
  Transcript,
} from './transcriptParts';
import type { IntroScene } from './types';

/* The automations intro's chat step: setting an automation up by asking the
   Chief of Staff for it. The agent keeps each automation as a JSON file in
   `.agents/user/automations/` and edits it with its ordinary file tools, so
   the transcript draws those calls as the app draws a Read or a Write on
   one: the Automations clock glyph, the running verb, then the past-tense
   title with the file's name as its pill. Before it creates one it sums the
   automation up and waits for a yes. */
/* eslint-disable react-refresh/only-export-components -- scene module: a
   private scene component and its record entry */

/** The thread page at the size a resting shot frames whole: the camera's
    zoom never leaves the page's own edges. */
const PAGE = { width: VIEW.width / REST, height: VIEW.height / REST };

/** The chat column's width (px), centred on the page as the app sets it. */
const COL = 240;

/** The column's inset under the header, and so where the transcript's top
    edge falls. The push-in's own top edge sits just under it, so the camera
    cuts no line the transcript has not, and the composer keeps a margin at
    the foot of the view. The transcript sits on the composer, so its lines
    stay put whatever the header's height; the inset keeps its top edge at
    36px, where the push-in clears the line it frames first. */
const INSET = 8;
const TRANSCRIPT_TOP = HEADER_H + INSET;

/** The ask typed into the composer and sent, the agent's summary of what it
    would set up, the yes, the Write of the new file, and the line saying it
    is done. */
const ASK = {
  ask: 'Every weekday at 8, brief me on my holdings.',
  cps: 30,
  type: 500,
  send: 2200,
  summary:
    "**Here's what I'd set up:** Morning holdings brief, weekdays at 8:00 AM ET. Each run covers overnight moves, news and today's events for your holdings, in a new thread each time. Create it?",
  summaryAt: 2550,
  summaryCps: 140,
  yes: 'Yes.',
  typeYes: 5300,
  sendYes: 5700,
  write: 5950,
  written: 6400,
  file: 'morning-holdings-brief',
  created:
    "**Created.** The first brief runs tomorrow at 8:00 AM, and it's on your Automations page.",
  createdAt: 6500,
  createdCps: 150,
  length: 10000,
};

/** The whole page at rest, the composer and the newest lines above it as
    the ask is typed and answered; a short push in on the Write as it runs
    and the line saying the brief is set up, from the transcript's top edge
    down to the composer; and back out to the page. */
const ASK_SHOTS: [Shot, ...Shot[]] = [
  shot(0, PAGE.width / 2, PAGE.height / 2, REST),
  shot(6250, PAGE.width / 2, TRANSCRIPT_TOP + 2 + VIEW.height / 2 / CLOSE, CLOSE, 700),
  shot(9300, PAGE.width / 2, PAGE.height / 2, REST, 700),
];

/** One call on an automation's file: the clock glyph, `reading` or
    `updating automations...` while it runs, then `Read` or `Updated
    automations` with the file's name. */
function AutomationRow({
  write,
  file,
  at,
  done,
}: {
  write: boolean;
  file: string;
  at: number;
  done: number;
}) {
  const { t } = useTranslation();
  return (
    <ToolRow
      dense
      icon={Clock}
      running={{
        label: t(`toolArtifact.inProgress.${write ? 'updating' : 'reading'}_automations`),
      }}
      settled={{
        label: t(`toolArtifact.completed.${write ? 'updated' : 'read'}_automations`),
        pill: file,
      }}
      at={at}
      done={done}
    />
  );
}

function AutomationAsk() {
  const { t } = useTranslation();
  const names = useAgentNames();
  const now = useSceneTime();
  const composer =
    now >= ASK.type && now < ASK.send ? (
      <Typed text={ASK.ask} at={ASK.type} cps={ASK.cps} />
    ) : now >= ASK.typeYes && now < ASK.sendYes ? (
      <Typed text={ASK.yes} at={ASK.typeYes} cps={ASK.cps} />
    ) : undefined;
  return (
    <Camera shots={ASK_SHOTS} page={PAGE}>
      <div className="flex flex-col" style={PAGE}>
        {/* the thread's header: back, its place, and the file panel's
            toggle. The place is the Chief of Staff, so no agent tag repeats it */}
        <ThreadHeader title={names.home}>
          <PanelToggle />
        </ThreadHeader>
        <div
          className="mx-auto flex min-h-0 flex-1 flex-col gap-1.5 pb-2.5"
          style={{ width: COL, paddingTop: INSET }}
        >
          {/* the transcript settles on the composer: the newest lines just
              above it, the oldest running off the top as the turn grows */}
          <Transcript settled>
            <UserBubble at={PAST}>When does AVGO report?</UserBubble>
            <ToolRow
              dense
              icon={Globe}
              settled={{ label: t('toolArtifact.tool.webSearch'), pill: 'AVGO earnings date' }}
            />
            <Reply text="**Thursday, Dec 10**, after the close." at={PAST} />
            <UserBubble at={PAST} wrap>
              I&apos;m off next week. Back on Monday, November 2.
            </UserBubble>
            <Reply
              text="**Noted.** Your automations keep running while you're away, and each run's results wait in the feed. Want any of them paused?"
              at={PAST}
            />
            <UserBubble at={PAST} wrap>
              Pause the AVGO earnings preview until I&apos;m back.
            </UserBubble>
            {/* the file is read, then written back with its status paused */}
            <Timeline dense>
              <AutomationRow write={false} file="avgo-earnings-preview" at={PAST} done={PAST} />
              <AutomationRow write file="avgo-earnings-preview" at={PAST} done={PAST} />
            </Timeline>
            <Reply
              text="**Paused.** It won't run again until you turn it back on, here or on the Automations page."
              at={PAST}
            />
            {/* the ask, the summary it waits on, the yes, and the new file */}
            <UserBubble at={ASK.send + 60} wrap>
              {ASK.ask}
            </UserBubble>
            <Reply text={ASK.summary} at={ASK.summaryAt} cps={ASK.summaryCps} />
            <UserBubble at={ASK.sendYes + 60}>{ASK.yes}</UserBubble>
            <AutomationRow write file={ASK.file} at={ASK.write} done={ASK.written} />
            <Reply text={ASK.created} at={ASK.createdAt} cps={ASK.createdCps} />
          </Transcript>
          <InputDock text={composer} sendAt={[ASK.send, ASK.sendYes]} />
        </div>
      </div>
    </Camera>
  );
}

/** The scene's entry in the automations record. Centred: the chat column
    sits in the middle of the page. */
export const askScene: IntroScene = {
  Scene: AutomationAsk,
  length: ASK.length,
  anchor: 'center',
};
