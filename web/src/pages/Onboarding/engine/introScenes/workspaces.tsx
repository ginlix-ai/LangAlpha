import type { IntroVisualId } from '../../registry';
import { during, useSceneTime } from './clock';
import { CLOSE, Camera, REST, type Shot, VIEW, shot, wide } from './camera';
import { Appear, CenteredDialog, Scrim, pointerScript, pressAt } from './kit';
import { GalleryPage } from './galleryParts';
import { ComputerMenu, FORM_W, NewComputerForm } from './computerParts';
import { MODAL_W, NewWorkspaceModal } from './newWorkspaceParts';
import type { IntroScene } from './types';

/* Scenes for the workspace gallery: adding a computer from its toolbar, and
   creating a workspace over it. The gallery, the computer form and the
   workspace modal are in `galleryParts`, `computerParts` and
   `newWorkspaceParts`; this file holds each scene's script. */
/* eslint-disable react-refresh/only-export-components -- scene module:
   private scene components, one record export */

/** The gallery comes in, then the pointer opens the computer filter, picks
    New computer, and in the form that opens picks This Mac. */
const GRID = {
  cards: [150, 260, 330, 400, 470, 540, 610],
  clickFilter: 2000,
  menu: 2050,
  clickNew: 3000,
  form: 3150,
  clickMac: 4500,
  length: 7000,
};

/** In page px. Each point sits 2px up and left of its target, where the
    pointer's tip lands. */
const GRID_POINTER = pointerScript(
  { at: 1300, x: 300, y: 220 },
  {
    filter: { x: 238, y: 38, arrive: 1900, click: GRID.clickFilter, leave: 2300 },
    newComputer: { x: 212, y: 124, arrive: 2850, click: GRID.clickNew, leave: 3700 },
    thisMac: { x: 190, y: 186, arrive: 4400, click: GRID.clickMac },
  }
);

/** Opens on the whole gallery, pushes in on the filter and its menu, takes
    in the form whole as it opens, closes on its providers for the pick, then
    settles on the remote control setup This Mac brings. */
const GRID_SHOTS: [Shot, ...Shot[]] = [
  wide(0),
  shot(2050, 215, 85, CLOSE, 700),
  wide(3600, 600),
  shot(4400, 200, 165, REST, 700),
  shot(5800, 200, 230, REST, 800),
];

function WorkspaceGrid() {
  const now = useSceneTime();
  const { newComputer } = GRID_POINTER.beats;
  const menuOpen = during(now, GRID.menu, GRID.clickNew + 80);
  const formOpen = now >= GRID.form;
  return (
    <Camera shots={GRID_SHOTS} pointer={GRID_POINTER}>
      <div className="relative" style={VIEW}>
        <GalleryPage cards={GRID.cards} pressFilter={pressAt(now, GRID.clickFilter)} />
        {/* the filter's menu, opening under it at its right edge */}
        {menuOpen && (
          <div className="absolute right-[68px] top-[49px]">
            <Appear at={GRID.menu} pop style={{ transformOrigin: 'top right' }}>
              <ComputerMenu hover={now >= newComputer.arrive} />
            </Appear>
          </div>
        )}
        {/* New computer's form, over the page's scrim */}
        <Scrim on={formOpen} />
        {formOpen && (
          <CenteredDialog width={FORM_W}>
            <Appear at={GRID.form} pop>
              <NewComputerForm mac={now >= GRID.clickMac} />
            </Appear>
          </CenteredDialog>
        )}
      </div>
    </Camera>
  );
}

/* ── chat · step 5: the new-workspace modal over the gallery ───────────── */

/** Creating a workspace, played out: the pointer clicks New workspace, the
    modal opens, the name and description type in, a click on the drop zone
    adds a file, and the pointer clicks Create. */
const NEW_WS = {
  name: 'Hyperscaler capex tracker',
  desc: 'Capex guidance for MSFT, AMZN, GOOGL, META…',
  clickNew: 1100,
  modal: 1250,
  typeName: 2000,
  clickDesc: 3000,
  typeDesc: 3150,
  clickDrop: 4900,
  file: 5050,
  upload: 700,
  clickCreate: 6700,
  length: 7600,
};

/** The whole gallery, as the step before ends on it, while the pointer goes
    to New workspace; then on the form as the modal opens: name and
    description, down to the files, and out to the whole page for Create. */
const NEW_WS_SHOTS: [Shot, ...Shot[]] = [
  wide(0),
  shot(1950, 200, 80, REST, 700),
  shot(4800, 200, 250, REST, 700),
  wide(6400, 700),
];

/** New workspace, then the description's free space past its text, the
    drop zone's browse link, and Create, in page px. Each point sits 2px up
    and left of its target, where the pointer's tip lands; on Create it
    lands in the padding past the label, so the finished frame still reads
    it. */
const NEW_WS_POINTER = pointerScript(
  { at: 200, x: 300, y: 130 },
  {
    new: { x: 350, y: 15, arrive: 1050, click: NEW_WS.clickNew, leave: 2100 },
    desc: { x: 298, y: 121, arrive: 2950, click: NEW_WS.clickDesc, leave: 4300 },
    drop: { x: 228, y: 250, arrive: 4850, click: NEW_WS.clickDrop, leave: 5900 },
    create: { x: 308, y: 301, arrive: 6650, click: NEW_WS.clickCreate },
  }
);

function CreateWorkspace() {
  const now = useSceneTime();
  const { drop } = NEW_WS_POINTER.beats;
  return (
    <Camera shots={NEW_WS_SHOTS} pointer={NEW_WS_POINTER}>
      <div className="relative" style={VIEW}>
        {/* the gallery, under the modal's scrim once it opens */}
        <GalleryPage pressNew={pressAt(now, NEW_WS.clickNew)} />
        <Scrim on={now >= NEW_WS.modal} />
        <CenteredDialog width={MODAL_W}>
          <NewWorkspaceModal script={NEW_WS} overDrop={during(now, drop.arrive, drop.leave)} />
        </CenteredDialog>
      </div>
    </Camera>
  );
}

export const workspaceScenes = {
  workspaceGrid: { Scene: WorkspaceGrid, length: GRID.length, anchor: 'center' },
  createWorkspace: { Scene: CreateWorkspace, length: NEW_WS.length, anchor: 'center' },
} satisfies Partial<Record<IntroVisualId, IntroScene>>;
