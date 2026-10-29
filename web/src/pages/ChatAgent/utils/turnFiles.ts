/**
 * The files one turn produced, for the card strip under the agent's reply.
 *
 * A turn's deliverables are named twice: the reply links them, and the write
 * tools record the paths they touched. The links carry the agent's own order
 * and reach files a script wrote, which no tool call names; the tool calls
 * reach a file the reply forgot to mention and carry what the edit changed.
 */

import { assistantText } from '../components/messageList/messageText';
import type { MessageRecord } from '../components/messageList/types';
import { fileKind, isFilePath, isImagePath, parseWsPath } from './filePaths';
import {
  classifyAgentPath,
  folderWorkspacePath,
  isAgentNotesPath,
  onOtherComputer,
  parseAgentHref,
  parseAgentPath,
  workspaceScopedPath,
  type AgentPathParts,
  type ComputerFolders,
} from './agentPaths';
import { splitFileLocation, type FileLocation } from './fileLocation';
import { isSystemPath, writeCalls, type TurnMessage } from './fileRefResolver';
import { normalizeFileRefs } from './normalizeFileRefs';
import { mapOutsideCode } from './markdownSegments';

export interface TurnFile {
  /** Workspace-relative path, with no location suffix. */
  path: string;
  /** Set when the reference names another workspace (a Chief of Staff relay, or another workspace's folder). */
  workspaceId?: string;
  /** The spot inside the file the reply pointed at, if it named one. */
  location?: FileLocation;
  /** Lines this turn's edits added and removed, for a file the Edit tool changed. */
  stats?: { added: number; removed: number };
}

/**
 * A markdown link or image, with the destination bare or in angle brackets.
 *
 * A bare destination carries one level of balanced parens, which CommonMark
 * allows and `normalizeFileRefs.LINK_DEST_RE` already reads, because
 * `report(1).pdf` is a real deliverable name. Without it the reply's own link
 * renders and opens while the card for it never appears, and an embedded
 * `![chart](chart(1).png)` stops counting as embedded and earns a second,
 * duplicate card for what the reader is already looking at.
 *
 * A destination may carry a CommonMark title, which is not part of the path and
 * so rides in its own non-capturing group. Without it the closing paren had to
 * follow the destination directly, and `[report](results/report.pdf "Download")`
 * earned no card at all: not a deliverable, and for the image form not even an
 * embed, so a titled chart the reader is already looking at could still collect
 * a duplicate card from its Write call. The three title forms are the ones the
 * secretary's `_TITLE` already accepts.
 *
 * A label may hold the image that is the link's own face:
 * `[![chart](charts/a.png)](work/report.md)` is one construct naming two files,
 * the chart on screen and the report a click opens. Reading only as far as the
 * image left the outer destination with no `[` in front of it, so the report
 * the reply cited that way earned no card at all.
 *
 * The label is bounded for the reason the secretary's twin already records
 * (`src/tools/secretary/utils.py`): the pattern is unanchored, so on a run of
 * `[` with no `]` an unbounded label rescans the line from every one of them.
 * That is quadratic, it runs on the main thread as a turn settles, and 512 is
 * past any real link label.
 */
const DEST = String.raw`(?:<[^<>\n]+>|(?:[^()\s]|\([^()\s]*\))+)`;
const TITLE = String.raw`(?:[ \t]+(?:"[^"\n]*"|'[^'\n]*'|\([^()\n]*\)))?`;
const LABEL = String.raw`[^\]\n]{0,512}`;
// The label may open with one image, which is the clickable chart, and the
// alternation stays outside any repetition so the scan keeps its cost.
const LINK_RE = new RegExp(
  String.raw`(!?)\[(?:!\[${LABEL}\]\(\s*(${DEST})${TITLE}\s*\))?${LABEL}\]\(\s*(${DEST})${TITLE}\s*\)`,
  'g',
);

/** Identity of a reference: two workspaces can hold one path, and those are two files. */
function refKey(file: { path: string; workspaceId?: string }): string {
  return file.workspaceId ? `${file.workspaceId}\u0000${file.path}` : file.path;
}

/**
 * What one Edit changed, counted after the lines both sides share.
 *
 * The tool's two strings carry the surrounding lines that anchor the swap, so
 * counting them whole reports an edit several times its real size.
 */
function editLines(text: unknown): string[] {
  // Empty text is no lines at all, and a trailing newline ends the last line
  // rather than opening another. `''.split('\n')` says one line to both, so a
  // deletion would report the line it removed and a phantom line added.
  if (typeof text !== 'string' || text === '') return [];
  return text.replace(/\n$/, '').split('\n');
}

function editStats(oldString: unknown, newString: unknown): { added: number; removed: number } {
  const before = editLines(oldString);
  const after = editLines(newString);
  let head = 0;
  while (head < before.length && head < after.length && before[head] === after[head]) head++;
  let tail = 0;
  while (
    tail < before.length - head
    && tail < after.length - head
    && before[before.length - 1 - tail] === after[after.length - 1 - tail]
  ) tail++;
  return { added: after.length - head - tail, removed: before.length - head - tail };
}

/** Whether a reference points at a workspace file the panel can open. */
function openablePath(path: string): boolean {
  return (
    !!path
    // A reply's links read from the workspace root, so one that still climbs
    // above it after normalization names nothing this deck can offer.
    && !path.startsWith('../')
    && fileKind(path) !== null
    && !isSystemPath(path)
    && classifyAgentPath(path).kind === 'file'
  );
}

/**
 * Where a reference lands, or null for one the deck keeps out: a notes file,
 * or a file on another computer outside its workspaces' folders, which the
 * panel would read on this computer and, missing, swap for a file here that
 * only shares its name. Another workspace's folder names that workspace as a
 * `__wsref__` does, so the card opens there; read from this workspace's
 * folder, it would name a file the workspace does not hold.
 */
function placeRef(
  parts: AgentPathParts,
  folders: ComputerFolders | null | undefined,
): { path: string; workspaceId?: string } | null {
  const other = folderWorkspacePath(parts, folders);
  if (other) {
    return isAgentNotesPath(parseAgentPath(other.path)) ? null : other;
  }
  const { dirName, previousDirNames, otherRoots } = folders ?? {};
  if (onOtherComputer(parts, folders)) return null;
  const path = workspaceScopedPath(parts, dirName, previousDirNames, otherRoots);
  return isAgentNotesPath(parts, dirName, previousDirNames, otherRoots) ? null : { path };
}

interface MessageFiles {
  /** Files the reply links, in the order it names them. */
  cited: TurnFile[];
  /** Files a write tool touched, in call order. */
  written: TurnFile[];
  /** Images the reply embeds, which the reader can already see. */
  embedded: string[];
}

function filesInMessage(message: TurnMessage, folders?: ComputerFolders | null): MessageFiles {
  const cited: TurnFile[] = [];
  const written: TurnFile[] = [];
  const embedded: string[] = [];

  if (message.role === 'assistant') {
    const text = assistantText(message);
    // Only prose is read. A link inside code is syntax the reply is showing,
    // not a file it produced: a fenced `[report](never-made.pdf)` would mint a
    // card for something nothing wrote, and a fenced `![chart](chart.png)`
    // would mark a real chart as already drawn and suppress the card its Write
    // earned. The scan is a read, but it is the same prose/code split every
    // rewrite in this pipeline goes through.
    mapOutsideCode(text ? normalizeFileRefs(text, folders?.rootDir, folders?.otherRoots) : '', (prose) => {
      const collect = (raw: string, embeds: boolean) => {
        const dest = raw.replace(/^<|>$/g, '');
        if (!isFilePath(dest, folders)) return;
        // `#L12` and `:42` are link syntax, so they come off before the path
        // rules run: those drop a fragment, and would take the location with it.
        const { path: href, location } = splitFileLocation(dest);
        const wsRef = parseWsPath(href);
        const placed = placeRef(parseAgentHref(href, folders?.rootDir), folders);
        if (!placed || !openablePath(placed.path)) return;
        const { path } = placed;
        const file: TurnFile = { path, workspaceId: wsRef?.workspaceId ?? placed.workspaceId, location: location ?? undefined };
        if (embeds || isImagePath(path)) {
          embedded.push(refKey(file));
          return;
        }
        cited.push(file);
      };
      for (const match of prose.matchAll(LINK_RE)) {
        // The chart wearing the link is on screen either way, so it is read as
        // an embed and the destination behind it as the citation it is.
        if (match[2]) collect(match[2], true);
        collect(match[3], match[1] === '!');
      }
      return prose;
    });
  }

  for (const { parts, call } of writeCalls(message, folders?.rootDir)) {
    const placed = placeRef(parts, folders);
    if (!placed || !openablePath(placed.path)) continue;
    const { path, workspaceId } = placed;
    // Only an Edit says what changed. A Write carries the new file alone, and
    // whether it replaced one, or how much of it, is not in the call. Neither
    // does a `replace_all` Edit, which carries one pair of strings for every
    // substitution it made: counting that pair once reports an edit N times
    // smaller than it was, and the card presents the number as fact.
    const args = call.toolCall?.args ?? {};
    const stats = call.toolName === 'Edit' && !args.replace_all
      ? editStats(args.old_string, args.new_string)
      : undefined;
    // An edit whose two strings match changed nothing, and `+0 -0` under a file
    // name reads as a measurement rather than as the absence of one.
    const file: TurnFile = workspaceId ? { path, workspaceId } : { path };
    written.push(stats && (stats.added || stats.removed) ? { ...file, stats } : file);
  }

  return { cited, written, embedded };
}

/**
 * The files a turn produced: the ones the reply names, in its order, then the
 * ones only a write tool names. An image the reply embeds is left out, since a
 * card for it would point at what the reader is already looking at.
 */
export function collectTurnFiles(
  messages: readonly TurnMessage[],
  folders?: ComputerFolders | null,
): TurnFile[] {
  const parsed = messages.filter(Boolean).map((m) => filesInMessage(m, folders));

  const embedded = new Set(parsed.flatMap((p) => p.embedded));
  const byRef = new Map<string, TurnFile>();
  const add = (file: TurnFile) => {
    const key = refKey(file);
    if (embedded.has(key)) return;
    const existing = byRef.get(key);
    if (!existing) {
      byRef.set(key, { ...file, stats: file.stats && { ...file.stats } });
      return;
    }
    if (!existing.location && file.location) existing.location = file.location;
    if (file.stats) {
      existing.stats = {
        added: (existing.stats?.added ?? 0) + file.stats.added,
        removed: (existing.stats?.removed ?? 0) + file.stats.removed,
      };
    }
  };

  for (const p of parsed) p.cited.forEach(add);
  for (const p of parsed) p.written.forEach(add);
  return [...byRef.values()];
}

// Keyed on the message that ends a turn, and checked against the whole member
// list, so the entry survives exactly as long as the turn's messages do. The
// folders are part of the check: the workspace record often lands after the
// transcript, and the entry built without them hides the wrong `agent.md`; so
// do the list of every workspace and the computers' roots, which the paths are
// read against. They are compared by value (`foldersKey`), so the same folders
// from another copy of the record still hit.
const turnCache = new WeakMap<object, {
  members: readonly TurnMessage[];
  folders: string;
  files: TurnFile[];
}>();

function cachedTurnFiles(
  members: readonly TurnMessage[],
  folders: ComputerFolders | null,
  foldersKey: string,
): TurnFile[] {
  const key = members[members.length - 1] as object | undefined;
  if (!key) return [];
  const hit = turnCache.get(key);
  if (
    hit
    && hit.folders === foldersKey
    && hit.members.length === members.length
    && hit.members.every((m, i) => m === members[i])
  ) {
    return hit.files;
  }
  const files = collectTurnFiles(members, folders);
  turnCache.set(key, { members: [...members], folders: foldersKey, files });
  return files;
}

/**
 * The files each settled turn produced, keyed by turn index.
 *
 * Each array holds its identity for as long as its turn's messages hold
 * theirs, because `MessageBubble` is memoized on exactly this value: the
 * transcript hands out a new `messages` array on every streamed token, and a
 * fresh array here would re-render every settled bubble that ever produced a
 * file, on every token of the turn being written now.
 *
 * A turn still streaming is left out entirely. Half a path is not a
 * deliverable yet, and the deck would rewrite itself as the rest arrived.
 */
export function turnFilesByTurn(
  projected: readonly { message: MessageRecord; turnIndex: number }[],
  folders?: ComputerFolders | null,
): Map<number, TurnFile[]> {
  const byTurn = new Map<number, TurnMessage[]>();
  const streaming = new Set<number>();
  for (const { message, turnIndex } of projected) {
    if (message.isStreaming) streaming.add(turnIndex);
    // The transcript is `Record<string, unknown>` at its source; this is the
    // one place the deliverables path gives it a shape.
    const member = message as TurnMessage;
    const bucket = byTurn.get(turnIndex);
    if (bucket) bucket.push(member);
    else byTurn.set(turnIndex, [member]);
  }

  // Once per call, not per turn: this runs on every streamed token, and the
  // folders hold every workspace on every computer. Every value comes from
  // `computerFolders`, which builds its keys in one order.
  const foldersKey = JSON.stringify(folders ?? null);
  const files = new Map<number, TurnFile[]>();
  for (const [turnIndex, members] of byTurn) {
    if (streaming.has(turnIndex)) continue;
    const collected = cachedTurnFiles(members, folders ?? null, foldersKey);
    if (collected.length > 0) files.set(turnIndex, collected);
  }
  return files;
}
