import type JSZip from 'jszip';
import type { SkillInfo, SkillLimits } from '@/pages/ChatAgent/utils/api';
import type { UploadProblem } from './skillUploadProblems';

/**
 * Turning what the user picked (a zip, a folder, one SKILL.md, or pasted
 * text) into the one thing the server takes: a zip with SKILL.md at its root
 * or inside a single top-level folder. A zip the user made goes up as it is;
 * whatever this side builds puts the skill's files at the zip's root.
 *
 * Checks here are the server's own rules, run early so a pick that is plainly
 * not a skill is refused before anything is zipped or sent. The server stays
 * the judge: anything this side cannot be sure of goes up and comes back as
 * the server's answer, and the size caps wait for the server to say what they
 * are (`limits` is undefined until it has).
 */

export interface SkillEntry {
  /** Relative, `/`-separated, no leading slash. */
  path: string;
  file: Blob;
}

export type SkillSource = 'zip' | 'folder' | 'file' | 'paste';

export interface PreparedSkill {
  source: SkillSource;
  /** The file or folder name as picked; null for loose dropped items or text. */
  label: string | null;
  /** The name SKILL.md declares, when it could be read. */
  name: string | null;
  fileCount: number | null;
  /** The zip that goes to the server. */
  upload: File;
}

export type Prepared = { skill: PreparedSkill; problem?: undefined } | { problem: UploadProblem; skill?: undefined };

// The server's _FRONTMATTER_RE: --- on the first line, a closing --- on its
// own line, and a newline after it.
const FRONTMATTER = /^---\s*\n([\s\S]*?)\n---\s*\n/;

export function hasFrontmatter(text: string): boolean {
  return FRONTMATTER.test(text);
}

/**
 * The top-level `name:` in SKILL.md's header. A plain scalar read, not a YAML
 * parse: it only labels the pick and finds what the upload would replace, and
 * the server reads the real value.
 */
export function declaredName(text: string): string | null {
  const header = FRONTMATTER.exec(text);
  if (!header) return null;
  const line = /^name[ \t]*:(.*)$/m.exec(header[1]);
  if (!line) return null;
  const raw = line[1].trim();
  const quoted = /^(['"])(.*)\1$/.exec(raw);
  const value = quoted ? quoted[2] : raw.replace(/\s+#.*$/, '');
  return value.trim() || null;
}

const JUNK_DIRS = new Set(['__MACOSX', '__pycache__', 'node_modules']);

/**
 * What a skill never holds: OS litter, caches, dependencies, hidden files and
 * any LICENSE.txt (the upload route sets the same aside, validate.py
 * `_ignored` and `_not_uploaded`). Left out of anything zipped here as well,
 * because the file browser the user picked from does not show hidden files,
 * so a `.env` or a `.git` folder would be sent without anyone having chosen
 * to send it.
 */
export function isJunk(path: string): boolean {
  const parts = path.split('/');
  return parts.at(-1) === 'LICENSE.txt' || parts.some((part) => JUNK_DIRS.has(part) || part.startsWith('.'));
}

// validate.py `_unsafe_component`: names with these are refused outright,
// since the paths later reach sandbox command lines.
const UNSAFE_CHARS = new Set(`'"\`$;|&<>*?\\`);
function unsafePath(path: string): boolean {
  return path.split('/').some(
    (part) =>
      part.startsWith('-') ||
      [...part].some((c) => UNSAFE_CHARS.has(c) || c.charCodeAt(0) < 0x20 || c.charCodeAt(0) === 0x7f),
  );
}

interface TreeFile {
  /** Relative to the skill's root. */
  path: string;
  /** Null where it is not known without unpacking (a zip's entries). */
  size: number | null;
}

/**
 * The server's rules over the files a skill is made of (validate.py), for
 * every kind of pick alike.
 */
function checkTree(files: readonly TreeFile[], limits: SkillLimits | undefined): UploadProblem | null {
  if (limits && files.length > limits.max_files) {
    return { kind: 'tooManyFiles', count: files.length, max: limits.max_files };
  }
  let total = 0;
  for (const { path, size } of files) {
    if (unsafePath(path)) return { kind: 'unsafePath', path };
    if (!limits || size === null) continue;
    if (size > limits.max_file_bytes) return { kind: 'fileTooLarge', file: path, max: limits.max_file_bytes };
    if (path === 'SKILL.md' && size > limits.max_skill_md_bytes) {
      return { kind: 'skillMdTooLarge', max: limits.max_skill_md_bytes };
    }
    total += size;
  }
  if (limits && total > limits.max_unpacked_bytes) {
    return { kind: 'unpackedTooLarge', max: limits.max_unpacked_bytes };
  }
  return null;
}

function sized(entries: readonly SkillEntry[]): TreeFile[] {
  return entries.map((e) => ({ path: e.path, size: e.file.size }));
}

/**
 * The most a zip of the skill's own files may be. The server keeps the skill
 * re-zipped, so the stored cap binds it as well as the wire cap.
 */
export function skillZipCap(limits: SkillLimits): number {
  return Math.min(limits.max_upload_bytes, limits.max_stored_bytes ?? Infinity);
}

function tooLarge(size: number, max: number | undefined): UploadProblem | null {
  return max !== undefined && size > max ? { kind: 'uploadTooLarge', size, max } : null;
}

/**
 * Where SKILL.md sits by the server's rule: '' at the root, 'dir/' inside the
 * one top-level folder everything is in, null when neither holds.
 */
export function skillRoot(paths: readonly string[]): string | null {
  if (paths.includes('SKILL.md')) return '';
  const tops = new Set(paths.map((p) => p.split('/')[0]));
  if (tops.size !== 1) return null;
  const [top] = tops;
  return paths.includes(`${top}/SKILL.md`) ? `${top}/` : null;
}

async function loadJSZip() {
  // Lazy: zipping is needed only once someone uploads, so the library stays
  // off first load.
  const { default: JSZip } = await import('jszip');
  return JSZip;
}

/** Zip `entries` at the archive root. */
export async function zipEntries(entries: readonly SkillEntry[], fileName: string): Promise<File> {
  const JSZip = await loadJSZip();
  const zip = new JSZip();
  // No folder entries, which the server never keeps, so this zip weighs no
  // more than the one the server stores and can be held to the stored cap.
  for (const entry of entries) zip.file(entry.path, entry.file, { createFolders: false });
  const blob = await zip.generateAsync({
    type: 'blob',
    mimeType: 'application/zip',
    compression: 'DEFLATE',
    compressionOptions: { level: 6 },
  });
  return new File([blob], fileName, { type: 'application/zip' });
}

function zipName(name: string | null, label: string | null): string {
  const base = name ?? label?.replace(/\.(md|zip)$/i, '') ?? 'skill';
  return `${base}.zip`;
}

/** Entries that passed `checkTree`, with SKILL.md's text, zipped for the server. */
async function finish(
  entries: SkillEntry[],
  source: SkillSource,
  label: string | null,
  text: string,
  limits: SkillLimits | undefined,
): Promise<Prepared> {
  if (!hasFrontmatter(text)) return { problem: { kind: 'noFrontmatter' } };
  const name = declaredName(text);
  const upload = await zipEntries(entries, zipName(name, label));
  const over = tooLarge(upload.size, limits && skillZipCap(limits));
  if (over) return { problem: over };
  return { skill: { source, label, name, fileCount: entries.length, upload } };
}

/**
 * A folder's files (paths relative to the folder) as an upload. `truncated`
 * says the walk stopped before the end, which only a folder far over the file
 * cap does.
 */
export async function prepareFolder(
  label: string | null,
  entries: readonly SkillEntry[],
  limits: SkillLimits | undefined,
  truncated = false,
): Promise<Prepared> {
  const kept = entries.filter((e) => !isJunk(e.path));
  const root = skillRoot(kept.map((e) => e.path));
  if (root === null) return { problem: { kind: 'folderNoSkillMd' } };
  if (truncated) return { problem: { kind: 'folderTooBig' } };
  const files = kept.map((e) => ({ path: e.path.slice(root.length), file: e.file }));
  const problem = checkTree(sized(files), limits);
  if (problem) return { problem };
  // skillRoot found it; read only now, so an oversized one is refused unread.
  const skillMd = files.find((e) => e.path === 'SKILL.md');
  if (!skillMd) return { problem: { kind: 'folderNoSkillMd' } };
  return finish(files, 'folder', label, await skillMd.file.text(), limits);
}

/**
 * One SKILL.md, from a file or from pasted text. Pasted text gets the
 * trailing newline the server's header match needs after the closing `---`,
 * which a header pasted on its own usually lacks.
 */
export async function prepareSkillMd(
  source: 'file' | 'paste',
  label: string | null,
  body: Blob | string,
  limits: SkillLimits | undefined,
): Promise<Prepared> {
  const text = typeof body === 'string' ? (body.endsWith('\n') ? body : `${body}\n`) : await body.text();
  const file = typeof body === 'string' ? new Blob([text], { type: 'text/markdown' }) : body;
  const entries = [{ path: 'SKILL.md', file }];
  const problem = checkTree(sized(entries), limits);
  if (problem) return { problem };
  return finish(entries, source, label, text, limits);
}

/**
 * A zip the user made, sent as it is: the server sets aside what `isJunk`
 * names and takes a skill's folder whatever it is called. A zip this side
 * cannot open goes up untouched, for the server to say why. Only the wire
 * cap is checked here: what the server keeps is smaller by everything it
 * sets aside, which this side cannot weigh without re-zipping.
 */
export async function prepareZip(file: File, limits: SkillLimits | undefined): Promise<Prepared> {
  const over = tooLarge(file.size, limits?.max_upload_bytes);
  if (over) return { problem: over };
  const asSent = (name: string | null, fileCount: number | null): Prepared => ({
    skill: { source: 'zip', label: file.name, name, fileCount, upload: file },
  });
  const JSZipLib = await loadJSZip();
  let zip: JSZip;
  try {
    zip = await JSZipLib.loadAsync(file);
  } catch {
    return asSent(null, null);
  }

  const byPath = new Map<string, JSZip.JSZipObject>();
  for (const entry of Object.values(zip.files)) {
    const path = entry.name.split('/').filter((p) => p && p !== '.').join('/');
    if (!entry.dir && path && !isJunk(path)) byPath.set(path, entry);
  }
  if (byPath.size === 0) return { problem: { kind: 'empty' } };
  const root = skillRoot([...byPath.keys()]);
  const skillMd = root === null ? undefined : byPath.get(`${root}SKILL.md`);
  if (root === null || !skillMd) return { problem: { kind: 'layout' } };
  // An entry's size is not known before it is unpacked, so the size caps are
  // left to the server; the count and the paths are not.
  const problem = checkTree(
    [...byPath.keys()].map((path) => ({ path: path.slice(root.length), size: null })),
    limits,
  );
  if (problem) return { problem };
  const text = await skillMd.async('string');
  if (!hasFrontmatter(text)) return { problem: { kind: 'noFrontmatter' } };
  return asSent(declaredName(text), byPath.size);
}

/** Every entry of a dropped folder, read to the end (the API pages them). */
export function listFolder(dir: FileSystemDirectoryEntry): Promise<FileSystemEntry[]> {
  const reader = dir.createReader();
  const all: FileSystemEntry[] = [];
  return new Promise((resolve, reject) => {
    const next = () =>
      reader.readEntries((batch) => {
        if (batch.length === 0) {
          resolve(all);
          return;
        }
        all.push(...batch);
        next();
      }, reject);
    next();
  });
}

function fileOf(entry: FileSystemFileEntry): Promise<File> {
  return new Promise((resolve, reject) => entry.file(resolve, reject));
}

/**
 * Read dropped items as one folder's contents. Breadth first, never entering
 * a hidden or junk folder, and stopping once past `cap` files, so a home
 * folder dropped by mistake is answered in a moment rather than walked to the
 * end. The top level is always read whole and a SKILL.md is always kept,
 * since those are what say where the skill is.
 */
export async function readDropped(
  top: readonly FileSystemEntry[],
  cap: number,
): Promise<{ entries: SkillEntry[]; truncated: boolean }> {
  const entries: SkillEntry[] = [];
  const pending: Array<[FileSystemDirectoryEntry, string]> = [];
  let truncated = false;
  const visit = async (item: FileSystemEntry, path: string) => {
    if (isJunk(path)) return;
    if (item.isDirectory) {
      pending.push([item as FileSystemDirectoryEntry, path]);
    } else if (item.isFile) {
      if (entries.length >= cap && item.name !== 'SKILL.md') {
        truncated = true;
        return;
      }
      entries.push({ path, file: await fileOf(item as FileSystemFileEntry) });
    }
  };
  for (const item of top) await visit(item, item.name);
  while (pending.length && !truncated) {
    const [dir, prefix] = pending.shift()!;
    for (const child of await listFolder(dir)) await visit(child, `${prefix}/${child.name}`);
  }
  return { entries, truncated };
}

/** The entries a folder picker (`webkitdirectory`) returned, relative to the picked folder. */
export function pickedFolder(files: readonly File[]): { label: string | null; entries: SkillEntry[] } {
  const label = files[0]?.webkitRelativePath.split('/')[0] || null;
  const entries = files.map((file) => ({
    path: file.webkitRelativePath.split('/').slice(1).join('/') || file.name,
    file,
  }));
  return { label, entries };
}

export type Clash =
  | { kind: 'builtIn' }
  | { kind: 'own'; skill: SkillInfo }
  | { kind: 'plugin'; skill: SkillInfo };

/**
 * What an upload named `name` meets where it lands. A built-in name or
 * command is refused by the server. A skill of the user's own in the same
 * scope is replaced in place; one a plugin brought is replaced too, and from
 * then on it is the user's, outside the plugin's updates.
 */
export function findClash(
  name: string,
  skills: readonly SkillInfo[],
  workspaceId: string | null,
): Clash | null {
  if (skills.some((s) => s.origin === 'platform' && (s.name === name || s.command === name))) {
    return { kind: 'builtIn' };
  }
  const same = skills.find((s) =>
    workspaceId
      ? s.origin === 'workspace' && s.workspace_id === workspaceId && s.name === name
      : s.origin === 'user' && !s.workspace_id && s.name === name,
  );
  if (!same) return null;
  return same.plugin_id ? { kind: 'plugin', skill: same } : { kind: 'own', skill: same };
}
