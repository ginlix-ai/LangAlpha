// How `la drive put` and `get` move files of any size: the bytes go in parts,
// a few at a time, between the file on disk and the Drive's object storage (or
// the server's relay), so memory stays at a few parts whatever the file size.
// An interrupted run is finished by running the same command again.
import { createHash } from "node:crypto";
import { closeSync, existsSync, openSync, readFileSync, renameSync, rmSync, statSync, writeFileSync, writeSync } from "node:fs";
import { type FileHandle, open } from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import { CliError, Exit } from "../errors.js";
import type { Api } from "../http.js";
import { printable } from "../output.js";
import { rawResponse } from "../rawHttp.js";

export interface Entry {
  id: string;
  path: string;
  name: string;
  kind: "file" | "folder";
  size: number | null;
  sha256: string | null;
  is_text: boolean | null;
  version: number | null;
  modified_at: string;
  modified_by: { kind: string; agent?: string | null; thread_id?: string | null } | null;
}

const MIB = 1024 * 1024;
/** The part size the server asks for below 10,000 parts; the md5 of each part is
 *  taken while hashing the file, on this guess, and again only if it is wrong. */
const PART_GUESS = 16 * MIB;
const PARALLEL = 4;
/** What the part buffers of one upload may hold together. The server grows the
 *  part past ``PART_GUESS`` for a file too large for the store's part count, and
 *  fewer parts then go at once rather than more memory being held. */
const BUFFERED_MAX = 256 * MIB;
const SIGN_BATCH = 8;
/** A signature is good for fifteen minutes; one older than this is made again rather than tried. */
const SIGNED_FRESH_MS = 10 * 60_000;
const ATTEMPTS = 5;

export function bytesOf(n: number | null): string {
  if (n === null) return "-";
  if (n < 1024) return `${n} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let v = n / 1024;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(v < 10 ? 1 : 0)} ${units[i]}`;
}

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/** A refusal worth sending again: the server restarting or busy, the network blinking. */
const transient = (e: unknown): boolean =>
  e instanceof CliError && (e.status === 503 || e.status === 502 || e.status === 504 || e.reason === "unreachable");

/** ``fn``, tried again after a pause while it fails in a way that may pass. */
async function again<T>(fn: () => Promise<T>, retryable: (e: unknown) => boolean = transient): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    try {
      return await fn();
    } catch (e) {
      if (!retryable(e) || attempt >= ATTEMPTS) throw e;
      await sleep(1000 * attempt);
    }
  }
}

/** Run ``fn`` over ``items``, ``n`` at a time. The first failure stops new work,
 *  and is thrown once every worker already started has settled: the caller
 *  closes the file next, and a worker still writing would meet a closed handle. */
export async function pool<T>(items: readonly T[], n: number, fn: (item: T) => Promise<void>): Promise<void> {
  let next = 0;
  let failure: { error: unknown } | undefined;
  const worker = async () => {
    while (!failure && next < items.length) {
      const item = items[next++]!;
      try {
        await fn(item);
      } catch (error) {
        failure ??= { error };
      }
    }
  };
  await Promise.all(Array.from({ length: Math.min(n, items.length) }, worker));
  if (failure) throw failure.error;
}

/** Write all of ``bytes`` at ``position``; ``write`` may take fewer than it is given. */
export async function writeAll(fh: Pick<FileHandle, "write">, bytes: Uint8Array, position: number): Promise<void> {
  let done = 0;
  while (done < bytes.length) {
    const { bytesWritten } = await fh.write(bytes, done, bytes.length - done, position + done);
    if (bytesWritten === 0) throw new CliError("The disk accepted no more bytes; is it full?");
    done += bytesWritten;
  }
}

/** One line on stderr that rewrites itself, only at a terminal. */
class Progress {
  private readonly started = Date.now();
  private last = 0;
  private shown = false;
  private readonly live: boolean;

  constructor(
    private readonly label: string,
    private readonly total: number,
    quiet: boolean,
  ) {
    this.live = !quiet && process.stderr.isTTY === true;
  }

  update(done: number, extra = ""): void {
    const now = Date.now();
    if (!this.live || now - this.last < 250) return;
    this.last = now;
    const rate = done / Math.max(0.001, (now - this.started) / 1000);
    const pct = this.total > 0 ? ` ${Math.floor((done / this.total) * 100)}%` : "";
    const line = `${this.label}  ${bytesOf(done)} of ${bytesOf(this.total)}${pct}  ${bytesOf(rate)}/s${extra ? `  ${extra}` : ""}`;
    process.stderr.write(`\r\x1b[K${printable(line)}`);
    this.shown = true;
  }

  note(text: string): void {
    if (!this.live) return;
    process.stderr.write(`\r\x1b[K${text}`);
    this.shown = true;
  }

  done(): void {
    if (this.shown) process.stderr.write("\r\x1b[K");
    this.shown = false;
  }
}

// What each refusal means for the person, and what to do about it.
const REFUSALS: Record<string, (limit?: number) => string> = {
  too_large: (limit) =>
    limit
      ? `That file is larger than the ${bytesOf(limit)} one file may be in this Drive. Split it into smaller files or compress it.`
      : "That file is larger than one file may be in this Drive. Split it into smaller files or compress it.",
  over_quota: () =>
    "The Drive has no room for that file. Remove files you no longer need with `la drive rm`; if it still does not fit, the user empties the trash in the app.",
  too_many_entries: () => "The Drive holds as many files as it may. Remove files you no longer need with `la drive rm`.",
  changed: () =>
    "The file changed since the version you named. See what it holds now with `la drive history`, then run the command again with its hash.",
  exists: () => "Something is already at that path. Leave out --new to replace it, or choose another path.",
  too_many_uploads: () =>
    "Eight uploads are already open. Run the same command again to continue an interrupted one; an upload nobody touches closes after a day.",
  mismatch: () =>
    "The stored bytes did not match the file's hash, so nothing was saved. Run the same command again; if it keeps failing, the file is changing while it is read.",
  unavailable: () => "The Drive's storage did not answer. Run the same command again; it continues where it stopped.",
};

const REFUSAL_STATUS: Record<string, number> = { changed: 412, exists: 409, too_large: 413, over_quota: 507, mismatch: 422 };

export function explain(e: unknown, limit?: number): unknown {
  if (!(e instanceof CliError) || !e.reason || !REFUSALS[e.reason]) return e;
  return new CliError(REFUSALS[e.reason]!(limit), e.code, { status: e.status, detail: e.detail, reason: e.reason });
}

function failedWith(type: string, message: string, limit?: number): CliError {
  const known = REFUSALS[type];
  const status = REFUSAL_STATUS[type] ?? 500;
  const code = status === 409 || status === 412 ? Exit.conflict : Exit.error;
  return new CliError(known ? known(limit) : message, code, { status, reason: type });
}

export interface Space {
  id: string;
  kind: string;
  usage: {
    bytes: number;
    /** Bytes only the trash holds; they do not count toward ``max_bytes``. */
    trash_bytes?: number;
    entries: number;
    max_bytes: number | null;
    max_entries: number;
    max_upload_bytes?: number;
  };
  trash_days: number;
}

/** The space ``want`` names, by id or kind; the person's own when it names none. */
export async function findSpace(api: Api, want: string | undefined): Promise<Space> {
  const name = want ?? "personal";
  const { spaces } = await api.json<{ spaces: Space[] }>("GET", "/api/v1/drive/spaces");
  const space = spaces.find((s) => s.id === name || s.kind === name);
  if (!space) throw new CliError(`No Drive space ${name}`, Exit.notFound);
  return space;
}

/** The most one file may be, from the usage the server reports; undefined when it does not say. */
export async function maxUpload(api: Api, want: string | undefined): Promise<number | undefined> {
  try {
    return (await findSpace(api, want)).usage.max_upload_bytes;
  } catch {
    return undefined;
  }
}

/** The SHA-256 of a file and the MD5 of each ``partSize`` run of it (none when
 *  ``partSize`` is 0). It reads into one buffer: a read stream allocates a chunk
 *  per read and leaves their release to the garbage collector, which let a 2 GB
 *  file take 190 MB of resident memory. */
export async function hashFile(
  file: string,
  size: number,
  partSize: number,
  onBytes: (n: number) => void,
  withSha = true,
): Promise<{ sha256: string; md5s: string[] }> {
  const sha = createHash("sha256");
  const md5s: string[] = [];
  let md5 = createHash("md5");
  let inPart = 0;
  let read = 0;
  const buf = Buffer.allocUnsafe(MIB);
  const fh = await open(file, "r");
  try {
    for (;;) {
      const { bytesRead } = await fh.read(buf, 0, MIB, read);
      if (bytesRead === 0) break;
      let chunk = buf.subarray(0, bytesRead);
      if (withSha) sha.update(chunk);
      while (partSize > 0 && chunk.length) {
        const take = Math.min(chunk.length, partSize - inPart);
        md5.update(chunk.subarray(0, take));
        inPart += take;
        chunk = chunk.subarray(take);
        if (inPart === partSize) {
          md5s.push(md5.digest("base64"));
          md5 = createHash("md5");
          inPart = 0;
        }
      }
      read += bytesRead;
      onBytes(read);
    }
  } finally {
    await fh.close();
  }
  if (partSize > 0 && (inPart > 0 || (size === 0 && md5s.length === 0))) md5s.push(md5.digest("base64"));
  return { sha256: sha.digest("hex"), md5s };
}

const FILE_CHANGED = "The file changed while it was being sent; run the command again";

/** A check that ``file`` is still as it is now: the same size and modification
 *  time. What was hashed is what the server is told about, so bytes read after
 *  an edit would be recorded under the wrong digest (when the Drive already
 *  holds the old bytes, nothing is read again at all). */
function watch(file: string): () => void {
  const at = statSync(file);
  return () => {
    const now = statSync(file, { throwIfNoEntry: false });
    if (!now || now.size !== at.size || now.mtimeMs !== at.mtimeMs) throw new CliError(FILE_CHANGED);
  };
}

async function readAt(fh: FileHandle, buf: Buffer, position: number): Promise<void> {
  let got = 0;
  while (got < buf.length) {
    const { bytesRead } = await fh.read(buf, got, buf.length - got, position + got);
    if (bytesRead === 0) throw new CliError(FILE_CHANGED);
    got += bytesRead;
  }
}

/** An upload the server holds open for parts, to the store (``direct``) or through itself. */
interface OpenUpload {
  mode: "direct" | "relay";
  upload_id: string;
  part_size: number;
  part_count: number;
  /** Absent while it is being checked. */
  parts_held?: { part_number: number }[];
  resumed: boolean;
  /** ``verifying``: an earlier upload of the same file, already sent and being checked. */
  state?: "uploading" | "verifying";
}

/** What begin answers: nothing to send, one request, or parts. */
type Begun = { mode: "held"; entry: Entry } | { mode: "single"; max_bytes: number } | OpenUpload;

interface Signed {
  part_number: number;
  url: string;
  headers: Record<string, string>;
}

export interface PutOptions {
  api: Api;
  base: string;
  file: string;
  size: number;
  path: string;
  note?: string;
  ifMatch?: string;
  ifNone?: boolean;
  limit?: number;
  quiet: boolean;
}

/** Signs parts a few at a time, as the workers reach them: each batch takes
 *  the next parts not yet asked for, so none is held long before it is used. */
class Signer {
  private ready = new Map<number, { signed: Signed; at: number }>();
  private chain: Promise<void> = Promise.resolve();
  /** Parts not yet asked for, in the order they go; a part leaves once a worker asks for it. */
  private readonly ahead: Set<number>;

  constructor(
    private readonly api: Api,
    private readonly upload: string,
    private readonly md5s: string[],
    todo: readonly number[],
  ) {
    this.ahead = new Set(todo);
  }

  /** A fresh signature for part ``n``; ``alone`` skips the batch, for a signature that failed. A
   *  part asked for after the first pass (one the server reports missing) is signed by itself. */
  get(n: number, alone = false): Promise<Signed> {
    this.ahead.delete(n);
    const job = this.chain.then(async () => {
      const held = this.ready.get(n);
      if (!alone && held && Date.now() - held.at < SIGNED_FRESH_MS) return;
      const batch = alone ? [n] : [n, ...[...this.ahead].filter((q) => !this.ready.has(q)).slice(0, SIGN_BATCH - 1)];
      const { parts } = await again(() =>
        this.api.json<{ parts: Signed[] }>("POST", `${this.upload}/parts`, {
          body: { parts: batch.map((part_number) => ({ part_number, md5: this.md5s[part_number - 1] })) },
        }),
      );
      const at = Date.now();
      for (const signed of parts) this.ready.set(signed.part_number, { signed, at });
    });
    this.chain = job.catch(() => undefined);
    return job.then(() => {
      const { signed } = this.ready.get(n)!;
      this.ready.delete(n);
      return signed;
    });
  }
}

/** PUT the part's bytes on its signed address. The status, or ``null`` when the
 *  store cannot be reached at all; the address appears in neither. */
async function putSigned(s: Signed, body: Buffer): Promise<number | null> {
  try {
    const res = await rawResponse(new URL(s.url), { method: "PUT", headers: s.headers, body });
    await res.body?.cancel();
    return res.status;
  } catch {
    return null;
  }
}

/** Upload one file; the entry it became. */
export async function putFile(o: PutOptions): Promise<Entry> {
  const { api, base, size } = o;
  const progress = new Progress(`Reading ${basename(o.file)}`, size, o.quiet);
  try {
    if (o.limit !== undefined && size > o.limit) throw failedWith("too_large", "", o.limit);
    const unchanged = watch(o.file);
    const hashed = await hashFile(o.file, size, PART_GUESS, (n) => progress.update(n));
    progress.done();
    const begun = await api.json<Begun>("POST", `${base}/uploads`, {
      body: {
        path: o.path,
        size,
        sha256: hashed.sha256,
        note: o.note,
        if_match: o.ifMatch,
        if_none_match: o.ifNone || undefined,
      },
    });
    unchanged();
    if (begun.mode === "held") return begun.entry;
    if (begun.mode === "single") return await putWhole(o, unchanged);
    return await putParts(o, begun, hashed, progress, unchanged);
  } catch (e) {
    throw explain(e, o.limit);
  } finally {
    progress.done();
  }
}

async function putWhole(o: PutOptions, unchanged: () => void): Promise<Entry> {
  const headers: Record<string, string> = {};
  if (o.ifNone) headers["if-none-match"] = "*";
  if (o.ifMatch !== undefined) headers["if-match"] = `"${o.ifMatch}"`;
  const body = readFileSync(o.file);
  unchanged();
  return o.api.json<Entry>("PUT", `${o.base}/content`, { query: { path: o.path, note: o.note }, body, headers });
}

/** How many parts of ``partSize`` are sent at once, each from a buffer of its own. */
export function buffersFor(partSize: number, parts: number): number {
  return Math.max(1, Math.min(PARALLEL, parts, Math.floor(BUFFERED_MAX / partSize)));
}

async function putParts(
  o: PutOptions,
  begun: OpenUpload,
  hashed: { md5s: string[] },
  progress: Progress,
  unchanged: () => void,
): Promise<Entry> {
  const { api } = o;
  const upload = `${o.base}/uploads/${begun.upload_id}`;
  const partSize = begun.part_size;
  const count = begun.part_count;
  let md5s = hashed.md5s;
  if (partSize !== PART_GUESS) md5s = (await hashFile(o.file, o.size, partSize, () => {}, false)).md5s;
  const lengthOf = (n: number) => Math.min(partSize, o.size - (n - 1) * partSize);
  const held = new Set((begun.parts_held ?? []).map((p) => p.part_number));
  const todo = Array.from({ length: count }, (_, i) => i + 1).filter((n) => !held.has(n));
  const beingChecked = begun.state === "verifying";
  if (beingChecked) {
    process.stderr.write("An earlier upload of this file is already being checked; waiting for it.\n");
  } else if (held.size > 0) {
    process.stderr.write(`Continuing an earlier upload: ${held.size} of ${count} parts are already in the Drive.\n`);
  }
  let sent = [...held].reduce((sum, n) => sum + lengthOf(n), 0);
  let partsDone = held.size;
  const tick = () => progress.update(sent, `parts ${partsDone}/${count}`);
  const fh = await open(o.file, "r");
  // At least one, so parts the server reports missing have a buffer when none were left to send.
  const buffers: Buffer[] = Array.from({ length: buffersFor(partSize, todo.length) }, () => Buffer.allocUnsafe(partSize));
  const signer = new Signer(api, upload, md5s, todo);
  let direct = begun.mode === "direct";
  let unreachable = 0;

  /** Part ``n`` from ``buf``: to the store first, then through the server, each tried again while it may pass. */
  const sendPart = async (n: number, buf: Buffer) => {
    const body = buf.subarray(0, lengthOf(n));
    const offset = (n - 1) * partSize;
    unchanged();
    await readAt(fh, body, offset);
    // The store first. One that cannot be reached at all (a firewall, an
    // address that never answers) is not asked again: the rest of the file
    // goes through the server.
    for (let t = 1; direct && t <= 2; t++) {
      const status = await putSigned(await signer.get(n, t > 1), body);
      if (status !== null && status >= 200 && status < 300) {
        unreachable = 0;
        return;
      }
      if (status === null) {
        if (++unreachable >= 2) direct = false;
        break;
      }
    }
    const badDigest = (e: unknown) => e instanceof CliError && e.reason === "bad_digest";
    // A digest the server could not match is read again from disk before the next try.
    let reread = false;
    await again(
      async () => {
        if (reread) await readAt(fh, body, offset);
        reread = true;
        await api.json("PUT", `${upload}/parts/${n}`, { body, headers: { "content-md5": md5s[n - 1]! } });
      },
      (e) => transient(e) || badDigest(e),
    );
  };

  const free = [...buffers];
  const sendAll = (parts: readonly number[], counted: boolean) =>
    pool(parts, buffers.length, async (n) => {
      const buf = free.pop()!;
      try {
        await sendPart(n, buf);
      } finally {
        free.push(buf);
      }
      if (!counted) return;
      sent += lengthOf(n);
      partsDone++;
      tick();
    });

  const complete = async () => {
    for (let round = 0; ; round++) {
      try {
        unchanged();
        await again(() => api.json("POST", `${upload}/complete`));
        return;
      } catch (e) {
        const missing = missingParts(e);
        if (!missing || round >= 2) throw e;
        await sendAll(missing, false);
      }
    }
  };
  const all = Array.from({ length: count }, (_, i) => i + 1);
  /** Sends ``parts`` and completes. Another sender of the same file (a second
   *  `la drive put`) may complete it first, which ends this send: the status says how it ends. */
  const finish = async (parts: readonly number[], counted: boolean) => {
    try {
      await sendAll(parts, counted);
      await complete();
    } catch (e) {
      if (!(e instanceof CliError && e.reason === "upload_ended")) throw e;
    }
  };

  // The file stays open until the upload ends: parts the server names as missing go back through ``sendPart``.
  try {
    if (!beingChecked) await finish(todo, true);
    const checking = Date.now();
    for (let reopened = 0; ; ) {
      const st = await again(() => api.json<UploadStatus>("GET", upload));
      if (st.state === "done") {
        if (st.entry) return st.entry;
        throw new CliError("The file landed, then was changed or removed before it could be read back; see `la drive ls`");
      }
      if (st.state === "failed") throw failedWith(st.error?.type ?? "failed", st.error?.message ?? "The upload failed", o.limit);
      if (st.state === "aborted") throw new CliError("The upload was cancelled; run the command again to start it anew");
      if (st.state === "uploading") {
        // Reopened after the check began, by another sender's complete that found parts missing.
        if (++reopened > 3) throw new CliError("The server keeps asking for parts again; run the command again");
        const has = new Set((st.parts_held ?? []).map((p) => p.part_number));
        await finish(all.filter((n) => !has.has(n)), false);
        continue;
      }
      progress.note(`Checking the file on the server (${Math.round((Date.now() - checking) / 1000)}s)`);
      await sleep(1000);
    }
  } finally {
    await fh.close();
  }
}

/** An upload's status: ``parts_held`` while it is open, ``entry`` only once it is done
 *  (and absent when the file went before it was read back), ``error`` only once it failed. */
type UploadStatus =
  | { state: "uploading"; parts_held?: { part_number: number }[] }
  | { state: "verifying" | "aborted" }
  | { state: "done"; entry?: Entry }
  | { state: "failed"; error?: { type: string; message: string } };

function missingParts(e: unknown): number[] | undefined {
  if (!(e instanceof CliError) || e.reason !== "parts_missing") return undefined;
  const detail = (e.detail as { detail?: { missing_parts?: unknown } } | undefined)?.detail;
  return Array.isArray(detail?.missing_parts) ? (detail.missing_parts as number[]) : undefined;
}

interface Link {
  url: string;
  signed: boolean;
  size: number;
  sha256: string;
  name: string;
}

export interface GetOptions {
  api: Api;
  base: string;
  path: string;
  rev?: number;
  out?: string;
  force: boolean;
  quiet: boolean;
}

/** Whether a process with this id is running; one that is not ours to signal still is. */
function alive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (e) {
    return (e as { code?: string }).code === "EPERM";
  }
}

/** Own ``path`` until the returned release runs, for one download's temporary
 *  file: two writers on it would publish mixed bytes. The file names its owner,
 *  so one a killed process left behind is taken over, which is what lets the
 *  next run resume. Taking it over renames it first, so of two that find it
 *  stale only one wins. */
function ownDownload(path: string, out: string): () => void {
  for (let tries = 0; tries < 3; tries++) {
    try {
      const fd = openSync(path, "wx");
      writeSync(fd, String(process.pid));
      closeSync(fd);
      return () => rmSync(path, { force: true });
    } catch (e) {
      if ((e as { code?: string }).code !== "EEXIST") throw e;
    }
    const owner = Number(readFileSync(path, "utf8").trim());
    if (alive(owner)) {
      throw new CliError(`Another \`la drive get\` is already writing ${out}; wait for it to finish.`, Exit.conflict);
    }
    try {
      renameSync(path, `${path}.${process.pid}`);
      rmSync(`${path}.${process.pid}`, { force: true });
    } catch {
      // another process took it over first; look again
    }
  }
  throw new CliError(`Cannot take ${out}'s download file; try again.`, Exit.conflict);
}

interface PartState {
  sha256: string;
  size: number;
  chunk: number;
  done: number[];
}

/** Download a file by ranges into a temporary file beside the target, then
 *  rename it into place once its hash is the one the link names. */
export async function getFile(o: GetOptions): Promise<{ path: string; output: string; size: number }> {
  const { api, base } = o;
  const query = { path: o.path, seq: o.rev };
  const fetchLink = () => api.json<Link>("GET", `${base}/content/link`, { query });
  let link = await fetchLink();
  const out = o.out ?? basename(link.name);
  if (!o.force && existsSync(out)) throw new CliError(`${out} already exists; pass --force to overwrite it`, Exit.conflict);
  const tmp = join(dirname(out), `.${basename(out)}.la-part`);
  const stateFile = `${tmp}.json`;
  const release = ownDownload(`${tmp}.lock`, out);
  try {
    const size = link.size;
    const chunk = PART_GUESS;
    const count = Math.max(1, Math.ceil(size / chunk));

    let state: PartState | undefined;
    try {
      const saved = JSON.parse(readFileSync(stateFile, "utf8")) as PartState;
      if (saved.sha256 === link.sha256 && saved.size === size && saved.chunk === chunk && existsSync(tmp)) state = saved;
    } catch {
      // no earlier attempt, or one that cannot be trusted: start over
    }
    const resumed = state !== undefined;
    state ??= { sha256: link.sha256, size, chunk, done: [] };
    const fh = await open(tmp, resumed ? "r+" : "w");
    const progress = new Progress(`Downloading ${basename(out)}`, size, o.quiet);
    try {
      if (!resumed) await fh.truncate(size);
      const have = new Set(state.done);
      if (resumed && have.size) {
        process.stderr.write(`Continuing an earlier download: ${have.size} of ${count} ranges are already on disk.\n`);
      }
      let got = [...have].reduce((sum, i) => sum + Math.min(chunk, size - i * chunk), 0);
      const todo = Array.from({ length: count }, (_, i) => i).filter((i) => !have.has(i) && size > 0);

      let linkedAt = Date.now();
      let refreshing: Promise<void> | undefined;
      const refresh = async () => {
        refreshing ??= (async () => {
          const next = await fetchLink();
          if (next.sha256 !== state!.sha256) throw new CliError("The file changed while it was downloading; run the command again");
          link = next;
          linkedAt = Date.now();
        })().finally(() => {
          refreshing = undefined;
        });
        await refreshing;
      };

      const range = async (i: number) => {
        const start = i * chunk;
        const end = Math.min(size, start + chunk) - 1;
        let mine = 0;
        for (let attempt = 1; ; attempt++) {
          // A signed address works for five minutes; a long download asks again.
          if (link.signed && Date.now() - linkedAt > 200_000) await refresh();
          try {
            const headers = { range: `bytes=${start}-${end}` };
            // A signed address needs no credentials; the API's own route needs the caller's, so it goes through Api.
            const res = link.signed
              ? await rawResponse(new URL(link.url), { method: "GET", headers }).catch(() => {
                  throw new CliError("Cannot reach the file store");
                })
              : await api.send("GET", link.url, { headers, accept: "*/*" });
            if (res.status < 200 || res.status >= 300 || (res.status !== 206 && !(start === 0 && end === size - 1)) || !res.body) {
              await res.body?.cancel();
              if (res.status === 403 && link.signed) await refresh();
              throw new CliError(
                res.status >= 200 && res.status < 300 ? "The server did not honour the byte range" : `The file store answered ${res.status}`,
              );
            }
            const reader = res.body.getReader();
            let at = start;
            mine = 0;
            try {
              for (let piece = await reader.read(); !piece.done; piece = await reader.read()) {
                const bytes = piece.value;
                if (at + bytes.length > end + 1) throw new CliError("The server sent more bytes than asked for");
                await writeAll(fh, bytes, at);
                at += bytes.length;
                got += bytes.length;
                mine += bytes.length;
                progress.update(got, `ranges ${state!.done.length}/${count}`);
              }
            } finally {
              // A range given up on mid-body frees its connection; a finished one has nothing left to cancel.
              await reader.cancel().catch(() => {});
            }
            if (at !== end + 1) throw new CliError("The download ended early");
            return;
          } catch (e) {
            got -= mine;
            if (attempt >= ATTEMPTS) throw e;
            await sleep(1000 * attempt);
          }
        }
      };

      await pool(todo, PARALLEL, async (i) => {
        await range(i);
        state!.done.push(i);
        writeFileSync(stateFile, JSON.stringify(state));
      });
    } finally {
      await fh.close();
      progress.done();
    }

    const check = new Progress(`Checking ${basename(out)}`, size, o.quiet);
    const { sha256 } = await hashFile(tmp, size, 0, (n) => check.update(n));
    check.done();
    if (sha256 !== link.sha256) {
      rmSync(tmp, { force: true });
      rmSync(stateFile, { force: true });
      throw new CliError("The downloaded bytes did not match the file's hash, so nothing was saved. Run the command again.");
    }
    if (!o.force && existsSync(out)) throw new CliError(`${out} already exists; pass --force to overwrite it`, Exit.conflict);
    renameSync(tmp, out);
    rmSync(stateFile, { force: true });
    return { path: o.path, output: out, size };
  } finally {
    release();
  }
}
