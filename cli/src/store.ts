import { randomBytes } from "node:crypto";
import { chmodSync, lstatSync, mkdirSync, readdirSync, readFileSync, renameSync, rmdirSync, rmSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { CliError, Exit } from "./errors.js";

export interface OAuthLogin {
  kind: "oauth";
  access_token: string;
  refresh_token?: string;
  /** Epoch ms. */
  expires_at: number;
  token_endpoint: string;
  client_id: string;
}

export interface PatLogin {
  kind: "pat";
  token: string;
}

export type Login = OAuthLogin | PatLogin;

export interface Store {
  current?: string;
  servers: Record<string, Login>;
}

// Longer than any refresh can hold the lock (its exchange times out well
// inside this), so a lock this old was left by a process that died holding it.
const LOCK_STALE_MS = 30_000;
const LOCK_POLL_MS = 50;

export function configDir(): string {
  return join(process.env.XDG_CONFIG_HOME || join(homedir(), ".config"), "langalpha");
}

/** Owner-only, including a directory an earlier version or the user made. */
function ensureConfigDir(): string {
  const dir = configDir();
  if (mkdirSync(dir, { recursive: true, mode: 0o700 }) === undefined) chmodSync(dir, 0o700);
  return dir;
}

function storePath(): string {
  return join(configDir(), "credentials.json");
}

/** The stored sign-ins, none where there is no file. A file that cannot be
 *  read is none to a reader, so a command given its own token still runs,
 *  but a writer refuses it: writing over it would drop every sign-in in it. */
function loadStore(strict: boolean): Store {
  let parsed: unknown;
  try {
    parsed = JSON.parse(readFileSync(storePath(), "utf8"));
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code === "ENOENT" || !strict) return { servers: {} };
    throw unreadable();
  }
  const store = parsed as Partial<Store> | null;
  if (
    !isRecord(store) ||
    (store.servers !== undefined && !isRecord(store.servers)) ||
    (store.current !== undefined && typeof store.current !== "string")
  ) {
    if (!strict) return { servers: {} };
    throw unreadable();
  }
  return { current: store.current, servers: store.servers ?? {} };
}

/** A JSON object; an array is an object too, but the server URLs set on one
 *  as keys would be dropped by `JSON.stringify`. */
function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function unreadable(): CliError {
  return new CliError(`${storePath()} cannot be read, so it is left as it is. Fix or remove it, then sign in again.`, Exit.error);
}

export function readStore(): Store {
  return loadStore(false);
}

/** Owner-only, and whole: written beside the file and renamed over it. */
function writeStore(store: Store): void {
  const path = join(ensureConfigDir(), "credentials.json");
  const tmp = `${path}.${process.pid}.tmp`;
  try {
    writeFileSync(tmp, JSON.stringify(store, null, 2), { mode: 0o600 });
    chmodSync(tmp, 0o600);
    renameSync(tmp, path);
  } catch (e) {
    // It holds the tokens too.
    rmSync(tmp, { force: true });
    throw e;
  }
}

/** Whoever writes the store holds its lock: a refresh finishing after
 *  ``la logout`` would otherwise write the sign-in back, and one racing
 *  ``la login`` would overwrite the new sign-in with the old one refreshed. */
export function saveLogin(baseUrl: string, login: Login, opts: { select?: boolean } = {}): Promise<void> {
  return withStoreLock(async () => putLogin(baseUrl, login, opts));
}

/** Make ``baseUrl`` the server later commands use, with no sign-in to keep:
 *  one that asks none, where a sign-in kept from before would still be sent. */
export function selectServer(baseUrl: string): Promise<void> {
  return withStoreLock(async () => {
    const store = loadStore(true);
    delete store.servers[baseUrl];
    store.current = baseUrl;
    writeStore(store);
  });
}

export function forgetLogin(baseUrl: string): Promise<boolean> {
  return withStoreLock(async () => {
    const store = loadStore(true);
    const had = baseUrl in store.servers;
    delete store.servers[baseUrl];
    if (store.current === baseUrl) store.current = undefined;
    writeStore(store);
    return had;
  });
}

/** ``saveLogin`` for a caller already holding the lock. ``select`` makes
 *  this server the one later commands use by default; a refresh leaves that
 *  choice alone. */
export function putLogin(baseUrl: string, login: Login, opts: { select?: boolean } = {}): void {
  const store = loadStore(true);
  store.servers[baseUrl] = login;
  if (opts.select) store.current = baseUrl;
  writeStore(store);
}

/** Run ``fn`` holding the store's lock.
 *
 *  The lock is a directory holding one file, named for its holder and the
 *  time it took the lock. Every step that changes who holds it is a rename
 *  from a name only one process can be moving: taking a free lock renames a
 *  prepared directory onto the lock's name, which fails while that directory
 *  holds a file, and taking over a stale lock renames the stale holder's
 *  file to the taker's, which only one taker can do, the rest finding it
 *  gone. A holder that outlived its staleness finds its file gone on the
 *  way out and leaves its successor's alone. */
export async function withStoreLock<T>(fn: () => Promise<T>): Promise<T> {
  const lock = join(ensureConfigDir(), "credentials.lock");
  const me = await acquire(lock);
  try {
    return await fn();
  } finally {
    release(lock, me);
  }
}

async function acquire(lock: string): Promise<string> {
  const id = `${process.pid}.${randomBytes(8).toString("hex")}`;
  for (;;) {
    const me = holderName(id);
    if (takeFree(lock, me)) return me;
    const legacy = fileLockAge(lock);
    if (legacy !== undefined) {
      // A lock file an earlier build left: waited out while it may be live.
      if (legacy > LOCK_STALE_MS) {
        try {
          rmSync(lock);
        } catch {
          // Removed, or replaced by a directory lock, meanwhile.
        }
      } else {
        await sleep(LOCK_POLL_MS);
      }
      continue;
    }
    const held = holders(lock);
    if (held === undefined) {
      // Let go between the two looks; waiting a beat keeps an error that
      // persists from spinning.
      await sleep(LOCK_POLL_MS);
      continue;
    }
    const [holder] = held;
    if (holder === undefined) {
      // Empty: a holder between removing its file and the directory, or one
      // that died there. Removing an empty directory frees nobody's lock.
      try {
        rmdirSync(lock);
      } catch {
        // Taken or removed meanwhile.
      }
      continue;
    }
    if (Date.now() - takenAt(holder) > LOCK_STALE_MS) {
      try {
        renameSync(join(lock, holder), join(lock, me));
        return me;
      } catch {
        // Another taker moved it first, or its holder let go.
        continue;
      }
    }
    await sleep(LOCK_POLL_MS);
  }
}

/** How long ago a plain file at the lock's name was written, or undefined
 *  when there is none. */
function fileLockAge(lock: string): number | undefined {
  try {
    const st = lstatSync(lock);
    return st.isFile() ? Date.now() - st.mtimeMs : undefined;
  } catch {
    return undefined;
  }
}

/** Take the lock if no one holds it: a directory prepared with our file is
 *  renamed onto the lock's name, which fails while one with a file is there. */
function takeFree(lock: string, me: string): boolean {
  const staged = `${lock}.${me}`;
  mkdirSync(staged, { mode: 0o700 });
  writeFileSync(join(staged, me), "", { mode: 0o600 });
  try {
    renameSync(staged, lock);
    return true;
  } catch (e) {
    const code = (e as NodeJS.ErrnoException).code;
    rmSync(staged, { recursive: true, force: true });
    // Windows refuses to rename onto any existing directory, with EPERM.
    // ENOTDIR: an earlier build's lock file is in the way.
    if (code === "ENOTEMPTY" || code === "EEXIST" || code === "EPERM" || code === "ENOTDIR") return false;
    throw e;
  }
}

function release(lock: string, me: string): void {
  try {
    rmSync(join(lock, me));
  } catch {
    // Taken over as stale: the lock is its successor's now.
    return;
  }
  try {
    rmdirSync(lock);
  } catch {
    // Already taken by the next holder.
  }
}

/** The lock's files, or undefined when the lock is gone. */
function holders(lock: string): string[] | undefined {
  try {
    return readdirSync(lock);
  } catch {
    return undefined;
  }
}

// The time goes in the name, since a takeover by rename keeps the mtime of
// the file it moves.
function holderName(id: string): string {
  return `${Date.now()}.${id}`;
}

function takenAt(holder: string): number {
  const at = Number(holder.split(".", 1)[0]);
  // A name not of this form was not written by a live holder.
  return Number.isFinite(at) ? at : 0;
}
