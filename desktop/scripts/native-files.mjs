/**
 * Native code in a tree, read off each file's headers: what format it is, and
 * per architecture slice the CPU, the Mach-O file type and the macOS it asks
 * for. The bundle's architecture and macOS checks, its thinning and the signing
 * plan are each a filter over this one reading.
 *
 * Read rather than asked of `file` or `lipo`, and rather than judged by
 * extension: Python's and Postgres's trees hold extension-less executables, and
 * `0xcafebabe` is both a universal Mach-O and a Java class, told apart by the
 * architecture count that follows.
 */
import { closeSync, lstatSync, openSync, readdirSync, readSync } from 'node:fs'
import path from 'node:path'

/**
 * Every entry under `dir` that is not a directory, as `{ file, stat }` from
 * `lstat`, so a symlink is listed and never followed. A caller after regular
 * files filters on `stat.isFile()`.
 */
export function* walkFiles(dir) {
  for (const entry of readdirSync(dir)) {
    const file = path.join(dir, entry)
    const stat = lstatSync(file)
    if (stat.isDirectory()) yield* walkFiles(file)
    else yield { file, stat }
  }
}

export const MH_OBJECT = 1
export const MH_EXECUTE = 2

const MH_MAGIC_64 = 0xfeedfacf
const MH_MAGIC = 0xfeedface
const FAT_MAGIC = 0xcafebabe
const ELF_MAGIC = 0x7f454c46
const LC_VERSION_MIN_MACOSX = 0x24
const LC_BUILD_VERSION = 0x32

// Named as Node names `process.arch`. Only the plain subtype is that
// architecture: `lipo` calls the others arm64e and x86_64h, and so does this.
const MACHO_CPU = {
  [0x0100000c]: { 0: 'arm64', 2: 'arm64e' },
  [0x01000007]: { 3: 'x64', 8: 'x86_64h' },
}
// e_machine from the ELF header: AArch64 and x86-64.
const ELF_CPU = { 183: 'arm64', 62: 'x64' }

function machoCpu(cputype, cpusubtype) {
  // The top byte of the subtype carries capability bits, not the subtype.
  const sub = cpusubtype & 0x00ffffff
  return MACHO_CPU[cputype]?.[sub] ?? `mach-${cputype.toString(16)}-${sub}`
}

function readAt(fd, offset, length) {
  const buf = Buffer.alloc(length)
  return buf.subarray(0, readSync(fd, buf, 0, length, offset))
}

/** The macOS a Mach-O header at `offset` asks for, as "major.minor", or null when it names none. */
function minMacOS(fd, offset, head) {
  if (head.length < 32) return null
  const magic = head.readUInt32LE(0)
  const commands = readAt(fd, offset + (magic === MH_MAGIC_64 ? 32 : 28), head.readUInt32LE(20))
  for (let at = 0, left = head.readUInt32LE(16); left > 0 && at + 16 <= commands.length; left--) {
    const cmd = commands.readUInt32LE(at)
    const version = cmd === LC_BUILD_VERSION ? commands.readUInt32LE(at + 12)
      : cmd === LC_VERSION_MIN_MACOSX ? commands.readUInt32LE(at + 8) : null
    if (version !== null) return `${version >>> 16}.${(version >>> 8) & 0xff}`
    at += commands.readUInt32LE(at + 4) || commands.length
  }
  return null
}

/**
 * One Mach-O image at `offset`. `cpu` comes from the fat table when there is
 * one, which is what `lipo` reports. A field the file is too short to hold is null.
 */
function machoSlice(fd, offset, cpu) {
  const head = readAt(fd, offset, 32)
  const magic = head.length >= 4 ? head.readUInt32LE(0) : 0
  if (magic !== MH_MAGIC_64 && magic !== MH_MAGIC) return { cpu: cpu ?? null, filetype: null, minOS: null }
  return {
    cpu: cpu ?? (head.length >= 12 ? machoCpu(head.readUInt32LE(4), head.readUInt32LE(8)) : null),
    filetype: head.length >= 16 ? head.readUInt32LE(12) : null,
    minOS: minMacOS(fd, offset, head),
  }
}

/**
 * A file's native code: `{ format, slices: [{ cpu, filetype, minOS }] }` with
 * format 'elf', 'macho' or 'macho-universal', or null for anything else. An ELF
 * file has one slice and no file type or macOS.
 */
export function readNative(file) {
  const fd = openSync(file, 'r')
  try {
    const head = readAt(fd, 0, 20)
    if (head.length < 8) return null
    const be = head.readUInt32BE(0)
    const le = head.readUInt32LE(0)
    if (be === FAT_MAGIC && head.readUInt32BE(4) < 30) {
      const fat = readAt(fd, 0, 8 + 20 * head.readUInt32BE(4))
      const slices = []
      for (let at = 8; at + 20 <= fat.length; at += 20) {
        slices.push(machoSlice(fd, fat.readUInt32BE(at + 8), machoCpu(fat.readUInt32BE(at), fat.readUInt32BE(at + 4))))
      }
      return { format: 'macho-universal', slices }
    }
    if (le === MH_MAGIC_64 || le === MH_MAGIC) return { format: 'macho', slices: [machoSlice(fd, 0)] }
    if (be === ELF_MAGIC) {
      const machine = head.length >= 20 ? head.readUInt16LE(18) : null
      return { format: 'elf', slices: [{ cpu: ELF_CPU[machine] ?? `elf-${machine}`, filetype: null, minOS: null }] }
    }
    return null
  } finally {
    closeSync(fd)
  }
}

/** Every regular file under `dir` that holds native code, as `{ file, stat, native }`. */
export function* nativeFiles(dir) {
  for (const { file, stat } of walkFiles(dir)) {
    if (!stat.isFile()) continue
    const native = readNative(file)
    if (native) yield { file, stat, native }
  }
}

/** Whether version "a.b" is later than "c.d". */
export function laterThan(version, floor) {
  const [major, minor] = version.split('.').map(Number)
  const [wantMajor, wantMinor] = floor.split('.').map(Number)
  return major > wantMajor || (major === wantMajor && minor > wantMinor)
}
