'use strict'

const fs = require('node:fs')
const path = require('node:path')

const MAX_BYTES = 10 * 1024 * 1024
const KEEP = 3
const TAIL_LINES = 40

/**
 * One process's log file, rotated by size, with its last lines kept in memory.
 *
 * In memory as well as on disk because the error screen has to show a tail the
 * moment a step fails, and a turn of heavy logging must not be able to fill a
 * user's disk: three old files at ten megabytes is the most any one process can
 * leave behind.
 */
function createLog(dir, name, { maxBytes = MAX_BYTES, keep = KEEP } = {}) {
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 })
  const file = path.join(dir, `${name}.log`)
  let fd = fs.openSync(file, 'a', 0o600)
  let size = fs.fstatSync(fd).size
  const lines = []
  let partial = ''

  // The fd is forgotten as it is closed, because the next file anything opens can
  // take its number, and a write to it would land in that file. A log deleted
  // while the app runs has nothing to rename, and logging carries on in a new one.
  const rotate = () => {
    const closing = fd
    fd = null
    try {
      fs.closeSync(closing)
      for (let i = keep - 1; i >= 1; i--) {
        const from = `${file}.${i}`
        if (fs.existsSync(from)) fs.renameSync(from, `${file}.${i + 1}`)
      }
      try {
        fs.renameSync(file, `${file}.1`)
      } catch (err) {
        if (err.code !== 'ENOENT') throw err
      }
    } finally {
      fd = fs.openSync(file, 'a', 0o600)
      size = 0
    }
  }

  const remember = (text) => {
    const pieces = (partial + text).split('\n')
    partial = pieces.pop()
    for (const line of pieces) lines.push(line)
    if (lines.length > TAIL_LINES) lines.splice(0, lines.length - TAIL_LINES)
  }

  return {
    file,
    write(chunk) {
      const text = Buffer.isBuffer(chunk) ? chunk.toString('utf8') : String(chunk)
      remember(text)
      try {
        if (fd !== null && size + Buffer.byteLength(text) > maxBytes) rotate()
        if (fd !== null) size += fs.writeSync(fd, text)
      } catch (err) {
        // A full disk must not take the app down from inside a log write. The
        // tail above still has it for the error screen.
        console.error(`[stack] could not write ${name}.log: ${err.message}`)
      }
    },
    note(text) {
      this.write(`[${new Date().toISOString()}] ${text}\n`)
    },
    tail(count = TAIL_LINES) {
      const all = partial ? [...lines, partial] : lines
      return all.slice(-count).join('\n')
    },
    close() {
      if (fd === null) return
      try { fs.closeSync(fd) } catch { /* already closed */ }
      fd = null
    },
  }
}

module.exports = { createLog }
