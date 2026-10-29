'use strict'

const fs = require('node:fs/promises')
const path = require('node:path')
const { app, dialog, ipcMain, BrowserWindow } = require('electron')
const notify = require('./notify')

// Electron's own enum. A name outside it throws inside printToPDF, and the
// value arrives from a page loaded over the network, so it is checked here
// rather than taken on trust.
const PAGE_SIZES = new Set([
  'A0', 'A1', 'A2', 'A3', 'A4', 'A5', 'A6', 'Legal', 'Letter', 'Tabloid', 'Ledger',
])

const MAX_STEM = 120

const clamp = (value, lo, hi) => Math.min(hi, Math.max(lo, value))

/**
 * One export at a time, decided here rather than in the page.
 *
 * The page has a guard of its own, but that one is advice: this channel is
 * reachable from any script the window is running, and every call renders a
 * whole PDF and then parks on a modal dialog, so a loop of them stacks native
 * dialogs and buffers with nothing to stop it.
 *
 * It is also what makes the background swap below safe. That reads the window's
 * colour, stands white in for it and puts the original back; two renders
 * overlapping would have the second read white as the original and restore it
 * permanently, leaving the window on white paper for the rest of the session.
 */
let rendering = false

/**
 * Translate the page's request into printToPDF options.
 *
 * Allowlisted rather than forwarded: an unrecognised or out-of-range field is
 * not ignored by Chromium, it throws, and the request arrives from a page
 * loaded over the network. Every value the page can influence is matched
 * against a known set or clamped, so a malformed request degrades to the
 * defaults here instead of failing the export.
 */
function normalizeOptions(raw) {
  const input = raw && typeof raw === 'object' ? raw : {}
  const options = {
    // The document's own `@page` rule is the layout its preview was measured
    // from, so it outranks anything named here; `pageSize` below is only the
    // answer for a document that declares none.
    preferCSSPageSize: true,
    printBackground: input.printBackground !== false,
    // The two things a browser's print dialog cannot produce, and much of the
    // reason this path exists: a tagged reading order and a real outline built
    // from the heading tree. Both default on; a caller can still decline.
    generateTaggedPDF: input.tagged !== false,
    generateDocumentOutline: input.outline !== false,
    landscape: input.landscape === true,
  }
  if (typeof input.pageSize === 'string' && PAGE_SIZES.has(input.pageSize)) {
    options.pageSize = input.pageSize
  }
  if (typeof input.scale === 'number' && Number.isFinite(input.scale)) {
    options.scale = clamp(input.scale, 0.1, 2)
  }
  if (typeof input.pageRanges === 'string' && /^[0-9,\s-]{1,64}$/.test(input.pageRanges)) {
    options.pageRanges = input.pageRanges
  }
  return options
}

/**
 * A filename safe to hand the save dialog as its opening suggestion.
 *
 * The name comes from a document title the agent wrote, so it is neither
 * trusted nor tidy. Separators are replaced rather than stripped with
 * `basename`: that makes the result something that cannot be a path at all,
 * which is a stronger statement than removing the directory part, and it keeps
 * the whole of an ordinary title. `basename` looked equivalent and was not:
 * it turned "profit/loss review" into "loss review". The same replacement
 * covers the characters that are legal on one desktop and rejected on another,
 * so the same export does not fail only on Windows.
 *
 * The extension comes off before the leading run does, or a name of `.pdf`
 * leaves `pdf` behind and saves as `pdf.pdf`.
 */
function safeFileName(raw) {
  const cleaned = (typeof raw === 'string' ? raw : '')
    .replace(/[\u0000-\u001f\u007f]/g, '')
    .replace(/[\\/:*?"<>|]/g, '-')
    .trim()
  const stem = cleaned
    .replace(/\.pdf$/i, '')
    .replace(/^[-.\s]+/, '')
    .slice(0, MAX_STEM)
    .trim()
  return `${stem || 'export'}.pdf`
}

/**
 * Render the window's contents to PDF bytes, on white paper.
 *
 * printToPDF composites the page over the window's own background colour, which
 * the shell sets from the theme (`theme.js`, #191919 in dark). Nothing paints
 * the margin area (a page's canvas background does not reach it), so whatever
 * is behind the window shows through, and every export from a dark window came
 * out on dark paper with the text block floating in it. White for the duration
 * of the render, put back immediately after.
 */
async function renderToBuffer(win, contents, options) {
  const previous = win.getBackgroundColor()
  win.setBackgroundColor('#ffffff')
  try {
    return await contents.printToPDF(options)
  } finally {
    // A long report can outlive its window. Restoring a colour on a window that
    // is gone throws from a `finally`, which would replace whatever the render
    // actually failed with by an unrelated 'Object has been destroyed'.
    if (!win.isDestroyed()) win.setBackgroundColor(previous)
  }
}

/**
 * Render the calling window to a PDF the user picks a home for.
 *
 * Prints the sender's own contents, never markup passed in: the page cannot
 * name a URL or hand over HTML, so this adds no way to render something the
 * window was not already showing. Rendering happens before the dialog opens
 * because `printToPDF` composes an off-screen print layout and leaves the
 * visible window untouched, so the user never watches their page reflow.
 *
 * Answers with an outcome instead of throwing. A rejected `invoke` reaches the
 * page as an opaque error it cannot tell from a shell too old to know this
 * channel, and the caller's fallback to browser print depends on telling those
 * apart. The saved path stays on this side; the page gets no filesystem detail.
 */
async function savePdf(event, request) {
  const win = BrowserWindow.fromWebContents(event.sender)
  if (!win || win.isDestroyed()) return { error: 'This window is closing.' }
  // `canceled`, matching the page's own re-entry answer: nothing was exported,
  // and the caller must not respond by opening a second dialog on top of the
  // one already up.
  if (rendering) return { canceled: true }
  rendering = true

  try {
    let data
    try {
      data = await renderToBuffer(win, event.sender, normalizeOptions(request))
    } catch (err) {
      console.error(`[pdf] render failed: ${err.message}`)
      return { error: `Could not render the PDF: ${err.message}` }
    }

    const suggestion = safeFileName(request && request.fileName)
    let chosen
    try {
      // Inside the try for the same reason the render is: this function answers
      // with an outcome, and a dialog that throws (a window closed while its
      // sheet was up) would escape as a rejected `invoke`, which the page cannot
      // tell from a shell too old to know the channel.
      chosen = await dialog.showSaveDialog(win, {
        defaultPath: path.join(app.getPath('downloads'), suggestion),
        filters: [{ name: 'PDF', extensions: ['pdf'] }],
      })
    } catch (err) {
      console.error(`[pdf] save dialog failed: ${err.message}`)
      return { error: 'Could not open the save dialog.' }
    }
    if (chosen.canceled || !chosen.filePath) return { canceled: true }
    const filePath = chosen.filePath

    // Staged and renamed rather than written in place. `writeFile` truncates
    // first, so a volume that filled or was pulled mid-write left a damaged file
    // wearing a .pdf name exactly where the user asked for one, while the UI
    // said the export had failed. Worse when they were saving over a previous
    // export: the file destroyed that way was one they already had. A sibling
    // keeps the rename on one filesystem, which is what makes it replace in one
    // step.
    const staged = `${filePath}.${process.pid}-${Date.now()}.part`
    try {
      await fs.writeFile(staged, data)
      await fs.rename(staged, filePath)
    } catch (err) {
      // The code, never the message. Node puts the full path in `err.message`,
      // and the page is remote: handing it back would publish the user's home
      // directory and account name over the same channel this function's
      // contract says keeps the path on this side.
      console.error(`[pdf] write failed: ${err.message}`)
      await fs.rm(staged, { force: true }).catch(() => {})
      return { error: `Could not save the file (${err.code || 'unknown error'}).` }
    }

    notify.fileLanded(filePath)
    console.log(`[pdf] saved ${suggestion} (${data.length} bytes)`)
    return { saved: true }
  } finally {
    rendering = false
  }
}

function registerIpc() {
  ipcMain.handle('shell:save-pdf', savePdf)
}

module.exports = { registerIpc, savePdf, normalizeOptions, safeFileName, renderToBuffer }
