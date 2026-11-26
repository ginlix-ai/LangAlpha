#!/usr/bin/env node
/**
 * Writes the GitHub release notes for the open-source desktop app to stdout.
 *
 *   node scripts/release-notes.mjs <index-dir> <tag> stable|nightly
 *
 * Reads `<index-dir>/latest.json` from make-release-index.mjs. Composed from
 * what the run actually had, read from the environment: fixed text promising
 * unsigned builds would keep saying so on the first signed one.
 *
 *   REPO                  owner/name, for the download links
 *   SIGNED_MAC, SIGNED_WIN  'true' when that platform was signed
 *   FEED                  the update feed's address, empty when there is none
 *   HOSTED_DOWNLOAD_PAGE  optional, linked as the alternative to running it
 *   SHA                   nightly only: the commit it was built from
 */
import { readFileSync } from 'node:fs'
import path from 'node:path'

const [dir, tag, kind] = process.argv.slice(2)
if (!dir || !tag || !['stable', 'nightly'].includes(kind)) {
  console.error('usage: release-notes.mjs <index-dir> <tag> stable|nightly')
  process.exit(1)
}

const env = process.env
const { artifacts } = JSON.parse(readFileSync(path.join(dir, 'latest.json'), 'utf8'))
const base = `https://github.com/${env.REPO}/releases/download/${tag}`
const LABEL = {
  'mac-arm64': 'macOS (Apple Silicon)', 'mac-x64': 'macOS (Intel)',
  'win-x64': 'Windows (x64)', 'win-arm64': 'Windows (ARM64)',
  'linux-x64': 'Linux (AppImage)', 'linux-deb': 'Linux (.deb)',
}
const signedMac = env.SIGNED_MAC === 'true'

const out = ['The LangAlpha desktop app for running LangAlpha yourself. On macOS and Linux it can run everything on your own computer, with no server to set up: on first launch it offers that or the address of a LangAlpha you already run. On Windows it asks for that address.', '']
if (kind === 'nightly') {
  out.unshift(
    `A nightly build of main at [\`${env.SHA.slice(0, 10)}\`](https://github.com/${env.REPO}/commit/${env.SHA}), made once its tests pass. It carries what was merged since the last stable release, and when it runs on your computer it upgrades the app's data to match.`,
    '',
  )
}
if (env.HOSTED_DOWNLOAD_PAGE) {
  out.push(`Prefer not to run anything yourself? The hosted app is at [${env.HOSTED_DOWNLOAD_PAGE.replace(/^https?:\/\//, '')}](${env.HOSTED_DOWNLOAD_PAGE}).`, '')
}
out.push('| Platform | Download |', '|---|---|')
for (const [k, file] of Object.entries(artifacts)) out.push(`| ${LABEL[k] || k} | [${file}](${base}/${file}) |`)
if (!signedMac) {
  out.push('', 'macOS builds are unsigned. Open the app once, then go to **System Settings > Privacy & Security** and click **Open Anyway**.')
}
if (env.SIGNED_WIN !== 'true') {
  out.push('', 'Windows builds are unsigned, so SmartScreen warns on first run. Click **More info**, then **Run anyway**.')
}

// An unsigned Mac build is built without the feed (desktop-build.yml), since
// macOS only applies an update signed by the same developer.
if (kind === 'nightly') {
  if (env.FEED) {
    out.push('', `${signedMac ? 'Installed nightlies' : 'On Windows and Linux, installed nightlies'} update themselves to the next one. To go back to stable releases, choose **Update Channel > Stable** in the app menu (the **File** menu on Windows and Linux). The app stays on this build until a stable release newer than it is out, since an older version cannot run on data a newer one has upgraded.`)
  }
} else if (!env.FEED) {
  out.push('', 'These builds do not update themselves. Watch this page for new versions.')
} else if (!signedMac) {
  out.push('', 'On Windows and Linux, installed copies update themselves. The macOS builds do not, so on a Mac watch this page for new versions.')
} else {
  out.push('', 'Installed copies update themselves, so this page is a record rather than something to watch.')
}
console.log(out.join('\n'))
