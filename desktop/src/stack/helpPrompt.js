'use strict'

const path = require('node:path')

const REPO = 'https://github.com/ginlix-ai/langalpha'
const HOSTED = 'https://langalpha.ai'
const HOSTED_DOWNLOAD = 'https://langalpha.ai/download'

const SYSTEM_NAMES = { darwin: 'macOS', linux: 'Linux' }

/** A fence no run of backticks in the text can close early. */
function fenced(text) {
  const longest = Math.max(2, ...(text.match(/`+/g) || []).map((run) => run.length))
  const fence = '`'.repeat(longest + 1)
  return `${fence}\n${text}\n${fence}`
}

/**
 * What a person pastes into a coding agent when the bundled stack will not start.
 *
 * It speaks for the app to the agent, about the person, so the agent knows who
 * wrote it and whom it is helping. The agent runs on the same machine, so it is
 * pointed at the log files rather than handed them, and given the source the
 * install was built from. It reads freely and asks before any change, because
 * the data folder holds the person's only copy of their database and "reset it
 * and start over" is the first fix an agent reaches for.
 */
function helpPrompt({ failure, logsDir, dataDir, source, appVersion, system }) {
  const lines = [
    'This message comes from the LangAlpha desktop app. LangAlpha could not start the local server it runs on this computer, and the person who pasted this message would like your help finding out why and fixing it. You run on the same computer, so you can read the files named below.',
    '',
  ]

  if (failure) {
    lines.push('What the app showed them:')
    lines.push(`- Step that failed: ${failure.label || failure.step}`)
    if (failure.message) lines.push(`- Error: ${failure.message}`)
    if (failure.dataNote) lines.push(`- About their data: ${failure.dataNote}`)
    if (failure.tail) {
      lines.push('', `The last lines of ${failure.log ? path.basename(failure.log) : 'its log'}:`, fenced(failure.tail))
    }
    lines.push('')
  }

  const where = SYSTEM_NAMES[system.platform] || system.platform
  lines.push('The install:')
  lines.push(`- LangAlpha ${appVersion} on ${where}${system.version ? ` ${system.version}` : ''} (${system.arch})`)
  if (source) {
    lines.push(`- Source: ${REPO} at commit ${source.commit}${source.modified ? ', with uncommitted changes on top' : ''}. Read the code at this commit, since main may differ.`)
  } else {
    lines.push(`- Source: ${REPO}. The commit it was built from is not recorded, so read main.`)
  }
  lines.push('- The desktop app starts everything from desktop/src/stack/; the server is in src/.')
  lines.push(`- Logs: ${logsDir}. supervisor.log records each start step by step; server.log, postgres.log, migrate.log and valkey.log are the processes it runs.`)
  lines.push(`- Data: ${dataDir}. Their database and the app's settings live here.`)
  lines.push('')

  lines.push('How to help:')
  lines.push(`1. Read the logs, starting with supervisor.log${failure && failure.log ? ` and ${path.basename(failure.log)}` : ''}, and the source of the step that failed, until you can explain the failure. Reading needs no permission.`)
  lines.push('2. Report to them what went wrong and what you propose to do. Before any change that could break the install or lose data (editing or deleting files, running a database command, stopping processes, reinstalling), wait for them to confirm.')
  lines.push(`3. If the fix is on their machine, carry it out once they confirm, or walk them through it. If it is a bug in LangAlpha, draft an issue for ${REPO}/issues and show it to them before filing it.`)
  lines.push('')

  lines.push(`When you report, also ask whether they would rather use LangAlpha.ai, the hosted version, which runs in the cloud with nothing to install or maintain: ${HOSTED}, with a desktop app at ${HOSTED_DOWNLOAD}. If they want to keep running it themselves, carry on with the fix.`)
  lines.push('')

  lines.push('Hard rules:')
  lines.push('- Leave the data folder as it is unless they agree to a specific change. It holds their only copy of the database, so deleting or resetting it loses everything they have done in LangAlpha.')
  lines.push('- Never show the contents of secrets.json or any password, key or token from the logs, since anything you print stays in this chat.')

  return lines.join('\n')
}

module.exports = { helpPrompt, REPO, HOSTED }
