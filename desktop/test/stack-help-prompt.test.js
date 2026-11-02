'use strict'

/** What a person pastes into a coding agent when the bundled stack will not start. */

const { test, describe } = require('node:test')
const assert = require('node:assert/strict')
const path = require('node:path')

const { SRC } = require('./stackFixture')
const { helpPrompt, HOSTED } = require(path.join(SRC, 'helpPrompt.js'))

describe('the help prompt', () => {
  const base = {
    logsDir: '/Users/me/Library/Application Support/LangAlpha OSS/logs',
    dataDir: '/Users/me/Library/Application Support/LangAlpha OSS/stack',
    source: { commit: 'abc123', modified: false },
    appVersion: '0.1.3',
    system: { platform: 'darwin', version: '26.1', arch: 'arm64' },
  }

  test('carries what failed, where the logs and data are, and the source it was built from', () => {
    const text = helpPrompt({
      ...base,
      failure: {
        step: 'migrate',
        label: 'Updating the database',
        message: 'The database update failed.',
        dataNote: 'Your data was not changed.',
        log: '/x/logs/migrate.log',
        tail: 'alembic: Multiple head revisions',
      },
    })
    assert.match(text, /Step that failed: Updating the database/)
    assert.match(text, /Error: The database update failed\./)
    assert.match(text, /^This message comes from the LangAlpha desktop app\./)
    assert.match(text, /- About their data: Your data was not changed\./)
    assert.match(text, /last lines of migrate\.log/)
    assert.match(text, /alembic: Multiple head revisions/)
    assert.match(text, /LangAlpha 0\.1\.3 on macOS 26\.1 \(arm64\)/)
    assert.match(text, /at commit abc123\. Read the code at this commit/)
    assert.match(text, /starting with supervisor\.log and migrate\.log/)
    assert.ok(text.includes(base.logsDir) && text.includes(base.dataDir))
    // It reads freely, asks before changing anything, and offers the hosted app.
    assert.match(text, /Before any change that could break the install or lose data .* wait for them to confirm\./)
    assert.match(text, /Leave the data folder as it is unless they agree to a specific change\./)
    assert.ok(text.includes(`ask whether they would rather use LangAlpha.ai, the hosted version`) && text.includes(HOSTED))
  })

  test('a fence the log cannot close, and no commit it does not know', () => {
    const text = helpPrompt({
      ...base,
      source: null,
      failure: { step: 'server', label: 'Starting LangAlpha', message: 'x', log: '/x/server.log', tail: 'a ``` b' },
    })
    assert.match(text, /\n````\na ``` b\n````/)
    assert.match(text, /The commit it was built from is not recorded, so read main\./)
  })

  test('local changes on top are said, and no failure section without a failure', () => {
    const text = helpPrompt({ ...base, source: { commit: 'abc123', modified: true }, failure: null })
    assert.match(text, /at commit abc123, with uncommitted changes on top\./)
    assert.doesNotMatch(text, /What the app showed/)
  })
})
