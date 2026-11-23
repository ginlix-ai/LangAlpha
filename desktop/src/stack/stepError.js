'use strict'

/** A step that failed, with what the user should read and whose log to show. */
class StepError extends Error {
  constructor(step, message, { log = null, dataNote = null } = {}) {
    super(message)
    this.step = step
    this.log = log
    this.dataNote = dataNote
  }
}

module.exports = { StepError }
