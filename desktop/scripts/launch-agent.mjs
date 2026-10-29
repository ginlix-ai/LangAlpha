/**
 * The launchd plist for the remote-control helper, one per edition.
 *
 * Generated at package time rather than committed, because both strings in it
 * that matter follow the edition: the label, which is the login item's identity
 * to macOS, and the product name, which names the helper's folder. A committed
 * pair would be two more copies of the identity table to keep in step.
 */

/** The binary's name in `Contents/MacOS`, which `BundleProgram` resolves against the bundle. */
export const HELPER_BINARY = 'langalpha-helper'

const escapeXml = (value) => String(value).replace(/[&<>"']/g, (c) => (
  { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&apos;' }[c]
))

/**
 * The folder is spelled from `~/` because this plist ships inside the bundle and
 * is shared by every account on the Mac, so it cannot name one home. launchd
 * passes the argument through unexpanded and the helper resolves it against the
 * account it runs as, which is the same folder `helperDir()` in
 * src/remoteControl.js builds from that account's home. The log goes beside the
 * app's own, outside the folder, so undoing a failed pairing keeps the log its
 * message points to.
 */
export function launchAgentPlist({ label, productName }) {
  const args = [
    HELPER_BINARY, 'run',
    '--dir', `~/Library/Application Support/${productName}/helper`,
    '--log', `~/Library/Logs/${productName}/helper.log`,
  ]
  const string = (value) => `<string>${escapeXml(value)}</string>`
  return [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">',
    '<plist version="1.0">',
    '<dict>',
    '  <key>Label</key>',
    `  ${string(label)}`,
    '  <key>BundleProgram</key>',
    `  ${string(`Contents/MacOS/${HELPER_BINARY}`)}`,
    '  <key>ProgramArguments</key>',
    '  <array>',
    ...args.map((arg) => `    ${string(arg)}`),
    '  </array>',
    '  <key>RunAtLoad</key>',
    '  <true/>',
    '  <key>KeepAlive</key>',
    '  <true/>',
    // Otherwise launchd kills whatever is left in the job's process group each
    // time it stops the helper, and work the agent started should not end
    // because the helper restarted.
    '  <key>AbandonProcessGroup</key>',
    '  <true/>',
    // As macOS runs its own sshd. With no type launchd throttles the job's CPU
    // and I/O, and every command the agent runs here is the job's.
    '  <key>ProcessType</key>',
    `  ${string('Interactive')}`,
    '</dict>',
    '</plist>',
    '',
  ].join('\n')
}
