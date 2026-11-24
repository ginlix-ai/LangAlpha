// Preloaded into electron-builder by build.mjs. Its macOS signer opens every file
// in the app at once to find the binaries, tens of thousands with the bundled
// backend, which no open-file limit reliably covers. graceful-fs queues an open
// that hits EMFILE instead of failing it. Before anything loads, since modules
// keep the `fs.open` they first saw.
require('graceful-fs').gracefulify(require('fs'))
