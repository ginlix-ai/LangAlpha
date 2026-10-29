# The folder's files, by example

The desktop app and the helper talk through three files in the helper's
folder: `pairing`, which the app writes to ask for a pairing; `state`, which
the helper writes and the app reads; and `machine`, the hardware UUID of the
Mac the helper made its keys on, which the app checks before it believes
`state`. These are examples of each, exactly as written.
The helper's tests (`src/tests.rs`) and the desktop's
(`desktop/test/remote-control.test.js`) both read them, so a change to either
side's format fails the other side's tests too.
