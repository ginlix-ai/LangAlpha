// @ts-check
// What a frame inside a page carries when its file is one of the workspace's
// (<iframe src="detail.html">), with the stand-ins (opaque.js) and none of the
// rest: the frame is the page's, with no viewer to speak to. What it loads is
// on a timeline of its own, which the page's observer never sees, so it goes up
// to the frame's parent, with what a frame inside this one sends, and the page
// reports it as its own (core.js, loads.js). Sent to '*', since an opaque
// parent has no origin to name, and only addresses on this frame's own host,
// which its parent framed it by. Each report names this document too: heard
// by the viewer itself, it says a script or a refresh took the viewer's own
// frame to this file without the view, which the viewer loads again with it.

var frameParent = window.parent;
/** @type {{ [url: string]: true }} */
var sentUp = Object.create(null);

/** @param {unknown[]} urls */
function sendUp(urls) {
  /** @type {string[]} */
  var fresh = [];
  for (var i = 0; i < urls.length; i++) {
    var address;
    try {
      address = new URL(String(urls[i]));
    } catch (e) {
      continue;
    }
    if (address.host !== location.host || sentUp[address.href]) continue;
    sentUp[address.href] = true;
    fresh.push(address.href);
  }
  if (fresh.length) frameParent.postMessage({ type: 'page:frame-loads', urls: fresh, doc: location.href }, '*');
}

// This file too, which the parent's own timeline may leave out.
sendUp([location.href]);
onLoadsHeard.push(sendUp);

if (typeof PerformanceObserver === 'function') {
  try {
    new PerformanceObserver(function (list) {
      sendUp(list.getEntries().map(function (entry) { return entry.name; }));
    }).observe({ type: 'resource', buffered: true });
  } catch (e) {
    // No resource timing here: this file's own address still goes up.
  }
}

// The window's first listener, so the frame's own script never hears a child
// frame's loads on their way up.
window.addEventListener('message', function (event) {
  var data = event.data;
  if (!data || data.type !== 'page:frame-loads' || !Array.isArray(data.urls)) return;
  /** @type {any} */
  var from = event.source;
  try {
    if (!from || from === window || from.parent !== window) return;
  } catch (e) {
    return;
  }
  event.stopImmediatePropagation();
  sendUp(data.urls);
}, true);
