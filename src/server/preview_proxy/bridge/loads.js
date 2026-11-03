// @ts-check
// Every workspace file this document has loaded, and every address it loaded
// from the root of its host (loadedPath), for the publish dialog: a path
// built in code, 'data/' + ticker + '.csv', is known only once it runs, so no
// scan of the source can name it. A page can claim anything here, and the claim
// only adds a line to the list its owner reviews.

/** @type {{ [path: string]: true }} */
var loads = Object.create(null);
/** @type {typeof import('../../../../web/src/lib/pageLoads').MAX_PAGE_LOADS} */
var MAX_PAGE_LOADS = 500;

// Named by the file the document says it is, since a tab may hold another
// spelling of the path. Held until the document settles: each report costs the
// viewer a read of the whole file list, and a page still loading would pay it
// once per batch.
function reportLoads() {
  var path = docPath();
  var paths = Object.keys(loads);
  if (settled && path && paths.length) send({ type: 'page:loads', path: path, paths: paths });
}

/**
 * How the list names an address the document loaded: a workspace file by its
 * path, which never starts with "/"; an address at the root of the document's
 * own host, under no route that serves files ('/assets/' + theme + '.json'),
 * by its path from there, "/" first. No view of a workspace file serves that
 * one, so the review warns of it and never takes it for a file. Anything else
 * is not the review's.
 * @param {URL} address
 * @returns {string | null}
 */
function loadedPath(address) {
  var file = ownerFile(address);
  if (file !== null) return file;
  if (address.protocol !== location.protocol || address.host !== location.host) return null;
  if (grantPrefixOf(address.pathname) !== null) return null;
  try {
    return decodeURIComponent(address.pathname);
  } catch (e) {
    return null;
  }
}

/**
 * @param {unknown[]} urls Absolute, as the browser names what it loaded; a
 *   worker's and a framed file's come by message, so anything may be here.
 * @returns {boolean} Whether the list grew.
 */
function noteUrls(urls) {
  var grew = false;
  for (var i = 0; i < urls.length; i++) {
    var address;
    try {
      address = new URL(String(urls[i]));
    } catch (e) {
      continue;
    }
    var path = loadedPath(address);
    if (path && !loads[path] && Object.keys(loads).length < MAX_PAGE_LOADS) {
      loads[path] = true;
      grew = true;
    }
  }
  return grew;
}

/** @param {PerformanceEntry[]} entries */
function noteLoads(entries) {
  return noteUrls(entries.map(function (entry) { return entry.name; }));
}

if (ownerView) {
  /** @type {PerformanceObserver | null} */
  var loadObserver = null;
  if (typeof PerformanceObserver === 'function') {
    try {
      loadObserver = new PerformanceObserver(function (list) {
        if (noteLoads(list.getEntries())) reportLoads();
      });
      loadObserver.observe({ type: 'resource', buffered: true });
    } catch (e) {
      // No resource timing here: the scan of the source still names what it can.
      loadObserver = null;
    }
  }
  // What the page's workers and the files it frames loaded (opaque.js, core.js).
  onLoadsHeard.push(function (urls) {
    if (noteUrls(urls)) reportLoads();
  });
  onSpoke.push(reportLoads);
  // An entry waits for the observer's next turn, and the viewer takes settled
  // to mean the list is whole.
  onSettled.push(function () {
    if (loadObserver) noteLoads(loadObserver.takeRecords());
    reportLoads();
  });
}
