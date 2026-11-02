// @ts-check
// The requests the document has out, for settling (core.js). Unlike the rest
// of the bridge, this part changes what the document's own code sees: it
// replaces window.fetch and XMLHttpRequest.prototype.send with wrappers that
// count a request out and back, and otherwise behave as the originals. A
// page that compares them with the native functions, or reads their source,
// sees the wrappers. Once the document has settled they count nothing more.

// Requests the page has out. A page that builds itself from data it
// fetches after load changes nothing while the data is on its way, so a
// quiet spell then is a wait, not the page done: settled there, it was
// shown and put back where the reader was while still empty, and every
// anchor counted as missing.
var inFlight = 0;

function requestBack() {
  inFlight = Math.max(0, inFlight - 1);
  settleSoon();
}

if (typeof window.fetch === 'function') {
  var pageFetch = window.fetch;
  /**
   * @type {typeof window.fetch}
   * @this {Window}
   */
  var countedFetch = function () {
    var request = pageFetch.apply(this, /** @type {any} */ (arguments));
    if (!settled) {
      inFlight++;
      // Back once its body is, which is what the page builds from; a copy
      // read here, before the page's own handler runs, leaves its body unread.
      request.then(function (response) {
        /** @type {Promise<ArrayBuffer> | null} */
        var body = null;
        try {
          body = response.clone().arrayBuffer();
        } catch (e) {
          body = null;
        }
        if (body) body.then(requestBack, requestBack);
        else requestBack();
      }, requestBack);
    }
    return request;
  };
  window.fetch = countedFetch;
}
if (window.XMLHttpRequest && window.XMLHttpRequest.prototype) {
  var pageSend = window.XMLHttpRequest.prototype.send;
  /** @this {XMLHttpRequest} */
  window.XMLHttpRequest.prototype.send = function () {
    if (settled) return pageSend.apply(this, /** @type {any} */ (arguments));
    inFlight++;
    this.addEventListener('loadend', requestBack);
    try {
      return pageSend.apply(this, /** @type {any} */ (arguments));
    } catch (e) {
      this.removeEventListener('loadend', requestBack);
      requestBack();
      throw e;
    }
  };
}
