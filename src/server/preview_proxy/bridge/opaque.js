// @ts-check
// A document on an opaque origin (the owner's live view, and anything served
// on the API origin) is refused storage and workers from its own files, by the
// browser and whatever the policy says. Most pages read localStorage for
// nothing more than a theme, and the refusal is an exception that stops the
// rest of their script, so here it gets stand-ins that last as long as the
// document. IndexedDB has no stand-in small enough to carry in every page, so
// it reads as absent: a library that probes for it keeps to localStorage
// rather than meeting the refusal. A frame on an origin of its own keeps
// storage, but a browser drops its cookies as cross-site, with no exception to
// tell the page, so a cookie gets one too.

// What a worker the page started, or a frame inside the page, loaded, and the
// requests a worker has out: each is on a timeline of its own, which the
// document's observer never sees. Heard by the part that reports loads
// (loads.js, or nested.js in a frame) and the one that counts requests
// (requests.js).
/** @type {Array<(urls: unknown[]) => void>} */
var onLoadsHeard = [];
/** @type {Array<(delta: number) => void>} */
var onWorkerRequests = [];

/** @param {unknown[]} urls */
function heardLoads(urls) {
  for (var i = 0; i < onLoadsHeard.length; i++) onLoadsHeard[i](urls);
}

/** @returns {Storage} */
function memoryStorage() {
  /** @type {Map<string, string>} */
  var items = new Map();
  var methods = {
    /** @param {string} key */
    getItem: function (key) {
      var value = items.get(String(key));
      return value === undefined ? null : value;
    },
    /** @param {string} key @param {string} value */
    setItem: function (key, value) {
      items.set(String(key), String(value));
    },
    /** @param {string} key */
    removeItem: function (key) {
      items.delete(String(key));
    },
    clear: function () {
      items.clear();
    },
    /** @param {number} index */
    key: function (index) {
      var key = Array.from(items.keys())[index];
      return key === undefined ? null : key;
    },
  };
  // A proxy because pages also write localStorage.theme = 'dark'.
  return /** @type {Storage} */ (/** @type {unknown} */ (new Proxy(methods, {
    get: function (target, key) {
      if (key === 'length') return items.size;
      if (key in target) return /** @type {any} */ (target)[key];
      return typeof key === 'string' ? items.get(key) : undefined;
    },
    set: function (_target, key, value) {
      items.set(String(key), String(value));
      return true;
    },
    has: function (target, key) {
      return key in target || items.has(String(key));
    },
    deleteProperty: function (_target, key) {
      items.delete(String(key));
      return true;
    },
    ownKeys: function () {
      return Array.from(items.keys());
    },
    getOwnPropertyDescriptor: function (_target, key) {
      var value = items.get(String(key));
      if (value === undefined) return undefined;
      return { value: value, enumerable: true, configurable: true, writable: true };
    },
  })));
}

/**
 * Whether a cookie's attributes end it, read as a browser reads them: a
 * Max-Age of zero or less, else an Expires already past. Max-Age wins over
 * Expires, the last of each counts, and a value that is no number or date is
 * no attribute at all.
 * @param {string[]} attributes
 */
function cookieEnded(attributes) {
  /** @type {number | null} */
  var maxAge = null;
  /** @type {number | null} */
  var expires = null;
  for (var i = 0; i < attributes.length; i++) {
    var at = attributes[i].indexOf('=');
    var key = (at < 0 ? attributes[i] : attributes[i].slice(0, at)).trim().toLowerCase();
    var value = at < 0 ? '' : attributes[i].slice(at + 1).trim();
    if (key === 'max-age' && /^-?[0-9]+$/.test(value)) maxAge = Number(value);
    else if (key === 'expires' && !isNaN(Date.parse(value))) expires = Date.parse(value);
  }
  return maxAge !== null ? maxAge <= 0 : expires !== null && expires <= Date.now();
}

function standInCookie() {
  /** @type {Map<string, string>} */
  var jar = new Map();
  Object.defineProperty(document, 'cookie', {
    configurable: true,
    get: function () {
      return Array.from(jar, function (pair) { return pair[0] + '=' + pair[1]; }).join('; ');
    },
    /** @param {string} value */
    set: function (value) {
      var parts = String(value).split(';');
      var at = parts[0].indexOf('=');
      var name = (at < 0 ? '' : parts[0].slice(0, at)).trim();
      if (cookieEnded(parts.slice(1))) jar.delete(name);
      else jar.set(name, parts[0].slice(at + 1).trim());
    },
  });
}

// An opaque origin may only start a worker from a blob of its own, which then
// loads the real script. Inside, the worker's address is the blob's, which no
// relative URL resolves against, so the blob first gives the worker the
// script's address as its location and points its own requests there
// (workerPrelude). What the worker loads and
// has out it says in messages under a name this document picked, which the
// stand-in takes before any listener of the page's hears them.
var WORKER_TAG = '__page_worker_' + Math.random().toString(36).slice(2);

/**
 * Runs in the worker, from its source text, ahead of the page's script.
 * @param {string} script The page's script, absolute.
 * @param {string} tag
 * @param {boolean} module
 * @param {boolean} shared Whether it stands in for a shared worker.
 */
function workerPrelude(script, tag, module, shared) {
  /** @type {any} */
  var scope = self;
  /** @param {{ urls?: string[], requests?: number, failed?: true }} said */
  function say(said) {
    /** @type {{ [tag: string]: unknown }} */
    var message = {};
    message[tag] = said;
    scope.postMessage(message);
  }
  function back() {
    say({ requests: -1 });
  }
  // A URL that does not parse goes as it was, for the browser to refuse.
  /** @param {unknown} url */
  function absolute(url) {
    if (typeof url !== 'string' && !(url instanceof URL)) return url;
    try {
      return new URL(String(url), script).href;
    } catch (e) {
      return url;
    }
  }
  // Where a real worker of this script is, for a script that resolves its own
  // paths against self.location (an Emscripten build finds its .wasm so).
  var here = new URL(script);
  var Place = scope.WorkerLocation;
  /** @type {any} */
  var place = Object.create(typeof Place === 'function' ? Place.prototype : Object.prototype);
  ['href', 'origin', 'protocol', 'host', 'hostname', 'port', 'pathname', 'search', 'hash'].forEach(function (key) {
    Object.defineProperty(place, key, { value: /** @type {any} */ (here)[key], enumerable: true });
  });
  Object.defineProperty(place, 'toString', { value: function () { return here.href; } });
  try {
    Object.defineProperty(scope, 'location', { value: place, configurable: true, enumerable: true });
  } catch (e) {
    // Kept as it was: the requests below still resolve against the script.
  }
  /** @param {IArguments} args @param {number} at */
  function resolved(args, at) {
    var list = Array.prototype.slice.call(args);
    if (list.length > at) list[at] = absolute(list[at]);
    return list;
  }

  var NativeRequest = scope.Request;
  if (typeof NativeRequest === 'function') {
    /** @param {unknown} _input @param {RequestInit} [_init] */
    var AbsoluteRequest = function (_input, _init) {
      return Reflect.construct(NativeRequest, resolved(arguments, 0), new.target || NativeRequest);
    };
    AbsoluteRequest.prototype = NativeRequest.prototype;
    scope.Request = AbsoluteRequest;
  }
  // Counted out and back as requests.js counts the document's, so the
  // document's settling waits for what its worker fetches.
  var nativeFetch = scope.fetch;
  if (typeof nativeFetch === 'function') {
    scope.fetch = function () {
      var request = nativeFetch.apply(this, resolved(arguments, 0));
      say({ requests: 1 });
      request.then(function (/** @type {Response} */ response) {
        /** @type {Promise<ArrayBuffer> | null} */
        var body = null;
        try {
          body = response.clone().arrayBuffer();
        } catch (e) {
          body = null;
        }
        if (body) body.then(back, back);
        else back();
      }, back);
      return request;
    };
  }
  var XHR = scope.XMLHttpRequest;
  if (XHR && XHR.prototype) {
    var open = XHR.prototype.open;
    XHR.prototype.open = function () {
      return open.apply(this, resolved(arguments, 1));
    };
    var send = XHR.prototype.send;
    XHR.prototype.send = function () {
      say({ requests: 1 });
      this.addEventListener('loadend', back);
      try {
        return send.apply(this, arguments);
      } catch (e) {
        this.removeEventListener('loadend', back);
        back();
        throw e;
      }
    };
  }
  var nativeImport = scope.importScripts;
  if (typeof nativeImport === 'function') {
    scope.importScripts = function () {
      return nativeImport.apply(this, Array.prototype.map.call(arguments, absolute));
    };
  }
  try {
    new scope.PerformanceObserver(function (/** @type {PerformanceObserverEntryList} */ list) {
      say({ urls: list.getEntries().map(function (entry) { return entry.name; }) });
    }).observe({ type: 'resource', buffered: true });
  } catch (e) {
    // No resource timing in this worker: the stand-in noted the script itself.
  }

  // Each of a shared worker's connections is a port sent in under the tag
  // (standInSharedWorker), dispatched as the connect event its global would
  // get, once the script has run to hear it.
  /** @type {MessagePort[] | null} */
  var early = [];
  /** @param {MessagePort} port */
  function connect(port) {
    var event = new scope.MessageEvent('connect', { data: '', ports: [port], source: port });
    scope.dispatchEvent(event);
    if (typeof scope.onconnect === 'function') scope.onconnect(event);
  }
  if (shared) {
    // Declared, as a shared worker's global declares it, so a module's
    // `onconnect = ...` assigns rather than throws.
    scope.onconnect = null;
    scope.addEventListener('message', function (/** @type {MessageEvent} */ event) {
      var said = event.data && event.data[tag];
      if (!said || !said.connect) return;
      event.stopImmediatePropagation();
      if (early) early.push(event.ports[0]);
      else connect(event.ports[0]);
    });
  }
  function ran() {
    back();
    var waiting = early || [];
    early = null;
    waiting.forEach(connect);
  }
  function failed() {
    back();
    say({ failed: true });
  }

  // The stand-in counts the worker out until its script has run, so a request
  // the script makes at once is counted before the start is back.
  if (!module) {
    try {
      scope.importScripts(script);
    } catch (error) {
      failed();
      throw error;
    }
    ran();
    return;
  }
  // A module is imported from a classic worker (startWorker). A failure is
  // thrown again outside the promise, so the worker's error event fires as it
  // would.
  import(script).then(ran, function (error) {
    failed();
    setTimeout(function () {
      throw error;
    });
  });
}

/**
 * The worker's own messages, which the page never hears: this listener is the
 * worker's first. What the worker has out is counted from its start.
 * @param {Worker} worker
 * @param {() => void} [failed] Told when its script never ran.
 */
function hearWorker(worker, failed) {
  var out = 0;
  /** @param {number} delta */
  function count(delta) {
    delta = Math.max(delta, -out);
    if (!delta) return;
    out += delta;
    for (var i = 0; i < onWorkerRequests.length; i++) onWorkerRequests[i](delta);
  }
  worker.addEventListener('message', function (event) {
    var said = event.data && event.data[WORKER_TAG];
    if (!said) return;
    event.stopImmediatePropagation();
    if (Array.isArray(said.urls)) heardLoads(said.urls);
    if (typeof said.requests === 'number') count(said.requests);
    if (said.failed && failed) failed();
  });
  // Ended, a worker says nothing more, so what it had out is back.
  var terminate = worker.terminate;
  worker.terminate = function () {
    count(-out);
    return terminate.call(worker);
  };
  count(1);
}

var NativeWorker = window.Worker;

/**
 * The page's script in a worker started from a blob of this document's.
 * @param {URL} url
 * @param {WorkerOptions} options
 * @param {boolean} shared
 * @param {() => void} [failed]
 */
function startWorker(url, options, shared, failed) {
  var source = '(' + String(workerPrelude) + ')(' + JSON.stringify(url.href) + ', '
    + JSON.stringify(WORKER_TAG) + ', ' + (options.type === 'module') + ', ' + shared + ');';
  // Classic either way: Chromium starts no module worker from a blob on an
  // opaque origin, and a classic one may import a module.
  var classic = Object.assign({}, options, { type: 'classic' });
  var worker = new NativeWorker(URL.createObjectURL(new Blob([source], { type: 'text/javascript' })), classic);
  hearWorker(worker, failed);
  heardLoads([url.href]);
  return worker;
}

function standInWorker() {
  /**
   * @param {string | URL} script
   * @param {WorkerOptions} [options]
   */
  function Worker(script, options) {
    var url = new URL(String(script), document.baseURI);
    if (url.protocol === 'blob:' || url.protocol === 'data:') return new NativeWorker(script, options);
    return startWorker(url, Object.assign({}, options), false);
  }
  Worker.prototype = NativeWorker.prototype;
  window.Worker = /** @type {any} */ (Worker);
}

// A shared worker is refused there too, so each is a dedicated one of this
// document's, one per script and name as a browser keeps one per origin. Each
// construction is a channel: one end is the page's `port`, the other goes in
// to arrive as the worker's connect event (workerPrelude).
function standInSharedWorker() {
  if (typeof window.SharedWorker !== 'function') return;
  /** @type {Map<string, { worker: Worker, connected: any[] }>} */
  var running = new Map();
  /**
   * @param {string | URL} script
   * @param {string | WorkerOptions} [options]
   */
  function SharedWorker(script, options) {
    /** @type {WorkerOptions} */
    var given = typeof options === 'string' ? { name: options } : Object.assign({}, options);
    var url = new URL(String(script), document.baseURI);
    var key = url.href + '\n' + (given.name || '');
    var instance = running.get(key);
    if (!instance) {
      /** @type {any[]} */
      var connected = [];
      // A script that never ran is every connection's error, and the next
      // construction starts it afresh.
      var failed = function () {
        running.delete(key);
        connected.forEach(function (shared) {
          var event = new Event('error');
          if (typeof shared.onerror === 'function') shared.onerror(event);
          shared.dispatchEvent(event);
        });
      };
      instance = { worker: startWorker(url, given, true, failed), connected: connected };
      running.set(key, instance);
    }
    /** @type {any} */
    var shared = Reflect.construct(EventTarget, [], new.target || SharedWorker);
    var channel = new MessageChannel();
    shared.port = channel.port1;
    shared.onerror = null;
    instance.connected.push(shared);
    /** @type {{ [tag: string]: unknown }} */
    var message = {};
    message[WORKER_TAG] = { connect: true };
    instance.worker.postMessage(message, [channel.port2]);
    return shared;
  }
  SharedWorker.prototype = Object.create(EventTarget.prototype);
  window.SharedWorker = /** @type {any} */ (SharedWorker);
}

/** @param {() => void} attempt */
function standIn(attempt) {
  try {
    attempt();
  } catch (e) {
    // A browser that will not let the name be replaced keeps its own refusal.
  }
}

function cookiesAreKept() {
  try {
    document.cookie = '__page_cookie_check=1';
    var kept = document.cookie.indexOf('__page_cookie_check=1') >= 0;
    document.cookie = '__page_cookie_check=; max-age=0';
    return kept;
  } catch (e) {
    return false;
  }
}

if (window.origin === 'null') {
  standIn(function () {
    Object.defineProperty(window, 'localStorage', { configurable: true, value: memoryStorage() });
  });
  standIn(function () {
    Object.defineProperty(window, 'sessionStorage', { configurable: true, value: memoryStorage() });
  });
  standIn(function () {
    Object.defineProperty(window, 'indexedDB', { configurable: true, value: undefined });
  });
  standIn(standInWorker);
  standIn(standInSharedWorker);
}
if (!cookiesAreKept()) standIn(standInCookie);
