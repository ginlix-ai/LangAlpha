/*!
 * LangAlpha dashboard kit, v1.
 *
 * Copy this file into your app and load it with <script src="kit.js"></script>.
 * The app holds no credentials: inside LangAlpha, the page framing it answers
 * every read over postMessage, as the signed-in user and within the scopes
 * the user granted. Opened anywhere else, the components say so and nothing
 * throws.
 *
 * Kit components declare the scopes they need. Declare more for your own
 * la.get calls with <meta name="la-scopes" content="quotes news portfolio">.
 */
(function () {
  'use strict';
  if (window.la && window.la.version) return; // loaded twice

  var HELLO_EVERY = 500;
  var GIVE_UP_AFTER = 10000;
  var REQUEST_TIMEOUT = 20000;

  // [minimum, default] refresh period per method, in ms. The page caches
  // answers as well; the floor keeps a busy dashboard from hammering it.
  var CADENCE = {
    'quotes.get': [15e3, 30e3],
    'bars.get': [60e3, 60e3],
    'bars.batch': [60e3, 60e3],
    'profiles.get': [6e5, 36e5],
    'news.list': [3e5, 3e5],
    'brief.today': [9e5, 9e5],
    'earnings.calendar': [36e5, 36e5],
    'watchlists.list': [60e3, 3e5],
    'portfolio.holdings': [60e3, 3e5],
    'threads.recent': [30e3, 60e3]
  };

  var TAG_SCOPES = {
    'la-quote': ['quotes'],
    'la-sparkline': ['quotes'],
    'la-chart': ['quotes'],
    'la-watchlist': ['watchlists', 'quotes'],
    'la-news': ['news'],
    'la-brief': ['brief'],
    'la-earnings': ['earnings', 'watchlists'],
    'la-portfolio': ['portfolio', 'quotes'],
    'la-threads': ['threads']
  };

  var SCOPE_NAME = {
    quotes: 'market quotes',
    watchlists: 'your watchlists',
    news: 'news',
    brief: 'the daily brief',
    earnings: 'the earnings calendar',
    portfolio: 'your portfolio',
    threads: 'your threads'
  };

  // The kit's words in the user's language, sent by the page with its
  // welcome; each falls back to the English written where it is used.
  var strings = {};
  function say(key, fallback, vars) {
    var s = typeof strings[key] === 'string' && strings[key] ? strings[key] : fallback;
    return vars ? s.replace(/\{\{\s*(\w+)\s*\}\}/g, function (m, k) { return vars[k] != null ? String(vars[k]) : m; }) : s;
  }
  function takeStrings(v) {
    if (!v || typeof v !== 'object') return;
    var next = {};
    Object.keys(v).forEach(function (k) { if (typeof v[k] === 'string') next[k] = v[k]; });
    strings = next;
  }

  var INTERVALS = ['1min', '5min', '15min', '30min', '1hour', '4hour', '1day'];

  var framed;
  try { framed = window.parent !== window; } catch (e) { framed = true; }

  var la = {
    version: 1,
    connected: false,
    /** 'connecting' until the page welcomes the app, then 'connected', or 'offline' after giving up. */
    status: 'connecting',
    scopes: [],
    pending: [],
    locale: navigator.language || 'en-US',
    theme: null
  };
  var resolveReady;
  la.ready = new Promise(function (r) { resolveReady = r; });

  var pageOrigin = null; // the welcome's origin; nothing is accepted from elsewhere after it
  var target = null;     // targetOrigin for posts; '*' only for an opaque ('null') page origin
  var outbox = [];
  var seq = 0;
  var calls = {};        // id -> { timer, res(msg) }
  var helloTimer = 0;
  var giveUpTimer = 0;
  // This document's id, on every message: the page answers only the document
  // it welcomed, and a frame's documents all share its window.
  var docId = (function () {
    try { var a = new Uint32Array(2); crypto.getRandomValues(a); return a[0].toString(36) + a[1].toString(36); } catch (e) { /* no crypto */ }
    return Math.random().toString(36).slice(2) + Date.now().toString(36);
  })();

  function laError(code, message) {
    var e = new Error(message || code);
    e.code = code;
    return e;
  }

  function emit(name, detail) {
    try { window.dispatchEvent(new CustomEvent(name, { detail: detail })); } catch (e) { /* old browsers */ }
  }

  /** Send to the page, or queue it until the welcome. Returns null, or the error that kept it from going. */
  function post(msg) {
    msg.la = 1;
    msg.doc = docId;
    if (la.status === 'connected') {
      try { window.parent.postMessage(msg, target); } catch (e) {
        // Params that cannot be cloned are the caller's to fix; anything else is the frame gone.
        return laError(e && e.name === 'DataCloneError' ? 'bad_request' : 'unavailable', (e && e.message) || 'Could not reach LangAlpha');
      }
      return null;
    }
    if (la.status === 'connecting') { outbox.push(msg); return null; }
    return laError('unavailable', say('notConnected', 'Not connected to LangAlpha'));
  }

  function uniq(list) {
    var out = [];
    (list || []).forEach(function (s) { if (s && out.indexOf(s) < 0) out.push(s); });
    return out;
  }

  // ---------------------------------------------------------------- theme

  var themeKeys = [];

  function injectBase() {
    if (document.getElementById('la-kit-base')) return;
    var style = document.createElement('style');
    style.id = 'la-kit-base';
    // :where() has zero specificity, so any app rule wins.
    style.textContent =
      ':where(html){font-family:var(--font-ui,system-ui,-apple-system,"Segoe UI",sans-serif);' +
      'color:var(--color-text-primary,CanvasText);background:var(--color-bg-page,Canvas)}';
    var head = document.head || document.documentElement;
    head.insertBefore(style, head.firstChild);
  }

  function applyTheme(theme) {
    if (!theme || typeof theme !== 'object') return;
    la.theme = theme;
    var root = document.documentElement;
    var vars = theme.vars && typeof theme.vars === 'object' ? theme.vars : {};
    var keys = Object.keys(vars).filter(function (k) { return k.indexOf('--') === 0; });
    // A theme swap sends the full set; drop what the old one set and the new one doesn't.
    themeKeys.forEach(function (k) { if (keys.indexOf(k) < 0) root.style.removeProperty(k); });
    keys.forEach(function (k) { root.style.setProperty(k, String(vars[k])); });
    themeKeys = keys;
    if (theme.mode === 'light' || theme.mode === 'dark') {
      root.setAttribute('data-theme', theme.mode);
      root.style.colorScheme = theme.mode;
    }
  }

  // ------------------------------------------------------------ handshake

  function declaredScopes() {
    var out = [];
    var metas = document.querySelectorAll('meta[name="la-scopes"]');
    for (var i = 0; i < metas.length; i++) {
      String(metas[i].getAttribute('content') || '').split(/[\s,]+/).forEach(function (s) {
        if (s) out.push(s.toLowerCase());
      });
    }
    Object.keys(TAG_SCOPES).forEach(function (tag) {
      if (document.querySelector(tag)) out = out.concat(TAG_SCOPES[tag]);
    });
    return uniq(out);
  }

  function start() {
    injectBase();
    if (!framed) { goOffline(); return; }
    var hello = { la: 1, type: 'hello', scopes: declaredScopes(), doc: docId };
    var send = function () {
      // '*' is safe here: hello carries only scope names, and nothing else goes out before the welcome.
      try { window.parent.postMessage(hello, '*'); } catch (e) { /* detached */ }
    };
    send();
    helloTimer = setInterval(send, HELLO_EVERY);
    giveUpTimer = setTimeout(goOffline, GIVE_UP_AFTER);
  }

  function stopHello() {
    clearInterval(helloTimer);
    clearTimeout(giveUpTimer);
    helloTimer = giveUpTimer = 0;
  }

  function goOffline() {
    stopHello();
    if (la.status !== 'connecting') return;
    la.status = 'offline';
    la.connected = false;
    outbox = [];
    Object.keys(calls).forEach(function (id) {
      calls[id].res({ ok: false, error: { code: 'unavailable', message: say('notConnected', 'Not connected to LangAlpha') } });
    });
    resolveReady(la);
    emit('la:offline');
    refreshAll('status');
  }

  function onWelcome(m, origin) {
    var first = la.status !== 'connected';
    stopHello();
    pageOrigin = origin;
    target = origin === 'null' ? '*' : origin;
    la.status = 'connected';
    la.connected = true;
    la.scopes = uniq(Array.isArray(m.scopes) ? m.scopes : []);
    la.pending = uniq(Array.isArray(m.pending) ? m.pending : []).filter(function (s) { return la.scopes.indexOf(s) < 0; });
    if (typeof m.locale === 'string' && m.locale) la.locale = m.locale;
    takeStrings(m.strings);
    applyTheme(m.theme);
    if (first) {
      outbox.splice(0).forEach(function (msg) { post(msg); });
      resolveReady(la);
      emit('la:ready', la);
      refreshAll('status');
    } else {
      // A second welcome answers a hello that was already in flight: only unblock and repaint.
      refreshAll('grant');
      refreshAll('theme');
    }
  }

  // The list is everything granted now. A scope no longer on it was taken
  // away, and waits to be allowed again as it did before the user was asked.
  function onGrant(scopes) {
    var now = uniq(Array.isArray(scopes) ? scopes : []);
    var added = now.filter(function (s) { return la.scopes.indexOf(s) < 0; });
    var lost = la.scopes.filter(function (s) { return now.indexOf(s) < 0; });
    la.scopes = now;
    la.pending = uniq(la.pending.concat(lost)).filter(function (s) { return now.indexOf(s) < 0; });
    emit('la:grant', added);
    refreshAll('grant');
  }

  function onLocale(m) {
    if (typeof m.locale === 'string' && m.locale) la.locale = m.locale;
    takeStrings(m.strings);
    emit('la:locale', la.locale);
    refreshAll('locale');
    // Rows carry words made when they were read (a freshness label, a
    // source), so each read is made again now rather than at its next turn.
    pollers.forEach(function (p) { p.run(); });
  }

  window.addEventListener('message', function (e) {
    if (!framed || e.source !== window.parent) return;
    var m = e.data;
    if (!m || typeof m !== 'object' || m.la !== 1) return;
    if (pageOrigin !== null && e.origin !== pageOrigin) return;
    // Meant for another document of this frame (the one before, after a reload).
    if (typeof m.doc === 'string' && m.doc !== docId) return;
    switch (m.type) {
      case 'welcome': onWelcome(m, e.origin); break;
      case 'grant': if (la.connected) onGrant(m.scopes); break;
      case 'theme': if (la.connected) { applyTheme(m.theme); emit('la:theme', la.theme); refreshAll('theme'); } break;
      case 'locale': if (la.connected) onLocale(m); break;
      case 'res': if (calls[m.id]) calls[m.id].res(m); break;
      case 'evt': if (wires[m.id]) wires[m.id].onEvent(m.data); break;
    }
  });

  // Gone, the page stops answering this frame: whatever loads in it next
  // (a link followed) is not this page. Not on a back-forward cache stash,
  // from which the page and this document return together.
  window.addEventListener('pagehide', function (e) {
    if (e.persisted || la.status !== 'connected') return;
    try { window.parent.postMessage({ la: 1, type: 'bye', doc: docId }, target); } catch (err) { /* detached */ }
  });

  // -------------------------------------------------------------- requests

  /** Post a read or a subscription and wait for the page's answer to it. */
  function ask(msg) {
    if (la.status === 'offline') return Promise.reject(laError('unavailable', 'Not connected to LangAlpha'));
    return new Promise(function (resolve, reject) {
      var id = msg.id;
      calls[id] = {
        timer: setTimeout(function () {
          delete calls[id];
          reject(laError('failed', say('noAnswer', 'LangAlpha did not answer {{method}}', { method: msg.method })));
        }, REQUEST_TIMEOUT),
        res: function (m) {
          clearTimeout(calls[id] && calls[id].timer);
          delete calls[id];
          if (m.ok) resolve(m.data);
          else reject(laError((m.error && m.error.code) || 'failed', (m.error && m.error.message) || 'Request failed'));
        }
      };
      // Nothing went out, so no answer is coming: fail now, not at the timeout.
      var err = post(msg);
      if (err) calls[id].res({ ok: false, error: { code: err.code, message: err.message } });
    });
  }

  la.get = function (method, params) {
    if (typeof method !== 'string' || !method) return Promise.reject(laError('bad_request', 'method is required'));
    return ask({ type: 'req', id: 'r' + (++seq), method: method, params: params || {} });
  };

  // ---------------------------------------------------------------- polling

  var pollers = new Set();

  // One loop per distinct source, fanned out to every subscriber. It skips
  // ticks while the tab is hidden and catches up on the next show.
  function Poller(fetcher) {
    this.fetcher = fetcher;
    this.subs = new Map(); // token -> { cb, every }
    this.every = 60e3;
    this.timer = 0;
    this.busy = false;
    this.missed = false;
    this.has = false;
    this.data = null;
    this.err = null;
    this.onEmpty = null;
    this.backoff = 0; // ms until a read the page refused as too many is asked again
  }
  Poller.prototype.add = function (cb, every) {
    var token = {};
    this.subs.set(token, { cb: cb, every: every });
    this.retime();
    if (this.has) call(cb, this.data, this.err);
    if (this.subs.size === 1) { pollers.add(this); this.run(); }
    var self = this;
    return function () { self.remove(token); };
  };
  Poller.prototype.remove = function (token) {
    if (!this.subs.delete(token)) return;
    if (this.subs.size) { this.retime(); return; }
    clearTimeout(this.timer);
    this.timer = 0;
    pollers.delete(this);
    if (this.onEmpty) this.onEmpty();
  };
  Poller.prototype.retime = function () {
    var min = Infinity, was = this.every, self = this;
    this.subs.forEach(function (s) { if (s.every < min) min = s.every; });
    this.every = min === Infinity ? 60e3 : min;
    // The next read was set for the old period: due sooner (or later) now.
    if (this.every !== was && this.timer && !this.backoff) {
      clearTimeout(this.timer);
      this.timer = setTimeout(function () { self.run(); }, Math.max(0, (this.at || 0) + this.every - Date.now()));
    }
  };
  Poller.prototype.run = function () {
    clearTimeout(this.timer);
    this.timer = 0;
    if (!this.subs.size || this.busy) return;
    if (document.hidden) { this.missed = true; return; }
    this.missed = false;
    this.busy = true;
    var self = this;
    var done = function (data, err) {
      self.busy = false;
      self.at = Date.now();
      if (!self.subs.size) return;
      self.has = true;
      self.data = data;
      self.err = err;
      self.subs.forEach(function (s) { call(s.cb, data, err); });
      // Refused as too many, it asks again once the page's allowance refills
      // (1s, 2s, 4s...), not a whole period later.
      self.backoff = err && err.code === 'rate' ? Math.min(self.backoff ? self.backoff * 2 : 1000, self.every) : 0;
      if (self.subs.size) self.timer = setTimeout(function () { self.run(); }, self.backoff || self.every);
    };
    var p;
    try { p = Promise.resolve(this.fetcher()); } catch (e) { p = Promise.reject(e); }
    p.then(function (d) { done(d, null); }, function (e) { done(null, e); });
  };

  function call(cb, a, b) {
    try { cb(a, b); } catch (e) { setTimeout(function () { throw e; }); }
  }

  Poller.prototype.stale = function () {
    return !this.busy && (this.missed || !this.at || Date.now() - this.at >= this.every);
  };

  // Everything due is read again the moment the page could be looked at:
  // back on screen, back online, or awake. A laptop's sleep stops the timers'
  // clock with it, so a read due during the night would wait out the rest of
  // its interval after waking; the heartbeat sees the jump in wall time.
  // Ages on screen move with the clock too, so they are redrawn with it.
  function awake() {
    pollers.forEach(function (p) { if (p.subs.size && p.stale()) p.run(); });
    tickAll();
  }
  document.addEventListener('visibilitychange', function () {
    if (!document.hidden) awake();
  });
  window.addEventListener('online', awake);
  window.addEventListener('focus', awake);
  var lastBeat = Date.now();
  setInterval(function () {
    var now = Date.now();
    if (now - lastBeat > 60e3) awake();
    lastBeat = now;
  }, 15e3);

  function stableKey(params) {
    var o = params || {};
    var keys = Object.keys(o).sort();
    return JSON.stringify(keys.map(function (k) { return [k, o[k]]; }));
  }

  var watches = new Map();

  function cadence(method, opts) {
    var c = CADENCE[method] || [15e3, 60e3];
    var asked = opts && Number(opts.every);
    return Math.max(c[0], asked > 0 ? asked : c[1]);
  }

  la.watch = function (method, params, callback, opts) {
    if (typeof callback !== 'function') throw new TypeError('la.watch needs a callback');
    var every = cadence(method, opts);
    var key = method + ' ' + stableKey(params);
    var p = watches.get(key);
    if (!p) {
      p = new Poller(function () { return la.get(method, params); });
      p.onEmpty = function () { watches.delete(key); };
      watches.set(key, p);
    }
    return p.add(callback, every);
  };

  la.source = function (url, callback, opts) {
    if (typeof callback !== 'function') throw new TypeError('la.source needs a callback');
    var asked = opts && Number(opts.every);
    var every = Math.max(5000, asked > 0 ? asked : 60000);
    var p = new Poller(function () {
      return fetch(url, { credentials: 'same-origin' }).then(function (r) {
        if (!r.ok) throw laError('failed', url + ' answered ' + r.status);
        return r.json();
      });
    });
    return p.add(callback, every);
  };

  // --------------------------------------------------------------- streams

  // One stream per distinct set of symbols, shared by every callback on it.
  // The page answers with the symbols' quotes and sends each again whenever
  // it changes: live while the market stream is up, polled otherwise.
  var streams = new Map();
  // On the wire, the streams opened in one task share a subscription, so a
  // page of tiles costs the page one request on its first paint, not one each.
  var wires = {};    // subscription id -> Wire
  var opening = {};  // kind -> streams waiting for the next wire
  var WIRE_MAX = 200; // the page's limit per subscription
  var RETRY_MAX = 30e3;

  function toSymbols(list) {
    var raw = typeof list === 'string' ? list.split(',') : Array.isArray(list) ? list : [];
    return uniq(raw.map(function (s) { return String(s).trim().toUpperCase(); })).sort();
  }

  function Wire(kind) {
    this.kind = kind;
    this.id = null;
    this.streams = new Set();
    this.attempt = 0;
    this.timer = 0;
  }
  Wire.prototype.send = function () {
    var self = this, all = [];
    this.timer = 0;
    this.streams.forEach(function (s) { all = all.concat(s.symbols); });
    all = uniq(all).sort();
    if (!all.length) return;
    var id = this.id = 's' + (++seq);
    wires[id] = this;
    var params = { symbols: all };
    if (this.kind === 'index') params.kind = 'index';
    ask({ type: 'sub', id: id, method: 'quotes.stream', params: params }).then(function (rows) {
      if (self.id !== id) return;
      self.attempt = 0;
      var list = Array.isArray(rows) ? rows : [];
      self.streams.forEach(function (s) { s.answer(list); });
    }, function (err) {
      if (self.id !== id) return;
      delete wires[id];
      self.id = null;
      // An answer that never came is no refusal: the page may hold this one
      // still, and each retry would be one more stream there.
      post({ type: 'unsub', id: id });
      self.failed(err);
    });
  };
  Wire.prototype.failed = function (err) {
    var self = this, code = err && err.code, list = [];
    this.streams.forEach(function (s) { list.push(s); });
    if (code === 'bad_request' && list.length > 1) {
      // One stream's symbols spoil a shared subscription: each asks alone.
      this.streams.clear();
      list.forEach(function (s) { var w = new Wire(self.kind); w.streams.add(s); s.wire = w; w.send(); });
      return;
    }
    if (code === 'rate' || code === 'failed') {
      // Asked again with backoff, for every subscriber it has: the page's
      // allowance refills by the second.
      this.timer = setTimeout(function () { self.send(); }, Math.min(1000 * Math.pow(2, this.attempt++), RETRY_MAX));
      list.forEach(function (s) { s.notify(s.subs, err); });
      return;
    }
    // Refused for good (no access, bad symbols, no page): the next subscriber asks again.
    this.streams.clear();
    list.forEach(function (s) { streams.delete(s.key); s.wire = null; s.notify(s.subs, err); });
  };
  Wire.prototype.onEvent = function (q) {
    this.streams.forEach(function (s) { s.onEvent(q); });
  };
  Wire.prototype.drop = function (s) {
    if (!this.streams.delete(s) || this.streams.size) return;
    clearTimeout(this.timer);
    this.timer = 0;
    var id = this.id;
    this.id = null;
    if (!id) return;
    delete wires[id];
    if (calls[id]) { clearTimeout(calls[id].timer); delete calls[id]; }
    post({ type: 'unsub', id: id });
  };

  function flushOpening(kind) {
    var list = opening[kind] || [];
    delete opening[kind];
    var wire = null, count = 0;
    list.forEach(function (s) {
      if (!wire || count + s.symbols.length > WIRE_MAX) {
        if (wire) wire.send();
        wire = new Wire(kind);
        count = 0;
      }
      wire.streams.add(s);
      s.wire = wire;
      count += s.symbols.length;
    });
    if (wire) wire.send();
  }

  function Stream(symbols, kind, key) {
    this.key = key;
    this.kind = kind;
    this.symbols = symbols;
    // The page keys an index without its '^'.
    this.want = {};
    for (var i = 0; i < symbols.length; i++) this.want[kind === 'index' ? symbols[i].replace(/^\^/, '') : symbols[i]] = true;
    this.subs = new Set();
    this.quotes = {};
    this.answered = false;
    this.wire = null;
    var list = opening[kind];
    if (!list) {
      list = opening[kind] = [];
      setTimeout(function () { flushOpening(kind); }, 0);
    }
    list.push(this);
  }
  Stream.prototype.take = function (q) {
    if (q && typeof q.symbol === 'string' && this.want[q.symbol]) { this.quotes[q.symbol] = q; return true; }
    return false;
  };
  Stream.prototype.answer = function (rows) {
    var self = this;
    rows.forEach(function (q) { self.take(q); });
    this.answered = true;
    this.notify(this.subs, null);
  };
  Stream.prototype.onEvent = function (q) {
    if (this.take(q) && this.answered) this.notify(this.subs, null);
  };
  Stream.prototype.notify = function (subs, err) {
    var quotes = Object.assign({}, this.quotes);
    subs.forEach(function (cb) { call(cb, quotes, err); });
  };
  Stream.prototype.close = function () {
    streams.delete(this.key);
    var list = opening[this.kind], at = list ? list.indexOf(this) : -1;
    if (at >= 0) list.splice(at, 1);
    if (this.wire) this.wire.drop(this);
    this.wire = null;
  };

  /** callback(quotes, err): the symbols' quotes by symbol, on the answer and on every change. */
  la.stream = function (symbols, callback, opts) {
    if (typeof callback !== 'function') throw new TypeError('la.stream needs a callback');
    var list = toSymbols(symbols);
    if (!list.length) return function () {};
    var kind = opts && opts.kind === 'index' ? 'index' : '';
    var key = kind + ':' + list.join(',');
    var s = streams.get(key);
    if (!s) { s = new Stream(list, kind, key); streams.set(key, s); }
    var cb = function (q, e) { callback(q, e); }; // distinct per subscriber
    s.subs.add(cb);
    // A late subscriber starts from what the stream already holds.
    if (s.answered) Promise.resolve().then(function () { if (s.subs.has(cb)) s.notify([cb], null); });
    return function () {
      if (s.subs.delete(cb) && !s.subs.size) s.close();
    };
  };

  la.open = function (t) {
    if (!t || typeof t !== 'object') return;
    post({ type: 'open', target: t });
  };

  // ---------------------------------------------------------------- format

  function isNum(n) { return typeof n === 'number' && isFinite(n); }

  function nf(opts) {
    try { return new Intl.NumberFormat(la.locale, opts); } catch (e) {
      try { return new Intl.NumberFormat('en-US', opts); } catch (e2) { return new Intl.NumberFormat('en-US'); }
    }
  }
  function df(opts) {
    try { return new Intl.DateTimeFormat(la.locale, opts); } catch (e) { return new Intl.DateTimeFormat('en-US', opts); }
  }

  var MINUS = '−';
  var DASH = '—';
  var DAY_ONLY = /^\d{4}-\d{2}-\d{2}$/;

  function toTime(v) {
    if (v instanceof Date) return v.getTime();
    if (isNum(v)) return v;
    if (typeof v === 'string' && v) return Date.parse(v);
    return NaN;
  }

  var format = {
    price: function (n, currency) {
      if (!isNum(n)) return DASH;
      var d = n !== 0 && Math.abs(n) < 1 ? 4 : 2;
      var o = { minimumFractionDigits: d, maximumFractionDigits: d };
      if (currency) {
        try { return new Intl.NumberFormat(la.locale, { style: 'currency', currency: currency, minimumFractionDigits: d, maximumFractionDigits: d }).format(n); } catch (e) { /* unknown code */ }
      }
      return nf(o).format(n);
    },
    /** Signed number, two decimals: "+1.23". */
    change: function (n) {
      if (!isNum(n)) return DASH;
      var s = nf({ minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(Math.abs(n));
      return (n > 0 ? '+' : n < 0 ? MINUS : '') + s;
    },
    /** n is in percent units: 1.5 means 1.5%. */
    percent: function (n) {
      return isNum(n) ? format.change(n) + '%' : DASH;
    },
    compact: function (n) {
      if (!isNum(n)) return DASH;
      var a = Math.abs(n);
      var units = [[1e12, 'T'], [1e9, 'B'], [1e6, 'M'], [1e3, 'K']];
      for (var i = 0; i < units.length; i++) {
        if (a >= units[i][0]) {
          var v = (a / units[i][0]).toFixed(1).replace(/\.0$/, '');
          return (n < 0 ? MINUS : '') + v + units[i][1];
        }
      }
      return (n < 0 ? MINUS : '') + nf({ maximumFractionDigits: 2 }).format(a);
    },
    ago: function (v) {
      var t = toTime(v);
      if (!isFinite(t)) return '';
      var s = Math.round((Date.now() - t) / 1000);
      var future = s < 0;
      s = Math.abs(s);
      if (s < 45) return future ? say('ago.soon', 'in a moment') : say('ago.justNow', 'just now');
      var unit = s < 3600 ? ['minutes', 'm', Math.max(1, Math.round(s / 60))]
        : s < 86400 ? ['hours', 'h', Math.round(s / 3600)]
        : s < 7 * 86400 ? ['days', 'd', Math.round(s / 86400)] : null;
      if (!unit) return format.date(t);
      var key = future ? 'in' + unit[0].charAt(0).toUpperCase() + unit[0].slice(1) : unit[0];
      return say('ago.' + key, future ? 'in {{n}}' + unit[1] : '{{n}}' + unit[1] + ' ago', { n: unit[2] });
    },
    date: function (v, opts) {
      var t = toTime(v);
      if (!isFinite(t)) return '';
      var o = opts ? Object.assign({}, opts) : { month: 'short', day: 'numeric' };
      // A bare day is a calendar date, not midnight UTC shifted into the viewer's zone.
      if (typeof v === 'string' && DAY_ONLY.test(v) && !o.timeZone) o.timeZone = 'UTC';
      if (!opts) withYear(o, new Date(t));
      return df(o).format(new Date(t));
    }
  };
  la.format = format;

  // Bar times are market wall-clock seconds read as UTC, so they format in UTC.
  function barLabel(sec, intraday, withYear) {
    var o = { month: 'short', day: 'numeric', timeZone: 'UTC' };
    if (withYear) o.year = 'numeric';
    if (intraday) { o.hour = 'numeric'; o.minute = '2-digit'; }
    return df(o).format(new Date(sec * 1000));
  }

  function localDay(d) {
    var p = function (n) { return (n < 10 ? '0' : '') + n; };
    return d.getFullYear() + '-' + p(d.getMonth() + 1) + '-' + p(d.getDate());
  }

  function esc(v) {
    return String(v == null ? '' : v).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }

  function tone(n) { return isNum(n) ? (n > 0 ? 'up' : n < 0 ? 'down' : '') : ''; }

  function withYear(o, d) {
    if (d.getFullYear() !== new Date().getFullYear()) o.year = 'numeric';
    return o;
  }

  /**
   * A local time's format, with the date when it isn't today and the year when
   * it isn't this one; weekday puts the past week's days by name instead.
   */
  function timeFormat(d, weekday) {
    var now = new Date(), o = { hour: 'numeric', minute: '2-digit' };
    if (localDay(d) === localDay(now)) return o;
    if (weekday && now - d < 6 * 864e5) { o.weekday = 'short'; return o; }
    o.month = 'short';
    o.day = 'numeric';
    return withYear(o, d);
  }

  /** A local time, with the date when it isn't today: "4:00 PM", "Nov 6, 4:00 PM". */
  function stamp(v) {
    var t = toTime(v);
    if (!isFinite(t)) return '';
    var d = new Date(t);
    return df(timeFormat(d)).format(d);
  }

  // ------------------------------------------------------------- freshness

  // A quote or a series says how fresh it is: { state, label, tone, asOf, source }.
  // The page sends null, or an empty label, when it has nothing to state, and
  // a card also hears the app's own la-status events, whose shape nothing
  // checks; either way nothing is drawn, and tone goes into a class name.
  var TONES = ['live', 'neutral', 'warning'];

  function fresh(v) {
    if (!v || typeof v !== 'object' || typeof v.label !== 'string' || !v.label) return null;
    return {
      state: typeof v.state === 'string' && v.state ? v.state : v.label,
      label: v.label,
      tone: TONES.indexOf(v.tone) >= 0 ? v.tone : 'neutral',
      asOf: typeof v.asOf === 'string' || isNum(v.asOf) ? v.asOf : null,
      source: typeof v.source === 'string' && v.source ? v.source : null
    };
  }

  function freshKey(f) { return f ? [f.state, f.label, f.tone, f.asOf, f.source].join('\u0001') : ''; }

  function freshTitle(f) {
    return [f.label, f.source ? say('source', 'Source: {{source}}', { source: f.source }) : '', f.asOf ? say('asOf', 'As of {{time}}', { time: stamp(f.asOf) }) : '']
      .filter(Boolean).join('\n');
  }

  function tagHtml(f) {
    return '<span class="tag" part="tag" title="' + esc(freshTitle(f)) + '"><span class="br" aria-hidden="true">[</span>' +
      '<span class="fdot tone-' + f.tone + '"></span>' + esc(f.label) + '<span class="br" aria-hidden="true">]</span></span>';
  }

  function rowDot(f) {
    return '<span class="fdot tone-' + f.tone + '" role="img" aria-label="' + esc(f.label) + '" title="' + esc(f.label) + '"></span>';
  }

  /**
   * One tag for many rows: the state most of them share (the first seen on a
   * tie), carried by its oldest row, so the tag never claims fresher data than
   * a row has. mixed says some row is in another state and gets a dot.
   */
  function freshSummary(list) {
    var count = {}, order = [];
    list.forEach(function (f) {
      if (!f) return;
      if (!count[f.state]) { count[f.state] = 0; order.push(f.state); }
      count[f.state]++;
    });
    var top = null;
    order.forEach(function (s) { if (top === null || count[s] > count[top]) top = s; });
    if (top === null) return { main: null, mixed: false };
    var main = null;
    list.forEach(function (f) {
      if (!f || f.state !== top) return;
      var tf = toTime(f.asOf), tm = main ? toTime(main.asOf) : NaN;
      if (!main || (isFinite(tf) && (!isFinite(tm) || tf < tm))) main = f;
    });
    return { main: main, mixed: order.length > 1 };
  }

  function odd(f, summary) { return !!(f && summary.mixed && f.state !== summary.main.state); }

  /** The <la-card> a node sits in, across shadow roots; null outside one. */
  function cardOf(node) {
    var n = node && (node.parentNode || node.host);
    while (n) {
      if (n.localName === 'la-card') return n;
      n = n.parentNode || n.host;
    }
    return null;
  }

  // ------------------------------------------------------------------ cron

  // Standard 5-field cron (minute hour day-of-month month day-of-week), read
  // on a timezone's wall clock, so a page can tell a scheduled file that
  // missed its run from one that is merely old.
  var MONTHS = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
  var DAYS = ['SUN', 'MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT'];
  var CRON_RANGE = [[0, 59], [0, 23], [1, 31], [1, 12], [0, 7]];
  var LOOKBACK_DAYS = 400; // a yearly schedule's last run, and no further
  var cronCache = {};
  var wallFormats = {};

  function cronField(src, i) {
    var lo = CRON_RANGE[i][0], hi = CRON_RANGE[i][1];
    var names = i === 3 ? MONTHS : i === 4 ? DAYS : null;
    var bad = function () { throw laError('bad_request', 'Bad cron field ' + src); };
    var val = function (s) {
      var n = /^\d+$/.test(s) ? Number(s) : names && names.indexOf(s.toUpperCase()) >= 0 ? names.indexOf(s.toUpperCase()) + (i === 3 ? 1 : 0) : NaN;
      if (!(n >= lo && n <= hi)) bad();
      return n;
    };
    var on = [];
    src.split(',').forEach(function (part) {
      var m = /^([^/]+)(?:\/(\d+))?$/.exec(part);
      if (!m) bad();
      var step = m[2] ? Number(m[2]) : 1;
      if (!(step >= 1)) bad();
      var a = lo, b = hi;
      if (m[1] !== '*' && m[1] !== '?') {
        var r = m[1].split('-');
        if (r.length > 2) bad();
        a = val(r[0]);
        b = r.length === 2 ? val(r[1]) : m[2] ? hi : a;
        if (a > b) bad();
      }
      // Day-of-week 7 is Sunday, as 0 is.
      for (var n = a; n <= b; n += step) on[i === 4 ? n % 7 : n] = true;
    });
    return on;
  }

  function parseCron(expr) {
    var key = String(expr == null ? '' : expr).trim();
    if (Object.prototype.hasOwnProperty.call(cronCache, key)) return cronCache[key];
    var out = null;
    try {
      var f = key.split(/\s+/);
      if (f.length === 5) {
        out = {
          min: cronField(f[0], 0), hour: cronField(f[1], 1), dom: cronField(f[2], 2),
          mon: cronField(f[3], 3), dow: cronField(f[4], 4),
          // Vixie cron's rule: with both day fields restricted, a day matching either runs.
          either: !/^[*?]/.test(f[2]) && !/^[*?]/.test(f[4])
        };
      }
    } catch (e) { out = null; }
    cronCache[key] = out;
    return out;
  }

  /** The wall clock at instant t in tz (undefined: the browser's zone). Throws on an unknown zone. */
  function wallOf(t, tz) {
    var k = tz || '';
    var f = wallFormats[k];
    if (!f) {
      f = new Intl.DateTimeFormat('en-US', {
        timeZone: tz, hourCycle: 'h23', year: 'numeric', month: 'numeric', day: 'numeric', hour: 'numeric', minute: 'numeric'
      });
      wallFormats[k] = f;
    }
    var o = {};
    f.formatToParts(new Date(t)).forEach(function (p) { o[p.type] = p.value; });
    return { y: +o.year, mo: +o.month, d: +o.day, h: +o.hour % 24, mi: +o.minute };
  }

  /**
   * The instants showing this wall time in tz, earliest first: two in the
   * hour a fall-back repeats, and for one a spring-forward skips, the instant
   * after the gap, which is when cron runs it.
   */
  function instantsAt(y, mo, d, h, mi, tz) {
    var guess = Date.UTC(y, mo - 1, d, h, mi);
    var out = [], tries = [];
    var wallAt = function (t) { var w = wallOf(t, tz); return Date.UTC(w.y, w.mo - 1, w.d, w.h, w.mi); };
    [-864e5, 864e5].forEach(function (side) {
      var probe = guess + side;
      var c = guess - (wallAt(probe) - probe);
      tries.push(c);
      if (wallAt(c) === guess && out.indexOf(c) < 0) out.push(c);
    });
    if (out.length) return out.sort(function (a, b) { return a - b; });
    // In the gap: the zone's offsets either side bracket the jump, whose
    // first instant (the wall clock past the time) is when cron runs it.
    var lo = Math.min(tries[0], tries[1]), hi = Math.max(tries[0], tries[1]);
    while (hi - lo > 6e4) {
      var mid = lo + Math.floor((hi - lo) / 12e4) * 6e4;
      if (wallAt(mid) > guess) hi = mid; else lo = mid;
    }
    return [hi];
  }

  function lastRun(cron, timezone, now) {
    var c = parseCron(cron);
    var end = now == null ? Date.now() : toTime(now);
    if (!c || !isFinite(end)) return null;
    var tz = timezone || undefined;
    try {
      var w = wallOf(end, tz);
      var base = Date.UTC(w.y, w.mo - 1, w.d);
      // Today, only times up to now's wall clock are candidates; the hour of
      // slack covers a fall-back, whose repeated hour reads earlier than it is.
      var limit = w.h * 60 + w.mi + 60;
      for (var k = 0; k <= LOOKBACK_DAYS; k++) {
        var day = new Date(base - k * 864e5);
        var y = day.getUTCFullYear(), mo = day.getUTCMonth() + 1, d = day.getUTCDate();
        if (!c.mon[mo]) { k += d - 1; continue; } // to the previous month's last day
        var dm = !!c.dom[d], wm = !!c.dow[day.getUTCDay()];
        if (!(c.either ? dm || wm : dm && wm)) continue;
        for (var h = 23; h >= 0; h--) {
          if (!c.hour[h] || (k === 0 && h * 60 > limit)) continue;
          for (var mi = 59; mi >= 0; mi--) {
            if (!c.min[mi] || (k === 0 && h * 60 + mi > limit)) continue;
            var ts = instantsAt(y, mo, d, h, mi, tz);
            for (var j = ts.length - 1; j >= 0; j--) if (ts[j] <= end) return new Date(ts[j]);
          }
        }
      }
    } catch (e) { /* an unknown timezone */ }
    return null;
  }

  /** The schedule's most recent run at or before now (default: this moment), or null. */
  la.lastRun = function (cron, timezone, now) { return lastRun(cron, timezone, now); };

  /** The run asOf missed, once graceMinutes (15) past it, or null when asOf is current. */
  la.overdue = function (asOf, cron, timezone, graceMinutes) {
    var g = graceMinutes == null ? 15 : Number(graceMinutes);
    if (!(isFinite(g) && g >= 0)) g = 15;
    var run = lastRun(cron, timezone, Date.now() - g * 60e3);
    if (!run) return null;
    var t = toTime(asOf);
    return isFinite(t) && t >= run.getTime() ? null : run;
  };

  // ------------------------------------------------------------ components

  var live = new Set();
  function refreshAll(kind) { live.forEach(function (el) { el._onState(kind); }); }

  // Ages on screen ("5m ago", "as of 18m ago") move with the clock, not with the data.
  function tickAll() { live.forEach(function (el) { el._tick(); }); }
  setInterval(tickAll, 60e3);

  var TAG_CSS =
    '.tag{display:inline-flex;align-items:center;flex:0 0 auto;white-space:nowrap;font-family:var(--font-mono,ui-monospace,SFMono-Regular,Menlo,Consolas,monospace);' +
    'font-size:10.5px;line-height:1.2;font-weight:500;text-transform:uppercase;letter-spacing:.06em;color:var(--color-text-tertiary,#8b9099)}' +
    '.tag .br{color:var(--color-text-tertiary,#8b9099)}.tag .fdot{margin:0 5px 0 1px}' +
    '.fdot{display:inline-block;flex:0 0 auto;width:6px;height:6px;border-radius:50%;background:var(--color-text-tertiary,#8b9099)}' +
    '.fdot.tone-live{background:var(--color-up,#16a34a)}.fdot.tone-warning{background:var(--color-warning,#d97706)}';

  var BASE_CSS =
    ':host{display:block;min-width:0;color:var(--color-text-primary,inherit);font-family:var(--font-ui,inherit);font-size:13px;line-height:1.4}' +
    ':host([hidden]){display:none}' +
    '*{box-sizing:border-box}' +
    '.num{font-variant-numeric:tabular-nums}' +
    '.sec{color:var(--color-text-secondary,#6b7280)}' +
    '.muted{color:var(--color-text-tertiary,#8b9099)}' +
    '.up{color:var(--color-up,#16a34a)}.down{color:var(--color-down,#dc2626)}' +
    '.note{color:var(--color-text-tertiary,#8b9099);font-size:12px;padding:4px 0}' +
    '.sk{height:11px;margin:7px 0;border-radius:4px;background:var(--color-bg-hover,rgba(127,127,127,.14));animation:la-pulse 1.4s ease-in-out infinite}' +
    '@keyframes la-pulse{50%{opacity:.45}}' +
    '@media (prefers-reduced-motion:reduce){.sk{animation:none}}' +
    '.trunc{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;min-width:0}' +
    '.row{display:flex;align-items:baseline;gap:10px;padding:6px 4px;margin:0 -4px;border-top:1px solid var(--color-border-muted,rgba(127,127,127,.18))}' +
    '.row:first-child{border-top:0}' +
    '[data-sym],[data-thread]{cursor:pointer;border-radius:4px}' +
    '[data-sym]:hover,[data-thread]:hover{background:var(--color-bg-hover,rgba(127,127,127,.08))}' +
    '[data-sym]:focus-visible,[data-thread]:focus-visible{outline:2px solid var(--color-accent-primary,#2563eb);outline-offset:1px}' +
    '.r{margin-left:auto;text-align:right}' +
    '.sym{font-weight:600}' +
    'a{color:inherit;text-decoration:none}a:hover{text-decoration:underline}' +
    TAG_CSS +
    '.fresh-row{display:flex;justify-content:flex-end;margin:0 0 6px}.fresh-row:empty{display:none}' +
    '.body{transition:opacity .2s}.body.dim{opacity:.55}' +
    '.stale{display:flex;align-items:center;gap:6px;margin-top:6px;font-size:11.5px;color:var(--color-warning,#d97706)}.stale:empty{display:none}' +
    '.wdot{display:inline-block;flex:0 0 auto;width:6px;height:6px;border-radius:50%;background:currentColor}' +
    '@media (prefers-reduced-motion:reduce){.body{transition:none}}';

  class LaElement extends HTMLElement {
    constructor() {
      super();
      var root = this.attachShadow({ mode: 'open' });
      var style = document.createElement('style');
      style.textContent = BASE_CSS + (this.constructor.css || '');
      // The freshness tag when no card heading carries it, the data, and the
      // failed-refresh line: separate, so a paint replaces only the data.
      this.freshEl = document.createElement('div');
      this.freshEl.className = 'fresh-row';
      this.body = document.createElement('div');
      this.body.className = 'body';
      this.staleEl = document.createElement('div');
      this.staleEl.className = 'stale';
      this.staleEl.setAttribute('part', 'stale');
      root.append(style, this.freshEl, this.body, this.staleEl);
      this._subs = new Map(); // slot -> { key, stop }, see sub()
      this._fails = new Map(); // watch -> when it last read, while its re-reads fail
      this._gate = '';
      this._mounted = false;
      this._queued = false;
      this._painted = false;
      this._freshAt = null; // the card carrying this element's tag, or null for its own row
      this._freshKey = '';
      var open = function (e) {
        var el = e.target.closest && e.target.closest('[data-sym],[data-thread]');
        if (!el || (e.type === 'keydown' && e.key !== 'Enter' && e.key !== ' ')) return;
        if (e.type === 'keydown') e.preventDefault();
        if (el.dataset.thread) {
          var t = { thread: el.dataset.thread };
          if (el.dataset.ws) t.workspace = el.dataset.ws;
          la.open(t);
        } else {
          la.open({ symbol: el.dataset.sym });
        }
      };
      root.addEventListener('click', open);
      root.addEventListener('keydown', open);
    }
    connectedCallback() { this._mounted = true; live.add(this); this._setup(); }
    disconnectedCallback() { this._mounted = false; live.delete(this); this._stop(); }
    attributeChangedCallback(name) {
      if (!this._mounted) return;
      // Attributes that only change how the data is drawn repaint without a re-read.
      if ((this.constructor.repaint || []).indexOf(name) >= 0) { this._redraw(); return; }
      if (this._queued) return;
      this._queued = true;
      var self = this;
      // Batch attribute changes made in one task into one resubscribe.
      Promise.resolve().then(function () { self._queued = false; if (self._mounted) self._setup(); });
    }
    _stop() {
      this._painted = false;
      if (this._raf) { cancelAnimationFrame(this._raf); this._raf = 0; }
      var subs = this._subs;
      this._subs = new Map();
      subs.forEach(function (s) {
        try { if (s.stop) s.stop(); } catch (e) { /* ignore */ }
      });
      this._fails = new Map();
      this.markStale();
      this.setFreshness(null);
    }
    _setup() {
      this._stop();
      // An element that reads nothing from LangAlpha starts without it.
      if (this.constructor.standalone) { this._gate = 'ok'; this.start(); return; }
      if (la.status === 'connecting') { this._gate = 'wait'; this.skeleton(); return; }
      if (la.status === 'offline') { this._gate = 'offline'; this.note(say('offline', 'Open this page from LangAlpha to see live data.')); return; }
      var missing = this._missing();
      if (missing.length) {
        this._gate = 'scope';
        var asked = missing.filter(function (s) { return la.pending.indexOf(s) >= 0; });
        this.note(scopeNote(asked.length ? asked[0] : missing[0], asked.length > 0));
        return;
      }
      this._gate = 'ok';
      this._day = localDay(new Date());
      this.skeleton();
      this.start();
    }
    _onState(kind) {
      // New colors or words: what was drawn from data is drawn again in them,
      // and a gate's note is set again in the new words.
      if (kind === 'theme' || (kind === 'locale' && this._gate === 'ok')) {
        this._redraw();
        if (kind === 'locale') this.markStale();
        return;
      }
      // A grant changes nothing for an element that still has all it reads;
      // one that lost a scope stops and says so, as it would on a reload.
      if (this.constructor.standalone || (kind === 'grant' && this._gate === 'ok' && !this._missing().length)) return;
      this._setup();
    }
    _missing() {
      return (this.constructor.scopes || []).filter(function (s) { return la.scopes.indexOf(s) < 0; });
    }
    // An age drawn a minute ago is a minute off. The rest is left alone: a
    // repaint under the pointer would drop a chart's hover.
    _tick() {
      // A window counted from today (a week of earnings, a chart's range)
      // starts over on a new day; a page left open would keep yesterday's.
      if (this.constructor.daily && this._gate === 'ok' && this._day !== localDay(new Date())) { this._setup(); return; }
      if (this.constructor.ages) this._redraw();
      if (this._fails.size) this.markStale();
    }
    // Only what was drawn from data: a repaint before the first answer would
    // replace the skeleton with an empty state.
    _redraw() { if (this._painted) this.schedule(); }
    attr(name, fallback) {
      var v = this.getAttribute(name);
      return v == null || v === '' ? fallback : v;
    }
    num(name, fallback) {
      var n = Number(this.getAttribute(name));
      return this.hasAttribute(name) && isFinite(n) && n > 0 ? n : fallback;
    }
    html(s) { this.body.innerHTML = s; }
    note(text) { this.html('<div class="note" part="note">' + esc(text) + '</div>'); }
    skeleton(lines) {
      var n = lines || 3, s = '';
      for (var i = 0; i < n; i++) s += '<div class="sk" style="width:' + (92 - i * 17) + '%"></div>';
      this.html(s);
    }
    /** Show an error only when there is nothing better on screen. */
    fail(err, hasData) {
      if (hasData) return;
      if (err && err.code === 'scope') { this.note(scopeNote((this.constructor.scopes || [])[0], false)); return; }
      var msg = err && err.code === 'rate' ? say('rate', 'Too many requests; retrying shortly.')
        : err && err.code === 'unavailable' ? say('unavailable', 'Not available right now.')
        : err && err.message ? say('loadFailedBecause', "Couldn't load: {{message}}", { message: err.message })
        : say('loadFailed', "Couldn't load.");
      this.html('<div class="note trunc" part="note" title="' + esc(msg) + '">' + esc(msg) + '</div>');
    }
    /**
     * The element's one store of subscriptions: one per slot, kept until the
     * element stops and restarted only when its key changes. start returns
     * the stop, or nothing when there is nothing to follow.
     */
    sub(slot, key, start) {
      var s = this._subs.get(slot);
      if (s && s.key === key) return;
      this._subs.delete(slot);
      if (s && s.stop) s.stop();
      var stop = start();
      this._subs.set(slot, { key: key, stop: stop || null });
    }
    /**
     * Watch a method into a slot: each answer goes to take (as this), then the
     * element repaints. take may throw to count an answer as failed.
     */
    watch(slot, method, params, take, opts) {
      var self = this;
      this.sub(slot, method + ' ' + stableKey(params), function () {
        var has = false, at = 0, token = {};
        var every = cadence(method, opts);
        var failed = function (err) {
          if (!has) { self.fail(err, false); return; }
          // A missed beat or two is normal; the data is marked once it is
          // older than two refreshes, and stays on screen.
          if (Date.now() - at >= Math.max(2 * every, 60e3)) { self._fails.set(token, at); self.markStale(); }
        };
        var stop = la.watch(method, params, function (v, err) {
          if (err) { failed(err); return; }
          try { take.call(self, Array.isArray(v) ? v : []); } catch (e) { failed(e); return; }
          has = true;
          at = Date.now();
          if (self._fails.delete(token)) self.markStale();
          self.schedule();
        }, opts);
        return function () {
          stop();
          if (self._fails.delete(token)) self.markStale();
        };
      });
    }
    /**
     * Follow these symbols' quotes live, restarting only when the set changes.
     * opts.take(quotes) (as this) keeps what paint needs, by default as this.q,
     * and returns false when nothing drawn changed; opts.fail(err) hears a refusal.
     */
    quotes(symbols, opts) {
      var self = this, o = opts || {}, syms = toSymbols(symbols).slice(0, 200);
      var kind = o.kind === 'index' ? 'index' : '';
      var take = o.take || function (quotes) { this.q = quotes; };
      this.sub('quotes', kind + ':' + syms.join(','), function () {
        if (!syms.length) return null;
        return la.stream(syms, function (quotes, err) {
          if (err) { if (o.fail) o.fail.call(self, err); return; }
          if (take.call(self, quotes) !== false) self.schedule();
        }, { kind: kind });
      });
    }
    /** Dim the data and say so while a watch's re-reads fail; clear it once none do. */
    markStale() {
      var oldest = Infinity;
      this._fails.forEach(function (t) { if (t < oldest) oldest = t; });
      var on = oldest !== Infinity;
      this.body.classList.toggle('dim', on);
      this.staleEl.innerHTML = on
        ? '<span class="wdot"></span><span class="trunc">' + esc(say('stale', "Couldn't refresh · as of {{ago}} · retrying", { ago: format.ago(oldest) })) + '</span>'
        : '';
    }
    /**
     * Show how fresh the data is: in the heading of the <la-card> around the
     * element, which hears it as a bubbling la-status event, or else in a row
     * of its own at the top. null clears it.
     */
    setFreshness(f) {
      var key = freshKey(f);
      var card = this.isConnected ? cardOf(this) : null;
      if (card === this._freshAt && key === this._freshKey) return;
      // Detached, the element can no longer reach its old card by event.
      if (this._freshAt && this._freshAt !== card && typeof this._freshAt._dropStatus === 'function') this._freshAt._dropStatus(this);
      this._freshAt = card;
      this._freshKey = key;
      this.freshEl.innerHTML = !card && f ? tagHtml(f) : '';
      if (card) this.dispatchEvent(new CustomEvent('la-status', { detail: f, bubbles: true, composed: true }));
    }
    /**
     * The one way to paint: bursts of changes coalesce into one paint per
     * frame, and only a paint marks the element as drawn from data.
     */
    schedule() {
      if (this._raf) return;
      var self = this;
      this._raf = requestAnimationFrame(function () {
        self._raf = 0;
        if (self._gate === 'ok') { self.paint(); self._painted = true; }
      });
    }
  }

  function scopeNote(scope, pending) {
    var name = say('scope.' + scope, SCOPE_NAME[scope] || scope);
    return pending
      ? say('scopePending', 'Allow access to {{name}} in LangAlpha to show this.', { name: name })
      : say('scopeMissing', 'This dashboard has no access to {{name}}.', { name: name });
  }

  function define(tag, cls) {
    cls.scopes = TAG_SCOPES[tag] || [];
    if (!customElements.get(tag)) customElements.define(tag, cls);
  }

  // <la-quote symbol kind="stock|index">
  class LaQuote extends LaElement {
    static get observedAttributes() { return ['symbol', 'kind']; }
    start() {
      var index = this.attr('kind', 'stock') === 'index';
      // An index is keyed without its '^', as the page's quotes are.
      var sym = this.attr('symbol', '').trim().toUpperCase();
      if (index) sym = sym.replace(/^\^/, '');
      if (!sym) { this.note(say('setSymbol', 'Set a symbol.')); return; }
      this.sym = sym;
      this.index = index;
      this.q = {};
      this.quotes([sym], { kind: index ? 'index' : '', fail: function (err) { this.fail(err, !!this.q[sym]); } });
    }
    paint() {
      var q = this.q[this.sym];
      if (!q) { this.setFreshness(null); this.note(say('noQuote', 'No quote for {{symbol}}.', { symbol: this.sym })); return; }
      var t = tone(q.change), f = fresh(q.freshness);
      // In a card the tag goes to its heading, as a chart's and a list's do;
      // alone it stays beside the name.
      var inCard = !!cardOf(this);
      this.setFreshness(inCard ? f : null);
      this.html(
        '<div class="q" part="quote" data-sym="' + esc(this.sym) + '" role="link" tabindex="0">' +
        '<div class="top"><span class="sym" part="symbol">' + esc(this.sym) + '</span><span class="muted trunc" part="name">' + esc(q.name || '') + '</span>' +
        (f && !inCard ? tagHtml(f) : '') + '</div>' +
        // An index is a level, not an amount of money.
        '<div class="px num" part="price">' + format.price(q.price, this.index ? null : q.currency) + '</div>' +
        '<div class="chg num ' + t + '" part="change">' + format.change(q.change) + ' (' + format.percent(q.changePercent) + ')</div></div>'
      );
    }
  }
  LaQuote.css =
    '.q{padding:2px 4px;margin:0 -4px}' +
    '.top{display:flex;gap:8px;align-items:baseline;font-size:12px}.top .tag{margin-left:auto}' +
    '.px{font-size:24px;font-weight:600;letter-spacing:-.01em;margin-top:2px}' +
    '.chg{font-size:12px}';
  define('la-quote', LaQuote);

  function barsParams(el, interval) {
    var p = { symbol: el.attr('symbol', '').trim().toUpperCase(), interval: interval };
    if (el.attr('kind', '') === 'index') p.kind = 'index';
    return p;
  }

  function validInterval(v, fallback) { return INTERVALS.indexOf(v) >= 0 ? v : fallback; }

  /** The bars of the last n sessions. Bar times are market wall clock, so a day of them is one session. */
  function lastSessions(bars, n) {
    var days = uniq(bars.map(function (b) { return String(Math.floor(b.time / 86400)); }));
    var last = days.slice(-n);
    return bars.filter(function (b) { return last.indexOf(String(Math.floor(b.time / 86400))) >= 0; });
  }

  /** The closes' low and high, and y(close) placing one between pad and h - pad, the high at the top. */
  function closeScale(bars, h, pad) {
    var min = Infinity, max = -Infinity;
    bars.forEach(function (x) { if (x.close < min) min = x.close; if (x.close > max) max = x.close; });
    var span = max - min || 1;
    return { min: min, max: max, y: function (v) { return pad + (1 - (v - min) / span) * (h - 2 * pad); } };
  }

  // <la-sparkline symbol interval="5min" kind height="40">
  class LaSparkline extends LaElement {
    static get observedAttributes() { return ['symbol', 'interval', 'kind', 'height']; }
    skeleton() { this.html('<div class="sk" style="height:' + this.num('height', 40) + 'px;margin:0"></div>'); }
    start() {
      var p = barsParams(this, validInterval(this.attr('interval', '5min'), '5min'));
      if (!p.symbol) { this.note(say('setSymbol', 'Set a symbol.')); return; }
      this.intraday = p.interval !== '1day';
      this.watch('bars', 'bars.get', p, function (bars) { this.bars = bars; });
    }
    paint() {
      var b = (this.bars || []).filter(function (x) { return isNum(x.close) && isNum(x.time); });
      // Intraday, the line is the latest session, so it rises and falls with the day's change beside it.
      if (this.intraday) b = lastSessions(b, 1);
      var h = this.num('height', 40);
      if (b.length < 2) { this.note(say('noData', 'No data.')); return; }
      var y = closeScale(b, h, 2).y;
      var pts = b.map(function (x, i) {
        return (i / (b.length - 1) * 100).toFixed(2) + ',' + y(x.close).toFixed(2);
      }).join(' ');
      var t = b[b.length - 1].close >= b[0].close ? 'up' : 'down';
      this.html('<svg class="' + t + '" viewBox="0 0 100 ' + h + '" preserveAspectRatio="none" style="height:' + h + 'px" aria-hidden="true">' +
        '<polyline points="' + pts + '"/></svg>');
    }
  }
  LaSparkline.css = 'svg{display:block;width:100%;overflow:visible}polyline{fill:none;stroke:currentColor;stroke-width:1.5;vector-effect:non-scaling-stroke;stroke-linejoin:round}';
  define('la-sparkline', LaSparkline);

  var RANGE_DAYS = { '1D': 5, '5D': 10, '1M': 31, '3M': 92, '6M': 183, '1Y': 366, '5Y': 1827 };

  // <la-chart symbol interval range="6M" kind height="240">
  class LaChart extends LaElement {
    static get observedAttributes() { return ['symbol', 'interval', 'range', 'kind', 'height']; }
    skeleton() { this.html('<div class="sk" style="height:' + this.num('height', 240) + 'px;margin:0;border-radius:6px"></div>'); }
    start() {
      var range = this.attr('range', '6M').toUpperCase();
      if (!RANGE_DAYS[range] && range !== 'YTD') range = '6M';
      var def = range === '1D' ? '5min' : range === '5D' ? '30min' : '1day';
      var interval = validInterval(this.attr('interval', def), def);
      var from = new Date();
      if (range === 'YTD') from = new Date(from.getFullYear(), 0, 1);
      else from.setDate(from.getDate() - RANGE_DAYS[range]);
      var p = barsParams(this, interval);
      if (!p.symbol) { this.note(say('setSymbol', 'Set a symbol.')); return; }
      p.from = localDay(from);
      this.range = range;
      this.intraday = interval !== '1day';
      this.bars = null;
      this.fresh = null;
      this.cur = null;
      // bars.batch with the one symbol, for the series' freshness, which bars.get does not carry.
      var bp = { symbols: [p.symbol], interval: p.interval, from: p.from };
      if (p.kind) bp.kind = p.kind;
      this.watch('bars', 'bars.batch', bp, function (rows) {
        var r = rows.filter(function (x) { return x && String(x.symbol).toUpperCase() === p.symbol; })[0] || rows[0] || {};
        var bars = Array.isArray(r.bars) ? r.bars : [];
        if (r.error && !bars.length) throw laError('failed', String(r.error));
        this.bars = bars;
        this.fresh = fresh(r.freshness);
      }, { every: this.intraday ? 60e3 : 3e5 });
      // A series carries no currency, so a stock's comes from its quote; an
      // index is a level, not an amount of money, and has none.
      if (!p.kind) {
        this.quotes([p.symbol], {
          take: function (quotes) {
            var c = (quotes[p.symbol] || {}).currency || null;
            if (c === this.cur) return false; // a tick moves no axis label
            this.cur = c;
          }
        });
      }
    }
    sessions(bars) {
      // 1D and 5D fetch a few spare calendar days to cover weekends and holidays, then keep the last sessions.
      var keep = this.range === '1D' ? 1 : this.range === '5D' ? 5 : 0;
      return keep ? lastSessions(bars, keep) : bars;
    }
    paint() {
      if (!this.bars) return; // the series is still being read
      var self = this;
      var b = this.sessions(this.bars.filter(function (x) { return isNum(x.close) && isNum(x.time); }));
      var h = this.num('height', 240);
      var sym = this.attr('symbol', '').toUpperCase();
      if (b.length < 2) { this.setFreshness(null); this.note(say('noChartData', 'No chart data for {{symbol}}.', { symbol: sym })); return; }
      this.setFreshness(this.fresh);
      var sc = closeScale(b, 100, 8), min = sc.min, max = sc.max, yPct = sc.y;
      var xPct = function (i) { return i / (b.length - 1) * 100; };
      var line = b.map(function (x, i) { return (i ? 'L' : 'M') + (xPct(i) * 10).toFixed(1) + ' ' + yPct(x.close).toFixed(2); }).join('');
      var first = b[0].close, last = b[b.length - 1].close;
      var t = last >= first ? 'up' : 'down';
      var multiYear = new Date(b[0].time * 1000).getUTCFullYear() !== new Date(b[b.length - 1].time * 1000).getUTCFullYear();
      var label = function (x) { return barLabel(x.time, self.intraday, multiYear && !self.intraday); };
      var cur = this.cur;
      this.html(
        '<div class="head"><span class="sym" part="symbol">' + esc(sym) + '</span><span class="num" part="price">' + format.price(last, cur) + '</span>' +
        '<span class="num ' + t + '" part="change">' + format.percent((last - first) / first * 100) + '</span><span class="muted">' + esc(this.range) + '</span></div>' +
        '<div class="plot ' + t + '" part="chart" style="height:' + Math.max(60, h - 44) + 'px">' +
        '<svg viewBox="0 0 1000 100" preserveAspectRatio="none" aria-hidden="true">' +
        '<path class="area" d="' + line + 'L1000 100L0 100Z"/><path class="line" d="' + line + '"/></svg>' +
        '<span class="yl num" style="top:' + yPct(max) + '%">' + format.price(max, cur) + '</span>' +
        '<span class="yl num" style="top:' + yPct(min) + '%">' + format.price(min, cur) + '</span>' +
        '<div class="xh"></div><div class="dot"></div><div class="tip num"></div></div>' +
        '<div class="xl muted num"><span>' + esc(label(b[0])) + '</span><span>' + esc(label(b[b.length - 1])) + '</span></div>'
      );
      var plot = this.body.querySelector('.plot');
      var xh = plot.querySelector('.xh'), dot = plot.querySelector('.dot'), tip = plot.querySelector('.tip');
      var hide = function () { plot.classList.remove('hover'); };
      plot.addEventListener('pointermove', function (e) {
        var r = plot.getBoundingClientRect();
        var i = Math.round(Math.min(Math.max((e.clientX - r.left) / r.width, 0), 1) * (b.length - 1));
        var x = xPct(i), y = yPct(b[i].close);
        xh.style.left = dot.style.left = x + '%';
        dot.style.top = y + '%';
        tip.textContent = format.price(b[i].close, cur) + ' · ' + label(b[i]);
        tip.style.left = x > 60 ? '' : 'calc(' + x + '% + 8px)';
        tip.style.right = x > 60 ? 'calc(' + (100 - x) + '% + 8px)' : '';
        plot.classList.add('hover');
      });
      plot.addEventListener('pointerleave', hide);
    }
  }
  LaChart.css =
    '.head{display:flex;gap:8px;align-items:baseline;margin-bottom:6px;font-size:12px}' +
    '.plot{position:relative;touch-action:pan-y}' +
    'svg{position:absolute;inset:0;width:100%;height:100%;display:block}' +
    '.line{fill:none;stroke:currentColor;stroke-width:1.5;vector-effect:non-scaling-stroke;stroke-linejoin:round}' +
    '.area{fill:currentColor;fill-opacity:.07;stroke:none}' +
    '.yl{position:absolute;right:0;transform:translateY(-50%);font-size:11px;padding:0 4px;border-radius:3px;' +
    'color:var(--color-text-tertiary,#8b9099);background:var(--color-bg-card,var(--color-bg-page,transparent));opacity:.9;pointer-events:none}' +
    '.xh{position:absolute;top:0;bottom:0;width:1px;background:var(--color-text-tertiary,#8b9099);opacity:.5;display:none;pointer-events:none}' +
    '.dot{position:absolute;width:7px;height:7px;margin:-3.5px 0 0 -3.5px;border-radius:50%;background:currentColor;display:none;pointer-events:none}' +
    '.tip{position:absolute;top:0;font-size:11px;padding:2px 6px;border-radius:4px;white-space:nowrap;pointer-events:none;display:none;' +
    'color:var(--color-text-primary,inherit);background:var(--color-bg-elevated,var(--color-bg-card,Canvas));border:1px solid var(--color-border-muted,rgba(127,127,127,.25))}' +
    '.hover .xh,.hover .dot,.hover .tip{display:block}' +
    '.xl{display:flex;justify-content:space-between;font-size:11px;margin-top:4px}';
  LaChart.daily = true;
  define('la-chart', LaChart);

  /** The columns= names this element knows, in the order given; the default when none is known. */
  function columnsOf(el, known, fallback) {
    var lower = known.map(function (c) { return c.toLowerCase(); });
    var pick = function (src) {
      return uniq(String(src).split(/[\s,]+/).map(function (c) { return known[lower.indexOf(c.toLowerCase())]; }));
    };
    var out = pick(el.attr('columns', ''));
    if (!out.length) out = pick(fallback);
    // A row needs something that says what it is.
    if (out.indexOf('symbol') < 0 && out.indexOf('name') < 0) out.unshift('symbol');
    return out;
  }

  /**
   * The number columns of the two lists, drawn from a row (see quoteRows). part
   * defaults to the column's name; cls is a fixed class; hides marks a column
   * that gives way in a narrow card; cell(r) returns [tone class, inner html].
   */
  var COLUMNS = {
    price: { head: 'Price', hides: true, cell: function (r) { return ['', format.price(r.q.price, r.cur)]; } },
    change: { part: 'change', cls: 'chg', cell: function (r) { return [tone(r.q.change), format.change(r.q.change)]; } },
    changePercent: { part: 'change', cls: 'pct', cell: function (r) { return [tone(r.q.changePercent), format.percent(r.q.changePercent)]; } },
    quantity: { head: 'Qty', hides: true, cell: function (r) { return ['', nf({ maximumFractionDigits: 4 }).format(r.h.quantity)]; } },
    value: { head: 'Value', cell: function (r) { return ['', format.price(r.mv, r.cur)]; } },
    pl: {
      head: 'P/L',
      cell: function (r) { return [tone(r.gain), r.gain === null ? DASH : format.change(r.gain) + '<div class="sub">' + format.percent(r.gainPct) + '</div>']; }
    },
    cost: { head: 'Cost', hides: true, cell: function (r) { return ['', format.price(r.paid, r.costCur || r.cur)]; } }
  };

  function columnPart(c) { return COLUMNS[c].part || c; }

  /**
   * Each symbol's row: its quote (by upper-cased symbol), its freshness and
   * the dot that marks a row whose freshness differs from the list's tag.
   * fr is the list's summary, whose main state the tag shows.
   */
  function quoteRows(symbols, q) {
    var rows = symbols.map(function (s) {
      var sym = String(s).toUpperCase(), r = q[sym] || {};
      return { sym: sym, q: r, f: fresh(r.freshness), cur: r.currency };
    });
    var fr = freshSummary(rows.map(function (r) { return r.f; }));
    rows.forEach(function (r) { r.dot = odd(r.f, fr) ? rowDot(r.f) : ''; });
    return { rows: rows, fr: fr };
  }

  // <la-watchlist list columns="symbol,name,price,changePercent">
  class LaWatchlist extends LaElement {
    static get observedAttributes() { return ['list', 'columns']; }
    start() {
      this.wl = null;
      this.q = {};
      this.watch('lists', 'watchlists.list', {}, function (lists) {
        var want = this.attr('list', '').toLowerCase();
        var wl = (want && lists.filter(function (l) { return String(l.id).toLowerCase() === want || String(l.name).toLowerCase() === want; })[0]) || lists[0] || null;
        this.wl = wl || { id: '', name: '', items: [], none: true };
        this.quotes((this.wl.items || []).map(function (i) { return i.symbol; }));
      });
    }
    paint() {
      var wl = this.wl;
      if (!wl) return;
      if (wl.none) { this.setFreshness(null); this.note(say('noWatchlists', 'No watchlists yet.')); return; }
      if (!wl.items || !wl.items.length) { this.setFreshness(null); this.note(say('emptyWatchlist', 'This watchlist is empty.')); return; }
      var cols = columnsOf(this, ['symbol', 'name', 'price', 'change', 'changePercent'], 'symbol,name,price,changePercent');
      // In a card too narrow for both, the move stays and the price gives way.
      var hasMove = cols.indexOf('change') >= 0 || cols.indexOf('changePercent') >= 0;
      var list = quoteRows(wl.items.map(function (it) { return it.symbol; }), this.q), fr = list.fr;
      this.setFreshness(fr.main);
      this.html(wl.items.map(function (it, i) {
        var r = list.rows[i], pushed = false;
        var out = '<div class="row" part="row" data-sym="' + esc(r.sym) + '" role="link" tabindex="0">';
        if (fr.mixed) out += '<span class="fs">' + r.dot + '</span>';
        cols.forEach(function (c) {
          if (c === 'symbol') { out += '<span class="sym c1" part="symbol">' + esc(r.sym) + '</span>'; return; }
          if (c === 'name') { out += '<span class="muted trunc c2" part="name">' + esc(it.name || r.q.name || '') + '</span>'; return; }
          var col = COLUMNS[c], cell = col.cell(r);
          // The first number is pushed right, which a name column does by itself.
          var cls = 'num' + (col.cls ? ' ' + col.cls : '') + (pushed ? '' : ' r') + (col.hides && hasMove ? ' opt' : '') + (cell[0] ? ' ' + cell[0] : '');
          pushed = true;
          out += '<span class="' + cls + '" part="' + columnPart(c) + '">' + cell[1] + '</span>';
        });
        return out + '</div>';
      }).join(''));
    }
  }
  LaWatchlist.repaint = ['columns'];
  // The number columns keep one width down the list so they line up, and give
  // way together in a narrow card, where four columns and their gaps overflow;
  // narrower still, the price goes and the move takes its place at the right.
  LaWatchlist.css = ':host{container-type:inline-size}' +
    '.c1{flex:0 0 auto;min-width:52px}.c2{flex:1 1 auto}.pct,.chg{flex:0 0 64px;text-align:right}' +
    '@container (max-width:300px){.row{gap:6px}.c1{min-width:44px}.pct,.chg{flex-basis:56px}}' +
    '@container (max-width:230px){.opt{display:none}.opt+.num{margin-left:auto}}' +
    '.fs{flex:0 0 6px;align-self:center;display:flex;margin-right:-4px}';
  define('la-watchlist', LaWatchlist);

  /** A list read from one method: the items' markup, or a note when there are none. */
  class LaFeed extends LaElement {
    start() {
      var spec = this.constructor.feed;
      this.items = null;
      this.watch('items', spec.method, spec.params ? spec.params(this) : {}, function (items) { this.items = items; });
    }
    paint() {
      var spec = this.constructor.feed, items = this.items || [];
      if (!items.length) { this.note(say(spec.empty[0], spec.empty[1])); return; }
      this.html(items.map(spec.item).join(''));
    }
  }

  /** spec: method, params(el), attrs (observed), empty: [key, English], item(row) -> markup, css. */
  function defineFeed(tag, spec) {
    var attrs = spec.attrs || [];
    class Feed extends LaFeed { static get observedAttributes() { return attrs; } }
    Feed.feed = spec;
    Feed.ages = true;
    Feed.css = spec.css;
    define(tag, Feed);
  }

  // <la-news tickers="AAPL,MSFT" limit="10">
  defineFeed('la-news', {
    method: 'news.list',
    attrs: ['tickers', 'limit'],
    params: function (el) {
      var p = { limit: Math.min(50, Math.round(el.num('limit', 10))) };
      var tickers = toSymbols(el.attr('tickers', ''));
      if (tickers.length) p.tickers = tickers;
      return p;
    },
    empty: ['noNews', 'No news right now.'],
    item: function (n) {
      var safe = typeof n.url === 'string' && /^https?:\/\//i.test(n.url);
      var title = safe ? '<a href="' + esc(n.url) + '" target="_blank" rel="noopener noreferrer">' + esc(n.title) + '</a>' : esc(n.title);
      var meta = [n.source, format.ago(n.publishedAt)].filter(Boolean).map(esc).join(' · ');
      var chips = (n.tickers || []).slice(0, 4).map(function (t) {
        return '<span class="chip" part="symbol" data-sym="' + esc(t) + '" role="link" tabindex="0">' + esc(t) + '</span>';
      }).join('');
      return '<div class="item" part="item"><div class="t" part="title">' + title + '</div><div class="m"><span class="muted" part="meta">' + meta + '</span>' + chips + '</div></div>';
    },
    css:
      '.item{padding:7px 0;border-top:1px solid var(--color-border-muted,rgba(127,127,127,.18))}.item:first-child{border-top:0;padding-top:0}' +
      '.t{font-weight:500;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}' +
      '.m{display:flex;flex-wrap:wrap;gap:6px;align-items:center;font-size:11px;margin-top:3px}' +
      '.chip{padding:0 5px;border-radius:4px;font-size:10.5px;font-weight:600;color:var(--color-text-secondary,#6b7280);background:var(--color-bg-tag,rgba(127,127,127,.12))}'
  });

  // <la-brief>
  defineFeed('la-brief', {
    method: 'brief.today',
    empty: ['noBrief', 'No brief yet today.'],
    item: function (b) {
      return '<div class="item" part="item"><div class="t" part="title">' + esc(b.title) + '</div>' +
        (b.summary ? '<div class="s sec">' + esc(b.summary) + '</div>' : '') +
        (b.createdAt ? '<div class="muted d" part="meta">' + esc(format.ago(b.createdAt)) + '</div>' : '') + '</div>';
    },
    css:
      '.item+.item{margin-top:10px;padding-top:10px;border-top:1px solid var(--color-border-muted,rgba(127,127,127,.18))}' +
      '.t{font-weight:600}.s{margin-top:3px;display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;overflow:hidden}.d{font-size:11px;margin-top:3px}'
  });

  var EARNINGS_TIME = { bmo: 'Before open', amc: 'After close', dmh: 'During market' };

  // <la-earnings days="7" symbols=""> (no symbols: the names on the user's watchlists)
  class LaEarnings extends LaElement {
    static get observedAttributes() { return ['days', 'symbols']; }
    start() {
      var self = this;
      var days = Math.min(90, Math.round(this.num('days', 7)));
      var to = new Date();
      to.setDate(to.getDate() + days);
      var range = { from: localDay(new Date()), to: localDay(to) };
      this.days = days;
      this.items = null;
      this.empty = false;
      var calendar = function (syms) {
        var was = self.empty;
        self.empty = !syms.length;
        if (self.empty) {
          self.items = null;
          self.sub('calendar', '', function () { return null; });
        } else {
          // A new set of names keeps the old calendar on screen until its own answers.
          self.watch('calendar', 'earnings.calendar', { from: range.from, to: range.to, symbols: syms }, function (items) { this.items = items; });
        }
        // Out of the note, the calendar is loading (the note itself is painted
        // with the watchlists' answer that emptied them).
        if (was && !self.empty) self.skeleton();
      };
      var syms = toSymbols(this.attr('symbols', ''));
      this.own = !syms.length;
      if (syms.length) { calendar(syms); return; }
      // The whole calendar is thousands of names a week; without a list, the
      // names the user follows are the ones whose earnings matter.
      this.watch('lists', 'watchlists.list', {}, function (lists) {
        var all = [];
        lists.forEach(function (l) { (l.items || []).forEach(function (i) { all.push(i.symbol); }); });
        calendar(toSymbols(all).slice(0, 200));
      });
    }
    paint() {
      if (this.own && this.empty) { this.note(say('earningsNeedWatchlist', 'Add names to a watchlist to see their earnings here.')); return; }
      if (!this.items) return; // the calendar is still being read
      var items = this.items.slice().sort(function (a, b) { return a.date < b.date ? -1 : a.date > b.date ? 1 : 0; });
      if (!items.length) {
        this.note(this.own
          ? say('noEarningsOwn', 'No earnings in the next {{days}} days for your watchlists.', { days: this.days })
          : say('noEarnings', 'No earnings in the next {{days}} days.', { days: this.days }));
        return;
      }
      var out = '', day = null;
      items.forEach(function (e) {
        if (e.date !== day) {
          day = e.date;
          out += '<div class="day muted" part="day">' + esc(format.date(day, { weekday: 'short', month: 'short', day: 'numeric' })) + '</div>';
        }
        var code = String(e.time || '').toLowerCase();
        var when = e.time ? (EARNINGS_TIME[code] ? say('earningsTime.' + code, EARNINGS_TIME[code]) : e.time) : '';
        out += '<div class="row" part="row" data-sym="' + esc(e.symbol) + '" role="link" tabindex="0"><span class="sym c1" part="symbol">' + esc(e.symbol) + '</span>' +
          '<span class="muted trunc">' + esc(when) + '</span>' +
          '<span class="r num sec">' + (isNum(e.epsEstimated) ? esc(say('eps', 'EPS {{value}}', { value: nf({ minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(e.epsEstimated) })) : '') + '</span>' +
          '<span class="num sec rev">' + (isNum(e.revenueEstimated) ? format.compact(e.revenueEstimated) : '') + '</span></div>';
      });
      this.html(out);
    }
  }
  LaEarnings.css =
    '.day{font-size:11px;font-weight:500;padding:8px 0 2px}.day:first-child{padding-top:0}' +
    '.c1{min-width:52px}.rev{flex:0 0 56px;text-align:right}' +
    '.day+.row{border-top:0}';
  LaEarnings.daily = true;
  define('la-earnings', LaEarnings);

  // <la-portfolio columns="symbol,name,quantity,price,value,pl">
  // A narrow card keeps value and P/L, which are what the table is for.
  class LaPortfolio extends LaElement {
    static get observedAttributes() { return ['columns']; }
    start() {
      this.q = {};
      this.watch('holdings', 'portfolio.holdings', {}, function (rows) {
        this.holdings = rows;
        this.quotes(rows.map(function (h) { return h.symbol; }));
      });
    }
    paint() {
      var hs = this.holdings;
      if (!hs) return;
      if (!hs.length) { this.setFreshness(null); this.note(say('noHoldings', 'No holdings yet.')); return; }
      var cols = columnsOf(this, ['symbol', 'name', 'quantity', 'price', 'value', 'pl', 'cost'], 'symbol,name,quantity,price,value,pl');
      var showSym = cols.indexOf('symbol') >= 0, showName = cols.indexOf('name') >= 0;
      var nums = cols.filter(function (c) { return c !== 'symbol' && c !== 'name'; });
      var list = quoteRows(hs.map(function (h) { return h.symbol; }), this.q), fr = list.fr;
      this.setFreshness(fr.main);
      // A holding's own currency is the one its cost was entered in; its price
      // and value are in the quote's, which is another for a holding costed
      // in the user's money and listed abroad.
      var code = function (c) { return c ? String(c).trim().toUpperCase() : null; };
      var money = hs.map(function (h, i) {
        var quoted = code(list.rows[i].cur) || code(h.currency) || 'USD';
        return { quoted: quoted, cost: code(h.currency) || quoted };
      });
      // Each total is in the currency of the figures it adds: values in their
      // quotes', costs in theirs. One that would add unlike units is left out;
      // the rows carry theirs.
      var curs = { value: [], cost: [], pl: [] };
      var one = function (codes) { var u = uniq(codes); return u.length === 1 ? u[0] : null; };
      var value = 0, cost = 0, pnl = 0, basis = 0, priced = 0, gained = 0, costed = 0;
      var td = function (c, cls, inner) {
        return '<td class="num' + (COLUMNS[c].hides ? ' opt' : '') + (cls ? ' ' + cls : '') + '" part="' + columnPart(c) + '">' + inner + '</td>';
      };
      var rows = hs.map(function (h, i) {
        var r = list.rows[i];
        r.h = h;
        r.cur = money[i].quoted;
        r.costCur = money[i].cost;
        r.mv = isNum(r.q.price) ? r.q.price * h.quantity : null;
        r.paid = isNum(h.averageCost) ? h.averageCost * h.quantity : null;
        // A cost in one currency cannot be measured against a value in another.
        r.gain = r.cur === r.costCur && r.mv !== null && r.paid !== null ? r.mv - r.paid : null;
        r.gainPct = r.gain !== null && h.averageCost ? r.gain / r.paid * 100 : null;
        if (r.mv !== null) { value += r.mv; priced++; curs.value.push(r.cur); }
        if (r.gain !== null) { pnl += r.gain; cost += r.paid; gained++; curs.pl.push(r.cur); }
        if (r.paid !== null) { basis += r.paid; costed++; curs.cost.push(r.costCur); }
        var name = esc(h.name || r.q.name || '');
        return '<tr part="row" data-sym="' + esc(r.sym) + '" role="link" tabindex="0"><td>' +
          (r.dot ? '<span class="fs">' + r.dot + '</span>' : '') +
          (showSym ? '<span class="sym" part="symbol">' + esc(r.sym) + '</span>' : '') +
          (showName ? '<div class="trunc' + (showSym ? ' muted nm' : '') + '" part="name">' + name + '</div>' : '') + '</td>' +
          nums.map(function (c) { var cell = COLUMNS[c].cell(r); return td(c, cell[0], cell[1]); }).join('') + '</tr>';
      }).join('');
      // A total counts the holdings that have the figure: one with no quote
      // has no value, one with no cost no P/L. Short of all of them it says
      // how many, so a part is never read as the whole.
      var of = function (n) {
        return n < hs.length ? '<div class="sub of">' + esc(say('partialTotal', '{{part}} of {{all}} holdings', { part: n, all: hs.length })) + '</div>' : '';
      };
      var totals = {
        quantity: '',
        price: '',
        value: one(curs.value) ? format.price(value, one(curs.value)) + of(priced) : DASH,
        pl: one(curs.pl) ? format.change(pnl) + '<div class="sub">' + format.percent(cost ? pnl / cost * 100 : null) + '</div>' + of(gained) : DASH,
        cost: one(curs.cost) ? format.price(basis, one(curs.cost)) + of(costed) : DASH
      };
      var total = '<tr class="tot" part="total"><td>' + esc(say('total', 'Total')) + '</td>' +
        nums.map(function (c) { return td(c, c === 'pl' ? tone(one(curs.pl) ? pnl : null) : '', totals[c]); }).join('') + '</tr>';
      var head = '<th part="header">' + esc(showSym ? say('column.symbol', 'Symbol') : say('column.name', 'Name')) + '</th>' + nums.map(function (c) {
        return '<th part="header"' + (COLUMNS[c].hides ? ' class="opt"' : '') + '>' + esc(say('column.' + c, COLUMNS[c].head)) + '</th>';
      }).join('');
      // The dot slot is reserved only while some row's freshness differs from the rest.
      this.html('<table' + (fr.mixed ? ' class="mixed"' : '') + '><thead><tr>' + head + '</tr></thead><tbody>' + rows + total + '</tbody></table>');
    }
  }
  LaPortfolio.repaint = ['columns'];
  LaPortfolio.css =
    ':host{container-type:inline-size}@container (max-width:420px){.opt{display:none}}' +
    'table{width:100%;border-collapse:collapse;table-layout:auto}' +
    'th{font-size:11px;font-weight:500;color:var(--color-text-tertiary,#8b9099);text-align:right;padding:0 0 4px 8px}th:first-child{text-align:left;padding-left:0}' +
    'td{padding:6px 0 6px 8px;text-align:right;vertical-align:top;border-top:1px solid var(--color-border-muted,rgba(127,127,127,.18))}td:first-child{text-align:left;padding-left:0;max-width:0;width:40%}' +
    '.mixed td:first-child,.mixed th:first-child{padding-left:12px}.mixed td:first-child{position:relative}' +
    '.fs{position:absolute;left:0;top:12px;display:flex}' +
    '.nm{font-size:11px}.sub{font-size:11px;opacity:.85}.of{font-weight:400;white-space:nowrap;color:var(--color-text-tertiary,#8b9099)}' +
    '.tot td{font-weight:600}.tot:hover{background:none}';
  define('la-portfolio', LaPortfolio);

  // <la-threads limit="8">
  defineFeed('la-threads', {
    method: 'threads.recent',
    attrs: ['limit'],
    params: function (el) { return { limit: Math.min(50, Math.round(el.num('limit', 8))) }; },
    empty: ['noThreads', 'No threads yet.'],
    item: function (th) {
      return '<div class="row" part="row" data-thread="' + esc(th.id) + '" data-ws="' + esc(th.workspaceId || '') + '" role="link" tabindex="0">' +
        '<span class="trunc" part="title">' + esc(th.title || say('untitled', 'Untitled')) + '</span><span class="r muted when" part="meta">' + esc(format.ago(th.updatedAt)) + '</span></div>';
    },
    css: '.when{flex:0 0 auto;font-size:11px}'
  });

  /** The time a data file says it was written, by a dotted key; seconds or ms since the epoch, or a date string. */
  function readStamp(data, field) {
    var v = data;
    String(field).split('.').forEach(function (k) { v = v != null && typeof v === 'object' ? v[k] : undefined; });
    if (isNum(v)) return v < 1e11 ? v * 1000 : v;
    return typeof v === 'string' && v ? Date.parse(v) : NaN;
  }

  /** A run's time for a sentence: "8:30 AM" today, "Fri 4:30 PM" this week, "Oct 3, 4:30 PM" before (with a year past this one). */
  function runLabel(d) { return df(timeFormat(d, true)).format(d); }

  // <la-updated src="data/screen.json" field="as_of" due="30 8,16 * * 1-5" timezone="America/New_York" grace="15">
  // How old the page's own data file is, and whether its scheduled run missed.
  // It reads a file beside the page, so it needs no scope and works outside LangAlpha.
  class LaUpdated extends LaElement {
    static get observedAttributes() { return ['src', 'field', 'due', 'timezone', 'grace']; }
    start() {
      var self = this, src = this.attr('src', ''), field = this.attr('field', 'as_of'), loaded = false;
      this.asOf = NaN;
      if (!src) { this.schedule(); return; }
      this.html('<div class="u" part="updated">&nbsp;</div>');
      this.sub('src', src + ' ' + field, function () {
        return la.source(src, function (data, err) {
          if (!err) self.asOf = readStamp(data, field);
          // A failed re-read (say the file mid-rewrite) keeps the time already shown.
          else if (!loaded) self.asOf = NaN;
          loaded = true;
          self.schedule();
        }, { every: 60e3 });
      });
    }
    paint() {
      var at = this.asOf;
      if (!isFinite(at)) { this.html('<div class="u" part="updated">' + esc(say('noDataYet', 'No data yet')) + '</div>'); return; }
      var g = Number(this.getAttribute('grace'));
      if (!this.hasAttribute('grace') || !isFinite(g) || g < 0) g = 15;
      var due = this.attr('due', '');
      var missed = due ? la.overdue(at, due, this.attr('timezone', ''), g) : null;
      // A clock behind the writer's would read "in 2m"; it was written, so it is now.
      var ago = format.ago(Math.min(at, Date.now()));
      var text = missed
        ? say('updatedMissed', 'Updated {{ago}} · missed the {{run}} update', { ago: ago, run: runLabel(missed) })
        : say('updated', 'Updated {{ago}}', { ago: ago });
      this.html('<div class="u' + (missed ? ' late' : '') + '" part="updated' + (missed ? ' stale' : '') + '" title="' + esc(say('updatedAt', 'Updated {{time}}', { time: stamp(at) })) + '">' +
        (missed ? '<span class="wdot"></span>' : '') + '<span class="trunc">' + esc(text) + '</span></div>');
    }
  }
  LaUpdated.repaint = ['due', 'timezone', 'grace'];
  // Nothing it shows comes from LangAlpha but its words.
  LaUpdated.standalone = true;
  LaUpdated.ages = true;
  LaUpdated.css =
    '.u{display:flex;align-items:center;gap:6px;font-size:11.5px;color:var(--color-text-tertiary,#8b9099)}' +
    '.u.late{color:var(--color-warning,#d97706)}';
  define('la-updated', LaUpdated);

  // <la-grid cols="3">: children may take span="2".
  class LaGrid extends HTMLElement {
    static get observedAttributes() { return ['cols']; }
    constructor() {
      super();
      this.attachShadow({ mode: 'open' }).innerHTML =
        '<style>:host{display:grid;gap:12px;grid-template-columns:repeat(var(--la-cols,3),minmax(0,1fr));align-items:start}' +
        '::slotted([span="2"]){grid-column:span 2}::slotted([span="3"]){grid-column:span 3}::slotted([span="4"]){grid-column:span 4}' +
        '@media (max-width:640px){:host{grid-template-columns:minmax(0,1fr)}::slotted(*){grid-column:auto!important}}</style><slot></slot>';
    }
    connectedCallback() { this.attributeChangedCallback(); }
    attributeChangedCallback() {
      var n = Math.round(Number(this.getAttribute('cols')));
      this.style.setProperty('--la-cols', String(n >= 1 && n <= 12 ? n : 3));
    }
  }
  if (!customElements.get('la-grid')) customElements.define('la-grid', LaGrid);

  // <la-card title> (or heading=, which skips the browser's hover tooltip that title= brings)
  // A la-status event from inside (detail: a freshness, or null) puts its tag
  // in the heading; kit quotes, lists and charts send one, and so may the page's own markup.
  class LaCard extends HTMLElement {
    static get observedAttributes() { return ['title', 'heading']; }
    constructor() {
      super();
      var root = this.attachShadow({ mode: 'open' });
      root.innerHTML =
        '<style>:host{display:block;min-width:0;background:var(--color-bg-card,rgba(127,127,127,.06));' +
        'border:1px solid var(--color-border-muted,rgba(127,127,127,.2));border-radius:10px;padding:14px}' +
        '.t{display:flex;align-items:baseline;gap:8px;font-size:12px;font-weight:500;color:var(--color-text-tertiary,#8b9099);margin:0 0 8px}.t:empty{display:none}' +
        '.h{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.t .tag{margin-left:auto}' + TAG_CSS + '</style>' +
        '<div class="t" part="heading"></div><slot></slot>';
      this.titleEl = root.querySelector('.t');
      this._status = new Map(); // sender -> freshness
      var self = this;
      this.addEventListener('la-status', function (e) {
        var from = (e.composedPath && e.composedPath()[0]) || e.target;
        // A card inside this one carries its own content's tags.
        if (cardOf(from) !== self) return;
        var f = fresh(e.detail);
        if (f) self._status.set(from, f); else self._status.delete(from);
        self.render();
      });
    }
    _dropStatus(from) { if (this._status.delete(from)) this.render(); }
    connectedCallback() { this.render(); }
    attributeChangedCallback() { this.render(); }
    render() {
      var list = [], status = this._status;
      status.forEach(function (f, from) { if (from.isConnected) list.push(f); else status.delete(from); });
      var main = freshSummary(list).main;
      var heading = this.getAttribute('heading') || this.getAttribute('title') || '';
      // Empty when there is neither, so .t:empty hides the row.
      this.titleEl.innerHTML = (heading ? '<span class="h">' + esc(heading) + '</span>' : '') + (main ? tagHtml(main) : '');
    }
  }
  if (!customElements.get('la-card')) customElements.define('la-card', LaCard);

  window.la = la;
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', start);
  else start();
})();
