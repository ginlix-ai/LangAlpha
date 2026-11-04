// @ts-check
// Where the document's links and forms lead: the view carried to its next file,
// another workspace file opened beside this one, another site in a tab of its
// own, a download that keeps the page, and a renewed grant moved to.

// The view rides on its URL (?inject=), which a link in the document does
// not carry: followed as written, it would land on the next file with no
// bridge, unthemed, and a reader of the owner's live view could not comment
// on it. So a link that stays under this view's prefix (the owner's file
// grant, or a shared thread's serve route) gets this document's inject value
// before the browser follows it, its fragment and other parameters kept.
// Other origins and prefixes are left as written. A GET form carries it as a
// field (carryFormMode); a script that sets location itself is not covered.
var LIVE_PREFIX = /^.*?\/(?:wsfiles\/g\/[^\/]+|api\/v1\/public\/shared\/[^\/]+\/files\/serve)\//;

/** @param {string} pathname */
function livePrefixOf(pathname) {
  var match = LIVE_PREFIX.exec(pathname);
  return match ? match[0] : null;
}

var livePrefix = livePrefixOf(location.pathname);
// The part of the address a renewed grant replaces: the live view's prefix, or
// a published version's grant (/artifacts/g/<grant>/v/<seq>/ on the API
// origin, /g/<grant>/v/<seq>/ on a version's own host).
var GRANT_PREFIX = /^.*?\/(?:wsfiles\/g\/[^\/]+|api\/v1\/public\/shared\/[^\/]+\/files\/serve|g\/[^\/]+\/v\/[0-9]+)\//;

/** @param {string} pathname */
function grantPrefixOf(pathname) {
  var match = GRANT_PREFIX.exec(pathname);
  return match ? match[0] : null;
}

var grantPrefix = grantPrefixOf(location.pathname);
// In the owner's view, a link to another of the workspace's files opens beside
// this one, in a tab of the viewer's with the viewer that suits it (a CSV as a
// table), and this document stays. A section of this document stays here. A
// share's route keeps to the frame, where only what it shares answers.
var ownerView = livePrefix !== null && /\/wsfiles\/g\/[^\/]+\/$/.test(livePrefix);
/** @type {string | null} */
var LIVE_MODE = null;
try {
  // The last, as the server reads it: a form may send a field of the page's
  // own by that name ahead of the view's (carryFormMode).
  var modes = new URLSearchParams(location.search).getAll('inject');
  LIVE_MODE = modes.length ? modes[modes.length - 1] : null;
} catch (e) {
  LIVE_MODE = null;
}

/** @param {Element} link */
function carryMode(link) {
  var href = link.getAttribute('href');
  if (href === null || !livePrefix || !LIVE_MODE) return;
  var url;
  try {
    url = new URL(href, document.baseURI);
  } catch (e) {
    return;
  }
  if (url.protocol !== location.protocol || url.host !== location.host) return;
  if (url.pathname.indexOf(livePrefix) !== 0) return;
  // Already carried, a section of this same document included: rewritten,
  // the jump would reload the page.
  if (url.searchParams.get('inject') === LIVE_MODE) return;
  url.searchParams.set('inject', LIVE_MODE);
  link.setAttribute('href', url.href);
}

/**
 * The link a click is on, if any.
 * @param {Event} event
 */
function linkOf(event) {
  var target = /** @type {Element | null} */ (event.target);
  return target && target.closest ? target.closest('a[href], area[href]') : null;
}

// Capturing, so the address is set before any handler, the page's own
// included, reads it or follows it.
window.addEventListener('click', function (event) {
  var link = linkOf(event);
  if (link) carryMode(link);
}, true);

// A renewed grant reloads nothing, so a document left open moves to it, or what
// it loads next would fail when the old grant ends. WebKit refuses the move on
// an opaque origin, and there the document keeps the grant it has.
listen('page:address', function (message) {
  var prefix = grantPrefix && grantPrefixOf(new URL(message.src, location.href).pathname);
  if (!grantPrefix || !prefix || prefix === grantPrefix) return;
  try {
    history.replaceState(history.state, '', location.href.replace(grantPrefix, prefix));
  } catch (e) {
    return;
  }
  if (livePrefix === grantPrefix) livePrefix = prefix;
  grantPrefix = prefix;
});

/**
 * The workspace path an address names in the owner's view, when it is another
 * file than this document.
 * @param {{ host: string, pathname: string }} address
 * @returns {string | null}
 */
function ownerFile(address) {
  if (!ownerView || !livePrefix || address.host !== location.host) return null;
  if (address.pathname === location.pathname || address.pathname.indexOf(livePrefix) !== 0) return null;
  try {
    return decodeURIComponent(address.pathname.slice(livePrefix.length)) || null;
  } catch (e) {
    return null;
  }
}

// An opaque origin has no same origin, so WebKit ignores download on a link to
// the document's own file, and the frame goes to the file and loses the page. A
// blob is the document's own in every browser. Asked for without the view's
// mode, which would put the bridge in a saved HTML file.
/** @param {HTMLAnchorElement | HTMLAreaElement} link */
function saveOwnFile(link) {
  var url = new URL(link.href);
  url.searchParams.delete('inject');
  fetch(url.href).then(function (answer) {
    if (!answer.ok) throw new Error(String(answer.status));
    return answer.blob();
  }).then(function (blob) {
    var save = document.createElement('a');
    save.href = URL.createObjectURL(blob);
    save.download = link.getAttribute('download') || decodeURIComponent(url.pathname.split('/').pop() || '');
    document.body.appendChild(save);
    save.click();
    save.remove();
    window.setTimeout(function () { URL.revokeObjectURL(save.href); }, 60000);
  }).catch(function () {
    window.open(url.href, '_blank', 'noopener,noreferrer');
  });
}

/**
 * What to do in place of following a link, or null when the browser should
 * follow it: a link within the page, a section of it included.
 * @param {HTMLAnchorElement | HTMLAreaElement} link
 * @returns {(() => void) | null}
 */
function linkAction(link) {
  // Another site opens in a tab of its own: the viewer lets a page's frame load
  // only the page's own origin and the mail and phone hand-offs, so followed in
  // place it would be a dead click.
  // By origin, not host: the same host over http is another site too.
  if (link.origin !== location.origin) {
    return function () { window.open(link.href, '_blank', 'noopener,noreferrer'); };
  }
  if (link.hasAttribute('download')) {
    return window.origin === 'null' ? function () { saveOwnFile(link); } : null;
  }
  var file = viewerOrigin === null ? null : ownerFile(link);
  if (file === null) return null;
  // The panel opens a file by its path, so a link that carries a query of its
  // own is followed in the frame, where the page can read it.
  var query = new URLSearchParams(link.search);
  query.delete('inject');
  if (query.toString()) return null;
  var path = file;
  return function () { send({ type: 'page:open-file', path: path, fragment: link.hash.slice(1) }); };
}

// Any scheme but the web's is the browser's. On window and bubbling, so the
// page's own handlers hear the click first and one that took it has said so.
window.addEventListener('click', function (event) {
  if (event.defaultPrevented || event.button !== 0) return;
  var link = /** @type {HTMLAnchorElement | HTMLAreaElement | null} */ (linkOf(event));
  // An SVG link's href is an object, which is no address to act on.
  if (!link || typeof link.href !== 'string' || !/^https?:/i.test(link.href)) return;
  var action = linkAction(link);
  if (!action) return;
  event.preventDefault();
  action();
});

/**
 * A GET form's fields replace its address's query, the view's with it, so a
 * form sent to this view's own files carries the view as a field of its own.
 * @param {HTMLFormElement} form
 * @param {URL} target
 * @param {string} method
 */
function carryFormMode(form, target, method) {
  if (!livePrefix || !LIVE_MODE || livePrefixOf(target.pathname) !== livePrefix) return;
  if (method.toLowerCase() !== 'get') return;
  // The bridge's own field, last: the server reads the last `inject`, so a
  // field of the page's own by that name still reaches the page's script.
  var field = /** @type {HTMLInputElement | null} */ (form.querySelector('input[data-view-mode]'));
  if (!field) {
    field = document.createElement('input');
    field.type = 'hidden';
    field.name = 'inject';
    field.setAttribute('data-view-mode', '');
  }
  field.value = LIVE_MODE;
  form.appendChild(field);
}

// A form sent to another site opens in a tab of its own, as such a link does.
// The button that sent it may name its own address, method and window, which
// win over the form's. Attributes, not properties: an input named "action"
// shadows that. With no address, a form is sent to this document's own, and
// with no window, or `_self`, to this frame.
window.addEventListener('submit', function (event) {
  var form = /** @type {HTMLFormElement} */ (event.target);
  var by = /** @type {SubmitEvent} */ (event).submitter || null;
  /**
   * @param {string} own
   * @param {string} forms
   */
  function said(own, forms) {
    return by && by.hasAttribute(own) ? by.getAttribute(own) : form.getAttribute(forms);
  }
  var where = (said('formtarget', 'target') || '').trim().toLowerCase();
  if (event.defaultPrevented || (where && where !== '_self')) return;
  var target;
  try {
    target = new URL(said('formaction', 'action') || location.href, document.baseURI);
  } catch (e) {
    return;
  }
  if (target.host === location.host) {
    carryFormMode(form, target, said('formmethod', 'method') || 'get');
    return;
  }
  // On the button when one sent it, so another button's send stays here.
  if (by) by.setAttribute('formtarget', '_blank');
  else form.setAttribute('target', '_blank');
  form.setAttribute('rel', 'noopener noreferrer');
});
