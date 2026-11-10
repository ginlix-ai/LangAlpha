/**
 * Keeps React alive on a page the browser has translated.
 *
 * Chrome's translator (and others like it) swaps each text node React owns for
 * `<font>` wrappers holding the translation, so the node React kept a handle to
 * is no longer in the document. When React later removes that node, or inserts
 * a sibling before it, the DOM throws NotFoundError from the commit phase; no
 * boundary catches that, and React unmounts the root, leaving a blank page
 * (facebook/react#11538, which React does not plan to fix).
 *
 * Both calls run as before and absorb only that NotFoundError. The check is the
 * DOM's own, not a read of `parentNode`, which a form control named
 * `parentNode` can shadow: DOMPurify removes a clobbered form by calling
 * `removeChild` and relies on it to throw if it cannot.
 */
export function tolerateTranslatedDom(onAbsorb?: (error: DOMException) => void): () => void {
  if (typeof Node !== 'function') return () => {};
  const proto = Node.prototype;
  const removeChild = proto.removeChild;
  const insertBefore = proto.insertBefore;
  // Runs inside a DOM call React is committing, so a reporter that throws must
  // not turn the absorbed error back into a blank page.
  const report = (error: DOMException): void => {
    try {
      onAbsorb?.(error);
    } catch {
      // The warning below still records it.
    }
  };

  proto.removeChild = function <T extends Node>(this: Node, child: T): T {
    try {
      return removeChild.call(this, child) as T;
    } catch (error) {
      if (!isNotFound(error)) throw error;
      report(error);
      console.warn('[translatedDom] removeChild skipped: the node has another parent', child, this);
      return child;
    }
  };

  proto.insertBefore = function <T extends Node>(this: Node, node: T, child: Node | null): T {
    try {
      return insertBefore.call(this, node, child) as T;
    } catch (error) {
      if (!isNotFound(error)) throw error;
      report(error);
      // The reference node is gone, so its place is unknown. Appending keeps
      // the node in the document, where its refs and events work and React can
      // insert its later siblings before it; dropping it would fail those too.
      console.warn('[translatedDom] insertBefore appended: the reference node has another parent', child, this);
      return insertBefore.call(this, node, null) as T;
    }
  };

  return () => {
    proto.removeChild = removeChild;
    proto.insertBefore = insertBefore;
  };
}

/**
 * Whether a translator has rewritten the page's text. Chrome marks the root
 * with a class; it and Google Translate wrap each translation in
 * `<font style="vertical-align: inherit">`. A bare `<font>` is not enough: a
 * Mermaid label can hold one (`A["<font color=red>x</font>"]`).
 */
export function pageIsTranslated(): boolean {
  if (typeof document === 'undefined') return false;
  return document.querySelector('html.translated-ltr, html.translated-rtl, font[style*="vertical-align"]') !== null;
}

function isNotFound(error: unknown): error is DOMException {
  return error instanceof DOMException && error.name === 'NotFoundError';
}
