import { desktop, type SavePdfResult } from './desktop';
import { A4_PAGE_RULE } from './pageGeometry';
import '@/styles/printExport.css';

/**
 * Page geometry, appended for the render and taken away after.
 *
 * `@page` cannot be gated on a selector the way every rule in printExport.css is,
 * so a stylesheet carrying it would set the paper size for the app's own Ctrl+P
 * on a page the user never asked to export. Injecting it only while the render
 * is in flight is what keeps it scoped.
 */
const PAGE_CSS = `@media print { ${A4_PAGE_RULE} }`;

/** The id printExport.css keys every export rule on. */
const ROOT_ID = 'export-pdf-root';

/**
 * The render root is a document-wide resource: two exports at once would put two
 * nodes under the same id and the print rules would keep both in the flow, so the
 * file comes out with the content twice. A module-level guard is the only scope
 * that covers a widget export and a report export racing each other.
 */
let inFlight = false;

/**
 * Render a detached node tree to a PDF through the desktop shell.
 *
 * `printToPDF` renders the whole document and reflows it partway through, which
 * unmounts React's own tree. So `populate` builds into a plain node owned here,
 * parked off-screen, and removed afterwards, while the print rules drop every
 * other body child out of the flow. It may be async: a widget has to measure
 * itself before its box is the right height.
 *
 * Answers `null` when there is no shell channel at all, which is the one case a
 * caller must handle by falling back to browser print. `canceled` is the user's
 * own answer and must NOT fall back: reopening a print dialog on top of a save
 * dialog they just dismissed is the one response that reads as the app ignoring
 * them.
 */
export async function renderToPdf(
  populate: (root: HTMLElement) => void | Promise<void>,
  fileName: string,
): Promise<SavePdfResult | null> {
  const savePdf = desktop?.savePdf;
  if (!savePdf) return null;
  // Re-entry answers `canceled`, not `null`: nothing was exported, and the
  // caller must not respond by opening a second dialog.
  if (inFlight) return { canceled: true };
  inFlight = true;

  const page = document.createElement('style');
  page.textContent = PAGE_CSS;
  const root = document.createElement('div');
  root.id = ROOT_ID;

  try {
    document.head.appendChild(page);
    document.body.appendChild(root);
    await populate(root);
    return await savePdf({ fileName });
  } catch (err) {
    // A shell that knows the channel but fails inside it still leaves the user
    // wanting a PDF, so this reports as an error rather than as "no shell".
    return { error: err instanceof Error ? err.message : String(err) };
  } finally {
    root.remove();
    page.remove();
    inFlight = false;
  }
}
