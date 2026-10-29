/**
 * How many charts and pictures each sheet holds. The grid draws neither, and
 * ExcelJS is told to skip drawings altogether (see `parseWorkbook`), so this
 * reads the package's own relationships: workbook → sheet → drawing.
 */
import JSZip from 'jszip';

export interface DrawingCount {
  charts: number;
  pictures: number;
}

const NS = {
  main: 'http://schemas.openxmlformats.org/spreadsheetml/2006/main',
  r: 'http://schemas.openxmlformats.org/officeDocument/2006/relationships',
  rels: 'http://schemas.openxmlformats.org/package/2006/relationships',
  xdr: 'http://schemas.openxmlformats.org/drawingml/2006/spreadsheetDrawing',
  a: 'http://schemas.openxmlformats.org/drawingml/2006/main',
  mc: 'http://schemas.openxmlformats.org/markup-compatibility/2006',
};

const DRAWING_REL = `${NS.r}/drawing`;

// The classic chart, and the kind Excel 2016 added (waterfall, funnel, ...).
const CHART_URIS = new Set([
  'http://schemas.openxmlformats.org/drawingml/2006/chart',
  'http://schemas.microsoft.com/office/drawing/2014/chartex',
]);

// DOMParser is main-thread only. Moved into a worker, this counts nothing
// rather than failing the load.
async function readXml(zip: JSZip, path: string): Promise<Document | null> {
  const file = zip.file(path);
  return file ? new DOMParser().parseFromString(await file.async('string'), 'application/xml') : null;
}

/** A relationship target as a zip path: Excel writes it relative to the part, openpyxl from the root. */
function resolveTarget(part: string, target: string): string {
  const path = target.startsWith('/') ? [] : part.split('/').slice(0, -1);
  for (const segment of target.split('/')) {
    if (segment === '..') path.pop();
    else if (segment && segment !== '.') path.push(segment);
  }
  return path.join('/');
}

/** The parts a part points at, by relationship id. */
async function relationships(zip: JSZip, part: string): Promise<Map<string, { type: string; path: string }>> {
  const slash = part.lastIndexOf('/');
  const doc = await readXml(zip, `${part.slice(0, slash + 1)}_rels/${part.slice(slash + 1)}.rels`);
  const out = new Map<string, { type: string; path: string }>();
  for (const rel of doc ? Array.from(doc.getElementsByTagNameNS(NS.rels, 'Relationship')) : []) {
    const id = rel.getAttribute('Id');
    const target = rel.getAttribute('Target');
    if (!id || !target || rel.getAttribute('TargetMode') === 'External') continue;
    out.set(id, { type: rel.getAttribute('Type') ?? '', path: resolveTarget(part, target) });
  }
  return out;
}

// An mc:Fallback is the same object drawn again for an older reader, so it
// would count everything its mc:Choice holds a second time.
function isFallback(el: Element): boolean {
  for (let p = el.parentElement; p; p = p.parentElement) {
    if (p.namespaceURI === NS.mc && p.localName === 'Fallback') return true;
  }
  return false;
}

function countDrawing(doc: Document): DrawingCount {
  const charts = Array.from(doc.getElementsByTagNameNS(NS.a, 'graphicData'))
    .filter((el) => CHART_URIS.has(el.getAttribute('uri') ?? '') && !isFallback(el)).length;
  const pictures = Array.from(doc.getElementsByTagNameNS(NS.xdr, 'pic')).filter((el) => !isFallback(el)).length;
  return { charts, pictures };
}

/**
 * Counts by `sheetId`, which ExcelJS keeps as `Worksheet.id`; a name would
 * not do, since ExcelJS truncates a long one on load. Never throws: a
 * workbook this cannot read reports no drawings and still opens.
 */
export async function countDrawings(buffer: ArrayBuffer): Promise<Map<number, DrawingCount>> {
  const counts = new Map<number, DrawingCount>();
  try {
    const zip = await JSZip.loadAsync(buffer);
    const book = await readXml(zip, 'xl/workbook.xml');
    if (!book) return counts;
    const sheetParts = await relationships(zip, 'xl/workbook.xml');
    for (const sheet of Array.from(book.getElementsByTagNameNS(NS.main, 'sheet'))) {
      const part = sheetParts.get(sheet.getAttributeNS(NS.r, 'id') ?? '');
      if (!part) continue;
      // The sheet's rels name its drawing; the sheet part itself holds every
      // cell and can run to megabytes.
      for (const rel of (await relationships(zip, part.path)).values()) {
        if (rel.type !== DRAWING_REL) continue;
        const drawing = await readXml(zip, rel.path);
        if (drawing) counts.set(Number(sheet.getAttribute('sheetId')), countDrawing(drawing));
      }
    }
    return counts;
  } catch (err) {
    console.warn('[ExcelViewer] Could not count charts and pictures:', err);
    return new Map();
  }
}
