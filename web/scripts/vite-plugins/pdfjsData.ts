import fs from 'node:fs';
import path from 'node:path';
import type { Plugin } from 'vite';

// The pdfjs-dist directories pdf.js fetches from at runtime: image decoders
// (JBIG2, CCITT, JPEG 2000, ICC color), the predefined CMaps a non-embedded CJK
// font is encoded with, the fonts it substitutes for non-embedded Symbol and
// ZapfDingbats, and the CMYK output profile. Without them those glyphs, images
// and colors are silently dropped or degraded.
const PDFJS_DATA = ['wasm', 'cmaps', 'standard_fonts', 'iccs'];

const TYPES: Record<string, string> = { '.wasm': 'application/wasm', '.js': 'text/javascript' };

// Serves PDFJS_DATA at assets/pdfjs/<version>/<dir>/, the URLs PdfViewer passes.
// pdf.js fetches by bare filename, so the files cannot take hashed names; the
// version in the path keeps a long-cached copy from pairing a new worker with
// old data. Emitted as loose assets, nothing imports them into a chunk.
export function pdfjsData(root: string): Plugin {
  const pkg = path.resolve(root, 'node_modules/pdfjs-dist');
  const { version } = JSON.parse(fs.readFileSync(path.join(pkg, 'package.json'), 'utf8')) as { version: string };
  const route = `assets/pdfjs/${version}/`;
  return {
    name: 'la-pdfjs-data',
    configureServer(server) {
      // Plugin middleware runs before Vite strips the base, so a non-root base
      // stays on the request URL and the mount has to carry it.
      server.middlewares.use(`${server.config.base}${route}`, (req, res, next) => {
        const [dir = '', name = '', ...rest] = (req.url ?? '').split('?')[0].split('/').filter(Boolean);
        // A backslash separates paths on Windows, so a name holding one would
        // walk out of the directory.
        if (!PDFJS_DATA.includes(dir) || rest.length || path.basename(name) !== name) return next();
        const file = path.join(pkg, dir, name);
        if (!fs.statSync(file, { throwIfNoEntry: false })?.isFile()) return next();
        res.setHeader('Content-Type', TYPES[path.extname(file)] ?? 'application/octet-stream');
        fs.createReadStream(file).pipe(res);
      });
    },
    generateBundle() {
      for (const dir of PDFJS_DATA) {
        for (const name of fs.readdirSync(path.join(pkg, dir))) {
          const source = fs.readFileSync(path.join(pkg, dir, name));
          this.emitFile({ type: 'asset', fileName: `${route}${dir}/${name}`, source });
        }
      }
    },
  };
}
