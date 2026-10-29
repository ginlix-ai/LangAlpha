// @vitest-environment node
import { describe, it, expect } from 'vitest';
import fs from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';

// The build serves pdf.js's data under the version of the pdfjs-dist it was
// installed with, and the viewer asks for it under react-pdf's `pdfjs.version`.
// A react-pdf that brings its own copy splits the two, and the 404s that follow
// render CJK text blank rather than failing anything.
describe('pdf.js', () => {
  it('is one version for the data the build serves and the viewer that reads it', () => {
    const root = path.resolve(__dirname, '../../../../../..');
    const fromRoot = createRequire(path.join(root, 'package.json'));
    const reactPdf = fs.realpathSync(path.dirname(fromRoot.resolve('react-pdf/package.json')));
    const fromReactPdf = createRequire(path.join(reactPdf, 'package.json'));

    expect(fromReactPdf('pdfjs-dist/package.json').version).toBe(fromRoot('pdfjs-dist/package.json').version);
  });
});
