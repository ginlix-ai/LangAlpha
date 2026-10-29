import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

/**
 * A workbook openpyxl 3.1.5 wrote, the library the agent writes its workbooks
 * with, from synthetic data: Model holds a bar chart and a picture, Charts two
 * line charts, Notes one picture, Inputs cells only. openpyxl declares the
 * drawing namespace as the default (`<wsDr xmlns=...>`) where Excel writes
 * `xdr:`, and names parts from the package root where Excel names them
 * relative to their owner. Made with `ws.add_chart(BarChart(), 'D2')`,
 * `ws.add_image(Image(png), 'D20')` and the like, then `wb.save()`.
 */
export function openpyxlWorkbook(): ArrayBuffer {
  const file = readFileSync(resolve(__dirname, '__fixtures__/openpyxl-drawings.xlsx'));
  return file.buffer.slice(file.byteOffset, file.byteOffset + file.byteLength) as ArrayBuffer;
}
