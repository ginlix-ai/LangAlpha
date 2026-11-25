/**
 * The picture of each region a comment on a chart holds, written into the
 * workspace as the comment is saved, as a box on an image's is: the region
 * with a margin around it, outlined, cut from the chart as it is painted
 * then. A region out of view, or a chart that cannot be read, is saved
 * without one: its dates and prices still say where it is.
 */
import type { ArtifactCommentRegionBox, ChartMark } from '@/types/artifacts';
import { chartParts } from '@/lib/bars/chartId';
import {
  inTurns, PICTURES_AT_ONCE, saveToken, stampOf, writePicture, type PictureSource, type PictureWriter,
} from '../region/regionPictures';
import { boxOn, onArea, type ChartPlane } from './chartSpots';

/** The chart as painted, and how many of its pixels make one of the content's. */
export interface ChartShot {
  canvas: HTMLCanvasElement;
  scale: number;
}

/** A name's part for the chart: its symbol in the letters a picture's name takes, and its timeframe. */
function stemOf(chartId: string): string {
  const parts = chartParts(chartId);
  const symbol = (parts?.symbol ?? chartId).replace(/[^A-Za-z0-9._-]+/g, '-').replace(/^[^A-Za-z0-9]+|-+$/g, '');
  const timeframe = (parts?.timeframe ?? '').replace(/[^A-Za-z0-9]+/g, '');
  return [symbol || 'chart', timeframe].filter(Boolean).join('-');
}

/** A picture's file name: the chart, when the save began, the save's own token, and which region of several. */
export function chartPictureName(chartId: string, at: Date, token: string, index: number | null): string {
  return `${stemOf(chartId)}-${stampOf(at)}-${token}${index === null ? '' : `-${index + 1}`}.png`;
}

/** The writer for a chart's regions, cut from `shoot()`, the chart as it is painted when a comment is saved. */
export function chartPictures(plane: ChartPlane, shoot: () => ChartShot | null): PictureWriter {
  return async (anchor, { workspaceId, path }) => {
    if (anchor.kind !== 'chart') return anchor;
    const regions = anchor.marks.filter((m) => m.type === 'region' && !m.picture);
    const area = plane.area();
    const shot = regions.length ? shoot() : null;
    if (!area || !shot) return anchor;
    // Every crop is placed with the screenshot, before any upload: a zoom,
    // a resize or a new bar while the first ones upload moves the chart, not
    // the picture already taken.
    const boxes = new Map(regions.map((mark, index) => [mark, { index, box: cropOn(plane, mark, area) }]));
    const width = area.width * shot.scale;
    const height = area.height * shot.scale;
    // The price pane is the top-left of what the chart paints.
    const source: PictureSource = {
      maxScale: 1,
      size: async () => ({ width, height }),
      async paint(canvas, _box, crop) {
        canvas.getContext('2d')?.drawImage(
          shot.canvas, crop.x * width, crop.y * height, crop.w * width, crop.h * height, 0, 0, canvas.width, canvas.height,
        );
      },
    };
    const at = new Date();
    const token = saveToken();
    const marks = await inTurns(anchor.marks, PICTURES_AT_ONCE, async (mark) => {
      const placed = boxes.get(mark);
      if (!placed?.box) return mark;
      const name = chartPictureName(path, at, token, regions.length > 1 ? placed.index : null);
      const picture = await writePicture(workspaceId, source, placed.box, name);
      return picture ? { ...mark, picture } : mark;
    });
    return { ...anchor, marks };
  };
}

/** Where a region is on the plot area now, as fractions of it; null where none of it is in view. */
function cropOn(plane: ChartPlane, mark: ChartMark, area: { width: number; height: number }): ArtifactCommentRegionBox | null {
  const placed = boxOn(plane, mark);
  const shown = placed && onArea(placed, area);
  if (!shown || shown.right <= shown.left || shown.bottom <= shown.top) return null;
  return {
    x: shown.left / area.width,
    y: shown.top / area.height,
    w: (shown.right - shown.left) / area.width,
    h: (shown.bottom - shown.top) / area.height,
  };
}
