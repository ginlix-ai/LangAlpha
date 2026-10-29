import type { ReactNode } from 'react';
import { easeInOut, useSceneTime } from './clock';
import { Cursor, rv, type PointerScript } from './kit';

/*
 * A camera over a scene's page. The page is drawn once, at its own size, and
 * the scene shows only the part that matters: the camera frames a crop of it
 * and moves between crops on the scene clock, panning and zooming in to the
 * detail a step is about. Text stays sharp at any zoom because the page is
 * DOM, scaled, not a picture of it.
 */
/* eslint-disable react-refresh/only-export-components -- the camera's
   geometry helpers travel with it */

/** The scene's design size; the stage scales it to fit the panel. */
export const VIEW = { width: 400, height: 330 };

/**
 * Where the camera looks once it arrives at `at` ms: the page point (px) it
 * centres on, as near as the page's edges allow (`look`), and the zoom. The
 * move into a shot takes `dur` ms and ends at `at`; before it starts, the
 * camera holds the previous shot.
 */
export interface Shot {
  at: number;
  x: number;
  y: number;
  zoom: number;
  dur?: number;
}

type View = Pick<Shot, 'x' | 'y' | 'zoom'>;

/** A page's size in page px, as `VIEW` gives the view's. */
interface Size {
  width: number;
  height: number;
}

/**
 * How close the camera sits. Scenes draw their pages in the view's px, so a
 * zoom of 1 shows a view-sized page whole. A resting shot sits at `REST`,
 * which keeps a pane whole with its header and an edge of its neighbour, so
 * the viewer can tell which part of the page they are looking at. A push-in
 * on one detail goes no closer than `CLOSE`.
 */
export const REST = 1.25;
export const CLOSE = 1.5;

/** The shot arriving at `at`, aimed at page point (`x`, `y`) at `zoom`, the
    move in taking `dur` ms. */
export function shot(at: number, x: number, y: number, zoom: number, dur?: number): Shot {
  return { at, dur, x, y, zoom };
}

/** The wide shot: a view-sized page whole, as the app draws it. */
export function wide(at: number, dur?: number): Shot {
  return shot(at, VIEW.width / 2, VIEW.height / 2, 1, dur);
}

/** The shot whose view has its top-left corner on page point (`left`,
    `top`) at `zoom`, for a crop whose edges have to fall in the gap between
    two rows rather than through a line of text. */
export function corner(at: number, left: number, top: number, zoom: number, dur?: number): Shot {
  return shot(at, left + VIEW.width / 2 / zoom, top + VIEW.height / 2 / zoom, zoom, dur);
}

/**
 * The view at `zoom` centred as near the page point (`x`, `y`) as `page`
 * allows: the view stops at the page's edge rather than show past it, and a
 * page smaller than the view sits centred in it.
 */
function look(x: number, y: number, zoom: number, page: Size): View {
  const hold = (c: number, size: number, v: number) => {
    const half = v / 2 / zoom;
    return size <= 2 * half ? size / 2 : Math.min(Math.max(c, half), size - half);
  };
  return { zoom, x: hold(x, page.width, VIEW.width), y: hold(y, page.height, VIEW.height) };
}

/** Where the camera is at `t` over `page`. Zoom eases geometrically, so a
    push-in reads even. */
function viewAt(shots: Shot[], t: number, page: Size): View {
  const aim = (s: Shot) => look(s.x, s.y, s.zoom, page);
  let from = aim(shots[0]);
  for (let i = 1; i < shots.length; i++) {
    const s = shots[i];
    const to = aim(s);
    const dur = s.dur ?? 700;
    if (t < s.at) {
      const p = easeInOut(t, s.at - dur, dur);
      return {
        x: from.x + (to.x - from.x) * p,
        y: from.y + (to.y - from.y) * p,
        zoom: from.zoom * (to.zoom / from.zoom) ** p,
      };
    }
    from = to;
  }
  return from;
}

/**
 * The view: a window onto `children` (the page, drawn at its own size,
 * `page`) as the camera frames it at this moment. The `pointer` plays in
 * page px and is drawn over the view at its true size, so it never zooms
 * with the page.
 */
export function Camera({
  shots,
  page = VIEW,
  pointer,
  children,
}: {
  shots: [Shot, ...Shot[]];
  page?: Size;
  pointer?: PointerScript;
  children: ReactNode;
}) {
  const { x, y, zoom } = viewAt(shots, useSceneTime(), page);
  const dx = VIEW.width / 2 - x * zoom;
  const dy = VIEW.height / 2 - y * zoom;
  return (
    <div
      className="intro-mk intro-rv overflow-hidden"
      style={{ ...rv(0), width: VIEW.width, height: VIEW.height }}
    >
      <div
        className="absolute left-0 top-0 origin-top-left"
        style={{ transform: `translate(${dx}px, ${dy}px) scale(${zoom})` }}
      >
        {children}
      </div>
      {pointer && (
        <Cursor
          path={pointer.path}
          clicks={pointer.clicks}
          project={(p) => ({ x: dx + p.x * zoom, y: dy + p.y * zoom })}
        />
      )}
    </div>
  );
}
