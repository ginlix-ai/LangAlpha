/**
 * A page's report of what it loaded stands for the page while a frame shows
 * it. What an earlier document loaded is kept apart, for the owner to leave
 * out, since the agent may have changed the code since it reported.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { runAuthResets } from '@/lib/authResets';
import { MAX_PAGE_LOADS, NO_PAGE_LOADS, notePageLoads, notePageOpen, pageLoadsOf } from '../pageLoads';

const releases: (() => void)[] = [];
const shown = (frame: object, scout = false) => {
  const release = notePageOpen('ws', 'a.html', frame, { scout });
  releases.push(release);
  return release;
};

describe('pageLoads', () => {
  afterEach(() => {
    releases.splice(0).forEach((release) => release());
    runAuthResets();
  });

  it('reports what the frames showing the page loaded, and keeps it after as earlier', () => {
    const [panelFrame, viewFrame] = [{}, {}];
    const panel = shown(panelFrame);
    shown(viewFrame);
    notePageLoads('ws', 'a.html', ['data/a.csv'], panelFrame);
    notePageLoads('ws', 'a.html', ['data/b.csv'], viewFrame);
    expect(pageLoadsOf('ws', 'a.html')).toEqual({ now: ['data/a.csv', 'data/b.csv'], earlier: [] });
    panel();
    expect(pageLoadsOf('ws', 'a.html')).toEqual({ now: ['data/b.csv'], earlier: ['data/a.csv'] });
  });

  // The owner's artifact view keeps the code it loaded until the owner asks
  // again, and the review's own load ran the code as it is.
  it("takes the review's own load for the page while it is open, the other frames' for earlier", () => {
    const [panelFrame, scoutFrame, secondScout] = [{}, {}, {}];
    shown(panelFrame);
    notePageLoads('ws', 'a.html', ['data/a.csv', 'data/old.csv'], panelFrame);
    const before = pageLoadsOf('ws', 'a.html');

    const scout = shown(scoutFrame, true);
    const scouting = pageLoadsOf('ws', 'a.html');
    expect(scouting).not.toBe(before);
    expect(scouting).toEqual({ now: [], earlier: ['data/a.csv', 'data/old.csv'] });
    notePageLoads('ws', 'a.html', ['data/a.csv', 'data/new.csv'], scoutFrame);
    expect(pageLoadsOf('ws', 'a.html')).toEqual({ now: ['data/a.csv', 'data/new.csv'], earlier: ['data/old.csv'] });

    // A second scout changes nothing until it reports.
    const reported = pageLoadsOf('ws', 'a.html');
    const second = shown(secondScout, true);
    expect(pageLoadsOf('ws', 'a.html')).toBe(reported);
    second();
    expect(pageLoadsOf('ws', 'a.html')).toBe(reported);

    // Closed, the scout's report is earlier, and the panel's is the page again.
    scout();
    expect(pageLoadsOf('ws', 'a.html')).toEqual({ now: ['data/a.csv', 'data/old.csv'], earlier: ['data/new.csv'] });
  });

  it('replaces what a frame reported when its document reloads, keeping the rest as earlier', () => {
    const frame = {};
    shown(frame);
    notePageLoads('ws', 'a.html', ['data/a.csv', 'data/b.csv'], frame);
    notePageLoads('ws', 'a.html', ['data/b.csv'], frame);
    expect(pageLoadsOf('ws', 'a.html')).toEqual({ now: ['data/b.csv'], earlier: ['data/a.csv'] });
  });

  it('takes a path out of earlier while a document loads it now', () => {
    const [old, next] = [{}, {}];
    shown(old)();
    notePageLoads('ws', 'a.html', ['data/a.csv'], old);
    shown(next);
    notePageLoads('ws', 'a.html', ['data/a.csv'], next);
    expect(pageLoadsOf('ws', 'a.html')).toEqual({ now: ['data/a.csv'], earlier: [] });
  });

  it('gives the same report until it changes, for a store snapshot', () => {
    const frame = {};
    shown(frame);
    expect(pageLoadsOf('ws', 'a.html')).toBe(NO_PAGE_LOADS);
    notePageLoads('ws', 'a.html', ['data/a.csv'], frame);
    const first = pageLoadsOf('ws', 'a.html');
    notePageLoads('ws', 'a.html', ['data/a.csv'], frame);
    expect(pageLoadsOf('ws', 'a.html')).toBe(first);
  });

  it('leaves out a path longer than any the server takes', () => {
    const frame = {};
    shown(frame);
    notePageLoads('ws', 'a.html', ['data/a.csv', `data/${'x'.repeat(1100)}.csv`], frame);
    expect(pageLoadsOf('ws', 'a.html').now).toEqual(['data/a.csv']);
  });

  it('keeps as many as the server takes, in order', () => {
    const frame = {};
    shown(frame);
    const full = Array.from({ length: MAX_PAGE_LOADS + 1 }, (_, i) => `data/b${String(i).padStart(3, '0')}.csv`);
    notePageLoads('ws', 'a.html', ['data/a.csv', ...full], frame);
    const kept = pageLoadsOf('ws', 'a.html').now;
    expect(kept).toHaveLength(MAX_PAGE_LOADS);
    expect(kept[0]).toBe('data/a.csv');
  });

  it('keeps one workspace apart from another', () => {
    const frame = {};
    shown(frame);
    notePageLoads('ws', 'a.html', ['data/a.csv'], frame);
    expect(pageLoadsOf('other', 'a.html')).toBe(NO_PAGE_LOADS);
  });
});
