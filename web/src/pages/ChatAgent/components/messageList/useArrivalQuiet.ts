import { useEffect, useMemo, useRef, useState } from 'react';
import { ALWAYS_LIVE_TOOLS, HIDDEN_TOOL_CALL_NAMES, MAX_IN_PROGRESS_MS } from './buildRenderBlocks';
import type { ToolCallProcessRecord } from './types';

/**
 * True while no new text has landed for `quietMs`, false again the moment more
 * arrives. Drives the streaming indicator: arriving text is its own proof the
 * turn is alive, so the spinner shows only in the pauses (model thinking, a
 * tool running) where nothing else says so. Starts quiet, so a turn that has
 * produced nothing yet shows it at once, and returns to quiet when it goes
 * inactive so a bubble that stopped mid-arrival is not left reading busy.
 */
export function useArrivalQuiet(seq: number, active: boolean, quietMs: number): boolean {
  const [quiet, setQuiet] = useState(true);
  const lastSeqRef = useRef(seq);
  useEffect(() => {
    if (!active) {
      // Still note the sequence, so text that landed while inactive is not
      // mistaken for a fresh arrival the moment the hook goes active again.
      lastSeqRef.current = seq;
      setQuiet(true);
      return;
    }
    if (seq !== lastSeqRef.current) {
      lastSeqRef.current = seq;
      setQuiet(false);
    }
    const timer = setTimeout(() => setQuiet(true), quietMs);
    return () => clearTimeout(timer);
  }, [seq, active, quietMs]);
  return quiet;
}

/**
 * True while a running tool is still shown in the live zone. A tool's own card
 * already says it is busy, but only while the zone shows it: a regular tool
 * folds into the archive after MAX_IN_PROGRESS_MS and reads as finished there,
 * and a reconnect stamps its tools folded from the start. The spinner may hide
 * behind a visible card, never behind a folded or never-drawn one, so hidden
 * tools do not count and this re-evaluates at the moment the youngest visible
 * card folds.
 */
export function useLiveToolRunning(
  processes: Record<string, ToolCallProcessRecord> | undefined,
  active: boolean,
): boolean {
  const [now, setNow] = useState(() => Date.now());
  const { running, nextFold } = useMemo(() => {
    let running = false;
    let nextFold = Infinity;
    if (!active) return { running, nextFold };
    for (const p of Object.values(processes ?? {})) {
      if (!p.isInProgress) continue;
      const toolName = p.toolName as string;
      if (HIDDEN_TOOL_CALL_NAMES.has(toolName)) continue;
      if (ALWAYS_LIVE_TOOLS.has(toolName)) {
        running = true;
        continue;
      }
      const createdAt = p._createdAt as number | undefined;
      const foldAt = createdAt ? createdAt + MAX_IN_PROGRESS_MS : 0;
      if (foldAt > now) {
        running = true;
        nextFold = Math.min(nextFold, foldAt);
      }
    }
    return { running, nextFold };
  }, [processes, active, now]);
  useEffect(() => {
    if (nextFold === Infinity) return;
    const timer = setTimeout(() => setNow(Date.now()), Math.max(0, nextFold - Date.now()) + 1);
    return () => clearTimeout(timer);
  }, [nextFold]);
  return running;
}
