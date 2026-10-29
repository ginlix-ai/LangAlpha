/**
 * The thread's newest run as this view has seen it. A replay names the run
 * the server read when it began, while a live frame names one as it starts,
 * so a run a live frame named during a replay may be newer than the
 * replay's. Live writers go through {@link noteLiveRun}, which counts them,
 * and a replay applies its run only when that count held still.
 */

interface LatestRunRefs {
  latestRunIdRef: { current: string | null };
  latestRunEpochRef: { current: number };
}

export const noteLiveRun = (refs: LatestRunRefs, runId: string): void => {
  refs.latestRunIdRef.current = runId;
  refs.latestRunEpochRef.current += 1;
};
