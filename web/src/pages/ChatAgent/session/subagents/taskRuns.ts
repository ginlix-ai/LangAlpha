/**
 * A subagent run's ledger facts, one record per run.
 *
 * A run's start, finish, model and launching Task call never change once
 * known, so they are kept once, by run id, beside the subagent history
 * (`historyStore.ts`), not on the card or the history entry, which are
 * rebuilt whole. Each writer files what it learned under the run it learned
 * it of: the mux as the run's channel opens and closes, replay from each
 * launch's artifact, the status route from the task's latest run. A
 * predecessor's backlog therefore cannot put its facts on its successor, and
 * a run heard of before its task has a card loses nothing.
 */

/** A model as a run used it: its id, and the provider id that served it
 *  (the keys of `llmProviderArt`). */
export interface SubagentModel {
  id: string;
  provider: string;
}

/** A model as the wire carries it (`{id, provider}`), or undefined for
 *  anything else: the field is absent whenever the model is not news. */
export function parseSubagentModel(value: unknown): SubagentModel | undefined {
  if (!value || typeof value !== 'object') return undefined;
  const { id, provider } = value as { id?: unknown; provider?: unknown };
  return typeof id === 'string' && id && typeof provider === 'string' && provider
    ? { id, provider }
    : undefined;
}

/** One run of a task, as far as it is known. */
export interface TaskRun {
  /** The task's agent id (`task:<id>`). */
  taskId: string;
  /** The Task call that launched the run. A continuation after a question,
   *  or an instruction sent from the page, has none. */
  launchToolCallId?: string;
  /** The ledger's bounds (epoch ms); no finish while it runs. */
  startedAtMs?: number;
  finishedAtMs?: number;
  /** Present only when it was not its launcher's model. */
  model?: SubagentModel;
}

/** What a writer learned of a run, its task aside. */
export type TaskRunFacts = Omit<TaskRun, 'taskId'>;

export type TaskRunIndex = ReadonlyMap<string, TaskRun>;

/** The facts the server stamps on a launch's artifact and the status route
 *  (`started_ms`, `finished_ms`, `model`), each only when present. */
export function taskRunFromWire(p: { started_ms?: unknown; finished_ms?: unknown; model?: unknown }): TaskRunFacts {
  const model = parseSubagentModel(p.model);
  return {
    ...(typeof p.started_ms === 'number' ? { startedAtMs: p.started_ms } : {}),
    ...(typeof p.finished_ms === 'number' ? { finishedAtMs: p.finished_ms } : {}),
    ...(model ? { model } : {}),
  };
}

const FACT_KEYS = ['launchToolCallId', 'startedAtMs', 'finishedAtMs', 'model'] as const;

const sameModel = (a: SubagentModel | undefined, b: SubagentModel | undefined): boolean =>
  a === b || (!!a && !!b && a.id === b.id && a.provider === b.provider);

/**
 * `index` with `patch` filed under `runId`: each fact the patch defines fills
 * the run's, the rest stay. The same index when nothing changed, so a repeated
 * report publishes nothing.
 */
export function putTaskRun(index: TaskRunIndex, runId: string, patch: TaskRun): TaskRunIndex {
  const prev = index.get(runId);
  let next: TaskRun = prev ?? { taskId: patch.taskId };
  for (const key of FACT_KEYS) {
    const value = patch[key];
    if (value === undefined) continue;
    const same = key === 'model' ? sameModel(next.model, patch.model) : next[key] === value;
    if (!same) next = { ...next, [key]: value };
  }
  if (next === prev || (!prev && FACT_KEYS.every((key) => next[key] === undefined))) return index;
  return new Map(index).set(runId, next);
}

export function runsOfTask(index: TaskRunIndex, taskId: string): TaskRun[] {
  const runs: TaskRun[] = [];
  for (const run of index.values()) if (run.taskId === taskId) runs.push(run);
  return runs;
}

/** The run that started last, which is the one the task is on. A run with no
 *  start yet ranks below every start, and a tie goes to the run filed later. */
export function latestRun(runs: readonly TaskRun[]): TaskRun | undefined {
  let latest: TaskRun | undefined;
  for (const run of runs) {
    if (!latest || (run.startedAtMs ?? -Infinity) >= (latest.startedAtMs ?? -Infinity)) latest = run;
  }
  return latest;
}
