// @vitest-environment node
import { describe, it, expect } from 'vitest';
import { latestRun, putTaskRun, taskRunFromWire, type TaskRun, type TaskRunIndex } from '../taskRuns';

const TASK = 'task:T1';
const GLM = { id: 'glm-5.3-cn', provider: 'z-ai-cn' };
const EMPTY: TaskRunIndex = new Map();

describe('putTaskRun', () => {
  it('fills a run\'s facts from each report, keeping what a report leaves out', () => {
    let index = putTaskRun(EMPTY, 'run-1', { taskId: TASK, startedAtMs: 1_000 });
    index = putTaskRun(index, 'run-1', { taskId: TASK, launchToolCallId: 'launch-1', model: GLM });
    index = putTaskRun(index, 'run-1', { taskId: TASK, finishedAtMs: 3_000 });
    expect(index.get('run-1')).toEqual({
      taskId: TASK, launchToolCallId: 'launch-1', startedAtMs: 1_000, finishedAtMs: 3_000, model: GLM,
    });
  });

  it('hands back the same index for a report with nothing new, and makes no record of none', () => {
    const index = putTaskRun(EMPTY, 'run-1', { taskId: TASK, startedAtMs: 1_000, model: GLM });
    expect(putTaskRun(index, 'run-1', { taskId: TASK, startedAtMs: 1_000, model: { ...GLM } })).toBe(index);
    expect(putTaskRun(index, 'run-2', { taskId: TASK })).toBe(index);
  });
});

describe('latestRun', () => {
  const run = (startedAtMs: number | undefined, finishedAtMs?: number): TaskRun => ({ taskId: TASK, startedAtMs, finishedAtMs });

  it('takes the run that started last, a run with no start below every start', () => {
    expect(latestRun([run(5_000), run(undefined), run(1_000)])?.startedAtMs).toBe(5_000);
    expect(latestRun([run(undefined)])?.startedAtMs).toBeUndefined();
    expect(latestRun([])).toBeUndefined();
  });

  it('gives a tie to the run filed later', () => {
    expect(latestRun([run(5_000), run(5_000, 8_000)])?.finishedAtMs).toBe(8_000);
  });
});

describe('taskRunFromWire', () => {
  it('reads the stamped facts that are there, and only those', () => {
    expect(taskRunFromWire({ started_ms: 1_000, finished_ms: null, model: GLM })).toEqual({ startedAtMs: 1_000, model: GLM });
    expect(taskRunFromWire({})).toEqual({});
  });
});
