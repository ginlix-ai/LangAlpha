import { describe, it, expect } from 'vitest';
import { getPreviewItems, toAgentPlanItems, type TodoItem } from '../TodoDrawer';

describe('getPreviewItems', () => {
  it('returns empty array for empty todos', () => {
    expect(getPreviewItems([])).toEqual([]);
  });

  it('returns all in_progress items when present', () => {
    const todos: TodoItem[] = [
      { status: 'completed' },
      { status: 'in_progress', activeForm: 'Searching' },
      { status: 'in_progress', activeForm: 'Analyzing' },
      { status: 'pending' },
    ];
    const result = getPreviewItems(todos);
    expect(result).toHaveLength(2);
    expect(result[0]).toEqual({ item: todos[1], index: 1 });
    expect(result[1]).toEqual({ item: todos[2], index: 2 });
  });

  it('falls back to last stale item when no in_progress', () => {
    const todos: TodoItem[] = [
      { status: 'completed' },
      { status: 'stale', activeForm: 'First stale' },
      { status: 'stale', activeForm: 'Last stale' },
    ];
    const result = getPreviewItems(todos);
    expect(result).toHaveLength(1);
    expect(result[0]).toEqual({ item: todos[2], index: 2 });
  });

  it('falls back to last completed when no in_progress or stale', () => {
    const todos: TodoItem[] = [
      { status: 'completed', activeForm: 'First' },
      { status: 'completed', activeForm: 'Last' },
    ];
    const result = getPreviewItems(todos);
    expect(result).toHaveLength(1);
    expect(result[0]).toEqual({ item: todos[1], index: 1 });
  });

  it('falls back to first pending when no other statuses', () => {
    const todos: TodoItem[] = [
      { status: 'pending', activeForm: 'First pending' },
      { status: 'pending', activeForm: 'Second pending' },
    ];
    const result = getPreviewItems(todos);
    expect(result).toHaveLength(1);
    expect(result[0]).toEqual({ item: todos[0], index: 0 });
  });

  it('falls back to first item as last resort', () => {
    const todos: TodoItem[] = [{ status: 'pending' as const }];
    const result = getPreviewItems(todos);
    expect(result).toHaveLength(1);
    expect(result[0]).toEqual({ item: todos[0], index: 0 });
  });
});

describe('toAgentPlanItems', () => {
  it('uses activeForm as label when present', () => {
    const todos: TodoItem[] = [{ status: 'in_progress', activeForm: 'Searching' }];
    const result = toAgentPlanItems(todos);
    expect(result[0].label).toBe('Searching');
    expect(result[0].id).toBe('Searching');
    expect(result[0].status).toBe('in_progress');
  });

  it('falls back to content when activeForm is absent', () => {
    const todos: TodoItem[] = [{ status: 'completed', content: 'Analyze data' }];
    const result = toAgentPlanItems(todos);
    expect(result[0].label).toBe('Analyze data');
  });

  it('falls back to Task N+1 when both absent', () => {
    const todos: TodoItem[] = [{ status: 'pending' }, { status: 'pending' }];
    const result = toAgentPlanItems(todos);
    expect(result[0].label).toBe('Task 1');
    expect(result[1].label).toBe('Task 2');
  });

  it('uses content-based id with index fallback', () => {
    const todos: TodoItem[] = [
      { status: 'pending', activeForm: 'Search' },
      { status: 'pending', content: 'Analyze' },
      { status: 'pending' },
    ];
    const result = toAgentPlanItems(todos);
    expect(result[0].id).toBe('Search');
    expect(result[1].id).toBe('Analyze');
    expect(result[2].id).toBe('task-2');
  });

  it('maps all status values correctly', () => {
    const statuses: TodoItem['status'][] = ['pending', 'in_progress', 'completed', 'stale'];
    const todos = statuses.map(s => ({ status: s, activeForm: s }));
    const result = toAgentPlanItems(todos);
    statuses.forEach((s, i) => expect(result[i].status).toBe(s));
  });
});
