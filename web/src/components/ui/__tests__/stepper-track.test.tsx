import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import StepperList, { StepperTrack, type AgentPlanItem } from '../stepper-track';

// Mock ThemeContext
vi.mock('@/contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'dark' as const, preference: 'dark' as const, setTheme: () => {}, toggleTheme: () => {} }),
}));

const makeItems = (statuses: AgentPlanItem['status'][]): AgentPlanItem[] =>
  statuses.map((s, i) => ({ id: `item-${i}`, label: `Task ${i + 1}`, status: s }));

describe('StepperTrack', () => {
  it('returns null for empty items', () => {
    const { container } = render(<StepperTrack items={[]} />);
    expect(container.innerHTML).toBe('');
  });

  it('renders nodes for each item', () => {
    const items = makeItems(['completed', 'in_progress', 'pending']);
    const { container } = render(<StepperTrack items={items} />);
    const nodes = container.querySelectorAll('[style*="border-radius: 50%"]');
    expect(nodes).toHaveLength(3);
  });

  it('renders connectors between nodes', () => {
    const items = makeItems(['completed', 'in_progress', 'pending']);
    const { container } = render(<StepperTrack items={items} />);
    const connectors = container.querySelectorAll('.relative.overflow-hidden');
    expect(connectors).toHaveLength(2);
  });

  it('caps nodes at MAX_VISIBLE_NODES with overflow indicator', () => {
    const items = makeItems(Array(15).fill('pending'));
    const { container } = render(<StepperTrack items={items} />);
    const nodes = container.querySelectorAll('[style*="border-radius: 50%"]');
    expect(nodes.length).toBeLessThanOrEqual(12);
    expect(container.textContent).toContain('+3');
  });

  it('applies glow to in_progress nodes', () => {
    const items = makeItems(['in_progress']);
    const { container } = render(<StepperTrack items={items} />);
    const node = container.querySelector('[style*="border-radius: 50%"]');
    expect(node?.getAttribute('style')).toContain('box-shadow');
  });
});

describe('StepperList', () => {
  it('returns null for empty items', () => {
    const { container } = render(<StepperList items={[]} />);
    expect(container.innerHTML).toBe('');
  });

  it('renders correct status labels', () => {
    const items = makeItems(['completed', 'in_progress', 'stale', 'pending']);
    render(<StepperList items={items} />);
    expect(screen.getByText('Done')).toBeTruthy();
    expect(screen.getByText('Active')).toBeTruthy();
    expect(screen.getByText('Stale')).toBeTruthy();
    expect(screen.getByText('Queue')).toBeTruthy();
  });

  it('renders item labels', () => {
    const items: AgentPlanItem[] = [
      { id: '0', label: 'Search for data', status: 'completed' },
      { id: '1', label: 'Analyze results', status: 'in_progress' },
    ];
    render(<StepperList items={items} />);
    expect(screen.getByText('Search for data')).toBeTruthy();
    expect(screen.getByText('Analyze results')).toBeTruthy();
  });

  it('applies line-through to completed and stale items', () => {
    const items: AgentPlanItem[] = [
      { id: '0', label: 'Finished', status: 'completed' },
      { id: '1', label: 'Abandoned', status: 'stale' },
      { id: '2', label: 'Working', status: 'in_progress' },
    ];
    render(<StepperList items={items} />);
    expect(screen.getByText('Finished')).toHaveStyle({ textDecoration: 'line-through' });
    expect(screen.getByText('Abandoned')).toHaveStyle({ textDecoration: 'line-through' });
    expect(screen.getByText('Working')).toHaveStyle({ textDecoration: 'none' });
  });
});
