import { describe, it, expect, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import type { SubagentsResponse, SubagentView } from '@/pages/ChatAgent/utils/api';
import { PreferredBlock, type SubagentModels } from '../SubagentAside';

/**
 * A preferred skill switched off in Skills, itself or with its plugin, reads
 * as off the way a switched-off connector does, and is not offered to add;
 * nor is one the server leaves out, behind a feature the user hasn't turned on.
 */

vi.mock('@/hooks/useSkills', () => ({
  useSkills: () => ({
    data: [
      { name: 'charts', command: 'chart', enabled: true, plugin_enabled: null, workspace_id: null },
      { name: 'filings', command: 'sec', enabled: false, plugin_enabled: null, workspace_id: null },
      { name: 'bundle-skill', command: null, enabled: true, plugin_enabled: false, workspace_id: null },
      { name: 'market-watch', command: 'market-watch', enabled: true, plugin_enabled: null, workspace_id: null },
    ],
  }),
}));

// What the server lists for a user who hasn't turned on Market watch.
const VALID = ['bundle-skill', 'charts', 'filings'];

function renderBlock(preferredSkills: string[], skills: string[] = VALID) {
  const view = {
    name: 'r5-preferred-sa',
    kind: 'custom',
    base: null,
    problems: [],
    preferred_connectors: [],
    preferred_skills: preferredSkills,
  } as unknown as SubagentView;
  const data = { connectors: [], skills } as unknown as SubagentsResponse;
  render(<PreferredBlock view={view} data={data} models={{} as SubagentModels} write={() => {}} />);
}

describe('PreferredBlock skills', () => {
  it("marks a switched-off skill's chip off, under its own command", () => {
    renderBlock(['filings', 'bundle-skill', 'charts']);
    const off = screen.getByText('/sec').closest('.sa-chip');
    expect(off).toHaveClass('is-off');
    expect(off).toHaveAttribute('title', 'Switched off in Skills');
    expect(screen.getByText('/bundle-skill').closest('.sa-chip')).toHaveClass('is-off');
    const on = screen.getByText('/chart').closest('.sa-chip');
    expect(on).not.toHaveClass('is-off');
    expect(on).toHaveAttribute('title', 'charts');
  });

  it('offers only the skills that are on', async () => {
    renderBlock([]);
    fireEvent.pointerDown(screen.getByRole('button', { name: 'Add skill' }), { button: 0, ctrlKey: false });
    const items = await screen.findAllByRole('menuitem');
    expect(items.map((el) => el.textContent)).toEqual(['/chart']);
  });

  it('offers a skill behind a feature once the server lists it', async () => {
    renderBlock([], [...VALID, 'market-watch']);
    fireEvent.pointerDown(screen.getByRole('button', { name: 'Add skill' }), { button: 0, ctrlKey: false });
    const items = await screen.findAllByRole('menuitem');
    expect(items.map((el) => el.textContent)).toEqual(['/chart', '/market-watch']);
  });
});
