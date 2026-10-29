import { describe, it, expect, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import type { SubagentView } from '@/pages/ChatAgent/utils/api';
import { SubagentCell } from '../pluginsPage/componentCells';

/**
 * A plugin's subagent opens its page from the plugin's own page, unless the
 * Subagents list leaves it out, as it does while the plugin is off: that link
 * led to a page that closed itself at once.
 */
describe('SubagentCell', () => {
  it('opens a subagent the list shows', () => {
    const onOpen = vi.fn();
    const view = { name: 'screener', enabled: true, edited: false, description: 'Screens.' } as SubagentView;
    render(<SubagentCell name="screener" view={view} onOpen={onOpen} />);
    fireEvent.click(screen.getByRole('button', { name: 'screener' }));
    expect(onOpen).toHaveBeenCalledOnce();
  });

  it('offers no link to one the list leaves out', () => {
    render(<SubagentCell name="screener" view={null} onOpen={() => {}} />);
    expect(screen.getByText('screener')).toBeInTheDocument();
    expect(screen.queryByRole('button')).not.toBeInTheDocument();
  });
});
