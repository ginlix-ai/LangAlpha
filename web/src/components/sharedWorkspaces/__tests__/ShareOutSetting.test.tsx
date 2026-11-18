import { describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { ShareOutSetting } from '../ShareOutSetting';

describe('ShareOutSetting', () => {
  it('lets a manager pick one of the three', async () => {
    const onChange = vi.fn();
    render(<ShareOutSetting value="managers" canManage organization="Northfield" onChange={onChange} />);
    expect(screen.getByText('Who can share a thread or file with the rest of Northfield, or publish it to the library.')).toBeInTheDocument();
    expect(screen.getByRole('radio', { name: 'Managers only' })).toBeChecked();
    await userEvent.click(screen.getByText('No one'));
    expect(onChange).toHaveBeenCalledWith('none');
  });

  it('shows a member the choice without a control', () => {
    render(<ShareOutSetting value="none" canManage={false} organization="Northfield" onChange={vi.fn()} />);
    expect(screen.queryByRole('radio')).not.toBeInTheDocument();
    expect(screen.getByText('No one')).toBeInTheDocument();
    expect(screen.getByText('Links open only for people in this workspace.')).toBeInTheDocument();
  });
});
