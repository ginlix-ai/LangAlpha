import { describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemberContextSetting } from '../MemberContextSetting';

describe('MemberContextSetting', () => {
  it('lets a manager turn it off', async () => {
    const onChange = vi.fn();
    render(<MemberContextSetting on canManage onChange={onChange} />);
    const sw = screen.getByRole('switch', { name: /Members' own context/ });
    expect(sw).toBeChecked();
    await userEvent.click(sw);
    expect(onChange).toHaveBeenCalledWith(false);
  });

  it('lets a manager turn it back on', async () => {
    const onChange = vi.fn();
    render(<MemberContextSetting on={false} canManage onChange={onChange} />);
    await userEvent.click(screen.getByRole('switch'));
    expect(onChange).toHaveBeenCalledWith(true);
  });

  it('shows a member the state without letting them change it', () => {
    render(<MemberContextSetting on canManage={false} onChange={vi.fn()} />);
    const sw = screen.getByRole('switch');
    expect(sw).toBeChecked();
    expect(sw).toBeDisabled();
  });
});
