/**
 * Coverage for the inline-card telemetry row added below the existing
 * type-badge / status row:
 *  - hidden when both toolCalls === 0 and tokenUsage.total === 0
 *  - shows tools count when only toolCalls > 0
 *  - shows compact-formatted tokens when only tokenUsage.total > 0
 *  - both segments rendered (separator dot) when both > 0
 *  - title attribute on the tokens segment exposes the input/output split
 */
import React from 'react';
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import SubagentTaskMessageContent from '../SubagentTaskMessageContent';

const baseProps = {
  subagentId: 'tc-abc',
  description: 'Research AAPL Q3',
  type: 'research',
  status: 'completed' as const,
};

describe('SubagentTaskMessageContent — telemetry row', () => {
  it('hides the row when both metrics are zero', () => {
    render(
      <SubagentTaskMessageContent
        {...baseProps}
        toolCalls={0}
        tokenUsage={{ input: 0, output: 0, total: 0 }}
      />,
    );
    expect(screen.queryByTestId('subagent-telemetry')).toBeNull();
  });

  it('hides the row when telemetry props are omitted entirely', () => {
    render(<SubagentTaskMessageContent {...baseProps} />);
    expect(screen.queryByTestId('subagent-telemetry')).toBeNull();
  });

  it('shows only the tool count when tokens are zero', () => {
    render(
      <SubagentTaskMessageContent
        {...baseProps}
        toolCalls={4}
        tokenUsage={{ input: 0, output: 0, total: 0 }}
      />,
    );
    const row = screen.getByTestId('subagent-telemetry');
    expect(row).toHaveTextContent('4 tools');
    expect(row).not.toHaveTextContent(/tokens/i);
  });

  it('uses the singular "tool" label when count is exactly 1', () => {
    render(
      <SubagentTaskMessageContent
        {...baseProps}
        toolCalls={1}
        tokenUsage={{ input: 0, output: 0, total: 0 }}
      />,
    );
    expect(screen.getByTestId('subagent-telemetry')).toHaveTextContent(/^1 tool$/);
  });

  it('shows only compact tokens when tool count is zero', () => {
    render(
      <SubagentTaskMessageContent
        {...baseProps}
        toolCalls={0}
        tokenUsage={{ input: 4000, output: 1142, total: 5142 }}
      />,
    );
    const row = screen.getByTestId('subagent-telemetry');
    // compactNumber locale-formats 5142 → "5.1K" — case-insensitive match
    // because Intl emits uppercase suffixes; the code does not lowercase.
    expect(row.textContent).toMatch(/5\.1K\s+tokens/i);
    expect(row).not.toHaveTextContent(/tools/);
  });

  it('renders both segments with a separator dot when both are non-zero', () => {
    render(
      <SubagentTaskMessageContent
        {...baseProps}
        toolCalls={7}
        tokenUsage={{ input: 4000, output: 1142, total: 5142 }}
      />,
    );
    const row = screen.getByTestId('subagent-telemetry');
    expect(row).toHaveTextContent('7 tools');
    expect(row.textContent).toMatch(/5\.1K\s+tokens/i);
    expect(row).toHaveTextContent('·');
  });

  it('exposes the input/output split via the title attribute on the tokens segment', () => {
    render(
      <SubagentTaskMessageContent
        {...baseProps}
        toolCalls={3}
        tokenUsage={{ input: 4000, output: 1142, total: 5142 }}
      />,
    );
    const tokensSpan = screen.getByTitle('4000 in · 1142 out');
    expect(tokensSpan).toBeInTheDocument();
  });
});

describe('SubagentTaskMessageContent — status discriminator', () => {
  it('renders Running label with spin animation for action=init + status=running', () => {
    render(
      <SubagentTaskMessageContent
        subagentId="tc-r"
        description="Long-running task"
        type="research"
        status="running"
        action="init"
      />,
    );
    expect(screen.getByText('Running')).toBeInTheDocument();
  });

  it('renders Completed label for action=init + status=completed', () => {
    render(
      <SubagentTaskMessageContent
        subagentId="tc-c"
        description="Done task"
        type="research"
        status="completed"
        action="init"
      />,
    );
    expect(screen.getByText('Completed')).toBeInTheDocument();
  });

  it('renders Updated label for action=update', () => {
    render(
      <SubagentTaskMessageContent
        subagentId="tc-u"
        description="Steered task"
        type="research"
        status="running"
        action="update"
      />,
    );
    expect(screen.getByText('Updated')).toBeInTheDocument();
  });

  it('renders Resumed label for action=resume', () => {
    render(
      <SubagentTaskMessageContent
        subagentId="tc-rs"
        description="Resumed task"
        type="research"
        status="running"
        action="resume"
      />,
    );
    expect(screen.getByText('Resumed')).toBeInTheDocument();
  });

  it('falls through to raw status text for unknown discriminator', () => {
    render(
      <SubagentTaskMessageContent
        subagentId="tc-x"
        description="Edge case"
        type="research"
        status="something-else"
        action="init"
      />,
    );
    // Neither Completed/Running/Updated/Resumed matches, so the raw status
    // string survives as the fallback label.
    expect(screen.getByText('something-else')).toBeInTheDocument();
  });
});

describe('SubagentTaskMessageContent — accessibility', () => {
  it('exposes the card as a keyboard-focusable button', () => {
    render(
      <SubagentTaskMessageContent
        subagentId="tc-a11y"
        description="Click me"
        type="research"
        status="completed"
      />,
    );
    // Without a hasResult body, the card root is the only role=button.
    // Without aria-label/title-as-accessible-name we just look up by role.
    const card = screen.getByRole('button');
    expect(card).toHaveAttribute('tabIndex', '0');
    expect(card).toHaveAttribute('title');
  });

  it('opens the secondary view-output action via an accessible button', () => {
    let captured: unknown = null;
    render(
      <SubagentTaskMessageContent
        subagentId="tc-output"
        description="Done"
        type="research"
        status="completed"
        toolCallProcess={{ toolCallResult: { content: 'output text' } }}
        onDetailOpen={(p) => { captured = p; }}
      />,
    );
    const viewButton = screen.getByRole('button', { name: 'View subagent output' });
    expect(viewButton).toBeInTheDocument();
    viewButton.click();
    expect(captured).toEqual({ toolCallResult: { content: 'output text' } });
  });
});
