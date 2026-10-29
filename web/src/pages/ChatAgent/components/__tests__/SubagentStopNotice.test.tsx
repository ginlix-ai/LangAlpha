/**
 * The credit-stop notices at the foot of a message. The spawn card that used
 * to live beside them is now the subagent run row (messageList/SubagentRun),
 * tested in messageList/__tests__/SubagentRun.test.tsx.
 */
import React from 'react';
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import { MemoryRouter } from 'react-router';
import { SubagentStopNotices } from '../SubagentStopNotice';
import { SubagentTelemetryContext } from '../SubagentTelemetryContext';
import type { SubagentTelemetry } from '../../session/subagents/resolveSubagentTelemetry';

describe('SubagentStopNotices', () => {
  const ZERO = { input: 0, output: 0, total: 0 };

  const withTelemetry = (telemetry: Partial<SubagentTelemetry> | undefined) =>
    render(
      <MemoryRouter>
        <SubagentTelemetryContext.Provider
          value={{
            resolve: () => (telemetry ? { toolCalls: 0, tokenUsage: ZERO, ...telemetry } : undefined),
            followsRuns: true,
          }}
        >
          <SubagentStopNotices subagentIds={['sub-1']} />
        </SubagentTelemetryContext.Provider>
      </MemoryRouter>,
    );

  it('surfaces a credit stop in the main transcript, with the account pages', () => {
    withTelemetry({
      stopReason: 'Monthly credit limit reached (50/50 credits)',
      stopReasonType: 'credit_stop',
    });
    const notice = screen.getByTestId('subagent-credit-stop-notice');
    expect(notice).toHaveTextContent('Monthly credit limit reached (50/50 credits)');
    expect(screen.getByRole('link', { name: 'Manage plan' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'View usage' })).toBeInTheDocument();
  });

  it('stays out of the way for a stop the user cannot act on', () => {
    withTelemetry({ stopReason: 'transport_lost: the stream tore mid-run' });
    expect(screen.queryByTestId('subagent-credit-stop-notice')).toBeNull();
  });

  it('renders nothing when no reason reached the client, as on a shared replay', () => {
    withTelemetry(undefined);
    expect(screen.queryByTestId('subagent-credit-stop-notice')).toBeNull();
  });

  const withTelemetries = (byId: Record<string, Partial<SubagentTelemetry>>) =>
    render(
      <MemoryRouter>
        <SubagentTelemetryContext.Provider
          value={{
            resolve: (id) => (byId[id] ? { toolCalls: 0, tokenUsage: ZERO, ...byId[id] } : undefined),
            followsRuns: true,
          }}
        >
          <SubagentStopNotices subagentIds={Object.keys(byId)} />
        </SubagentTelemetryContext.Provider>
      </MemoryRouter>,
    );

  it('shows one notice for tasks one denial stopped, and one per distinct reason', () => {
    const monthly = { stopReason: 'Monthly credit limit reached (50/50 credits)', stopReasonType: 'credit_stop' };
    const daily = { stopReason: 'Daily credit limit reached (5/5 credits)', stopReasonType: 'credit_stop' };
    withTelemetries({ a: monthly, b: monthly, c: daily, d: { stopReason: 'transport_lost' } });
    const notices = screen.getAllByTestId('subagent-credit-stop-notice');
    expect(notices).toHaveLength(2);
    expect(notices[0]).toHaveTextContent('Monthly credit limit reached');
    expect(notices[1]).toHaveTextContent('Daily credit limit reached');
  });

  it('clamps an unbounded reason', () => {
    withTelemetry({ stopReason: 'x'.repeat(400), stopReasonType: 'credit_stop' });
    const text = screen.getByTestId('subagent-credit-stop-notice').textContent || '';
    expect(text.length).toBeLessThan(400);
  });
});
