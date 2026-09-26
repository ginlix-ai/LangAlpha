import { AxiosError, AxiosHeaders, type AxiosResponse } from 'axios';
import { describe, expect, it } from 'vitest';
import { mutationErrorMessage } from '../errors';

/** A refused write as axios rejects it: its own message, and on `response`
 *  the body the automations router sent. The bodies below are what that
 *  router answers each request with, the validation lists verbatim. */
function refused(status: number, body: unknown): AxiosError {
  const config = { headers: new AxiosHeaders() };
  const response: AxiosResponse = { status, statusText: '', headers: {}, config, data: body };
  const code = status >= 500 ? AxiosError.ERR_BAD_RESPONSE : AxiosError.ERR_BAD_REQUEST;
  return new AxiosError(`Request failed with status code ${status}`, code, config, null, response);
}

describe('mutationErrorMessage', () => {
  it('names the field a price trigger was refused on, with the checks it failed', () => {
    // trigger_config is stored as sent, so its validator refuses it whole
    // and names the failing parts inside its own sentence.
    const err = refused(422, {
      detail: [
        {
          type: 'value_error',
          loc: ['body', 'trigger_config'],
          msg: "Value error, Invalid price trigger config: symbol: Use bare symbol (e.g. 'SPX', not '^SPX'); conditions.0.value: Input should be greater than 0",
          input: { symbol: '^SPX', conditions: [{ type: 'price_above', value: 0 }] },
          ctx: { error: {} },
        },
      ],
    });
    expect(mutationErrorMessage(err, 'fallback')).toBe(
      "trigger_config: Invalid price trigger config: symbol: Use bare symbol (e.g. 'SPX', not '^SPX'); conditions.0.value: Input should be greater than 0",
    );
  });

  it('reads every refused field of one request', () => {
    const err = refused(422, {
      detail: [
        {
          type: 'value_error',
          loc: ['body', 'cron_expression'],
          msg: "Value error, Invalid cron expression: 'not a cron'",
          input: 'not a cron',
          ctx: { error: {} },
        },
        {
          type: 'string_too_long',
          loc: ['body', 'name'],
          msg: 'String should have at most 255 characters',
          input: 'x'.repeat(256),
          ctx: { max_length: 255 },
        },
      ],
    });
    expect(mutationErrorMessage(err, 'fallback')).toBe(
      "cron_expression: Invalid cron expression: 'not a cron'; name: String should have at most 255 characters",
    );
  });

  it('keeps a whole-body refusal to its sentence', () => {
    // A create's model validator: the location is the body itself.
    const create = refused(422, {
      detail: [
        {
          type: 'value_error',
          loc: ['body'],
          msg: "Value error, next_run_at is required for trigger_type='once'",
          input: { name: 'Earnings check', trigger_type: 'once', instruction: 'Review the quarter.' },
          ctx: { error: {} },
        },
      ],
    });
    expect(mutationErrorMessage(create, 'fallback')).toBe("next_run_at is required for trigger_type='once'");

    // An edit checked against the stored kind after the body parsed: the
    // handler raises the list itself, with no location and no input.
    const edit = refused(422, {
      detail: [
        {
          type: 'value_error',
          loc: [],
          msg: "Value error, trigger_type can't change from 'cron' to 'once'; create a new automation instead",
        },
      ],
    });
    expect(mutationErrorMessage(edit, 'fallback')).toBe(
      "trigger_type can't change from 'cron' to 'once'; create a new automation instead",
    );
  });

  it('keeps a plain detail as it is', () => {
    // A handler's ValueError (409), a missing row (404), and the catch-all (500).
    expect(mutationErrorMessage(refused(409, { detail: "workspace_id is required for agent_mode='ptc'" }), 'fallback')).toBe(
      "workspace_id is required for agent_mode='ptc'",
    );
    expect(mutationErrorMessage(refused(404, { detail: 'Automation not found' }), 'fallback')).toBe('Automation not found');
    expect(mutationErrorMessage(refused(500, { detail: 'Failed to trigger automation' }), 'fallback')).toBe(
      'Failed to trigger automation',
    );
  });

  it("reads axios's own message when no body came back, and falls back when there is none", () => {
    expect(mutationErrorMessage(new AxiosError('Network Error', AxiosError.ERR_NETWORK), 'fallback')).toBe('Network Error');
    expect(mutationErrorMessage(new Error(''), 'Something went wrong')).toBe('Something went wrong');
  });
});
