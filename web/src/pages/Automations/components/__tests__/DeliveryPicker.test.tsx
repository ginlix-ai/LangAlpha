import { useState } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, screen, waitFor, within } from '@testing-library/react';
import { ApiError } from '@/api/client';
import i18n from '@/i18n';
import { queryKeys } from '@/lib/queryKeys';
import { renderWithProviders } from '@/test/utils';
import type { DeliveryOptions } from '@/types/automation';
import * as api from '../../utils/api';
import { type FormPatch, type FormState, INITIAL_FORM } from '../../utils/form';
import AutomationInlineForm from '../AutomationInlineForm';
import MoreOptions from '../MoreOptions';

vi.mock('@/hooks/useWorkspaces', () => ({
  useWorkspaces: () => ({ data: { workspaces: [{ workspace_id: 'ws-1', name: 'Research' }] } }),
}));

vi.mock('@/pages/ChatAgent/utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/ChatAgent/utils/api')>()),
  getFlashWorkspace: vi.fn(async () => ({ workspace_id: 'ws-flash' })),
}));

vi.mock('../../utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../utils/api')>()),
  getDeliveryOptions: vi.fn(),
  setDeliveryDefault: vi.fn(),
}));

afterEach(() => vi.clearAllMocks());

const label = (key: string, opts?: Record<string, unknown>) => i18n.t(key, opts);

const OPTIONS: DeliveryOptions = {
  enabled: true,
  apps: {
    discord: {
      chats: [
        { address: 'discord:G1/C7', name: '#alerts', kind: 'channel' },
        { address: 'discord:@me', name: 'Your Discord DM', kind: 'dm' },
      ],
      default: { address: 'discord:@me', name: 'Your Discord DM', via: 'dm' },
      error: null,
    },
    slack: {
      chats: [
        { address: 'slack:T1', name: 'Your Slack DM (Acme)', kind: 'dm' },
        { address: 'slack:T1/C1', name: '#demo', kind: 'channel' },
        { address: 'slack:T1/C2', name: '#research', kind: 'channel' },
      ],
      default: { address: 'slack:T1/C1', name: '#demo', via: 'workspace' },
      error: null,
    },
  },
};

function refusal(status: number, body: unknown): ApiError {
  const code = status >= 500 ? 'ERR_BAD_RESPONSE' : 'ERR_BAD_REQUEST';
  return new ApiError(`Request failed with status code ${status}`, code, { status, statusText: '', headers: {}, data: body });
}

function serve(options: DeliveryOptions | ApiError = OPTIONS) {
  if (options instanceof ApiError) vi.mocked(api.getDeliveryOptions).mockRejectedValue(options);
  else vi.mocked(api.getDeliveryOptions).mockResolvedValue({ data: options } as never);
}

const PTC: FormState = { ...INITIAL_FORM, agent_mode: 'ptc', workspace_id: 'ws-1' };

/** MoreOptions unfolded over a form it edits, with the picked entries on show. */
function Harness({ initial, open = true }: { initial: FormState; open?: boolean }) {
  const [form, setForm] = useState(initial);
  const [isOpen, setOpen] = useState(open);
  const patch: FormPatch = (key, value) => setForm((f) => ({ ...f, [key]: value }));
  return (
    <>
      <MoreOptions form={form} patch={patch} open={isOpen} onOpenChange={setOpen} />
      <output data-testid="methods">{JSON.stringify(form.delivery_methods)}</output>
    </>
  );
}

function renderPicker(methods: string[] = [], initial: FormState = PTC, open = true) {
  return renderWithProviders(<Harness initial={{ ...initial, delivery_methods: methods }} open={open} />);
}

const picked = () => JSON.parse(screen.getByTestId('methods').textContent ?? '[]') as string[];
const addButton = () => screen.findByRole('button', { name: new RegExp(label('automation.deliveryAdd')) });
// By class: an open list hides the rest of the page from the role queries.
const chips = () => [...document.querySelectorAll<HTMLElement>('.automation-delivery-chip')];

async function openList() {
  fireEvent.click(await addButton());
  return screen.findByRole('listbox');
}

describe('the delivery picker', () => {
  it('lists each app’s Default, DM and channels under the app, Slack first', async () => {
    serve();
    renderPicker();
    const list = await openList();

    const groups = within(list).getAllByRole('group');
    expect(groups.map((g) => within(g).getAllByRole('option').map((o) => o.textContent))).toEqual([
      [`${label('automation.deliveryDefaultOptionLands', { name: '#demo' })}${label('automation.deliveryViaWorkspace')}`, 'Your Slack DM (Acme)', '#demo', '#research'],
      // The DM goes first, whatever order the app listed it in.
      [`${label('automation.deliveryDefaultOptionLands', { name: 'Your Discord DM' })}${label('automation.deliveryViaDm')}`, 'Your Discord DM', '#alerts'],
    ]);
    expect(within(list).getByText('Slack')).toBeInTheDocument();
    expect(within(list).getByText('Discord')).toBeInTheDocument();
    expect(api.getDeliveryOptions).toHaveBeenCalledWith('ws-1');
  });

  it('says where an app’s Default lands, and how it got there', async () => {
    serve({
      ...OPTIONS,
      apps: { slack: { ...OPTIONS.apps.slack, default: { address: 'slack:T1/C2', name: '#research', via: 'preferred' } } },
    });
    renderPicker();
    const list = await openList();

    const option = within(list).getByRole('option', { name: new RegExp(label('automation.deliveryDefaultOptionLands', { name: '#research' })) });
    expect(within(option).getByText(label('automation.deliveryViaPreferred'))).toBeInTheDocument();
  });

  it('picks several chats, each a chip by its name', async () => {
    serve();
    renderPicker();
    const list = await openList();

    fireEvent.click(within(list).getByRole('option', { name: '#research' }));
    fireEvent.click(within(list).getByRole('option', { name: /Default · Your Discord DM/ }));

    expect(picked()).toEqual(['slack:T1/C2', 'discord']);
    expect(chips().map((c) => c.textContent)).toEqual(['#research', 'Discord (Your Discord DM)']);
  });

  it('removes a chip', async () => {
    serve();
    renderPicker(['slack', 'slack:T1/C2']);

    fireEvent.click(await screen.findByRole('button', { name: label('automation.deliveryRemove', { name: '#research' }) }));

    expect(picked()).toEqual(['slack']);
  });

  it('marks the picked entries in the list, and unpicks one there', async () => {
    serve();
    renderPicker(['slack:T1/C1', 'discord']);
    const list = await openList();

    expect(within(list).getByRole('option', { name: '#demo' })).toHaveAttribute('aria-selected', 'true');
    fireEvent.click(within(list).getByRole('option', { name: '#demo' }));

    expect(picked()).toEqual(['discord']);
  });

  it('keeps an entry it cannot list through an edit, named as best it can', async () => {
    serve();
    renderPicker(['slack:T1/C_GONE', 'telegram:@me', 'slack']);
    const list = await openList();

    fireEvent.click(within(list).getByRole('option', { name: '#research' }));

    expect(picked()).toEqual(['slack:T1/C_GONE', 'telegram:@me', 'slack', 'slack:T1/C2']);
    expect(chips().map((c) => c.textContent)).toEqual(['Slack:T1/C_GONE', 'Telegram DM', 'Slack (#demo)', '#research']);
  });

  it('names a chat from the last run when the apps no longer list it', async () => {
    serve();
    renderWithProviders(
      <MoreOptions
        form={{ ...PTC, delivery_methods: ['slack:T1/C_GONE'] }}
        patch={vi.fn()}
        open
        onOpenChange={vi.fn()}
        deliveryAttempts={[{ method: 'slack:T1/C_GONE', success: true, address: 'slack:T1/C_GONE', name: '#old', via: 'agent' }]}
      />,
    );

    await addButton();
    expect(chips().map((c) => c.textContent)).toEqual(['#old']);
  });

  it('shows an app’s error quietly', async () => {
    serve({ ...OPTIONS, apps: { ...OPTIONS.apps, telegram: { chats: [], default: null, error: 'list_failed' } } });
    renderPicker();

    expect(await screen.findByText("Telegram: Couldn't load chats")).toBeInTheDocument();
    expect(screen.queryByText(/list_failed/)).not.toBeInTheDocument();
  });

  it('offers the apps alone where no messaging service is connected', async () => {
    serve({ enabled: false, apps: {} });
    const { queryClient } = renderPicker(['slack']);

    // Nothing on screen changes when the answer lands, so wait on the answer.
    await waitFor(() =>
      expect(queryClient.getQueryState(queryKeys.automationDelivery.options('ws-1'))?.status).toBe('success'),
    );
    const group = screen.getByRole('group', { name: label('automation.delivery') });
    expect(within(group).getByRole('button', { name: 'Slack' })).toHaveAttribute('aria-pressed', 'true');
    expect(screen.queryByRole('button', { name: new RegExp(label('automation.deliveryAdd')) })).not.toBeInTheDocument();
    expect(screen.queryByText(label('automation.deliveryOptionsUnavailable'))).not.toBeInTheDocument();
  });

  it('offers the apps alone, and says so, when the messaging service is unavailable', async () => {
    serve(refusal(503, { detail: 'The messaging service is unavailable.' }));
    renderPicker();

    expect(await screen.findByText(label('automation.deliveryOptionsUnavailable'))).toBeInTheDocument();
    const group = screen.getByRole('group', { name: label('automation.delivery') });
    fireEvent.click(within(group).getByRole('button', { name: 'Discord' }));
    expect(picked()).toEqual(['discord']);
  });

  it('reads a Flash automation’s chats in the Flash workspace', async () => {
    serve();
    renderPicker([], { ...INITIAL_FORM, agent_mode: 'flash' });

    await addButton();
    expect(api.getDeliveryOptions).toHaveBeenCalledWith('ws-flash');
  });

  it('names the chats in the folded summary', async () => {
    serve();
    renderPicker(['slack', 'discord:@me'], PTC, false);

    expect(
      await screen.findByText(new RegExp(label('automation.deliversTo', { method: 'Slack (#demo), Your Discord DM' }).replace(/[()]/g, '\\$&'))),
    ).toBeInTheDocument();
  });
});

describe('the workspace default', () => {
  async function chipMenu(name: RegExp | string) {
    fireEvent.click(await screen.findByRole('button', { name }));
    return screen.findByRole('menu');
  }

  it('makes a picked chat this workspace’s default, then reads the chats again', async () => {
    serve();
    vi.mocked(api.setDeliveryDefault).mockResolvedValue({ data: { address: 'slack:T1/C2', name: '#research' } } as never);
    renderPicker(['slack:T1/C2']);
    await waitFor(() => expect(api.getDeliveryOptions).toHaveBeenCalledTimes(1));

    const menu = await chipMenu('#research');
    fireEvent.click(within(menu).getByRole('menuitem', { name: label('automation.deliveryUseAsDefault') }));

    await waitFor(() =>
      expect(api.setDeliveryDefault).toHaveBeenCalledWith({ workspace_id: 'ws-1', platform: 'slack', address: 'slack:T1/C2' }),
    );
    await waitFor(() => expect(api.getDeliveryOptions).toHaveBeenCalledTimes(2));
  });

  it('clears it from the chat that is the default', async () => {
    serve();
    vi.mocked(api.setDeliveryDefault).mockResolvedValue({ data: { address: null, name: null } } as never);
    renderPicker(['slack:T1/C1']);

    const menu = await chipMenu(/^#demo/);
    expect(within(menu).queryByRole('menuitem', { name: label('automation.deliveryUseAsDefault') })).not.toBeInTheDocument();
    fireEvent.click(within(menu).getByRole('menuitem', { name: label('automation.deliveryClearDefault') }));

    await waitFor(() =>
      expect(api.setDeliveryDefault).toHaveBeenCalledWith({ workspace_id: 'ws-1', platform: 'slack', address: null }),
    );
  });

  it('clears it from the app’s Default', async () => {
    serve();
    vi.mocked(api.setDeliveryDefault).mockResolvedValue({ data: { address: null, name: null } } as never);
    renderPicker(['slack']);

    const menu = await chipMenu('Slack (#demo)');
    fireEvent.click(within(menu).getByRole('menuitem', { name: label('automation.deliveryClearDefault') }));

    await waitFor(() =>
      expect(api.setDeliveryDefault).toHaveBeenCalledWith({ workspace_id: 'ws-1', platform: 'slack', address: null }),
    );
  });

  it('offers nothing to change on a Default the workspace did not set', async () => {
    serve();
    renderPicker(['discord']);

    await addButton();
    const chip = chips()[0];
    expect(within(chip).getAllByRole('button').map((b) => b.getAttribute('aria-label'))).toEqual([
      label('automation.deliveryRemove', { name: 'Discord (Your Discord DM)' }),
    ]);
  });

  it('offers no default on a chat the apps no longer list', async () => {
    serve();
    renderPicker(['slack:T1/C_GONE']);

    await addButton();
    const chip = chips()[0];
    expect(within(chip).getAllByRole('button').map((b) => b.getAttribute('aria-label'))).toEqual([
      label('automation.deliveryRemove', { name: 'Slack:T1/C_GONE' }),
    ]);
  });

  it('shows why a default was refused, beside the chips', async () => {
    serve();
    vi.mocked(api.setDeliveryDefault).mockRejectedValue(
      refusal(400, { detail: 'Not saved.', problems: [{ field: 'automation_output', message: 'The bot is not in #research.' }] }),
    );
    renderPicker(['slack:T1/C2']);

    const menu = await chipMenu('#research');
    fireEvent.click(within(menu).getByRole('menuitem', { name: label('automation.deliveryUseAsDefault') }));

    expect(await screen.findByRole('alert')).toHaveTextContent('The bot is not in #research.');
    expect(api.getDeliveryOptions).toHaveBeenCalledTimes(1);
  });

  it.each([
    [409, 'automation.deliveryDefaultBusy'],
    [503, 'automation.deliveryDefaultUnavailable'],
  ])('says a %s refusal in its own words', async (status, key) => {
    serve();
    vi.mocked(api.setDeliveryDefault).mockRejectedValue(refusal(status, { detail: 'nope' }));
    renderPicker(['slack:T1/C2']);

    const menu = await chipMenu('#research');
    fireEvent.click(within(menu).getByRole('menuitem', { name: label('automation.deliveryUseAsDefault') }));

    expect(await screen.findByRole('alert')).toHaveTextContent(label(key));
  });
});

describe('a save refused over delivery', () => {
  function renderForm(onSubmit: () => Promise<void>) {
    serve();
    return renderWithProviders(
      <AutomationInlineForm
        initialValues={{ ...PTC, name: 'Brief', instruction: 'Summarize.', delivery_methods: ['slack', 'slack:T1/C2'] }}
        original={null}
        onSubmit={onSubmit}
        onCancel={vi.fn()}
        loading={false}
      />,
    );
  }

  it('shows each refused entry by name, unfolding the options', async () => {
    // The server's shape: the joined sentence in `detail`, each entry beside it.
    const onSubmit = vi.fn().mockRejectedValue(
      refusal(409, {
        detail: "'slack:T1/C2': the bot isn't in the channel",
        problems: [{ entry: 'slack:T1/C2', message: "the bot isn't in the channel" }],
      }),
    );
    renderForm(onSubmit);
    await waitFor(() => expect(api.getDeliveryOptions).toHaveBeenCalled());

    fireEvent.click(screen.getByRole('button', { name: label('common.create') }));

    expect(await screen.findByRole('alert')).toHaveTextContent("#research: the bot isn't in the channel");
    expect(screen.getByRole('button', { name: new RegExp(label('automation.moreOptions')) })).toHaveAttribute('aria-expanded', 'true');
    const chip = chips().find((c) => c.textContent === '#research');
    expect(chip).toHaveAttribute('data-invalid');
  });

  it('leaves a refusal naming no entry to the message alone', async () => {
    const onSubmit = vi.fn().mockRejectedValue(refusal(409, { detail: "'slack:T1/C2': the bot isn't in the channel" }));
    renderForm(onSubmit);

    fireEvent.click(screen.getByRole('button', { name: label('common.create') }));

    await waitFor(() => expect(onSubmit).toHaveBeenCalled());
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: new RegExp(label('automation.moreOptions')) })).toHaveAttribute('aria-expanded', 'false');
  });

  it('forgets the refusal once the delivery changes', async () => {
    const onSubmit = vi.fn().mockRejectedValue(
      refusal(409, { detail: { message: 'x', problems: [{ entry: 'slack:T1/C2', message: 'gone' }] } }),
    );
    renderForm(onSubmit);
    fireEvent.click(screen.getByRole('button', { name: label('common.create') }));
    await screen.findByRole('alert');

    fireEvent.click(screen.getByRole('button', { name: label('automation.deliveryRemove', { name: '#research' }) }));

    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });
});
