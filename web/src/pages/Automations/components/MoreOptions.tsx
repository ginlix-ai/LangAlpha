import { useId, useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { ChevronRight } from 'lucide-react';
import {
  Select,
  SelectItem,
  SelectListBox,
  SelectPopover,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/aria-select';
import { Disclosure } from '@/components/ui/Disclosure';
import { Input } from '@/components/ui/input';
import { SegmentedControl } from '@/components/ui/segmented-control';
import { cn } from '@/lib/utils';
import type { DeliveryAttempt } from '@/types/automation';
import { useAutomationPlace } from '../hooks/useAutomationPlace';
import { useDeliveryOptions } from '../hooks/useDeliveryOptions';
import { useWorkspaceOptions, workspaceNameOf } from '../hooks/useWorkspaceOptions';
import { type DeliveryProblem, deliveryEntryName, deliveryMethodName, deliveryNames } from '../utils/delivery';
import { ALL_WORKSPACES, type FormPatch, type FormState } from '../utils/form';
import { MIN_COOLDOWN_MINUTES, RETRIGGER_MODES } from '../utils/price';
import CountInput from './CountInput';
import DeliveryPicker, { DeliveryProblems } from './DeliveryPicker';
import FormRow from './FormRow';

type DeliveryChoice = 'none' | 'slack' | 'discord';

/** The one segment the delivery control can press, or null for a set it
 *  cannot show: several channels, or one the agent chose that the form does
 *  not offer. Either survives an edit untouched until a segment is pressed. */
function deliveryChoice(methods: string[]): DeliveryChoice | null {
  if (methods.length === 0) return 'none';
  if (methods.length > 1) return null;
  return methods[0] === 'slack' || methods[0] === 'discord' ? methods[0] : null;
}

interface MoreOptionsProps {
  form: FormState;
  patch: FormPatch;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  /** The edited automation's last deliveries, which name chats the apps may
   *  no longer list. */
  deliveryAttempts?: DeliveryAttempt[] | null;
  /** The delivery entries the last save was refused over. */
  deliveryProblems?: DeliveryProblem[];
}

const NO_PROBLEMS: DeliveryProblem[] = [];

/** The settings most automations keep at their defaults, folded under one
 *  line that says what they are set to. */
export default function MoreOptions({
  form,
  patch,
  open,
  onOpenChange,
  deliveryAttempts,
  deliveryProblems = NO_PROBLEMS,
}: MoreOptionsProps) {
  const { t } = useTranslation();
  const uid = useId();
  const ids = {
    thread: `${uid}-thread`,
    delivery: `${uid}-delivery`,
    retrigger: `${uid}-retrigger`,
    cooldown: `${uid}-cooldown`,
    failures: `${uid}-failures`,
    description: `${uid}-description`,
    panel: `${uid}-panel`,
  };

  // The list leaves Home out; the Chief of Staff is offered in its place.
  const workspaces = useWorkspaceOptions();
  const allWorkspaces = form.place === ALL_WORKSPACES;
  const workspaceName = workspaceNameOf(workspaces, form.place);
  const { homeId, label: placeLabel } = useAutomationPlace();
  // Chats are asked with the workspace the run lands in, which for All
  // workspaces is Home, once its id is known.
  const delivery = useDeliveryOptions({ workspaceId: allWorkspaces ? homeId : form.place });
  // With no messaging service, or before its chats load, the form offers the
  // apps alone, as it always has.
  const picker = delivery.options?.enabled ? delivery.options : null;
  // Until the chats are known, entries the old control cannot show stay as
  // they are: it would replace them all with the one segment pressed.
  const unread = !delivery.options && (delivery.isPending || !!delivery.error);
  const hold = unread && form.delivery_methods.length > 0 && deliveryChoice(form.delivery_methods) === null;
  const names = useMemo(() => deliveryNames(picker, deliveryAttempts ?? []), [picker, deliveryAttempts]);
  const entryName = (entry: string) => deliveryEntryName(entry, t, picker, names);

  // What the folded options are set to, so they can stay folded.
  const summary = [
    allWorkspaces ? t('agents.chiefOfStaff') : placeLabel({ workspace_id: form.place }, workspaceName),
    t(form.thread_strategy === 'continue' ? 'automation.continueExisting' : 'automation.newThreadEachRun'),
    form.delivery_methods.length
      ? t('automation.deliversTo', { method: form.delivery_methods.map(entryName).join(', ') })
      : t('automation.noDelivery'),
    t('automation.stopsAfter', { count: form.max_failures }),
  ].join(' · ');

  return (
    <div className="automation-form-more">
      <button
        type="button"
        className="automation-form-row automation-form-more-toggle"
        aria-expanded={open}
        aria-controls={ids.panel}
        onClick={() => onOpenChange(!open)}
      >
        <span className="automation-form-label inline-flex items-center gap-1">
          {t('automation.moreOptions')}
          <ChevronRight className={cn('h-3.5 w-3.5 transition-transform', open && 'rotate-90')} />
        </span>
        {!open && <span className="automation-form-summary">{summary}</span>}
      </button>

      <Disclosure open={open} id={ids.panel} className="automation-form-rows automation-form-rows-nested">
        <FormRow label={t('thread.workspace')}>
          <Select
            aria-label={t('thread.workspace')}
            selectedKey={form.place || null}
            onSelectionChange={(key) => patch('place', key == null ? '' : String(key))}
            placeholder={t('automation.selectWorkspace')}
            className="max-w-sm"
          >
            <SelectTrigger>
              <SelectValue />
            </SelectTrigger>
            <SelectPopover>
              <SelectListBox>
                <SelectItem id={ALL_WORKSPACES}>{t('agents.chiefOfStaff')}</SelectItem>
                {workspaces.map((ws) => (
                  <SelectItem key={ws.workspace_id} id={ws.workspace_id}>
                    {ws.name}
                  </SelectItem>
                ))}
              </SelectListBox>
            </SelectPopover>
          </Select>
        </FormRow>

        <FormRow label={t('automation.threadStrategy')} labelId={ids.thread}>
          <SegmentedControl
            labelledBy={ids.thread}
            value={form.thread_strategy}
            onChange={(strategy) => patch('thread_strategy', strategy)}
            options={[
              { value: 'new', label: t('automation.newThreadEachRun') },
              { value: 'continue', label: t('automation.continueExisting') },
            ]}
          />
        </FormRow>

        <FormRow label={t('automation.delivery')} labelId={ids.delivery}>
          {picker ? (
            <DeliveryPicker
              methods={form.delivery_methods}
              onChange={(methods) => patch('delivery_methods', methods)}
              options={picker}
              stale={delivery.isPlaceholderData}
              workspaceId={delivery.workspaceId}
              names={names}
              problems={deliveryProblems}
              labelledBy={ids.delivery}
            />
          ) : hold ? (
            <>
              <ul className="automation-delivery-chips">
                {form.delivery_methods.map((entry) => (
                  <li key={entry} className="automation-delivery-chip">
                    <span className="automation-delivery-chip-label">
                      <span className="truncate">{entryName(entry)}</span>
                    </span>
                  </li>
                ))}
              </ul>
              {!!delivery.error && <p className="automation-form-readout">{t('automation.deliveryOptionsUnavailable')}</p>}
              <DeliveryProblems problems={deliveryProblems} nameOf={entryName} />
            </>
          ) : (
            <>
              <SegmentedControl<DeliveryChoice>
                labelledBy={ids.delivery}
                value={deliveryChoice(form.delivery_methods)}
                onChange={(choice) => patch('delivery_methods', choice === 'none' ? [] : [choice])}
                options={[
                  { value: 'none', label: t('automation.deliverNone') },
                  { value: 'slack', label: deliveryMethodName('slack', t) },
                  { value: 'discord', label: deliveryMethodName('discord', t) },
                ]}
              />
              {!!delivery.error && <p className="automation-form-readout">{t('automation.deliveryOptionsUnavailable')}</p>}
              <DeliveryProblems problems={deliveryProblems} nameOf={entryName} />
            </>
          )}
        </FormRow>

        {form.trigger_type === 'price' && (
          <FormRow label={t('automation.priceRetrigger')} labelId={ids.retrigger}>
            <SegmentedControl
              labelledBy={ids.retrigger}
              value={form.price_retrigger_mode}
              onChange={(mode) => patch('price_retrigger_mode', mode)}
              options={RETRIGGER_MODES.map((opt) => ({ value: opt.value, label: t(opt.labelKey) }))}
            />
          </FormRow>
        )}

        {form.trigger_type === 'price' && form.price_retrigger_mode === 'recurring' && (
          <FormRow label={t('automation.priceCooldown')} htmlFor={ids.cooldown}>
            <div className="flex items-center gap-2">
              <Input
                id={ids.cooldown}
                type="number"
                min={MIN_COOLDOWN_MINUTES}
                value={form.price_cooldown_minutes}
                onChange={(e) => patch('price_cooldown_minutes', e.target.value)}
                className="w-28 font-mono tabular-nums"
              />
              <span className="automation-form-joiner">{t('automation.minutes')}</span>
            </div>
            <p className="automation-form-readout">{t('automation.cooldownHint')}</p>
          </FormRow>
        )}

        <FormRow label={t('automation.maxFailures')} htmlFor={ids.failures}>
          <CountInput
            id={ids.failures}
            value={form.max_failures}
            min={1}
            max={100}
            onChange={(n) => patch('max_failures', n)}
            className="w-20 font-mono tabular-nums"
          />
        </FormRow>

        <FormRow label={t('common.description')} htmlFor={ids.description}>
          <Input
            id={ids.description}
            value={form.description}
            onChange={(e) => patch('description', e.target.value)}
            placeholder={t('automation.descPlaceholder')}
          />
        </FormRow>
      </Disclosure>
    </div>
  );
}
