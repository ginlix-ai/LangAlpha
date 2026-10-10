import { useId } from 'react';
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
import { HOST_MODE } from '@/config/hostMode';
import { Disclosure } from '@/components/ui/Disclosure';
import { Input } from '@/components/ui/input';
import { SegmentedControl } from '@/components/ui/segmented-control';
import { cn } from '@/lib/utils';
import { useAgentModeLabels } from '../hooks/useAgentModeLabels';
import { useWorkspaceOptions, workspaceNameOf } from '../hooks/useWorkspaceOptions';
import { deliveryMethodName } from '../utils/delivery';
import type { FormPatch, FormState } from '../utils/form';
import { MIN_COOLDOWN_MINUTES, RETRIGGER_MODES } from '../utils/price';
import CountInput from './CountInput';
import FormRow from './FormRow';

type DeliveryChoice = 'none' | 'email' | 'slack' | 'discord';

/** The one segment the delivery control can press, or null for a set it
 *  cannot show: several channels, or one the agent chose that the form does
 *  not offer. Either survives an edit untouched until a segment is pressed. */
function deliveryChoice(methods: string[]): DeliveryChoice | null {
  if (methods.length === 0) return 'none';
  if (methods.length > 1) return null;
  return methods[0] === 'email' || methods[0] === 'slack' || methods[0] === 'discord' ? methods[0] : null;
}

interface MoreOptionsProps {
  form: FormState;
  patch: FormPatch;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

/** The settings most automations keep at their defaults, folded under one
 *  line that says what they are set to. */
export default function MoreOptions({ form, patch, open, onOpenChange }: MoreOptionsProps) {
  const { t } = useTranslation();
  const uid = useId();
  const ids = {
    agent: `${uid}-agent`,
    thread: `${uid}-thread`,
    delivery: `${uid}-delivery`,
    retrigger: `${uid}-retrigger`,
    cooldown: `${uid}-cooldown`,
    failures: `${uid}-failures`,
    description: `${uid}-description`,
    panel: `${uid}-panel`,
  };

  const workspaces = useWorkspaceOptions();
  const workspaceName = workspaceNameOf(workspaces, form.workspace_id);
  const agentModes = useAgentModeLabels();

  // What the folded options are set to, so they can stay folded.
  const summary = [
    agentModes.label(form.agent_mode, workspaceName),
    t(form.thread_strategy === 'continue' ? 'automation.continueExisting' : 'automation.newThreadEachRun'),
    form.delivery_methods.length
      ? t('automation.deliversTo', { method: form.delivery_methods.map((m) => deliveryMethodName(m, t)).join(', ') })
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
        <FormRow label={t('automation.agentMode')} labelId={ids.agent}>
          <SegmentedControl
            labelledBy={ids.agent}
            value={form.agent_mode}
            onChange={(mode) => patch('agent_mode', mode)}
            options={[
              { value: 'flash', label: agentModes.options.flash },
              { value: 'ptc', label: agentModes.options.ptc },
            ]}
          />
        </FormRow>

        {form.agent_mode === 'ptc' && (
          <FormRow label={t('thread.workspace')}>
            <Select
              aria-label={t('thread.workspace')}
              selectedKey={form.workspace_id || null}
              onSelectionChange={(key) => patch('workspace_id', key == null ? '' : String(key))}
              placeholder={t('automation.selectWorkspace')}
              className="max-w-sm"
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectPopover>
                <SelectListBox>
                  {workspaces.map((ws) => (
                    <SelectItem key={ws.workspace_id} id={ws.workspace_id}>
                      {ws.name}
                    </SelectItem>
                  ))}
                </SelectListBox>
              </SelectPopover>
            </Select>
          </FormRow>
        )}

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
          <SegmentedControl<DeliveryChoice>
            labelledBy={ids.delivery}
            value={deliveryChoice(form.delivery_methods)}
            onChange={(choice) => patch('delivery_methods', choice === 'none' ? [] : [choice])}
            options={[
              { value: 'none', label: t('automation.deliverNone') },
              // Email goes to one operator-configured list, so the backend refuses it outside OSS.
              ...(HOST_MODE === 'oss' ? [{ value: 'email' as const, label: deliveryMethodName('email', t) }] : []),
              { value: 'slack', label: deliveryMethodName('slack', t) },
              { value: 'discord', label: deliveryMethodName('discord', t) },
            ]}
          />
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
