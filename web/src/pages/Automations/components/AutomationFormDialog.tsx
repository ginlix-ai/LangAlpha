import React, { useState, useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import { listWorkspaces } from '../utils/api';
import type { Automation } from '@/types/automation';

const COMMON_TIMEZONES = [
  'UTC',
  'America/New_York',
  'America/Chicago',
  'America/Denver',
  'America/Los_Angeles',
  'Europe/London',
  'Europe/Paris',
  'Europe/Berlin',
  'Asia/Tokyo',
  'Asia/Shanghai',
  'Asia/Singapore',
  'Australia/Sydney',
];

const inputStyle = {
  backgroundColor: 'var(--color-bg-card)',
  borderColor: 'var(--color-border-default)',
};

const labelClass = 'text-sm font-medium';
const radioGroupClass = 'flex gap-3';

interface RadioOptionProps {
  name: string;
  value: string;
  checked: boolean;
  onChange: (value: string) => void;
  label: string;
}

function RadioOption({ name, value, checked, onChange, label }: RadioOptionProps) {
  return (
    <label className="flex items-center gap-2 cursor-pointer text-sm" style={{ color: 'var(--color-text-secondary)' }}>
      <input
        type="radio"
        name={name}
        value={value}
        checked={checked}
        onChange={() => onChange(value)}
        className="accent-[var(--color-accent-primary)]"
      />
      {label}
    </label>
  );
}

interface FormState {
  name: string;
  description: string;
  trigger_type: string;
  cron_expression: string;
  timezone: string;
  next_run_at: string;
  agent_mode: string;
  workspace_id: string;
  instruction: string;
  thread_strategy: string;
  max_failures: number | string;
  delivery_methods: string[];
}

const INITIAL_FORM: FormState = {
  name: '',
  description: '',
  trigger_type: 'cron',
  cron_expression: '',
  timezone: 'UTC',
  next_run_at: '',
  agent_mode: 'flash',
  workspace_id: '',
  instruction: '',
  thread_strategy: 'new',
  max_failures: 3,
  delivery_methods: [],
};

interface WorkspaceOption {
  workspace_id: string;
  name: string;
}

interface AutomationFormDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onSubmit: (payload: Record<string, unknown>) => void;
  automation: Automation | null;
  loading: boolean;
}

export default function AutomationFormDialog({ open, onOpenChange, onSubmit, automation, loading }: AutomationFormDialogProps) {
  const { t } = useTranslation();
  const [form, setForm] = useState<FormState>(INITIAL_FORM);
  const [workspaces, setWorkspaces] = useState<WorkspaceOption[]>([]);

  const isEdit = !!automation;

  useEffect(() => {
    if (automation) {
      setForm({
        name: (automation.name as string) || '',
        description: (automation.description as string) || '',
        trigger_type: (automation.trigger_type as string) || 'cron',
        cron_expression: (automation.cron_expression as string) || '',
        timezone: (automation.timezone as string) || 'UTC',
        next_run_at: automation.next_run_at ? (automation.next_run_at as string).slice(0, 16) : '',
        agent_mode: (automation.agent_mode as string) || 'flash',
        workspace_id: (automation.workspace_id as string) || '',
        instruction: (automation.instruction as string) || '',
        thread_strategy: (automation.thread_strategy as string) || 'new',
        max_failures: (automation.max_failures as number) ?? 3,
        delivery_methods: (automation.delivery_config as any)?.methods || [],
      });
    } else {
      setForm(INITIAL_FORM);
    }
  }, [automation, open]);

  useEffect(() => {
    if (open) {
      listWorkspaces({ limit: 100 })
        .then(({ data }) => setWorkspaces((data.workspaces as WorkspaceOption[]) || []))
        .catch(() => {});
    }
  }, [open]);

  const set = (key: keyof FormState) => (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement> | string) =>
    setForm((f) => ({ ...f, [key]: typeof e === 'string' ? e : e.target.value }));

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const payload: Record<string, unknown> = { ...form };

    if (payload.trigger_type === 'cron') {
      delete payload.next_run_at;
    } else {
      delete payload.cron_expression;
      if (payload.next_run_at) {
        payload.next_run_at = new Date(payload.next_run_at as string).toISOString();
      }
    }

    if (payload.agent_mode !== 'ptc') {
      delete payload.workspace_id;
    }

    if (!payload.description) delete payload.description;

    payload.max_failures = parseInt(String(payload.max_failures), 10) || 3;

    payload.delivery_config = (payload.delivery_methods as string[]).length > 0
      ? { methods: payload.delivery_methods }
      : { methods: [] };
    delete payload.delivery_methods;

    onSubmit(payload);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent
        className="sm:max-w-lg border overflow-y-auto max-h-[90vh]"
        style={{ backgroundColor: 'var(--color-bg-elevated)', borderColor: 'var(--color-border-elevated)' }}
      >
        <DialogHeader>
          <DialogTitle style={{ color: 'var(--color-text-primary)' }}>
            {isEdit ? t('automation.editAutomation') : t('automation.createAutomation')}
          </DialogTitle>
        </DialogHeader>

        <form onSubmit={handleSubmit} className="flex flex-col gap-4">
          {/* Name */}
          <div className="flex flex-col gap-1.5">
            <label className={labelClass}>{t('common.name')}</label>
            <Input
              value={form.name}
              onChange={set('name')}
              placeholder={t('automation.namePlaceholder')}
              required
              className="placeholder:text-gray-500 border"
              style={inputStyle}
            />
          </div>

          {/* Description */}
          <div className="flex flex-col gap-1.5">
            <label className={labelClass}>{t('common.description')}</label>
            <Input
              value={form.description}
              onChange={set('description')}
              placeholder={t('automation.descPlaceholder')}
              className="placeholder:text-gray-500 border"
              style={inputStyle}
            />
          </div>

          {/* Trigger Type */}
          <div className="flex flex-col gap-1.5">
            <label className={labelClass}>{t('automation.triggerType')}</label>
            <div className={radioGroupClass}>
              <RadioOption name="trigger_type" value="cron" checked={form.trigger_type === 'cron'} onChange={set('trigger_type')} label={t('automation.cronRecurring')} />
              <RadioOption name="trigger_type" value="once" checked={form.trigger_type === 'once'} onChange={set('trigger_type')} label={t('automation.once')} />
            </div>
          </div>

          {/* Cron Expression */}
          {form.trigger_type === 'cron' && (
            <div className="flex flex-col gap-1.5">
              <label className={labelClass}>{t('automation.cronExpression')}</label>
              <Input
                value={form.cron_expression}
                onChange={set('cron_expression')}
                placeholder="*/30 * * * *"
                required
                className="placeholder:text-gray-500 border font-mono"
                style={inputStyle}
              />
              <span className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
                {t('automation.cronHelp')}
              </span>
            </div>
          )}

          {/* Run At */}
          {form.trigger_type === 'once' && (
            <div className="flex flex-col gap-1.5">
              <label className={labelClass}>{t('automation.runAt')}</label>
              <Input
                type="datetime-local"
                value={form.next_run_at}
                onChange={set('next_run_at')}
                required
                className="border"
                style={inputStyle}
              />
            </div>
          )}

          {/* Timezone */}
          <div className="flex flex-col gap-1.5">
            <label className={labelClass}>{t('settings.timezone')}</label>
            <select
              value={form.timezone}
              onChange={set('timezone')}
              className="flex h-10 w-full rounded-md border px-3 py-2 text-sm"
              style={inputStyle}
            >
              {COMMON_TIMEZONES.map((tz) => (
                <option key={tz} value={tz}>{tz}</option>
              ))}
            </select>
          </div>

          {/* Agent Mode */}
          <div className="flex flex-col gap-1.5">
            <label className={labelClass}>{t('automation.agentMode')}</label>
            <div className={radioGroupClass}>
              <RadioOption name="agent_mode" value="flash" checked={form.agent_mode === 'flash'} onChange={set('agent_mode')} label={t('automation.flash')} />
              <RadioOption name="agent_mode" value="ptc" checked={form.agent_mode === 'ptc'} onChange={set('agent_mode')} label={t('automation.ptcSandbox')} />
            </div>
          </div>

          {/* Workspace */}
          {form.agent_mode === 'ptc' && (
            <div className="flex flex-col gap-1.5">
              <label className={labelClass}>{t('thread.workspace')}</label>
              <select
                value={form.workspace_id}
                onChange={set('workspace_id')}
                required
                className="flex h-10 w-full rounded-md border px-3 py-2 text-sm"
                style={inputStyle}
              >
                <option value="">{t('automation.selectWorkspace')}</option>
                {workspaces.map((ws) => (
                  <option key={ws.workspace_id} value={ws.workspace_id}>
                    {ws.name}
                  </option>
                ))}
              </select>
            </div>
          )}

          {/* Instruction */}
          <div className="flex flex-col gap-1.5">
            <label className={labelClass}>{t('automation.instruction')}</label>
            <Textarea
              value={form.instruction}
              onChange={set('instruction')}
              placeholder={t('automation.instructionPlaceholder')}
              required
              rows={4}
              className="placeholder:text-gray-500 border"
              style={inputStyle}
            />
          </div>

          {/* Thread Strategy */}
          <div className="flex flex-col gap-1.5">
            <label className={labelClass}>{t('automation.threadStrategy')}</label>
            <div className={radioGroupClass}>
              <RadioOption name="thread_strategy" value="new" checked={form.thread_strategy === 'new'} onChange={set('thread_strategy')} label={t('automation.newThreadEachRun')} />
              <RadioOption name="thread_strategy" value="continue" checked={form.thread_strategy === 'continue'} onChange={set('thread_strategy')} label={t('automation.continueExisting')} />
            </div>
          </div>

          {/* Delivery */}
          <div className="flex flex-col gap-1.5">
            <label className={labelClass}>{t('automation.delivery')}</label>
            <label className="flex items-center gap-2 cursor-pointer text-sm" style={{ color: 'var(--color-text-secondary)' }}>
              <input
                type="checkbox"
                checked={form.delivery_methods.includes('slack')}
                onChange={(e) => {
                  setForm((f) => ({
                    ...f,
                    delivery_methods: e.target.checked
                      ? [...f.delivery_methods, 'slack']
                      : f.delivery_methods.filter((m) => m !== 'slack'),
                  }));
                }}
                className="accent-[var(--color-accent-primary)]"
              />
              {t('automation.deliverToSlack')}
            </label>
          </div>

          {/* Max Failures */}
          <div className="flex flex-col gap-1.5">
            <label className={labelClass}>{t('automation.maxFailures')}</label>
            <Input
              type="number"
              min={1}
              max={100}
              value={form.max_failures}
              onChange={set('max_failures')}
              className="border w-24"
              style={inputStyle}
            />
          </div>

          <DialogFooter className="pt-2 gap-2 sm:gap-0">
            <Button
              type="button"
              variant="ghost"
              onClick={() => onOpenChange(false)}
            >
              {t('common.cancel')}
            </Button>
            <Button
              type="submit"
              disabled={loading}
              style={{ backgroundColor: 'var(--color-accent-primary)', color: 'var(--color-text-on-accent)' }}
            >
              {loading ? t('common.saving') : isEdit ? t('automation.saveChanges') : t('common.create')}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
