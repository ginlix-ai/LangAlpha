import React, { useEffect, useId, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { motion } from 'framer-motion';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import { toast } from '@/components/ui/use-toast';
import type { Automation, AutomationPayload, AutomationUpdatePayload } from '@/types/automation';
import { PANE_ENTER } from '../utils/motion';
import {
  type FormPatch,
  type FormState,
  formStateToPayload,
  formStateToUpdatePayload,
  isFormChanged,
  validateForm,
  withZone,
} from '../utils/form';
import FormRow from './FormRow';
import MoreOptions from './MoreOptions';
import TriggerSection from './TriggerSection';
import './AutomationInlineForm.css';

/** The server's limit on a name. */
const NAME_MAX = 255;

export type FormSubmission =
  | { kind: 'create'; payload: AutomationPayload }
  | { kind: 'edit'; payload: AutomationUpdatePayload };

export interface AutomationInlineFormProps {
  initialValues: FormState;
  /** The automation being edited, or null for a new one. */
  original: Automation | null;
  onSubmit: (submission: FormSubmission) => void;
  onCancel: () => void;
  /** Told whether the form now differs from what it opened with. */
  onDirtyChange?: (dirty: boolean) => void;
  loading: boolean;
}

export default function AutomationInlineForm({
  initialValues,
  original,
  onSubmit,
  onCancel,
  onDirtyChange,
  loading,
}: AutomationInlineFormProps) {
  const { t } = useTranslation();
  // What the form opened with. The props follow the list as it refreshes,
  // but an edit is judged, and patched, against what the reader started from.
  const [opened] = useState(() => ({ values: initialValues, automation: original }));
  const isEdit = !!opened.automation;
  const [form, setForm] = useState<FormState>(initialValues);
  const [moreOpen, setMoreOpen] = useState(false);
  const uid = useId();
  const ids = { name: `${uid}-name`, instruction: `${uid}-instruction` };

  const patch: FormPatch = (key, value) => setForm((f) => ({ ...f, [key]: value }));

  const dirty = isFormChanged(opened.values, form);
  useEffect(() => {
    onDirtyChange?.(dirty);
  }, [dirty, onDirtyChange]);
  useEffect(() => () => onDirtyChange?.(false), [onDirtyChange]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const problem = validateForm(form, Date.now(), isEdit ? opened.values : undefined);
    if (problem) {
      // A field folded away cannot be fixed, so its section opens.
      if (problem.section === 'more') setMoreOpen(true);
      toast({ variant: 'destructive', description: t(problem.messageKey) });
      return;
    }
    onSubmit(
      opened.automation
        ? { kind: 'edit', payload: formStateToUpdatePayload(form, opened.values, opened.automation) }
        : { kind: 'create', payload: formStateToPayload(form) },
    );
  };

  return (
    <motion.div {...PANE_ENTER} exit={{ opacity: 0 }}>
      <form onSubmit={handleSubmit} className="automation-form">
        <div className="automation-form-rows">
          <FormRow label={t('common.name')} htmlFor={ids.name}>
            <Input
              id={ids.name}
              value={form.name}
              onChange={(e) => patch('name', e.target.value)}
              placeholder={t('automation.namePlaceholder')}
              maxLength={NAME_MAX}
              required
            />
          </FormRow>

          <TriggerSection
            form={form}
            patch={patch}
            isEdit={isEdit}
            onZone={(tz) => setForm((f) => withZone(f, tz))}
          />

          <FormRow label={t('automation.instruction')} htmlFor={ids.instruction}>
            <Textarea
              id={ids.instruction}
              value={form.instruction}
              onChange={(e) => patch('instruction', e.target.value)}
              placeholder={t('automation.instructionPlaceholder')}
              required
              rows={5}
            />
          </FormRow>

          <MoreOptions form={form} patch={patch} open={moreOpen} onOpenChange={setMoreOpen} />
        </div>

        <div className="automation-form-actions">
          <Button
            type="submit"
            disabled={loading}
            className="transition-opacity hover:opacity-90"
            style={{ backgroundColor: 'var(--color-btn-primary-bg)', color: 'var(--color-btn-primary-text)' }}
          >
            {loading ? t('common.saving') : isEdit ? t('automation.saveChanges') : t('common.create')}
          </Button>
          <Button type="button" variant="ghost" onClick={onCancel}>
            {t('common.cancel')}
          </Button>
        </div>
      </form>
    </motion.div>
  );
}
