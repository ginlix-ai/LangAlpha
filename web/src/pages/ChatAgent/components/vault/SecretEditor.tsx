import React, { useEffect, useId, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { ExternalLink, Eye, EyeOff } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { Field } from '@/components/mcp/McpPrimitives';
import { normalizeSecretName } from '@/lib/secretNames';
import type { VaultBlueprint } from '../../utils/api';

/**
 * The two secret-editing surfaces: the add form (name + value + description,
 * optionally driven by a blueprint) and the per-row edit form (value +
 * description, name fixed). Both are controlled — they hold no state of their
 * own; {@link SecretsManager}'s mode object is the single source of truth.
 */

export interface SecretDraft {
  name: string;
  value: string;
  description: string;
  /** Plaintext toggle on the value field. */
  valueVisible: boolean;
}

export const EMPTY_DRAFT: SecretDraft = {
  name: '',
  value: '',
  description: '',
  valueVisible: false,
};

type DraftPatch = (patch: Partial<SecretDraft>) => void;

const inputClass =
  'w-full px-3 py-2 text-sm rounded-md bg-transparent';
const inputStyle: React.CSSProperties = {
  color: 'var(--color-text-primary)',
  border: '1px solid var(--color-border-muted)',
};

/** Escape backs out of an open form, the same gesture that dismisses the
 *  inline confirm strip. Scoped to the form rather than the window so a
 *  keystroke aimed here never also closes the panel the form sits in; while a
 *  save is in flight there is nothing to back out of, so it stays inert. */
function FormShell({
  accented = false,
  onEscape,
  children,
}: {
  accented?: boolean;
  onEscape?: () => void;
  children: React.ReactNode;
}) {
  return (
    <div
      onKeyDown={(e) => {
        if (e.key !== 'Escape' || !onEscape) return;
        e.stopPropagation();
        onEscape();
      }}
      className="flex flex-col gap-2 p-3 rounded-lg"
      style={{
        backgroundColor: 'var(--color-bg-card)',
        // Spelled out rather than interpolated: tokenRefs.test.ts scans source
        // for literal var(--color-*) names.
        border: accented
          ? '1px solid var(--color-border-elevated)'
          : '1px solid var(--color-border-muted)',
      }}
    >
      {children}
    </div>
  );
}

function ValueField({
  id,
  inputRef,
  value,
  visible,
  placeholder,
  onChange,
  onToggleVisible,
}: {
  id: string;
  inputRef?: React.Ref<HTMLInputElement>;
  value: string;
  visible: boolean;
  placeholder: string;
  onChange: (value: string) => void;
  onToggleVisible: () => void;
}) {
  return (
    <div className="relative">
      <input
        id={id}
        ref={inputRef}
        type={visible ? 'text' : 'password'}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        className={`${inputClass} pr-9`}
        style={inputStyle}
        maxLength={4096}
      />
      <button
        type="button"
        onClick={onToggleVisible}
        className="absolute right-2 top-1/2 -translate-y-1/2 p-1 rounded transition-colors hover:bg-foreground/10"
        style={{ color: 'var(--color-text-tertiary)' }}
      >
        {visible ? <EyeOff className="h-3.5 w-3.5" /> : <Eye className="h-3.5 w-3.5" />}
      </button>
    </div>
  );
}

function DescriptionField({
  id,
  value,
  onChange,
}: {
  id: string;
  value: string;
  onChange: (value: string) => void;
}) {
  const { t } = useTranslation();
  return (
    <input
      id={id}
      type="text"
      value={value}
      onChange={(e) => onChange(e.target.value)}
      placeholder={t('vault.descriptionPlaceholder')}
      className={inputClass}
      style={inputStyle}
      maxLength={256}
    />
  );
}

function FormActions({
  submitLabel,
  saving,
  disabled = false,
  onCancel,
  onSubmit,
}: {
  submitLabel: string;
  saving: boolean;
  disabled?: boolean;
  onCancel: () => void;
  onSubmit: () => void;
}) {
  const { t } = useTranslation();
  return (
    <div className="flex justify-end gap-2 mt-1">
      <button
        type="button"
        onClick={onCancel}
        className="px-3 py-1.5 text-xs rounded-md transition-colors hover:bg-foreground/10"
        style={{ color: 'var(--color-text-tertiary)' }}
      >
        {t('common.cancel')}
      </button>
      <button
        type="button"
        onClick={onSubmit}
        disabled={saving || disabled}
        className="flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors disabled:opacity-50"
        style={{ color: 'var(--color-btn-primary-text)', backgroundColor: 'var(--color-btn-primary-bg)' }}
      >
        {saving && <Loader size={12} className="text-current" />}
        {submitLabel}
      </button>
    </div>
  );
}

export function SecretAddForm({
  draft,
  blueprint,
  saving,
  onChange,
  onCancel,
  onSave,
}: {
  draft: SecretDraft;
  blueprint: VaultBlueprint | null;
  saving: boolean;
  onChange: DraftPatch;
  onCancel: () => void;
  onSave: () => void;
}) {
  const { t } = useTranslation();
  const formId = useId();
  const nameId = `${formId}-name`;
  const valueId = `${formId}-value`;
  const descriptionId = `${formId}-description`;

  // Focus lands on the first field with work left in it, decided once on
  // mount: a blueprint card and the "Set up NAME" deep link both arrive with
  // the name already filled, and a cursor sent there would have to be moved.
  const [focusValue] = useState(() => draft.name.trim().length > 0);
  const focusRef = useRef<HTMLInputElement>(null);
  useEffect(() => {
    focusRef.current?.focus({ preventScroll: true });
  }, []);

  // Safe regex compile for the active blueprint. Invalid patterns from a
  // misconfigured agent_config.yaml must not crash the UI — on failure we just
  // skip the hint.
  const presetRegex = useMemo<RegExp | null>(() => {
    if (!blueprint?.regex) return null;
    try {
      return new RegExp(blueprint.regex);
    } catch {
      return null;
    }
  }, [blueprint]);
  const valueHintFailing =
    presetRegex !== null && draft.value.length > 0 && !presetRegex.test(draft.value);

  return (
    <FormShell onEscape={saving ? undefined : onCancel}>
      {blueprint && (
        <div className="flex items-center justify-between text-xs" style={{ color: 'var(--color-text-secondary)' }}>
          <span>
            {t('vault.settingUp')}{' '}
            <span style={{ color: 'var(--color-text-primary)' }}>{blueprint.label}</span>
          </span>
          {blueprint.docs_url && (
            <a
              href={blueprint.docs_url}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-1 hover:underline"
              style={{ color: 'var(--color-accent-primary)' }}
            >
              {t('vault.docs')} <ExternalLink className="h-3 w-3" />
            </a>
          )}
        </div>
      )}
      <Field label={t('vault.fields.name')} htmlFor={nameId}>
        <input
          id={nameId}
          type="text"
          value={draft.name}
          onChange={(e) => onChange({ name: normalizeSecretName(e.target.value) })}
          placeholder="SECRET_NAME"
          className={`${inputClass} font-mono`}
          style={inputStyle}
          maxLength={64}
          ref={focusValue ? undefined : focusRef}
        />
      </Field>
      <Field label={t('vault.fields.value')} htmlFor={valueId}>
        <ValueField
          id={valueId}
          inputRef={focusValue ? focusRef : undefined}
          value={draft.value}
          visible={draft.valueVisible}
          placeholder={t('vault.valuePlaceholder')}
          onChange={(value) => onChange({ value })}
          onToggleVisible={() => onChange({ valueVisible: !draft.valueVisible })}
        />
      </Field>
      {valueHintFailing && (
        <div className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
          {t('vault.valueHintInvalid', { label: blueprint?.label ?? t('vault.tokenFallback') })}
          {blueprint?.docs_url ? t('vault.valueHintDocs') : '.'}
          <span className="ml-1 opacity-70">{t('vault.valueHintStillSave')}</span>
        </div>
      )}
      <Field label={t('vault.fields.description')} htmlFor={descriptionId}>
        <DescriptionField
          id={descriptionId}
          value={draft.description}
          onChange={(description) => onChange({ description })}
        />
      </Field>
      <FormActions
        submitLabel={t('common.save')}
        saving={saving}
        disabled={!draft.name || !draft.value}
        onCancel={onCancel}
        onSubmit={onSave}
      />
    </FormShell>
  );
}

export function SecretEditForm({
  name,
  draft,
  saving,
  onChange,
  onCancel,
  onSave,
}: {
  name: string;
  draft: SecretDraft;
  saving: boolean;
  onChange: DraftPatch;
  onCancel: () => void;
  onSave: () => void;
}) {
  const { t } = useTranslation();
  const formId = useId();
  const valueId = `${formId}-value`;
  const descriptionId = `${formId}-description`;
  const focusRef = useRef<HTMLInputElement>(null);
  useEffect(() => {
    focusRef.current?.focus({ preventScroll: true });
  }, []);
  return (
    <FormShell accented onEscape={saving ? undefined : onCancel}>
      <div className="text-sm font-mono font-medium" style={{ color: 'var(--color-text-primary)' }}>
        {name}
      </div>
      <Field label={t('vault.fields.newValue')} htmlFor={valueId}>
        <ValueField
          id={valueId}
          inputRef={focusRef}
          value={draft.value}
          visible={draft.valueVisible}
          placeholder={t('vault.editValuePlaceholder')}
          onChange={(value) => onChange({ value })}
          onToggleVisible={() => onChange({ valueVisible: !draft.valueVisible })}
        />
      </Field>
      <Field label={t('vault.fields.description')} htmlFor={descriptionId}>
        <DescriptionField
          id={descriptionId}
          value={draft.description}
          onChange={(description) => onChange({ description })}
        />
      </Field>
      <FormActions
        submitLabel={t('vault.update')}
        saving={saving}
        onCancel={onCancel}
        onSubmit={onSave}
      />
    </FormShell>
  );
}
