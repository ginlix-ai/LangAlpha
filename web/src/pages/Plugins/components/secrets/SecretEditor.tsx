import React, { useEffect, useId, useLayoutEffect, useRef, useState } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import { ExternalLink, Info } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { useReducedMotion } from '@/lib/framer';
import { DURATION } from '@/lib/motion';
import { Field, FieldError, HeaderButton } from '@/components/mcp/McpPrimitives';
import { readersSentence } from '@/hooks/useSecretUsage';
import { useSecretNameInput } from '@/hooks/useSecretNameInput';
import { MAX_SECRET_VALUE_LENGTH, savedSecretName } from '@/lib/secretNames';
import type { UserVaultBlueprint } from '@/pages/ChatAgent/utils/api';
import { keyMismatch } from '../../utils/pluginSchemas';
import type { FormError, Landing, SecretDraft } from './secretDraft';
import { SecretValueInput } from './SecretValueInput';
import type { AddReason } from './useSecretsMode';

/**
 * The two secret-editing surfaces: the add form (name + value + description,
 * optionally driven by a blueprint) and the per-row edit form (value +
 * description, name fixed). Both are controlled: they hold no state of their
 * own, and the manager's mode (`useSecretsMode`) is the single source of truth.
 */

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
  landing = null,
  onEscape,
  children,
}: {
  accented?: boolean;
  /** Set when the form opened from a link or another control rather than in
   *  place, where it may be off screen. Read once, on mount. */
  landing?: Landing | null;
  onEscape?: () => void;
  children: React.ReactNode;
}) {
  const shellRef = useRef<HTMLDivElement>(null);
  const [landOnMount] = useState(landing);
  const reducedMotion = useReducedMotion();
  useEffect(() => {
    if (!landOnMount) return;
    // The form mounts inside a fold still at height 0, where the list has no
    // room below the row to scroll into, so land once the fold has opened.
    const timer = window.setTimeout(
      // Optional call: jsdom has no scrollIntoView.
      () =>
        shellRef.current?.scrollIntoView?.({
          block: landOnMount,
          behavior: reducedMotion ? 'auto' : 'smooth',
        }),
      reducedMotion ? 0 : DURATION.fold * 1000,
    );
    return () => window.clearTimeout(timer);
  }, [landOnMount, reducedMotion]);
  return (
    <div
      ref={shellRef}
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

/** Tallest the description grows before it scrolls: six lines of it. */
const DESCRIPTION_MAX_PX = 140;

/**
 * Grows with what it holds, so a description of two lines is read whole
 * rather than scrolled sideways inside a one-line box. Sized from the content
 * on every change, since `field-sizing` is above the browser floor.
 */
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
  const ref = useRef<HTMLTextAreaElement>(null);
  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return;
    el.style.height = 'auto';
    // scrollHeight leaves out the border, which a border-box height includes.
    const border = el.offsetHeight - el.clientHeight;
    el.style.height = `${Math.min(el.scrollHeight + border, DESCRIPTION_MAX_PX)}px`;
  }, [value]);
  return (
    <textarea
      ref={ref}
      id={id}
      rows={1}
      value={value}
      onChange={(e) => onChange(e.target.value)}
      placeholder={t('vault.descriptionPlaceholder')}
      className={`${inputClass} block resize-none overflow-y-auto leading-snug`}
      style={{ ...inputStyle, maxHeight: DESCRIPTION_MAX_PX }}
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

/** The line under a field: a refusal in the loss colour, anything else quiet. */
function FieldNote({ id, tone = 'quiet', children }: { id?: string; tone?: 'quiet' | 'error'; children: React.ReactNode }) {
  return (
    <p
      id={id}
      className="text-[0.6875rem] leading-snug"
      style={{ color: tone === 'error' ? 'var(--color-loss)' : 'var(--color-text-tertiary)' }}
    >
      {children}
    </p>
  );
}

export function SecretAddForm({
  draft,
  blueprint,
  reason,
  saving,
  error,
  existingName,
  capMessage,
  landing,
  onChange,
  onEditExisting,
  onCancel,
  onSave,
}: {
  draft: SecretDraft;
  blueprint: UserVaultBlueprint | null;
  reason: AddReason;
  saving: boolean;
  error: FormError | null;
  /** The stored key this name would collide with, if any. */
  existingName: string | null;
  /** Set at the cap: the form can be filled but not saved. */
  capMessage: string | null;
  landing: Landing | null;
  onChange: DraftPatch;
  onEditExisting: (name: string) => void;
  onCancel: () => void;
  onSave: () => void;
}) {
  const { t } = useTranslation();
  const formId = useId();
  const nameId = `${formId}-name`;
  const nameNoteId = `${formId}-name-note`;
  const valueId = `${formId}-value`;
  const descriptionId = `${formId}-description`;

  // Focus lands on the first field with work left in it, decided once on
  // mount: a blueprint card and the "Set up NAME" deep link both arrive with
  // the name already filled, and a cursor sent there would have to be moved.
  const [focusValue] = useState(() => draft.name.trim().length > 0);
  const nameRef = useRef<HTMLInputElement>(null);
  const valueRef = useRef<HTMLInputElement>(null);
  useEffect(() => {
    (focusValue ? valueRef : nameRef).current?.focus({ preventScroll: true });
  }, [focusValue]);

  const onNameInput = useSecretNameInput(nameRef, draft.name, (name) => onChange({ name }));
  const savedName = savedSecretName(draft.name);
  const nameUnusable = draft.name.length > 0 && savedName.length === 0;
  const nameRewritten = !nameUnusable && savedName !== draft.name;
  const nameError = error?.field === 'name' ? error : null;
  // The list says so before Save; the server says so when the list was stale.
  const taken = existingName ?? nameError?.existing ?? null;

  const valueHintFailing = keyMismatch(draft.value, blueprint?.regex) !== null;
  // Said while the name is still that key's: renamed, this is a new key.
  const said = reason.kind !== 'new' && savedName === reason.name ? reason : null;

  return (
    <FormShell landing={landing} onEscape={saving ? undefined : onCancel}>
      {/* The edit form this replaced closed without anyone closing it, or an
          edit link opened an add form, so the reason comes first. */}
      {said && (
        <p role="status" className="vault-cap-line" style={{ margin: 0 }}>
          <Info className="vault-cap-icon" aria-hidden />
          <span>
            {said.kind === 'readd' ? (
              t('vault.deletedElsewhere')
            ) : (
              <Trans
                i18nKey="vault.notStored"
                values={{ name: said.name }}
                components={{ name: <code className="vault-inline-name" /> }}
              />
            )}
          </span>
        </p>
      )}
      {blueprint && (
        <div className="flex items-center justify-between gap-3 text-xs" style={{ color: 'var(--color-text-secondary)' }}>
          <span className="min-w-0">
            {t('vault.settingUp')}{' '}
            <span style={{ color: 'var(--color-text-primary)' }}>{blueprint.label}</span>
          </span>
          {blueprint.docs_url && (
            <a
              href={blueprint.docs_url}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-1 shrink-0 hover:underline"
              style={{ color: 'var(--color-text-secondary)' }}
            >
              {t('vault.docs')} <ExternalLink className="h-3 w-3" />
            </a>
          )}
        </div>
      )}
      <Field label={t('vault.fields.name')} htmlFor={nameId}>
        <input
          id={nameId}
          ref={nameRef}
          type="text"
          value={draft.name}
          onChange={onNameInput}
          placeholder="SECRET_NAME"
          className={`${inputClass} font-mono`}
          style={inputStyle}
          maxLength={64}
          autoComplete="off"
          spellCheck={false}
          aria-invalid={nameUnusable || !!taken || !!nameError}
          aria-describedby={nameNoteId}
        />
        <div id={nameNoteId} className="flex flex-col gap-1 empty:hidden">
          {nameUnusable && <FieldNote tone="error">{t('vault.nameInvalid')}</FieldNote>}
          {nameRewritten && !taken && (
            <FieldNote>
              {t('vault.savedAs')}{' '}
              <code className="vault-inline-name">{savedName}</code>
            </FieldNote>
          )}
          {taken ? (
            <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
              <FieldNote tone="error">
                <code className="vault-inline-name">{taken}</code> {t('vault.nameTaken')}
              </FieldNote>
              <HeaderButton variant="secondary" onClick={() => onEditExisting(taken)}>
                {t('vault.editInstead', { name: taken })}
              </HeaderButton>
            </div>
          ) : (
            nameError && <FieldError error={nameError} />
          )}
        </div>
      </Field>
      <Field label={t('vault.fields.value')} htmlFor={valueId}>
        <SecretValueInput
          id={valueId}
          ref={valueRef}
          value={draft.value}
          visible={draft.valueVisible}
          placeholder={t('vault.valuePlaceholder')}
          className={`${inputClass} pr-9`}
          style={inputStyle}
          maxLength={MAX_SECRET_VALUE_LENGTH}
          onChange={(value) => onChange({ value })}
          onToggleVisible={() => onChange({ valueVisible: !draft.valueVisible })}
        />
        {error?.field === 'value' ? (
          <FieldError error={error} />
        ) : (
          blueprint &&
          valueHintFailing && (
            <FieldNote>
              {t('vault.valueHintInvalid', { label: blueprint.label })}{' '}
              {blueprint.docs_url ? t('vault.valueHintDocs') : t('vault.valueHintStillSave')}
            </FieldNote>
          )
        )}
      </Field>
      <Field label={t('vault.fields.description')} htmlFor={descriptionId}>
        <DescriptionField
          id={descriptionId}
          value={draft.description}
          onChange={(description) => onChange({ description })}
        />
        {error?.field === 'description' && <FieldError error={error} />}
      </Field>
      {/* A refusal for a full account says what the cap line says, so the
          line stands for both. */}
      {capMessage ? (
        <FieldNote tone="error">{capMessage}</FieldNote>
      ) : (
        error?.field === 'form' && <FieldError error={error} />
      )}
      <FormActions
        submitLabel={t('common.save')}
        saving={saving}
        disabled={!savedName || !draft.value || !!taken || !!capMessage}
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
  error,
  readers,
  landing,
  onChange,
  onCancel,
  onSave,
}: {
  name: string;
  draft: SecretDraft;
  saving: boolean;
  error: FormError | null;
  /** The connectors that read this key, or undefined while that is unknown. */
  readers: readonly string[] | undefined;
  landing: Landing | null;
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
  // Said before the save, because a rotated key is the moment someone asks
  // whether the run already in progress will see it. It will not until the
  // next message, which is when every workspace re-reads its keys.
  const reach = readersSentence(readers ?? [], {
    none: 'vault.reach.workspaces',
    one: 'vault.reach.oneConnector',
    many: 'vault.reach.connectors',
  });
  return (
    <FormShell accented landing={landing} onEscape={saving ? undefined : onCancel}>
      <div className="text-sm font-mono font-medium break-all" style={{ color: 'var(--color-text-primary)' }}>
        {name}
      </div>
      <Field label={t('vault.fields.newValue')} htmlFor={valueId}>
        <SecretValueInput
          id={valueId}
          ref={focusRef}
          value={draft.value}
          visible={draft.valueVisible}
          placeholder={t('vault.editValuePlaceholder')}
          className={`${inputClass} pr-9`}
          style={inputStyle}
          maxLength={MAX_SECRET_VALUE_LENGTH}
          onChange={(value) => onChange({ value })}
          onToggleVisible={() => onChange({ valueVisible: !draft.valueVisible })}
        />
        {error?.field === 'value' ? <FieldError error={error} /> : <FieldNote>{t(reach.key, reach.values)}</FieldNote>}
      </Field>
      <Field label={t('vault.fields.description')} htmlFor={descriptionId}>
        <DescriptionField
          id={descriptionId}
          value={draft.description}
          onChange={(description) => onChange({ description })}
        />
        {error?.field === 'description' && <FieldError error={error} />}
      </Field>
      {error && error.field !== 'value' && error.field !== 'description' && <FieldError error={error} />}
      <FormActions
        submitLabel={t('common.save')}
        saving={saving}
        onCancel={onCancel}
        onSubmit={onSave}
      />
    </FormShell>
  );
}
