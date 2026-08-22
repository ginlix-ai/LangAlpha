import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Loader } from '@/components/ui/loader';
import type { UserVaultBlueprint } from '@/pages/ChatAgent/utils/api';

/**
 * Step 4 of the install wizard: the credentials the plugin declared and the
 * user vault doesn't hold yet. Values land in the user vault via the plugin
 * bindings endpoint. Skipping is always allowed — an unfilled binding leaves
 * the server at needs_secret, which the MCP list already renders with its
 * own "set up" affordance.
 */

export function PluginBindingsStep({
  required,
  blueprints,
  onBind,
  binding,
  error,
  onSkip,
}: {
  /** Declared secret names still missing from the vault. */
  required: string[];
  /** Blueprint metadata for those names, when the blueprints query has it. */
  blueprints: UserVaultBlueprint[];
  onBind: (secrets: Record<string, string>) => void;
  binding: boolean;
  error: string | null;
  onSkip: () => void;
}) {
  const { t } = useTranslation();
  const [values, setValues] = useState<Record<string, string>>({});

  const byName = new Map(blueprints.map((b) => [b.name, b]));
  const filled = Object.fromEntries(
    Object.entries(values).filter(([, v]) => v.trim().length > 0),
  );
  const filledCount = Object.keys(filled).length;

  return (
    <div className="flex flex-col gap-3">
      <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
        {t('plugins.install.bindingsHint')}
      </p>

      {required.map((name) => {
        const bp = byName.get(name);
        return (
          <div key={name} className="flex flex-col gap-1">
            <label
              className="flex items-center gap-2 text-xs"
              style={{ color: 'var(--color-text-secondary)' }}
              htmlFor={`plugin-binding-${name}`}
            >
              <span className="font-medium" style={{ color: 'var(--color-text-primary)' }}>
                {bp?.label || name}
              </span>
              <span
                className="font-mono px-1.5 py-0.5 rounded text-[0.6875rem]"
                style={{
                  color: 'var(--color-text-tertiary)',
                  backgroundColor: 'var(--color-bg-tag)',
                }}
              >
                {name}
              </span>
            </label>
            {bp?.description && (
              <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
                {bp.description}
              </p>
            )}
            <input
              id={`plugin-binding-${name}`}
              type="password"
              autoComplete="off"
              value={values[name] ?? ''}
              onChange={(e) => setValues({ ...values, [name]: e.target.value })}
              placeholder={t('plugins.install.bindingPlaceholder')}
              className="text-xs px-2 py-1.5 rounded-md outline-none"
              style={{
                color: 'var(--color-text-primary)',
                backgroundColor: 'var(--color-bg-input)',
                border: '1px solid var(--color-border-muted)',
              }}
            />
            {bp?.docs_url && (
              <a
                href={bp.docs_url}
                target="_blank"
                rel="noreferrer"
                className="text-[0.6875rem] underline"
                style={{ color: 'var(--color-text-tertiary)' }}
              >
                {t('plugins.install.bindingDocs')}
              </a>
            )}
          </div>
        );
      })}

      {error && (
        <p className="text-xs" style={{ color: 'var(--color-loss)' }}>
          {error}
        </p>
      )}

      <div className="flex justify-end gap-2">
        <button
          type="button"
          onClick={onSkip}
          disabled={binding}
          className="px-3 py-1.5 text-xs rounded-md"
          style={{
            color: 'var(--color-text-secondary)',
            border: '1px solid var(--color-border-muted)',
          }}
        >
          {t('plugins.install.bindingsSkip')}
        </button>
        <button
          type="button"
          onClick={() => onBind(filled)}
          disabled={filledCount === 0 || binding}
          className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md disabled:opacity-50"
          style={{
            color: 'var(--color-btn-primary-text)',
            backgroundColor: 'var(--color-btn-primary-bg)',
          }}
        >
          {binding && <Loader size={12} className="text-current" />}
          {t('plugins.install.bindingsSave', { count: filledCount })}
        </button>
      </div>
    </div>
  );
}
