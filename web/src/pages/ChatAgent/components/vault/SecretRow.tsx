import { useTranslation } from 'react-i18next';
import { Eye, EyeOff, Pencil, Trash2 } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import type { SecretItem } from './SecretsManager';

/**
 * A secret's resting row: identity + masked (or revealed) value, with the
 * reveal / edit / delete affordances. Delete arms an inline confirm in place of
 * the trash button rather than a dialog — the destructive verb never moves.
 */

interface SecretRowProps {
  secret: SecretItem;
  /** Plaintext once fetched; `undefined` keeps the masked value on screen. */
  revealedValue: string | undefined;
  revealing: boolean;
  confirmingDelete: boolean;
  deletePending: boolean;
  onToggleReveal: () => void;
  onEdit: () => void;
  onRequestDelete: () => void;
  onCancelDelete: () => void;
  onConfirmDelete: () => void;
}

export function SecretRow({
  secret,
  revealedValue,
  revealing,
  confirmingDelete,
  deletePending,
  onToggleReveal,
  onEdit,
  onRequestDelete,
  onCancelDelete,
  onConfirmDelete,
}: SecretRowProps) {
  const { t } = useTranslation();
  const revealed = revealedValue !== undefined;

  return (
    <div
      className="flex items-center justify-between py-2.5 px-3 rounded-lg"
      style={{ backgroundColor: 'var(--color-bg-card)' }}
    >
      <div className="min-w-0">
        <div className="flex items-center gap-2">
          <span className="text-sm font-mono font-medium" style={{ color: 'var(--color-text-primary)' }}>
            {secret.name}
          </span>
          <span className="text-xs font-mono truncate" style={{ color: 'var(--color-text-tertiary)' }}>
            {revealed ? revealedValue : secret.masked_value}
          </span>
        </div>
        {secret.description && (
          <p className="text-xs mt-0.5 truncate" style={{ color: 'var(--color-text-tertiary)' }}>
            {secret.description}
          </p>
        )}
      </div>
      <div className="flex items-center gap-1 flex-shrink-0 ml-2">
        <button
          type="button"
          onClick={onToggleReveal}
          disabled={revealing}
          className="p-1.5 rounded transition-colors hover:bg-foreground/10 disabled:opacity-50"
          style={{ color: 'var(--color-text-tertiary)' }}
          title={revealed ? t('vault.hideValue') : t('vault.revealValue')}
        >
          {revealing ? (
            <Loader size={14} className="text-current" />
          ) : revealed ? (
            <EyeOff className="h-3.5 w-3.5" />
          ) : (
            <Eye className="h-3.5 w-3.5" />
          )}
        </button>
        <button
          type="button"
          onClick={onEdit}
          className="p-1.5 rounded transition-colors hover:bg-foreground/10"
          style={{ color: 'var(--color-text-tertiary)' }}
          title={t('vault.edit')}
        >
          <Pencil className="h-3.5 w-3.5" />
        </button>
        {confirmingDelete ? (
          <div className="flex items-center gap-1">
            <button
              type="button"
              onClick={onConfirmDelete}
              disabled={deletePending}
              className="px-2 py-1 text-xs rounded transition-colors disabled:opacity-50"
              style={{ color: 'var(--color-loss)', backgroundColor: 'var(--color-bg-card)' }}
            >
              {deletePending ? t('vault.deleting') : t('vault.deleteConfirmYes')}
            </button>
            <button
              type="button"
              onClick={onCancelDelete}
              className="px-2 py-1 text-xs rounded transition-colors hover:bg-foreground/10"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              {t('common.cancel')}
            </button>
          </div>
        ) : (
          <button
            type="button"
            onClick={onRequestDelete}
            className="p-1.5 rounded transition-colors hover:bg-foreground/10"
            style={{ color: 'var(--color-text-tertiary)' }}
            title={t('vault.delete')}
          >
            <Trash2 className="h-3.5 w-3.5" />
          </button>
        )}
      </div>
    </div>
  );
}
