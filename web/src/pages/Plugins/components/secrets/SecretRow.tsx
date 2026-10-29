import { useEffect, useId, useRef, useState } from 'react';
import { Link } from 'react-router';
import { Trans, useTranslation } from 'react-i18next';
import { ChevronDown, Eye, EyeOff, Pencil, Trash2 } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { readersSentence } from '@/hooks/useSecretUsage';
import { cn } from '@/lib/utils';
import type { UserVaultSecret } from '@/pages/ChatAgent/utils/api';
import { detailHref } from '../../utils/detailParam';

/**
 * A secret's resting row: identity + masked (or revealed) value, which
 * connectors read it, and the reveal / edit / delete affordances. Delete arms a
 * confirm under the row (the manager's), so the trash button never turns into
 * a different verb in place.
 *
 * A line of a table rather than a card of its own: name and value in columns
 * that line up down the list, the note under them, so eleven keys read as one
 * inventory instead of eleven boxes.
 */

interface SecretRowProps {
  secret: UserVaultSecret;
  /** Plaintext once fetched; `undefined` keeps the masked value on screen. */
  revealedValue: string | undefined;
  revealing: boolean;
  /** The connectors that read this key; undefined while that is unknown. */
  readers: readonly string[] | undefined;
  confirmingDelete: boolean;
  /** Added a moment ago: marked, and brought on screen once. */
  justAdded: boolean;
  onToggleReveal: () => void;
  onEdit: () => void;
  onRequestDelete: () => void;
}

export function SecretRow({
  secret,
  revealedValue,
  revealing,
  readers,
  confirmingDelete,
  justAdded,
  onToggleReveal,
  onEdit,
  onRequestDelete,
}: SecretRowProps) {
  const { t } = useTranslation();
  const revealed = revealedValue !== undefined;
  const rowRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (justAdded) rowRef.current?.scrollIntoView?.({ block: 'nearest' });
  }, [justAdded]);

  return (
    <div ref={rowRef} className={cn('vault-row', justAdded && 'is-just-added')}>
      <div className="vault-row-text">
        <span className="vault-row-name">{secret.name}</span>
        <span className={revealed ? 'vault-row-value is-revealed' : 'vault-row-value'}>
          {revealed ? revealedValue : secret.masked_value}
        </span>
        <span className="vault-row-desc">{secret.description}</span>
        {readers && <SecretReaders readers={readers} />}
      </div>
      <div className="vault-row-actions">
        <button
          type="button"
          onClick={onToggleReveal}
          disabled={revealing}
          className="vault-icon-btn"
          aria-label={revealed ? t('vault.hideValue') : t('vault.revealValue')}
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
          className="vault-icon-btn"
          aria-label={t('vault.edit')}
          title={t('vault.edit')}
        >
          <Pencil className="h-3.5 w-3.5" />
        </button>
        <button
          type="button"
          onClick={onRequestDelete}
          className={cn('vault-icon-btn', confirmingDelete && 'is-armed')}
          aria-label={t('vault.delete')}
          aria-expanded={confirmingDelete}
          title={t('vault.delete')}
        >
          <Trash2 className="h-3.5 w-3.5" />
        </button>
      </div>
    </div>
  );
}

/**
 * Which connectors read the key. One is named outright; several fold behind
 * their count, since a row that listed seven names would no longer read as one
 * line of an inventory. Each name opens that connector.
 */
function SecretReaders({ readers }: { readers: readonly string[] }) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const listId = useId();

  const { key, values } = readersSentence(readers, {
    none: 'vault.readers.none',
    one: 'vault.readers.one',
    many: 'vault.readers.many',
  });
  const href = (name: string) => detailHref({ kind: 'server', name });
  if (readers.length < 2) {
    return (
      <span className={cn('vault-row-readers', readers.length === 0 && 'is-none')}>
        {/* Not `<link>`: Trans parses the string as HTML, where that tag is void. */}
        <Trans
          i18nKey={key}
          values={values}
          components={{ connector: <Link to={href(values.connector)} className="vault-reader-link" /> }}
        />
      </span>
    );
  }
  return (
    <span className="vault-row-readers">
      <button
        type="button"
        className="vault-readers-toggle"
        aria-expanded={open}
        aria-controls={listId}
        onClick={() => setOpen((v) => !v)}
      >
        {t(key, values)}
        <ChevronDown className={cn('h-3 w-3 transition-transform', open && 'rotate-180')} aria-hidden />
      </button>
      {open && (
        <span id={listId} className="vault-readers-list">
          {readers.map((name) => (
            <Link key={name} to={href(name)} className="vault-reader-link">
              {name}
            </Link>
          ))}
        </span>
      )}
    </span>
  );
}
