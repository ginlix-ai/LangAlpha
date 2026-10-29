import { Trans, useTranslation } from 'react-i18next';

const CODE = <code className="accounts-aside-code" />;
const SNIPPET = 'from vault import get\nkey = get("SECRET_NAME")';

/** How sandbox code reads a secret, and what keeps the value away from the
 *  model, as blocks in the vault's side column. Each sentence is one key with
 *  its code inline, so each locale sets the spacing around it. */
export function VaultUsageNote() {
  const { t } = useTranslation();
  return (
    <>
      <section className="detail-aside-block">
        <h3>{t('vault.usage.title')}</h3>
        <p className="accounts-aside-text">{t('vault.usage.lead')}</p>
        <pre className="accounts-aside-snippet">{SNIPPET}</pre>
      </section>
      <section className="detail-aside-block">
        <h3>{t('vault.security.title')}</h3>
        <ul className="accounts-aside-list">
          <li>{t('vault.security.encrypted')}</li>
          <li>
            <Trans i18nKey="vault.security.agent" values={{ call: 'vault.get()' }} components={{ code: CODE }} />
          </li>
          <li>{t('vault.security.leakScan')}</li>
          <li>
            <Trans i18nKey="vault.security.store" values={{ module: 'vault' }} components={{ code: CODE }} />
          </li>
        </ul>
      </section>
    </>
  );
}
