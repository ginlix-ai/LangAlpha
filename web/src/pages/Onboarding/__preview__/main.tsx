/* eslint-disable react-refresh/only-export-components -- entry file, not a module */
import { StrictMode, useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import i18n from '@/i18n';
import '@/index.css';
import { ANNOUNCEMENTS, PAGE_INTROS } from '@/pages/Onboarding/registry';
import { PageIntroModal } from '@/pages/Onboarding/engine/PageIntroModal';
import { WhatsNewModal } from '@/pages/Onboarding/engine/WhatsNewModal';

/**
 * Dev-only harness rendering the REAL PageIntroModal and WhatsNewModal for
 * design iteration. Open /intro-preview.html on the dev server. Not bundled
 * in production (vite builds index.html only) and not mounted anywhere in
 * the app.
 *
 * Note: the dialogs are modal, so clicking the toolbar counts as an
 * outside-click and re-mounts the modal at step 1 — which conveniently
 * replays the entrance animations after every control change.
 */

// Synthetic newer release (copy reused from the real entry) so the modal's
// multi-release grouping is previewable before a second announcement exists.
const SECOND_RELEASE = ANNOUNCEMENTS.map((a) => ({
  ...a,
  key: `${a.key}-preview2`,
  releaseVersion: '2026.06.12',
}));

const THEMES = ['light', 'dark'] as const;
const LOCALES = ['en-US', 'zh-CN'] as const;

function Btn({
  on,
  onClick,
  children,
}: {
  on: boolean;
  onClick: () => void;
  children: string;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      style={{
        font: 'inherit',
        fontSize: 12,
        cursor: 'pointer',
        padding: '5px 12px',
        borderRadius: 999,
        border: '1px solid var(--color-border-default)',
        background: on ? 'var(--color-accent-primary)' : 'var(--color-bg-card)',
        color: on ? 'var(--color-text-on-accent)' : 'var(--color-text-secondary)',
      }}
    >
      {children}
    </button>
  );
}

function Preview() {
  const [surface, setSurface] = useState<'intro' | 'whatsNew'>('intro');
  const [twoReleases, setTwoReleases] = useState(false);
  const [introId, setIntroId] = useState(PAGE_INTROS[0].id);
  const [theme, setTheme] = useState<(typeof THEMES)[number]>('light');
  const [locale, setLocale] = useState<(typeof LOCALES)[number]>('en-US');
  const [run, setRun] = useState(0); // bump = remount modal at step 1

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme);
    document.body.style.background = 'var(--color-bg-page)';
  }, [theme]);
  useEffect(() => {
    i18n.changeLanguage(locale);
  }, [locale]);

  const intro = PAGE_INTROS.find((i) => i.id === introId) ?? PAGE_INTROS[0];

  return (
    <>
      <div
        style={{
          position: 'fixed',
          top: 12,
          left: '50%',
          transform: 'translateX(-50%)',
          zIndex: 2000,
          pointerEvents: 'auto',
          display: 'flex',
          gap: 8,
          alignItems: 'center',
          padding: '8px 14px',
          borderRadius: 999,
          border: '1px solid var(--color-border-default)',
          background: 'var(--color-bg-card)',
          boxShadow: '0 4px 24px rgba(0,0,0,.25)',
          fontFamily: 'system-ui, sans-serif',
          fontSize: 12,
          color: 'var(--color-text-tertiary)',
        }}
      >
        {PAGE_INTROS.map((i) => (
          <Btn
            key={i.id}
            on={surface === 'intro' && i.id === introId}
            onClick={() => {
              setSurface('intro');
              setIntroId(i.id);
            }}
          >
            {`${i.id} (${i.steps.length})`}
          </Btn>
        ))}
        <Btn on={surface === 'whatsNew'} onClick={() => setSurface('whatsNew')}>
          whatsNew
        </Btn>
        {surface === 'whatsNew' && (
          <Btn on={twoReleases} onClick={() => setTwoReleases((v) => !v)}>
            +2nd release
          </Btn>
        )}
        <span style={{ width: 1, height: 18, background: 'var(--color-border-default)' }} />
        {THEMES.map((t) => (
          <Btn key={t} on={t === theme} onClick={() => setTheme(t)}>
            {t}
          </Btn>
        ))}
        <span style={{ width: 1, height: 18, background: 'var(--color-border-default)' }} />
        {LOCALES.map((l) => (
          <Btn key={l} on={l === locale} onClick={() => setLocale(l)}>
            {l}
          </Btn>
        ))}
      </div>
      {surface === 'intro' ? (
        <PageIntroModal
          key={`${intro.id}-${locale}-${run}`}
          intro={intro}
          onClose={() => setRun((n) => n + 1)}
        />
      ) : (
        <WhatsNewModal
          key={`wn-${locale}-${twoReleases}-${run}`}
          announcements={twoReleases ? [...ANNOUNCEMENTS, ...SECOND_RELEASE] : ANNOUNCEMENTS}
          onAcknowledge={() => setRun((n) => n + 1)}
        />
      )}
    </>
  );
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <Preview />
  </StrictMode>
);
