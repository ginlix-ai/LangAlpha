/* eslint-disable react-refresh/only-export-components -- entry file, not a module */
import { StrictMode, Suspense, useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import i18n, { initI18n } from '@/i18n';
import { applyCardStyle, CARD_STYLES, getCardStyle, setCardStyle } from '@/lib/cardStyle';
import { switchLocale } from '@/lib/locale';
import '@/index.css';
import { ANNOUNCEMENTS, PAGE_INTROS } from '@/pages/Onboarding/registry';
import { PageIntroModal } from '@/pages/Onboarding/engine/PageIntroModal';
import { WhatsNewModal } from '@/pages/Onboarding/engine/WhatsNewModal';
import { INTRO_VISUALS, loadIntroScenes } from '@/pages/Onboarding/engine/introScenes';
import { GuidePreview } from './guide';
import { ScenePreview } from './scenes';

/**
 * Dev-only harness rendering the REAL PageIntroModal, WhatsNewModal and the
 * sidebar's getting-started entry for design iteration. Open /intro-preview.html on the dev server. Not bundled
 * in production (vite builds index.html only) and not mounted anywhere in
 * the app.
 *
 * Note: the dialogs are modal, so clicking the toolbar counts as an
 * outside-click and re-mounts the modal at step 1 — which conveniently
 * replays the entrance animations after every control change.
 *
 * URL parameters, for screenshots without clicking: `theme`, `locale` and
 * `surface` (intro, whatsNew, guide; `rail` for the guide's rail) set the
 * starting toggles; `scene=<visual id>` shows that one scene alone, live,
 * or with `frames=0,800,1600` (or `frames=8` for that many evenly spaced) as
 * a contact sheet of frozen frames. `cardStyle` (quiet, balanced, terminal)
 * sets the data cards' style, which the dashboard scenes draw their widgets
 * in, and keeps it for the next load, as Settings does.
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

const params = new URLSearchParams(window.location.search);
const initialTheme = THEMES.find((t) => t === params.get('theme')) ?? 'light';
const initialLocale = LOCALES.find((l) => l === params.get('locale')) ?? 'en-US';
const SURFACES = ['intro', 'whatsNew', 'guide'] as const;
const initialSurface = SURFACES.find((v) => v === params.get('surface')) ?? 'intro';
const sceneParam = params.get('scene');
const scene = INTRO_VISUALS.find((v) => v === sceneParam) ?? null;

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
        fontSize: '0.75rem',
        whiteSpace: 'nowrap',
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
  const [surface, setSurface] = useState<(typeof SURFACES)[number]>(initialSurface);
  const [guideVariant, setGuideVariant] = useState<'row' | 'rail'>(
    params.has('rail') ? 'rail' : 'row'
  );
  const [twoReleases, setTwoReleases] = useState(false);
  const [introId, setIntroId] = useState(PAGE_INTROS[0].id);
  const [theme, setTheme] = useState<(typeof THEMES)[number]>(initialTheme);
  const [locale, setLocale] = useState<(typeof LOCALES)[number]>(initialLocale);
  const [run, setRun] = useState(0); // bump = remount modal at step 1

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme);
    document.body.style.background = 'var(--color-bg-page)';
  }, [theme]);
  useEffect(() => {
    void switchLocale(i18n, locale);
  }, [locale]);

  const intro = PAGE_INTROS.find((i) => i.id === introId) ?? PAGE_INTROS[0];

  if (scene) return <ScenePreview key={locale} visual={scene} frames={params.get('frames')} />;

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
          fontSize: '0.75rem',
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
        <Btn on={surface === 'guide'} onClick={() => setSurface('guide')}>
          guide
        </Btn>
        {surface === 'guide' && (
          <Btn
            on={guideVariant === 'rail'}
            onClick={() => setGuideVariant((v) => (v === 'row' ? 'rail' : 'row'))}
          >
            rail
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
      {surface === 'guide' ? (
        <GuidePreview key={`${guideVariant}-${locale}-${run}`} variant={guideVariant} done={2} />
      ) : surface === 'intro' ? (
        // The modal still suspends for a tick on each intro and language: the
        // loads below are done, but `use` meets each promise for the first time.
        <Suspense fallback={null}>
          <PageIntroModal
            key={`${intro.id}-${locale}-${run}`}
            intro={intro}
            onClose={() => setRun((n) => n + 1)}
            onAbandon={() => console.warn(`[preview] no ${locale} copy for the ${intro.id} intro`)}
          />
        </Suspense>
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

// Every scene and the intro copy up front, so nothing here waits on a download.
// Stamped before the first render, as the app's own entry does.
const cardStyleParam = CARD_STYLES.find((s) => s === params.get('cardStyle'));
if (cardStyleParam) setCardStyle(cardStyleParam);
else applyCardStyle(getCardStyle());

void Promise.all([
  initI18n().then(() => i18n.loadNamespaces('intros')),
  loadIntroScenes(INTRO_VISUALS),
]).then(() =>
  createRoot(document.getElementById('root')!).render(
    <StrictMode>
      <Preview />
    </StrictMode>
  ),
);
