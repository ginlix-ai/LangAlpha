import { useLayoutEffect } from 'react';
import { useTranslation } from 'react-i18next';
import { SquarePen } from 'lucide-react';
import '@/components/Sidebar/Sidebar.css';
import { NAV_ITEMS } from '@/components/nav/navItems';
import { GETTING_STARTED_TASKS } from '@/pages/Onboarding/registry';
import { GettingStartedSection } from '@/pages/Onboarding/engine/GettingStarted';

/**
 * The sidebar's getting-started checklist, drawn in a stand-in sidebar that
 * uses the real classes: unfolded in the panel under a new user's short
 * workspace list. The first `done` tasks read as done.
 */
export function GuidePreview({ done }: { done: number }) {
  const { t } = useTranslation();
  const tasks = GETTING_STARTED_TASKS.map((def, i) => ({
    def,
    done: i < done,
  }));

  useLayoutEffect(() => {
    const root = document.documentElement;
    root.style.setProperty('--sidebar-width', '248px');
    return () => {
      root.style.removeProperty('--sidebar-width');
    };
  }, []);


  return (
    <>
      <aside className="sidebar sidebar--panel">
        <div className="sidebar-panel-header" />
        <nav className="sidebar-panel-nav">
          <button type="button" className="sidebar-panel-item">
            <SquarePen className="sidebar-panel-item-icon" />
            <span>{t('sidebar.newChat')}</span>
          </button>
          {NAV_ITEMS.filter((item) => item.key !== '/chat' && !item.requires).map((item) => (
            <button key={item.key} type="button" className="sidebar-panel-item">
              <item.icon className="sidebar-panel-item-icon" />
              <span>{t(item.labelKey)}</span>
            </button>
          ))}
        </nav>
        <div className="sidebar-section-header">
          <span className="sidebar-section-title">{t('sidebar.workspaces')}</span>
        </div>
        <div className="sidebar-tree" style={{ padding: '2px 10px' }}>
          {['Home', 'NVDA deep dive'].map((name) => (
            <span key={name} className="sidebar-panel-item">
              <span className="truncate">{name}</span>
            </span>
          ))}
        </div>
        <GettingStartedSection
          tasks={tasks}
          doneCount={done}
          onSelect={() => {}}
          onDismiss={() => {}}
          defaultOpen
        />
        <div className="sidebar-panel-bottom">
          <button type="button" className="sidebar-account-row">
            <span className="sidebar-account-row-avatar" aria-hidden="true">
              <span className="sidebar-account-initials">D</span>
            </span>
            <span className="sidebar-account-row-name">Demo user</span>
            <span className="sidebar-account-row-plan">Free</span>
          </button>
        </div>
      </aside>
      <main
        style={{
          marginLeft: 'var(--sidebar-width)',
          padding: '96px 64px',
          color: 'var(--color-text-secondary)',
          fontSize: '0.875rem',
        }}
      >
        <h1
          style={{
            fontSize: '1.5rem',
            fontWeight: 600,
            color: 'var(--color-text-primary)',
          }}
        >
          {t('sidebar.settings')}
        </h1>
      </main>
    </>
  );
}
