import { useTranslation } from 'react-i18next';
import { SlidersHorizontal } from 'lucide-react';
import { Popover, PopoverTrigger, PopoverContent } from '@/components/ui/popover';
import { useNavPrefs, setNavPrefs, type NavOrderBy } from '../utils/navPrefs';

const WORKSPACE_CHOICES: readonly (number | 'all')[] = [5, 10, 'all'];
const THREAD_CHOICES: readonly number[] = [5, 10, 20];
const ORDER_CHOICES: readonly NavOrderBy[] = ['activity', 'name', 'custom'];

interface ChoiceGroupProps<T extends string | number> {
  label: string;
  choices: readonly T[];
  value: T;
  format: (choice: T) => string;
  onSelect: (choice: T) => void;
}

function ChoiceGroup<T extends string | number>({ label, choices, value, format, onSelect }: ChoiceGroupProps<T>) {
  return (
    <div>
      <div className="text-xs font-medium mb-1.5" style={{ color: 'var(--color-text-secondary)' }}>
        {label}
      </div>
      <div
        className="flex rounded-lg overflow-hidden"
        style={{ border: '1px solid var(--color-border-muted)' }}
      >
        {choices.map((choice) => (
          <button
            type="button"
            key={String(choice)}
            onClick={() => onSelect(choice)}
            aria-pressed={value === choice}
            className="flex-1 flex items-center justify-center px-2 py-1.5 text-xs font-medium transition-colors"
            style={{
              backgroundColor: value === choice ? 'var(--color-accent-soft)' : 'transparent',
              color: value === choice ? 'var(--color-accent-primary)' : 'var(--color-text-tertiary)',
            }}
          >
            {format(choice)}
          </button>
        ))}
      </div>
    </div>
  );
}

/**
 * Display options for the navigation panel — how many workspaces stay visible
 * and the thread page size per workspace. Persisted via navPrefs.
 */
export default function NavDisplayOptions() {
  const { t } = useTranslation();
  const { workspaceLimit, threadPageSize, orderBy } = useNavPrefs();

  return (
    <Popover>
      <PopoverTrigger asChild>
        <button
          className="nav-panel-dismiss-btn"
          style={{
            padding: 4,
            background: 'transparent',
            border: 'none',
            cursor: 'pointer',
            borderRadius: 4,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
          }}
          title={t('nav.displayOptions')}
          aria-label={t('nav.displayOptions')}
        >
          <SlidersHorizontal className="h-4 w-4" style={{ color: 'var(--color-text-tertiary)' }} />
        </button>
      </PopoverTrigger>
      <PopoverContent align="start" sideOffset={6} className="w-56 p-3 space-y-3">
        <ChoiceGroup
          label={t('nav.orderBy')}
          choices={ORDER_CHOICES}
          value={orderBy}
          format={(c) => (c === 'activity' ? t('workspace.activity') : c === 'name' ? t('common.name') : t('workspace.custom'))}
          onSelect={(c) => setNavPrefs({ orderBy: c })}
        />
        <ChoiceGroup
          label={t('nav.workspacesShown')}
          choices={WORKSPACE_CHOICES}
          value={workspaceLimit}
          format={(c) => (c === 'all' ? t('nav.all') : String(c))}
          onSelect={(c) => setNavPrefs({ workspaceLimit: c })}
        />
        <ChoiceGroup
          label={t('nav.threadsPerWorkspace')}
          choices={THREAD_CHOICES}
          value={threadPageSize}
          format={(c) => String(c)}
          onSelect={(c) => setNavPrefs({ threadPageSize: c })}
        />
      </PopoverContent>
    </Popover>
  );
}
