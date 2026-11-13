import { useId, useState, type KeyboardEvent, type ReactNode } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useLocation } from 'react-router';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { Select } from '@/components/ui/select';
import { SegmentedControl } from '@/components/ui/segmented-control';
import { watchlistsQuery } from '@/hooks/membershipQueries';
import { desktop, type DesktopSettings } from '@/lib/desktop';
import {
  MAX_MENUBAR_ITEMS,
  MAX_PANEL_TILES,
  ROTATE_SECONDS,
  WATCHLIST_ROWS,
  choicesWith,
  useDesktopSettings,
} from '@/lib/desktopSettings';
import { SegmentedRow, SettingRow, SettingsGroup, ToggleRow } from '../components/SettingsLayout';
import { useSettingCopy } from '../components/useSettingCopy';
import { SETTINGS, settingAt } from '../sections';
import { acceleratorFromPress, formatAccelerator } from './desktopShortcut';
import { ItemChips, SectionList } from './DesktopLists';

type NotificationKind = keyof DesktopSettings['notifications'];
type Menubar = DesktopSettings['menubar'];
type Panel = DesktopSettings['panel'];

/**
 * Where the menu bar items appear on this platform: beside the icon on a Mac,
 * in the tray icon's tooltip on Windows, at the top of the tray menu on Linux.
 */
function itemsWhereKey(trayTitle: boolean, platform: string): string {
  if (trayTitle) return 'settings.desktop.menubarItemsDesc';
  return platform === 'linux' ? 'settings.desktop.menubarItemsDescMenu' : 'settings.desktop.menubarItemsDescTooltip';
}

/** The rows drawn only while the menu bar icon is on. */
const UNDER_ICON: ReadonlySet<string> = new Set(
  [
    SETTINGS.menubarItems,
    SETTINGS.menubarShow,
    SETTINGS.menubarExtendedHours,
    SETTINGS.panelSections,
    SETTINGS.panelTiles,
    SETTINGS.panelWatchlist,
  ].map((setting) => setting.anchor),
);

/**
 * Desktop app: what the shell keeps for this computer (notifications, the
 * menu bar and its panel, how the app starts). Read from the shell and
 * written back to it; the account holds none of it, so each computer is set
 * on its own. The shell also changes it from elsewhere (a switch in the
 * icon's menu), so the section draws whatever view the shell sent last. Rows a platform cannot do are left out rather than drawn off.
 */
export function DesktopPanel() {
  const { t } = useTranslation();
  const copy = useSettingCopy();
  const { view: settings, failed, save } = useDesktopSettings();
  const watchlists = useQuery({ ...watchlistsQuery(), enabled: settings?.menubar.enabled ?? false });
  const { hash } = useLocation();

  if (!settings) {
    return failed ? (
      <SettingsGroup>
        <SettingRow label={t('settings.desktop.unavailable')} />
      </SettingsGroup>
    ) : null;
  }

  const saveMenubar = (patch: Partial<Menubar>) => save({ menubar: patch });
  const savePanel = (patch: Partial<Panel>) => save({ panel: patch });

  const toggleNotification = (kind: NotificationKind) =>
    save({ notifications: { [kind]: !settings.notifications[kind] } });

  const notificationRows: { kind: NotificationKind; setting: (typeof SETTINGS)[keyof typeof SETTINGS] }[] = [
    { kind: 'turn', setting: SETTINGS.notifyTurn },
    { kind: 'needs_input', setting: SETTINGS.notifyNeedsInput },
    { kind: 'wakeup', setting: SETTINGS.notifyWakeup },
    { kind: 'automation', setting: SETTINGS.notifyAutomation },
    { kind: 'started', setting: SETTINGS.notifyStarted },
  ];

  const { support, menubar, panel } = settings;
  const menubarOn = menubar.enabled;
  const platform = desktop?.platform ?? '';
  // A search for a row the icon hides opens on the icon's switch, which
  // brings it back, rather than on nothing.
  const sought = settingAt(hash)?.anchor;
  const iconAnchor = !menubarOn && sought && UNDER_ICON.has(sought) ? sought : SETTINGS.menubarIcon.anchor;

  return (
    <>
      <SettingsGroup title={t('settings.desktop.notificationsGroup')} description={t('settings.desktop.notificationsGroupDesc')}>
        {notificationRows.map(({ kind, setting }) => (
          <ToggleRow
            key={kind}
            {...copy(setting)}
            checked={settings.notifications[kind]}
            onChange={() => toggleNotification(kind)}
          />
        ))}
      </SettingsGroup>

      <SettingsGroup title={t('settings.desktop.menubarGroup')}>
        <ToggleRow
          {...copy(SETTINGS.menubarIcon)}
          id={iconAnchor}
          checked={menubarOn}
          onChange={() => saveMenubar({ enabled: !menubarOn })}
        />
        {menubarOn && (
          <ListRow
            {...copy(SETTINGS.menubarItems)}
            description={t(itemsWhereKey(support.trayTitle, platform))}
          >
            {(labelledBy) => (
              <ItemChips
                items={menubar.items}
                onChange={(items) => saveMenubar({ items })}
                max={MAX_MENUBAR_ITEMS}
                allowPortfolio
                addLabel={t('settings.desktop.addSymbol')}
                labelledBy={labelledBy}
              />
            )}
          </ListRow>
        )}
        {menubarOn && support.trayTitle && (
          <StyleRow
            style={menubar.style}
            rotateSeconds={menubar.rotateSeconds}
            onStyle={(style) => { if (style !== menubar.style) saveMenubar({ style }); }}
            onRotateSeconds={(rotateSeconds) => saveMenubar({ rotateSeconds })}
          />
        )}
        {menubarOn && (
          <SegmentedRow<Menubar['show']>
            {...copy(SETTINGS.menubarShow)}
            value={menubar.show}
            onChange={(show) => { if (show !== menubar.show) saveMenubar({ show }); }}
            options={[
              { value: 'change', label: t('settings.desktop.menubarShowChange') },
              { value: 'price', label: t('settings.desktop.menubarShowPrice') },
              { value: 'both', label: t('settings.desktop.menubarShowBoth') },
            ]}
          />
        )}
        {menubarOn && (
          <ToggleRow
            {...copy(SETTINGS.menubarExtendedHours)}
            checked={menubar.extendedHours}
            onChange={() => saveMenubar({ extendedHours: !menubar.extendedHours })}
          />
        )}
        {menubarOn && support.shortcut && (
          <ShortcutRow
            shortcut={settings.shortcut}
            error={settings.shortcutError}
            onSave={(shortcut) => save({ shortcut })}
          />
        )}
      </SettingsGroup>

      {menubarOn && (
        <SettingsGroup title={t('settings.desktop.panelGroup')} description={t('settings.desktop.panelGroupDesc')}>
          <ListRow {...copy(SETTINGS.panelSections)}>
            {(labelledBy) => (
              <SectionList
                sections={panel.sections}
                onChange={(sections) => savePanel({ sections })}
                labelledBy={labelledBy}
              />
            )}
          </ListRow>
          <ListRow {...copy(SETTINGS.panelTiles)}>
            {(labelledBy) => (
              <ItemChips
                items={panel.tiles}
                onChange={(tiles) => savePanel({ tiles })}
                max={MAX_PANEL_TILES}
                addLabel={t('settings.desktop.addTile')}
                labelledBy={labelledBy}
              />
            )}
          </ListRow>
          <WatchlistRow
            watchlistId={panel.watchlistId}
            rows={panel.watchlistRows}
            lists={watchlists.data ?? []}
            onList={(watchlistId) => savePanel({ watchlistId })}
            onRows={(watchlistRows) => savePanel({ watchlistRows })}
          />
        </SettingsGroup>
      )}

      {(support.openAtLogin || support.keepRunningInTray) && (
        <SettingsGroup title={t('settings.desktop.startupGroup')}>
          {support.openAtLogin && (
            <ToggleRow
              label={t('settings.desktop.openAtLogin')}
              // With no Dock and no icon, a login launch opens the window
              // (desktop/AGENTS.md): a background start would be unfindable.
              description={t(platform !== 'darwin' && !menubarOn ? 'settings.desktop.openAtLoginWindowDesc' : 'settings.desktop.openAtLoginDesc')}
              checked={settings.openAtLogin}
              onChange={() => save({ openAtLogin: !settings.openAtLogin })}
            />
          )}
          {support.keepRunningInTray && (
            <ToggleRow
              label={t('settings.desktop.keepRunning')}
              description={t('settings.desktop.keepRunningDesc')}
              checked={settings.keepRunningInTray}
              onChange={() => save({ keepRunningInTray: !settings.keepRunningInTray })}
            />
          )}
        </SettingsGroup>
      )}
    </>
  );
}

/** A row whose control is a list, named by the row's label. */
function ListRow({ id, label, description, children }: {
  id?: string;
  label: string;
  description?: string;
  children: (labelledBy: string) => ReactNode;
}) {
  const labelId = useId();
  return (
    <SettingRow id={id} label={label} description={description} labelId={labelId}>
      {children(labelId)}
    </SettingRow>
  );
}

/** With more than one item beside the icon: one slot cycling (and how often), or side by side. */
function StyleRow({ style, rotateSeconds, onStyle, onRotateSeconds }: {
  style: Menubar['style'];
  rotateSeconds: number;
  onStyle: (style: Menubar['style']) => void;
  onRotateSeconds: (seconds: number) => void;
}) {
  const { t } = useTranslation();
  const labelId = useId();
  return (
    <SettingRow
      label={t('settings.desktop.menubarStyle')}
      description={t('settings.desktop.menubarStyleDesc')}
      labelId={labelId}
    >
      <div className="flex flex-wrap items-center justify-end gap-2">
        <SegmentedControl<Menubar['style']>
          labelledBy={labelId}
          value={style}
          onChange={onStyle}
          options={[
            { value: 'rotate', label: t('settings.desktop.menubarStyleRotate') },
            { value: 'side', label: t('settings.desktop.menubarStyleSide') },
          ]}
        />
        {style === 'rotate' && (
          <Select
            className="w-28"
            aria-label={t('settings.desktop.rotateEvery')}
            value={String(rotateSeconds)}
            onChange={(e) => onRotateSeconds(Number(e.target.value))}
          >
            {choicesWith(ROTATE_SECONDS, rotateSeconds).map((s) => (
              <option key={s} value={s}>{t('settings.desktop.rotateSeconds', { count: s })}</option>
            ))}
          </Select>
        )}
      </div>
    </SettingRow>
  );
}

/**
 * Which watchlist the panel shows, and how many of its rows. Null follows the
 * user's default list, so that list is offered once, as the default, rather
 * than a second time by its id.
 */
function WatchlistRow({ watchlistId, rows, lists, onList, onRows }: {
  watchlistId: string | null;
  rows: number;
  lists: ReadonlyArray<{ id: string; name: string; isDefault: boolean }>;
  onList: (id: string | null) => void;
  onRows: (rows: number) => void;
}) {
  const { t } = useTranslation();
  const copy = useSettingCopy();
  const fallback = lists.find((list) => list.isDefault) ?? lists[0];
  const others = lists.filter((list) => list !== fallback);
  const value = watchlistId && others.some((list) => list.id === watchlistId) ? watchlistId : '';
  return (
    <SettingRow {...copy(SETTINGS.panelWatchlist)}>
      <div className="flex flex-wrap items-center justify-end gap-2">
        <Select
          className="w-44"
          aria-label={t('settings.desktop.watchlistWhich')}
          value={value}
          onChange={(e) => onList(e.target.value || null)}
        >
          <option value="">
            {/* A list that is itself called "Default" reads as "Default (default)"
                when tagged, so it is shown by its name alone. */}
            {fallback?.name &&
            fallback.name.trim().toLocaleLowerCase() !==
              t('settings.desktop.watchlistDefault').toLocaleLowerCase()
              ? t('settings.desktop.watchlistDefaultNamed', { name: fallback.name })
              : t('settings.desktop.watchlistDefault')}
          </option>
          {others.map((list) => (
            <option key={list.id} value={list.id}>{list.name}</option>
          ))}
        </Select>
        <Select
          className="w-28"
          aria-label={t('settings.desktop.watchlistRowsLabel')}
          value={String(rows)}
          onChange={(e) => onRows(Number(e.target.value))}
        >
          {choicesWith(WATCHLIST_ROWS, rows).map((n) => (
            <option key={n} value={n}>{t('settings.desktop.watchlistRows', { count: n })}</option>
          ))}
        </Select>
      </div>
    </SettingRow>
  );
}

/**
 * The shortcut that opens the menu bar panel from anywhere. Recorded from the
 * next key press rather than typed, since an accelerator's spelling is the
 * shell's, not the user's.
 */
function ShortcutRow({
  shortcut,
  error,
  onSave,
}: {
  shortcut: string;
  error?: string;
  onSave: (shortcut: string) => void;
}) {
  const { t } = useTranslation();
  const [recording, setRecording] = useState(false);
  const platform = desktop?.platform ?? '';

  const onKeyDown = (e: KeyboardEvent<HTMLButtonElement>) => {
    if (!recording) return;
    e.preventDefault();
    e.stopPropagation();
    if (e.key === 'Escape') {
      setRecording(false);
      return;
    }
    const accelerator = acceleratorFromPress(e, platform);
    if (!accelerator) return;
    setRecording(false);
    if (accelerator !== shortcut) onSave(accelerator);
  };

  const label = recording
    ? t('settings.desktop.shortcutRecording')
    : shortcut
      ? formatAccelerator(shortcut, platform)
      : t('settings.desktop.shortcutNone');

  return (
    <SettingRow
      label={t('settings.desktop.shortcut')}
      description={
        error ? (
          <span role="alert" style={{ color: 'var(--color-icon-danger)' }}>
            {t('settings.desktop.shortcutError', { error })}
          </span>
        ) : (
          t('settings.desktop.shortcutDesc')
        )
      }
    >
      <div className="flex flex-wrap gap-2">
        <Button
          type="button"
          variant="outline"
          size="sm"
          className="h-8 min-w-24 font-mono"
          aria-pressed={recording}
          onClick={() => setRecording((r) => !r)}
          onKeyDown={onKeyDown}
          onBlur={() => setRecording(false)}
        >
          {label}
        </Button>
        {shortcut && !recording && (
          <Button
            type="button"
            variant="ghost"
            size="sm"
            className="h-8"
            style={{ color: 'var(--color-text-secondary)' }}
            onClick={() => onSave('')}
          >
            {t('settings.desktop.shortcutClear')}
          </Button>
        )}
      </div>
    </SettingRow>
  );
}
