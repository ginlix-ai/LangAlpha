import { useState } from 'react';
import { useLocation, useNavigate } from 'react-router';
import { FolderOpen, FolderPlus, Layers, Moon, Settings, Sparkles, SquarePen, Sun, Timer, Wand2 } from 'lucide-react';
import { useOpenCreateWorkspace } from '@/components/CreateWorkspaceHost';
import { useNavItems } from '@/components/nav/useNavItems';
import { useTheme } from '@/contexts/ThemeContext';
import { useAllWorkspaces } from '@/hooks/useAllWorkspaces';
import { useAppearance } from '@/hooks/useAppearance';
import { useSkills } from '@/hooks/useSkills';
import { useWorkspaceLabel } from '@/hooks/useWorkspaceLabel';
import { isDesktopShell } from '@/lib/desktop';
import { isHomeWorkspace } from '@/lib/homeWorkspace';
import { readRecentTickers } from '@/lib/recentTickers';
import { useStorageUser } from '@/lib/userStorage';
import type { SkillInfo } from '@/pages/ChatAgent/utils/api/skills';
import { useSaveFailureToast } from '@/pages/Settings/panels/usePreferenceWrite';
import { SECTIONS, searchableSettings, settingSearchText } from '@/pages/Settings/sections';
import type { Workspace } from '@/types/api';
import { runs, type Verbs } from './rows';
import type { PaletteItem } from './types';

/**
 * What the palette finds in the browser, as rows: the user's workspaces, the
 * skills of where new work goes, the app's pages, settings and actions, and
 * the tickers opened lately. Every keystroke matches these at once.
 */
export function useLocalSources(v: Verbs) {
  const { t, newWork } = v;
  const navigate = useNavigate();
  const location = useLocation();
  const label = useWorkspaceLabel();
  const navItems = useNavItems();
  const { theme } = useTheme();
  const { pickTheme } = useAppearance();
  const reportSaveFailure = useSaveFailureToast();
  const openCreateWorkspace = useOpenCreateWorkspace();
  const { data: workspacesData } = useAllWorkspaces({ sortBy: 'activity', includeHome: true });
  // The skills of the place a picked skill's thread runs in, Home's included.
  const { data: skills = [] } = useSkills({ workspaceId: newWork.workspaceId ?? null });
  // Reread when the signed-in user changes under an open palette (a sign-in
  // in another tab): the storage is theirs, the last list was not.
  const storageUser = useStorageUser();
  const [recent, setRecent] = useState(() => ({ user: storageUser, tickers: readRecentTickers() }));
  if (recent.user !== storageUser) setRecent({ user: storageUser, tickers: readRecentTickers() });

  const workspaceRow = (ws: Workspace): PaletteItem => {
    const name = label(ws);
    const isHome = isHomeWorkspace(ws);
    return {
      id: `workspace:${ws.workspace_id}`,
      icon: isHome ? Layers : FolderOpen,
      title: name,
      keywords: isHome ? t('agents.home') : undefined,
      meta: isHome ? t('agents.home') : t('palette.kind.workspace'),
      place: { kind: 'workspace', workspaceId: ws.workspace_id, name },
      actions: [
        runs('open', t('palette.do.open'), FolderOpen, () =>
          navigate(`/chat/${ws.workspace_id}`, { state: { workspaceName: name, workspaceStatus: ws.status ?? null } }),
        ),
        runs('new-thread', t('palette.do.newThreadIn', { workspace: name }), SquarePen, () => v.newThread(ws.workspace_id)),
      ],
    };
  };

  // Only a skill with a command can be picked in the composer.
  const skillRow = (skill: SkillInfo): PaletteItem => {
    const detail = `/plugins?detail=${encodeURIComponent(`skill:${skill.name}`)}${skill.workspace_id ? `&dws=${skill.workspace_id}` : ''}`;
    const plugins = runs('plugins', t('palette.do.openInPlugins'), Wand2, () => navigate(detail));
    const command = skill.command;
    return {
      id: `skill:${skill.workspace_id ?? ''}:${skill.name}`,
      icon: Wand2,
      title: skill.name,
      keywords: `${command ?? ''} ${skill.description}`,
      meta: skill.description,
      actions:
        command && newWork.canStartThread
          ? [
              runs(
                'use',
                t('palette.do.useSkill'),
                Sparkles,
                () =>
                  v.newThread(undefined, {
                    message: `/${command} `,
                    commands: [{ type: 'skill', name: command, skillName: skill.name, description: skill.description }],
                  }),
                true,
              ),
              plugins,
            ]
          : [plugins],
    };
  };

  const pages: PaletteItem[] = [
    ...navItems.map((nav) => ({ path: nav.key, title: t(nav.labelKey), icon: nav.icon })),
    { path: '/settings', title: t('sidebar.settings'), icon: Settings },
  ].map(({ path, title, icon }) => ({
    id: `page:${path}`,
    icon,
    title,
    keywords: t('palette.kind.page'),
    meta: t('palette.kind.page'),
    actions: [runs('open', t('palette.do.open'), icon, () => navigate(path))],
  }));

  const settings = searchableSettings(isDesktopShell()).map((entry): PaletteItem => {
    const section = SECTIONS[entry.section];
    return {
      id: `setting:${entry.anchor}`,
      icon: section.icon,
      title: t(entry.labelKey),
      keywords: settingSearchText(entry, t),
      meta: `${t('sidebar.settings')} › ${t(section.labelKey)}`,
      actions: [
        runs('open', t('palette.do.open'), section.icon, () =>
          navigate(`/settings?tab=${entry.section}#${entry.anchor}`, { state: { focusSetting: true } }),
        ),
      ],
    };
  });

  const nextTheme = theme === 'dark' ? 'light' : 'dark';
  /** One of the app's actions: `name` is its keywords' key, and its id. */
  const action = (name: string, title: string, icon: PaletteItem['icon'], run: () => void, meta?: string, startsThread = false): PaletteItem => ({
    id: `action:${name}`,
    icon,
    title,
    keywords: t(`palette.keywords.${name}`),
    meta,
    actions: [runs('run', t('palette.do.run'), icon, run, startsThread)],
  });
  const actions = [
    ...(newWork.canStartThread ? [action('newThread', t('palette.action.newThread'), SquarePen, () => v.newThread(undefined), newWork.where, true)] : []),
    // A turn later: the dialog's field takes focus as it mounts, and in the
    // same commit the palette's focus trap would take it back.
    action('newWorkspace', t('palette.action.newWorkspace'), FolderPlus, () => window.setTimeout(() => openCreateWorkspace?.(), 0)),
    // On the page already, the automation and run open there stay in the
    // address: a discard the page asks about first may be declined.
    action('newAutomation', t('palette.action.newAutomation'), Timer, () => {
      const params = new URLSearchParams(location.pathname === '/automations' ? location.search : '');
      params.set('new', 'custom');
      navigate(`/automations?${params}`);
    }),
    action(
      'theme',
      nextTheme === 'light' ? t('palette.action.lightTheme') : t('palette.action.darkTheme'),
      nextTheme === 'light' ? Sun : Moon,
      () => void reportSaveFailure(pickTheme(nextTheme)),
    ),
  ];

  return {
    workspaces: (workspacesData?.workspaces ?? []).map(workspaceRow),
    skills: skills.map(skillRow),
    pages,
    settings,
    actions,
    recentTickers: recent.tickers,
  };
}

export type LocalSources = ReturnType<typeof useLocalSources>;
