import { useState } from 'react';
import { useLocation, useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import {
  ChartCandlestick,
  Copy,
  ExternalLink,
  FolderOpen,
  FolderPlus,
  Layers,
  Link2,
  MessageSquare,
  Moon,
  Settings,
  Sparkles,
  SquarePen,
  Sun,
  Timer,
  Wand2,
} from 'lucide-react';
import { useOpenCreateWorkspace } from '@/components/CreateWorkspaceHost';
import { useChatRoute } from '@/components/Sidebar/useChatRoute';
import { toast } from '@/components/ui/use-toast';
import { useNavItems } from '@/components/nav/useNavItems';
import type { SlashCommand } from '@/components/ui/chat-input.types';
import { useTheme } from '@/contexts/ThemeContext';
import { useAllWorkspaces } from '@/hooks/useAllWorkspaces';
import { useAppearance } from '@/hooks/useAppearance';
import { homeWorkspaceQuery } from '@/hooks/useHomeWorkspace';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { useSkills } from '@/hooks/useSkills';
import { useSymbolSearch } from '@/hooks/useSymbolSearch';
import { useWorkspaceLabel } from '@/hooks/useWorkspaceLabel';
import { copyText } from '@/lib/clipboard';
import { isDesktopShell } from '@/lib/desktop';
import { resolveDualName } from '@/lib/displayName';
import { relativeTime } from '@/lib/format';
import { isHomeWorkspace } from '@/lib/homeWorkspace';
import { readMarkedTicker, readTypedTicker } from '@/lib/marketUtils';
import { queryKeys } from '@/lib/queryKeys';
import { readRecentTickers } from '@/lib/recentTickers';
import { useStorageUser } from '@/lib/userStorage';
import { queryWords } from '@/lib/textMatch';
import { sentMessageWords } from '@/pages/ChatAgent/components/messageList/sentMessage';
import { getRecentThreads } from '@/pages/ChatAgent/utils/api';
import type { SkillInfo } from '@/pages/ChatAgent/utils/api/skills';
import { buildMarketViewUrl } from '@/pages/MarketView/utils/marketRoute';
import { useSaveFailureToast } from '@/pages/Settings/panels/usePreferenceWrite';
import { SECTIONS, searchableSettings, settingSearchText } from '@/pages/Settings/sections';
import type { Thread, Workspace } from '@/types/api';
import { arrangeSearch, findItems, type Hit } from './search';
import type { PaletteAction, PaletteGroup, PaletteItem } from './types';

/** How many recent threads the palette reads, and filters until a server search answers instead. */
const RECENT_THREADS = 50;
const SYMBOL_HITS = 6;

/** How many rows each group shows while searching. */
const CAP = { threads: 5, tickers: 5, workspaces: 4, skills: 4, places: 5, actions: 4 } as const;

/**
 * Everything the palette shows for `query`, in groups, and the Ask row ⌘↵
 * runs from anywhere (none for an empty query, or with nowhere to send it).
 * Local sources (pages, settings, actions, workspaces, skills, the recent
 * threads already read) answer on every keystroke; tickers
 * answer once the symbol search does. `rowsPending` is true while rows may
 * still arrive above the first: the symbol search for a query that reads as a
 * ticker (the chart it most likely means), or the recent threads on a cold
 * open (the thread it names), so Enter can wait for them. `placePending` is
 * true while where new work
 * would go (the open thread's workspace, or Home) is still being looked up.
 *
 * Every action closes the palette before it runs: none leaves it open.
 */
export function usePaletteGroups(query: string, close: () => void) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const location = useLocation();
  const locale = useLocale();
  const now = useNow();
  const label = useWorkspaceLabel();
  const navItems = useNavItems();
  const { theme } = useTheme();
  const { pickTheme } = useAppearance();
  const reportSaveFailure = useSaveFailureToast();
  const { currentWorkspaceId, resolvingWorkspace } = useChatRoute();
  const { data: home, isPending: homePending } = useQuery(homeWorkspaceQuery(useQueryClient()));
  const openCreateWorkspace = useOpenCreateWorkspace();
  // Reread when the signed-in user changes under an open palette (a sign-in
  // in another tab): the storage is theirs, the last list was not.
  const storageUser = useStorageUser();
  const [recent, setRecent] = useState(() => ({ user: storageUser, tickers: readRecentTickers() }));
  if (recent.user !== storageUser) setRecent({ user: storageUser, tickers: readRecentTickers() });
  const recentTickers = recent.tickers;

  const words = queryWords(query);
  const typedTicker = readTypedTicker(query);

  const { data: threads = [], isLoading: threadsLoading } = useQuery({
    queryKey: queryKeys.threads.recent(RECENT_THREADS),
    queryFn: () => getRecentThreads(RECENT_THREADS, 0),
    select: (data) => data.threads,
    staleTime: 30_000,
  });
  const { data: workspacesData } = useAllWorkspaces({ sortBy: 'activity', includeHome: true });
  const workspaces = workspacesData?.workspaces ?? [];
  const workspaceById = new Map(workspaces.map((ws) => [ws.workspace_id, ws]));

  // Where new work goes: the workspace on screen, else Home and its Chief of
  // Staff. A thread opened by its link names its workspace only once looked
  // up; until then the place is pending, so Enter waits rather than send new
  // work to Home, and no row names a place yet.
  const placeId = currentWorkspaceId ?? home?.workspace_id ?? workspaces.find(isHomeWorkspace)?.workspace_id;
  // Outside a workspace on a cold load, Home may not be read yet either.
  const placePending = resolvingWorkspace || (!placeId && homePending);
  // With Home unread for good (its lookup failed), a new thread would open in
  // whichever workspace was in view last, so rows that start one are left
  // out. (A thread whose lookup fails is left for the chat home, where Home
  // is the place.)
  const canStartThread = placePending || !!placeId;
  const place = placeId ? workspaceById.get(placeId) : undefined;
  const placeName = label(place);
  const where = placePending
    ? undefined
    : (place ? isHomeWorkspace(place) : !currentWorkspaceId)
      ? t('agents.chiefOfStaff')
      : t('palette.analystIn', { workspace: placeName });

  // The skills of the place a picked skill's thread runs in, Home's included.
  const { data: skills = [] } = useSkills({ workspaceId: placeId ?? null });
  const { hits, loading: symbolsLoading } = useSymbolSearch(query, SYMBOL_HITS, { enabled: words.length > 0 });

  const act = (name: string, icon: PaletteAction['icon'], run: () => void): PaletteAction => ({
    label: name,
    icon,
    run: () => {
      close();
      run();
    },
  });
  const actInPlace = (name: string, icon: PaletteAction['icon'], run: () => void): PaletteAction => ({
    ...act(name, icon, run),
    startsThread: true,
  });
  const newThread = (draftMessage?: string, draftCommands?: SlashCommand[]) =>
    navigate('/chat/t/__default__', {
      state: { workspaceId: placeId, workspaceName: placeName, workspaceStatus: place?.status ?? null, draftMessage, draftCommands },
    });
  const copy = (text: string) =>
    void copyText(text).then(
      () => toast({ title: t('common.copied') }),
      () => toast({ variant: 'destructive', title: t('palette.copyFailed') }),
    );
  // On the page already, the automation and run open there stay in the
  // address: a discard the page asks about first may be declined.
  const newAutomation = () => {
    const params = new URLSearchParams(location.pathname === '/automations' ? location.search : '');
    params.set('new', 'custom');
    navigate(`/automations?${params}`);
  };

  const threadItem = (thread: Thread): PaletteItem => {
    const path = `/chat/t/${thread.thread_id}`;
    return {
      id: `thread:${thread.thread_id}`,
      icon: MessageSquare,
      title: thread.title || sentMessageWords(thread.first_query_content ?? '') || t('palette.untitledThread'),
      meta: [label(workspaceById.get(thread.workspace_id)), relativeTime(thread.updated_at, locale, now)]
        .filter(Boolean)
        .join(' · '),
      actions: [
        act(t('palette.do.open'), MessageSquare, () => navigate(path)),
        // The desktop shell sends a new window to the system browser, signed out.
        ...(isDesktopShell() ? [] : [act(t('palette.do.openInNewTab'), ExternalLink, () => window.open(path, '_blank', 'noopener'))]),
        act(t('palette.do.copyLink'), Link2, () => copy(`${window.location.origin}${path}`)),
      ],
    };
  };

  /** `name` may be empty: the row then shows the name its quote carries. */
  const tickerItem = (symbol: string, name: string, meta?: string): PaletteItem => ({
    id: `ticker:${symbol}`,
    icon: ChartCandlestick,
    symbol,
    title: name,
    meta,
    actions: [
      act(t('palette.do.openChart'), ChartCandlestick, () => navigate(buildMarketViewUrl({ symbol }))),
      ...(canStartThread ? [actInPlace(t('palette.do.newThreadAbout', { symbol }), Sparkles, () => newThread(`${symbol} `))] : []),
      act(t('palette.do.copySymbol'), Copy, () => copy(symbol)),
    ],
  });

  const workspaceItem = (ws: Workspace): PaletteItem => {
    const name = label(ws);
    const home = isHomeWorkspace(ws);
    const state = { workspaceName: name, workspaceStatus: ws.status ?? null };
    return {
      id: `workspace:${ws.workspace_id}`,
      icon: home ? Layers : FolderOpen,
      title: name,
      keywords: home ? t('agents.home') : undefined,
      meta: home ? t('agents.home') : t('palette.kind.workspace'),
      actions: [
        act(t('palette.do.open'), FolderOpen, () => navigate(`/chat/${ws.workspace_id}`, { state })),
        act(t('palette.do.newThreadIn', { workspace: name }), SquarePen, () =>
          navigate('/chat/t/__default__', { state: { ...state, workspaceId: ws.workspace_id } }),
        ),
      ],
    };
  };

  // Only a skill with a command can be picked in the composer.
  const skillItem = (skill: SkillInfo): PaletteItem => {
    const detail = `/plugins?detail=${encodeURIComponent(`skill:${skill.name}`)}${skill.workspace_id ? `&dws=${skill.workspace_id}` : ''}`;
    const plugins = act(t('palette.do.openInPlugins'), Wand2, () => navigate(detail));
    const command = skill.command;
    return {
      id: `skill:${skill.workspace_id ?? ''}:${skill.name}`,
      icon: Wand2,
      title: skill.name,
      keywords: `${command ?? ''} ${skill.description}`,
      meta: skill.description,
      actions: command && canStartThread
        ? [
            actInPlace(t('palette.do.useSkill'), Sparkles, () =>
              newThread(`/${command} `, [{ type: 'skill', name: command, skillName: skill.name, description: skill.description }]),
            ),
            plugins,
          ]
        : [plugins],
    };
  };

  const pageItems: PaletteItem[] = [
    ...navItems.map((nav) => ({ path: nav.key, title: t(nav.labelKey), icon: nav.icon })),
    { path: '/settings', title: t('sidebar.settings'), icon: Settings },
  ].map(({ path, title, icon }) => ({
    id: `page:${path}`,
    icon,
    title,
    keywords: t('palette.kind.page'),
    meta: t('palette.kind.page'),
    actions: [act(t('palette.do.open'), icon, () => navigate(path))],
  }));

  const settingItems = searchableSettings(isDesktopShell()).map((entry): PaletteItem => {
    const section = SECTIONS[entry.section];
    return {
      id: `setting:${entry.anchor}`,
      icon: section.icon,
      title: t(entry.labelKey),
      keywords: settingSearchText(entry, t),
      meta: `${t('sidebar.settings')} › ${t(section.labelKey)}`,
      actions: [
        act(t('palette.do.open'), section.icon, () =>
          navigate(`/settings?tab=${entry.section}#${entry.anchor}`, { state: { focusSetting: true } }),
        ),
      ],
    };
  });

  const nextTheme = theme === 'dark' ? 'light' : 'dark';
  const runLabel = t('palette.do.run');
  // A turn later: the dialog's field takes focus as it mounts, and in the
  // same commit the palette's focus trap would take it back.
  const openCreateWorkspaceAfterClose = () => window.setTimeout(() => openCreateWorkspace?.(), 0);
  const actionItem = (id: string, title: string, keywords: string, action: PaletteAction, meta?: string): PaletteItem => ({
    id: `action:${id}`,
    icon: action.icon,
    title,
    keywords,
    meta,
    actions: [action],
  });
  const actionItems = [
    ...(canStartThread
      ? [actionItem('new-thread', t('palette.action.newThread'), t('palette.keywords.newThread'), actInPlace(runLabel, SquarePen, () => newThread()), where)]
      : []),
    actionItem('new-workspace', t('palette.action.newWorkspace'), t('palette.keywords.newWorkspace'), act(runLabel, FolderPlus, openCreateWorkspaceAfterClose)),
    actionItem('new-automation', t('palette.action.newAutomation'), t('palette.keywords.newAutomation'), act(runLabel, Timer, newAutomation)),
    actionItem(
      'theme',
      nextTheme === 'light' ? t('palette.action.lightTheme') : t('palette.action.darkTheme'),
      t('palette.keywords.theme'),
      act(runLabel, nextTheme === 'light' ? Sun : Moon, () => void reportSaveFailure(pickTheme(nextTheme))),
    ),
  ];

  // Only a query can be asked: with none, ⌘↵ does nothing.
  const searching = words.length > 0;
  const ask: PaletteItem | null = !searching || !canStartThread ? null : {
    id: 'ask',
    icon: Sparkles,
    accent: true,
    title: t('palette.askTitle', { query: query.trim() }),
    meta: where,
    actions: [actInPlace(t('palette.do.ask'), Sparkles, () => newThread(query.trim()))],
  };

  let groups: PaletteGroup[];
  if (!searching) {
    // Recent places, newest first: threads and the charts opened lately.
    const recent = [
      ...threads.slice(0, 5).map((thread) => ({
        item: threadItem(thread),
        at: thread.updated_at ? new Date(thread.updated_at).getTime() : 0,
      })),
      ...recentTickers.slice(0, 3).map((row) => ({ item: tickerItem(row.symbol, '', t('palette.kind.chart')), at: row.at })),
    ].sort((a, b) => b.at - a.at);
    groups = [
      { id: 'recent', label: t('palette.group.recent'), items: recent.slice(0, 6).map((row) => row.item) },
      { id: 'actions', label: t('palette.group.actions'), items: actionItems.slice(0, 3) },
    ].filter((group) => group.items.length > 0);
  } else {
    // The last query's hits stay while the next one loads; they answer
    // another question, so no row offers them.
    const found: Hit[] = (symbolsLoading ? [] : hits)
      .map((hit, index) => {
        const name = resolveDualName(hit, hit.symbol);
        return {
          // The exact symbol typed is the hit everyone means.
          rank: typedTicker && hit.symbol.toUpperCase() === typedTicker.toUpperCase() ? -1 : index,
          item: tickerItem(hit.symbol, name.named ? name.primary : '', hit.exchangeShortName || hit.stockExchange || undefined),
        };
      })
      .sort((a, b) => a.rank - b.rank);
    // A symbol in a form no word takes is a chart even when the search did
    // not confirm it (it missed the symbol, or failed): Enter opens it rather
    // than asking. A plain word waits on a hit to say it is a ticker.
    const marked = readMarkedTicker(query);
    if (marked && !symbolsLoading && !found.some((hit) => hit.rank < 0)) {
      found.unshift({ rank: -1, item: tickerItem(marked, '') });
    }
    const tickers = found.slice(0, CAP.tickers);
    groups = arrangeSearch(
      {
        threads: findItems(threads.map(threadItem), words, CAP.threads),
        tickers,
        workspaces: findItems(workspaces.map(workspaceItem), words, CAP.workspaces),
        skills: findItems(skills.map(skillItem), words, CAP.skills),
        places: findItems([...pageItems, ...settingItems], words, CAP.places),
        actions: findItems(actionItems, words, CAP.actions),
      },
      ask,
      t,
    );
  }

  // Loading, not pending: a query paused offline would hold Enter for good.
  const rowsPending = searching && ((!!typedTicker && symbolsLoading) || threadsLoading);
  return { groups, ask, rowsPending, placePending };
}
