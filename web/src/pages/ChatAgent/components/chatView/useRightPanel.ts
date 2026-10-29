import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { Dispatch, SetStateAction } from 'react';
import { useLocation, useNavigate } from 'react-router';
import { appendPathSuffix, getPreviewUrl } from '../../utils/api';
import { getShareLink, parseShareLinkHref } from '../../utils/api/shareLinks';
import { desktop } from '@/lib/desktop';
import { opensShareLinksInPanel } from '@/lib/shareLinkOpen';
import type { ComputerFolders } from '../../utils/agentPaths';
import { collectRecentWritePaths, collectWriteLog } from '../../utils/fileRefResolver';
import { useStableHandler } from '@/hooks/useStableHandler';
import { useLatestRef } from '@/hooks/useLatestRef';
import { isValidUuid } from '../../utils/uuid';
import { clampPanelWidth as clampPanelWidthUtil } from '@/lib/panelUtils';
import { buildMarketViewUrl } from '@/pages/MarketView/utils/marketRoute';
import { CHART_SURFACE_MIN_WIDTH } from '@/pages/MarketView/components/chartSurfaceLayout';
import type { FileTab } from '../filePanel/useFileTabs';
import type { ChartTabSpec, UnsequencedTarget } from '../filePanel/types';
import type { PreviewData } from '../../hooks/utils/types';
import { NO_TRANSCRIPTS, useToolCallLookup, type TranscriptMessage } from './toolCallLookup';
import { useTranscriptReader } from '../filePanel/transcriptStore';
import { DEFAULT_PANEL_WIDTH, detailPanelWidth } from '../filePanel/detailWidth';
import { usePanelAsks, usePanelLinkOpen } from './usePanelLinks';

export { nextPanelOverride } from './usePanelLinks';

// A running app or a live chart opens wide, so its toolbar has room.
const PREVIEW_MAX_RATIO = 0.92;

// Held for a shortcut or a modified click; no intent of their own.
const MODIFIER_KEYS = new Set(['Shift', 'Control', 'Alt', 'Meta']);

/** Right-panel controller (carved out of ChatView, 5.9c): panel type/width,
 * target routing, tool-call detail, multi-port preview resolution,
 * divider drag, sources provenance, and mobile back-gesture integration. */
export function useRightPanel({
  isMobile,
  workspaceId,
  folders = null,
  threadId,
  isActive,
  containerRef,
  setFilePanelWorkspaceId,
  filePanelWorkspaceId = null,
  messages,
  subagentTranscripts = NO_TRANSCRIPTS,
  watching = false,
}: {
  isMobile: boolean;
  workspaceId: string;
  /** The workspace's folder, those a rename moved it out of, which older
   *  turns' paths still name, and the other workspaces on its computer. */
  folders?: ComputerFolders | null;
  /** The conversation a MarketView page opened from here resumes. */
  threadId?: string | null;
  isActive: boolean;
  containerRef: React.RefObject<HTMLDivElement | null>;
  setFilePanelWorkspaceId: Dispatch<SetStateAction<string | null>>;
  /** The cross-workspace override the panel shows (usePanelFiles). */
  filePanelWorkspaceId?: string | null;
  messages: readonly TranscriptMessage[];
  /** Each subagent's own messages, so a tool row clicked in its transcript resolves too. */
  subagentTranscripts?: readonly (readonly TranscriptMessage[])[];
  /** A market watch is running, so a watch call's row opens its live Status tab. */
  watching?: boolean;
}) {
  const location = useLocation();
  const navigate = useNavigate();

  // Guards one-shot consumption of the ?file= deep link (report share / copy link).
  const fileDeepLinkConsumedRef = useRef(false);

  const {
    panelTarget,
    askPanel,
    askCount,
    handleTargetHandled,
    handleTargetMemoryHandled,
    handleTargetMemoHandled,
    handleFilesLeaveGuardChange,
    leaveFiles,
  } = usePanelAsks();

  const isDraggingRef = useRef(false);
  const [isDragging, setIsDragging] = useState(false);
  // True for exactly one render after drag ends — forces transition duration:0
  // so Framer Motion jumps to the final width instead of animating from pre-drag.
  // State, not a ref read in render: it is a render input, and a render has to
  // follow it back to false so the next close or resize animates again.
  const [dragJustEnded, setDragJustEnded] = useState(false);
  // Armed for the duration of a divider drag; unmount mid-drag would otherwise
  // strand document listeners, app-wide col-resize/no-select body styles, and
  // pointer-events:none on every iframe.
  const dragCleanupRef = useRef<(() => void) | null>(null);
  useEffect(() => () => dragCleanupRef.current?.(), []);

  // Right panel management - can show 'file', 'detail', 'preview', or null
  // (closed). 'detail' and 'preview' are the mobile sheets; the desktop column
  // shows a tool result or a running app as a tab of the file view.
  const [rightPanelType, setRightPanelType] = useState<'file' | 'detail' | 'preview' | null>(null);

  // Expanded, the file panel lies over the whole view, for what wants the
  // width: a spreadsheet, a deck, a long report. Closing the panel ends it, so
  // a file opened from the chat later opens beside the chat. Mobile shows the
  // panel over the chat already and offers no toggle.
  const [expanded, setExpanded] = useState(false);
  if (expanded && rightPanelType !== 'file') setExpanded(false);
  const panelExpanded = expanded && !isMobile;
  const handleTogglePanelExpanded = useCallback(() => setExpanded((v) => !v), []);

  const [rightPanelWidth, setRightPanelWidth] = useState(750);
  // What the file panel has in front, reported by the panel as it changes. A
  // chart is the one tab with a width of its own: below the surface's floor
  // its toolbar collides, so while a chart is in front the divider stops
  // there and the panel grows to it on the way in. The container's own cap
  // still wins on a screen too narrow for both.
  const [activeTabKind, setActiveTabKind] = useState<FileTab['kind'] | null>(null);
  const handleActiveTabKindChange = useCallback((kind: FileTab['kind'] | null) => setActiveTabKind(kind), []);
  const panelMinWidth = activeTabKind === 'chart' ? CHART_SURFACE_MIN_WIDTH : 0;
  // Mobile-sheet-only preview state. On desktop a running app is a tab in the
  // file panel, which mints and refreshes its own URL; the bottom sheet has no
  // tab strip, shows one app at a time, and keeps this Map keyed by port in a
  // ref (non-active updates don't re-render) with the derived previewData
  // driving the sheet. resolvePreviewUrl/resolveAndSetPreview,
  // handleClosePreview and handleRefreshPreview below all serve this sheet.
  const previewMapRef = useRef<Map<number, PreviewData>>(new Map());
  const activePreviewPortRef = useRef<number | null>(null);
  const reloadCounterRef = useRef(0);
  // The latest `/a/` link click, advanced again by any input after it. A
  // lookup does not ask the panel, so a newer input is told apart here, and an
  // open nobody clicked (the stream's) by `askCount`, or on the mobile sheet by
  // advancing this too.
  const shareLinkClickRef = useRef(0);
  const [previewData, setPreviewData] = useState<PreviewData | null>(null);
  const panelWrapperRef = useRef<HTMLDivElement>(null);

  // Clear preview cache and cross-workspace state when workspace changes to avoid leaking old workspace data.
  useEffect(() => {
    previewMapRef.current.clear();
    activePreviewPortRef.current = null;
    setPreviewData(null);
    setFilePanelWorkspaceId(null);
  }, [workspaceId, setFilePanelWorkspaceId]);

  // What the mobile detail sheet shows; on desktop these land as tabs instead.
  // A tool call is held by id and read live below, like a tab holds it.
  const [detailToolCallId, setDetailToolCallId] = useState<string | null>(null);

  // The cap the open content asked for. A running app opens past the default
  // cap on purpose, and the divider has to hold that width instead of snapping
  // it back on the first pixel, so the ask is recorded rather than inferred
  // from the width.
  const panelMaxRatioRef = useRef<number | undefined>(undefined);
  const applyPanelWidth = useCallback((desired: number, maxRatio?: number) => {
    panelMaxRatioRef.current = maxRatio;
    setRightPanelWidth(clampPanelWidthUtil(desired, containerRef.current?.offsetWidth || window.innerWidth, maxRatio));
  }, [containerRef]);

  // Read by the landing below, which is called from handlers whose identity
  // must not follow the panel type.
  const rightPanelTypeRef = useLatestRef(rightPanelType);

  /**
   * Size the file panel for what is landing in it. A closed panel opens at
   * the landing's own width and cap. An open one only widens: a reader who
   * dragged it wide keeps that, and a tool result landing beside a chart does
   * not fold the panel back to the tool's width. The cap holds for the same
   * reason, since the wider one is still in the strip; every cap a landing
   * asks for is wider than the default, so the higher of the two is the one
   * to keep and "none asked" reads as the default.
   */
  const growPanelWidth = useCallback((desired: number, maxRatio?: number) => {
    const open = rightPanelTypeRef.current === 'file';
    const ratio = open ? (Math.max(panelMaxRatioRef.current ?? 0, maxRatio ?? 0) || undefined) : maxRatio;
    panelMaxRatioRef.current = ratio;
    const containerW = containerRef.current?.offsetWidth || window.innerWidth;
    setRightPanelWidth((prev) => clampPanelWidthUtil(open ? Math.max(prev, desired) : desired, containerW, ratio));
  }, [containerRef, rightPanelTypeRef]);

  // A chart coming to the front, by landing, by a click on its tab or by a
  // strip restored with it in front, widens the panel to its floor and no
  // further. Mobile shows the panel full width and has no divider.
  // The cap rises with it, as a chart landing raises it, so the floor holds
  // however the chart came to the front.
  useEffect(() => {
    if (isMobile || !panelMinWidth) return;
    panelMaxRatioRef.current = Math.max(panelMaxRatioRef.current ?? 0, PREVIEW_MAX_RATIO);
    const containerW = containerRef.current?.offsetWidth || window.innerWidth;
    setRightPanelWidth((prev) => clampPanelWidthUtil(Math.max(prev, panelMinWidth), containerW, panelMaxRatioRef.current));
  }, [isMobile, panelMinWidth, containerRef]);

  // Handle drag panel width: direct DOM manipulation for smooth, jank-free resize.
  // React state is only updated once on mouseup; during drag we bypass React/Framer.
  const handleDividerMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    isDraggingRef.current = true;
    setIsDragging(true);
    const startX = e.clientX;
    const startWidth = rightPanelWidth;
    const containerW = containerRef.current?.offsetWidth || window.innerWidth;
    const maxRatio = panelMaxRatioRef.current;
    const minWidth = panelMinWidth;

    // Immediately disable pointer events on iframes to prevent them from
    // capturing mouse events during resize (can't wait for React re-render).
    const iframes = containerRef.current?.querySelectorAll('iframe');
    iframes?.forEach(iframe => { (iframe as HTMLIFrameElement).style.pointerEvents = 'none'; });

    // Grab DOM elements for direct manipulation (no React re-renders during drag)
    const wrapperEl = panelWrapperRef.current;
    const innerEl = wrapperEl?.querySelector<HTMLElement>('[data-panel-inner]');
    let currentWidth = startWidth;

    const onMouseMove = (moveEvent: MouseEvent) => {
      if (!isDraggingRef.current) return;
      const delta = startX - moveEvent.clientX;
      // The floor goes in before the clamp so the container's cap still wins.
      currentWidth = clampPanelWidthUtil(Math.max(startWidth + delta, minWidth), containerW, maxRatio);
      if (wrapperEl) wrapperEl.style.width = `${currentWidth}px`;
      if (innerEl) innerEl.style.width = `${currentWidth}px`;
    };

    // Hoisted declarations: teardown and onMouseUp reference each other.
    function teardown() {
      dragCleanupRef.current = null;
      isDraggingRef.current = false;
      iframes?.forEach(iframe => { (iframe as HTMLIFrameElement).style.pointerEvents = ''; });
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('mouseup', onMouseUp);
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
    }

    function onMouseUp() {
      // Flag ensures the next render uses duration:0 so Framer doesn't
      // animate from the stale pre-drag width to the final width.
      setDragJustEnded(true);
      // Mouseup renders synchronously, so the frame after it has the final
      // width committed and later changes animate again.
      requestAnimationFrame(() => setDragJustEnded(false));
      setIsDragging(false);
      setRightPanelWidth(currentWidth);
      teardown();
    }

    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
    dragCleanupRef.current = teardown;
  }, [rightPanelWidth, containerRef, panelMinWidth]);

  // Push a sentinel history entry when a panel opens so that the browser back
  // gesture closes the panel instead of navigating away from ChatView.
  //
  // Key: we use raw pushState (not React Router's navigate) and CLONE the
  // current history.state so React Router's idx/key tracking stays intact.
  // When the sentinel is popped, RR sees delta=0 and bails out — no re-render,
  // no route change, no flicker. Only our popstate handler fires to close the panel.
  //
  // Programmatic history.back() (explicit close) does NOT trigger iOS's visual
  // page transition — only the edge swipe gesture does.
  const panelHistoryPushedRef = useRef(false);

  const pushPanelHistory = useCallback(() => {
    if (!isMobile || panelHistoryPushedRef.current) return;
    panelHistoryPushedRef.current = true;
    window.history.pushState(
      { ...window.history.state, _panelSentinel: true },
      '',
      window.location.href,
    );
  }, [isMobile]);

  const popPanelHistory = useCallback(() => {
    if (!isMobile || !panelHistoryPushedRef.current) return;
    panelHistoryPushedRef.current = false;
    window.history.back();
  }, [isMobile]);

  // Listen for popstate — close panel if our sentinel was popped by back gesture
  useEffect(() => {
    if (!isMobile) return;
    const onPopState = () => {
      if (panelHistoryPushedRef.current) {
        panelHistoryPushedRef.current = false;
        setRightPanelType(null);
        setDetailToolCallId(null);
        setPreviewData(null);
      }
    };
    window.addEventListener('popstate', onPopState);
    return () => window.removeEventListener('popstate', onPopState);
  }, [isMobile]);

  // Clean up sentinel on unmount (e.g. navigating away with panel still open).
  // Use replaceState to silently neutralize the sentinel instead of history.back(),
  // which would fire a popstate after our listener is already cleaned up and could
  // cause React Router to navigate backward unexpectedly.
  useEffect(() => {
    return () => {
      if (panelHistoryPushedRef.current) {
        panelHistoryPushedRef.current = false;
        const state = window.history.state;
        if (state?._panelSentinel) {
          window.history.replaceState(
            { ...state, _panelSentinel: undefined },
            '',
            window.location.href,
          );
        }
      }
    };
  }, []);

  /**
   * The one landing for everything the right panel is pointed at: stamps the
   * ask's `seq`, sizes the panel, opens it on the file view and arms the mobile
   * back gesture. Sizing is in one place so the divider reads the cap that was
   * asked for.
   */
  const landInFilePanel = useCallback((target: UnsequencedTarget, opts?: { maxRatio?: number; width?: number }) => {
    askPanel(target);
    growPanelWidth(opts?.width ?? DEFAULT_PANEL_WIDTH, opts?.maxRatio);
    setRightPanelType('file');
    pushPanelHistory();
  }, [askPanel, growPanelWidth, pushPanelHistory]);

  const handleOpenFileFromChat = usePanelLinkOpen({
    workspaceId,
    folders,
    override: filePanelWorkspaceId,
    setOverride: setFilePanelWorkspaceId,
    land: landInFilePanel,
    leave: leaveFiles,
  });

  // A turn's sources open as a tab of the file view, one per turn; the tab
  // reads its live records through `transcript`.
  const handleOpenSourcesFromChat = useCallback((messageId: string) => {
    landInFilePanel({ kind: 'sources', messageId });
  }, [landInFilePanel]);

  // Opens the Status tab (live market watch) from the persistent chip; the
  // tab reads the live watch state from `marketWatch`.
  const handleOpenStatusFromChat = useCallback(() => {
    landInFilePanel({ kind: 'status' });
  }, [landInFilePanel]);

  // What this controller reads a tool call through: remade per transcript
  // change and read at render, so the mobile sheet and a click see the call as
  // it is now (see useToolCallLookup for why a click-time copy would not do).
  const getToolCallProcess = useToolCallLookup(messages, subagentTranscripts);

  // The file panel reads the transcript through a subscription instead: a
  // tool or sources tab follows its records, and the changed-file dot the
  // write log, without the panel rendering on every streamed chunk. Only the
  // workspace's own folders place what it wrote, so a refreshed list of its
  // siblings does not remake the log.
  const dirName = folders?.dirName;
  const previousDirNames = folders?.previousDirNames;
  const rootDir = folders?.rootDir;
  const otherRoots = folders?.otherRoots;
  const writeScope = useMemo(
    () => ({ dirName, previousDirNames, rootDir, otherRoots }),
    [dirName, previousDirNames, rootDir, otherRoots],
  );
  const collectWrites = useCallback(
    (msgs: readonly TranscriptMessage[]) => collectWriteLog(msgs, writeScope),
    [writeScope],
  );
  const transcript = useTranscriptReader(messages, subagentTranscripts, collectWrites);

  // The mobile sheet's tool call comes through the lookup above, not through
  // `transcript`: the store publishes after the commit, so a subscription here
  // would render ChatView a second time for every change to the call.
  const detailToolCall = detailToolCallId ? getToolCallProcess(detailToolCallId) ?? null : null;

  // Read at click time rather than derived per render: the file panel only
  // needs this thread's Write/Edit paths when it resolves a reference, and a
  // memo over `messages` would rebuild on every streamed chunk.
  const getRecentWritePaths = useStableHandler(
    () => collectRecentWritePaths(messages, writeScope),
  );

  // One-shot ?file= deep link: opens the file panel targeting that file. Gated
  // on isActive so only the visible ChatView consumes it (ChatAgent keeps cached
  // hidden instances), and on workspaceId so the panel has something to read.
  // The param is stripped after consuming so it can't re-fire on re-render.
  useEffect(() => {
    if (!isActive || !workspaceId || fileDeepLinkConsumedRef.current) return;
    const params = new URLSearchParams(location.search);
    const raw = params.get('file');
    if (!raw) return;
    fileDeepLinkConsumedRef.current = true;
    // URLSearchParams.get already percent-decodes; a second decodeURIComponent
    // would throw on a literal '%' in the filename (e.g. 100%25_report.html).
    handleOpenFileFromChat(raw);
    params.delete('file');
    const search = params.toString();
    navigate(
      { pathname: location.pathname, search: search ? `?${search}` : '' },
      { replace: true, state: location.state },
    );
  }, [isActive, workspaceId, location.search, location.pathname, location.state, navigate, handleOpenFileFromChat]);

  // Resolve preview URL: always pass command so the backend can start the
  // server if the port is idle (common for history sessions where the
  // original server process is long gone).  The backend skips the start
  // when the port is already listening, so this is safe for live sessions.
  const resolvePreviewUrl = useCallback(async (wid: string, port: number, command?: string): Promise<string> => {
    try {
      const result = await getPreviewUrl(wid, port, command);
      return result.url;
    } catch (err: unknown) {
      const status = (err as { response?: { status?: number } })?.response?.status;
      if (status === 503 && command) {
        // Sandbox was stopped — retry (may trigger workspace start)
        const result = await getPreviewUrl(wid, port, command);
        return result.url;
      }
      throw err;
    }
  }, []);

  // Resolve a preview URL and update the Map entry for this port. Only syncs
  // to render state if this port is still active.
  const resolveAndSetPreview = useCallback((wid: string, port: number, command?: string, pathSuffix?: string) => {
    resolvePreviewUrl(wid, port, command)
      .then((baseUrl: string) => {
        const entry = previewMapRef.current.get(port);
        if (entry?.workspaceId !== wid) return;
        const url = appendPathSuffix(baseUrl, pathSuffix ?? entry.path);
        const updated = { ...entry, url, loading: false, error: undefined };
        previewMapRef.current.set(port, updated);
        if (activePreviewPortRef.current === port) setPreviewData(updated);
      })
      .catch(() => {
        const entry = previewMapRef.current.get(port);
        if (entry?.workspaceId !== wid) return;
        const updated = { ...entry, url: '', loading: false, error: true };
        previewMapRef.current.set(port, updated);
        if (activePreviewPortRef.current === port) setPreviewData(updated);
      });
  }, [resolvePreviewUrl]);

  /**
   * Show a running app. On desktop it lands as a tab in the file panel beside
   * the files it serves; the panel mints and refreshes its URL from there, so
   * nothing is resolved here. Mobile keeps the bottom sheet. An app in a
   * sibling (`data.workspaceId`) runs there and is asked for there.
   */
  const handleOpenPreview = useCallback((data: PreviewData) => {
    const appWorkspaceId = data.workspaceId ?? workspaceId;
    if (!isMobile) {
      const land = () => landInFilePanel(
        { kind: 'preview', port: data.port, title: data.title, path: data.path, command: data.command },
        { maxRatio: PREVIEW_MAX_RATIO },
      );
      // The panel mints an app's URL for the workspace it shows, so it shows
      // the app's: the view's own, or the sibling the app runs in.
      const next = appWorkspaceId && appWorkspaceId !== workspaceId ? appWorkspaceId : null;
      if (next !== filePanelWorkspaceId) {
        leaveFiles(() => {
          setFilePanelWorkspaceId(next);
          land();
        });
      } else {
        land();
      }
      return;
    }
    // The sheet takes no ask, so an app opened on it, the stream's included,
    // gives a pending link its notice here.
    shareLinkClickRef.current += 1;
    // Each entry names its workspace, so a URL minted for one never lands on
    // another's app on the same port.
    const entry = { ...data, workspaceId: appWorkspaceId ?? undefined };
    previewMapRef.current.set(data.port, entry);
    activePreviewPortRef.current = data.port;
    setPreviewData(entry);
    setRightPanelType('preview');
    applyPanelWidth(DEFAULT_PANEL_WIDTH, PREVIEW_MAX_RATIO);
    pushPanelHistory();
    // If opened with loading state (no URL yet), resolve via authenticated endpoint
    if (data.loading && !data.url && appWorkspaceId) {
      resolveAndSetPreview(appWorkspaceId, data.port, data.command, data.path);
    }
  }, [isMobile, landInFilePanel, filePanelWorkspaceId, leaveFiles, setFilePanelWorkspaceId, applyPanelWidth, pushPanelHistory, workspaceId, resolveAndSetPreview]);

  /**
   * An `/a/` link in a reply, opened as the panel tab its item already has:
   * the app on its port at the page the link names, or the file. Desktop shell
   * only, by the user's setting; the browser keeps its new tab. A code that is
   * not the user's, or that no longer resolves, goes to the browser as the
   * link would have.
   */
  const handleOpenShareLink = useCallback((href: string): boolean => {
    const target = parseShareLinkHref(href);
    if (!target || !workspaceId || !opensShareLinksInPanel()) return false;
    const click = ++shareLinkClickRef.current;
    const asked = askCount();
    // Any input after the click is a newer intent than the link: a tab picked,
    // a sheet opened, the panel closed. Most of those never ask the panel, so
    // the window hears them instead.
    const supersede = (e: Event) => {
      if (e instanceof KeyboardEvent && MODIFIER_KEYS.has(e.key)) return;
      shareLinkClickRef.current += 1;
    };
    window.addEventListener('pointerdown', supersede, true);
    window.addEventListener('keydown', supersede, true);
    const superseded = () => click !== shareLinkClickRef.current || askCount() !== asked;
    getShareLink(target.code)
      .then((link) => {
        if (superseded()) return;
        // A report-back carries an analyst's links into Home: each opens in
        // the workspace it lives in, shown in this panel.
        const sibling = link.workspace_id !== workspaceId ? link.workspace_id : undefined;
        if (link.kind === 'app') {
          // Always a page, the root as `/`: no path keeps the page a tab
          // already on this port shows.
          const page = target.path ?? link.path ?? '';
          handleOpenPreview({
            url: '',
            port: link.port,
            title: link.title ?? undefined,
            path: `/${page.replace(/^\/+/, '')}`,
            loading: true,
            reloadToken: ++reloadCounterRef.current,
            workspaceId: sibling,
          });
        } else {
          handleOpenFileFromChat(link.path, sibling);
        }
      })
      // The click's gesture is spent by now, so the shell is asked directly
      // rather than through a window.open it may treat as a popup. It opens
      // absolute URLs only, and a link may be relative.
      .catch(() => {
        if (superseded()) return;
        const url = new URL(href, window.location.origin).href;
        if (desktop?.openExternal) void desktop.openExternal(url);
        else window.open(url, '_blank', 'noopener,noreferrer');
      })
      .finally(() => {
        window.removeEventListener('pointerdown', supersede, true);
        window.removeEventListener('keydown', supersede, true);
      });
    return true;
  }, [workspaceId, askCount, handleOpenPreview, handleOpenFileFromChat]);

  // A lookup still pending when its thread is left must not open a tab or hand
  // the browser a link. A cached view is left by going inactive, any other by
  // unmounting, and a view moved to another workspace leaves this one's links
  // behind; each runs this cleanup.
  useEffect(() => () => { shareLinkClickRef.current += 1; }, [isActive, workspaceId]);

  // The sheets are the only surfaces that show a `preview` or `detail` panel
  // type; the desktop column shows a running app or a tool result as a Files
  // tab and renders nothing for those types. A viewport that widens with a
  // sheet open would otherwise leave an empty column, so what it was showing
  // lands as a tab instead.
  const wasMobile = useRef(isMobile);
  useEffect(() => {
    const widened = wasMobile.current && !isMobile;
    wasMobile.current = isMobile;
    if (!widened) return;
    if (rightPanelType === 'detail') {
      const toolCallId = detailToolCallId;
      setDetailToolCallId(null);
      if (toolCallId) landInFilePanel({ kind: 'tool', toolCallId }, { width: detailPanelWidth(getToolCallProcess(toolCallId) ?? null) });
      else setRightPanelType(null);
      return;
    }
    if (rightPanelType !== 'preview') return;
    const data = previewData;
    activePreviewPortRef.current = null;
    if (!data) {
      setRightPanelType(null);
      return;
    }
    // Through the opener, so the panel shows the workspace the app runs in,
    // a sibling's for an `/a/` link, before it mints the tab's URL.
    handleOpenPreview(data);
  }, [isMobile, rightPanelType, previewData, detailToolCallId, getToolCallProcess, landInFilePanel, handleOpenPreview]);

  /** The full MarketView page on one symbol, with a way back to this chat. */
  const handleOpenInMarketView = useCallback((spec: ChartTabSpec) => {
    navigate(buildMarketViewUrl({
      symbol: spec.symbol,
      timeframe: spec.timeframe,
      workspaceId: spec.workspaceId ?? workspaceId,
      threadId: threadId && threadId !== '__default__' ? threadId : null,
      returnTo: location.pathname + location.search,
    }));
  }, [workspaceId, threadId, location.pathname, location.search, navigate]);

  /**
   * Show a live chart. On desktop it lands as a tab in the file panel beside
   * the files about the same company, sized like a preview so the toolbar has
   * room. Mobile has no tab strip and goes to the MarketView page itself.
   */
  const handleOpenChart = useCallback((ask: ChartTabSpec) => {
    // The workspace comes off an agent artifact; one that is not an id would
    // stick to the tab and be asked for on every open, so the panel's is used.
    const spec = ask.workspaceId && !isValidUuid(ask.workspaceId) ? { ...ask, workspaceId: undefined } : ask;
    if (isMobile) {
      handleOpenInMarketView(spec);
      return;
    }
    landInFilePanel({ kind: 'chart', ...spec }, { maxRatio: PREVIEW_MAX_RATIO });
  }, [isMobile, handleOpenInMarketView, landInFilePanel]);

  /**
   * Show a tool call's result. On desktop it lands as a tab in the file panel
   * beside the files the call produced, sized for what it holds. Mobile keeps
   * the bottom sheet. A published app opens as a preview instead.
   */
  const handleToolCallDetailClick = useCallback((toolCallId: string) => {
    const toolCallProcess = getToolCallProcess(toolCallId);
    if (!toolCallProcess) {
      // A row whose record has left the transcript (a regenerated turn). The
      // tab says so in its body; the sheet has nothing to show for it.
      if (!isMobile) landInFilePanel({ kind: 'tool', toolCallId });
      return;
    }
    const artifact = toolCallProcess.toolCallResult?.artifact as Record<string, unknown> | undefined;
    if (artifact?.type === 'preview_url' && artifact.port && workspaceId) {
      const port = artifact.port as number;
      const title = artifact.title as string | undefined;
      const command = artifact.command as string | undefined;
      const path = artifact.path as string | undefined;
      const token = ++reloadCounterRef.current;
      // A cached entry seeds the title, command and path; the URL itself is
      // short-lived and re-minted, so the tab opens loading rather than on a
      // link that may have expired.
      const cached = previewMapRef.current.get(port);
      if (cached?.url && cached.workspaceId === workspaceId) {
        // Loading with no URL: handleOpenPreview resolves it on the sheet too.
        handleOpenPreview({ ...cached, url: '', loading: true, error: undefined, reloadToken: token, path });
        return;
      }
      // No cache: handleOpenPreview resolves (restarting the server if needed
      // via the 503 fallback) since loading=true and url=''
      handleOpenPreview({ url: '', port, title, command, path, loading: true, reloadToken: token });
      return;
    }
    // A watch the agent started already has its tab, which shows the prices
    // live; the call's own record would be a second, frozen view of the same
    // watch. A stop, or a watch since ended, has only the record to show.
    const watchCall = (toolCallProcess.toolName ?? toolCallProcess.toolCall?.name) === 'watch_market' && toolCallProcess.toolCall?.args?.action !== 'unwatch';
    if (watchCall && watching) {
      landInFilePanel({ kind: 'status' });
      return;
    }
    if (!isMobile) {
      landInFilePanel({ kind: 'tool', toolCallId }, { width: detailPanelWidth(toolCallProcess) });
      return;
    }
    setDetailToolCallId(toolCallId);
    applyPanelWidth(detailPanelWidth(toolCallProcess));
    setRightPanelType('detail');
    pushPanelHistory();
  }, [isMobile, getToolCallProcess, landInFilePanel, applyPanelWidth, pushPanelHistory, workspaceId, handleOpenPreview, watching]);

  // Close the mobile detail sheet
  const handleCloseDetailPanel = useCallback(() => {
    setRightPanelType(null);
    setDetailToolCallId(null);
    popPanelHistory();
  }, [popPanelHistory]);

  // A sheet reads its call live, so a record that leaves the transcript under
  // it leaves the sheet open on nothing, with the back sentinel still pushed.
  useEffect(() => {
    if (isMobile && rightPanelType === 'detail' && !detailToolCall) handleCloseDetailPanel();
  }, [isMobile, rightPanelType, detailToolCall, handleCloseDetailPanel]);

  // Close preview panel (keep Map cache for instant reopen, but stop background state updates)
  const handleClosePreview = useCallback(() => {
    activePreviewPortRef.current = null;
    setRightPanelType(null);
    popPanelHistory();
  }, [popPanelHistory]);

  // Refresh preview: restart process + resolve fresh signed URL (force bypasses cache)
  const handleRefreshPreview = useCallback(async () => {
    const wid = previewData?.workspaceId ?? workspaceId;
    if (!previewData || !wid) return;
    // Capture values before async gap to avoid stale closure if user switches ports
    const { port, command, path } = previewData;
    const loadingEntry = { ...previewData, loading: true, error: undefined };
    previewMapRef.current.set(port, loadingEntry);
    setPreviewData(loadingEntry);
    try {
      const result = await getPreviewUrl(wid, port, command, true);
      const token = ++reloadCounterRef.current;
      const url = appendPathSuffix(result.url, path);
      const entry = previewMapRef.current.get(port);
      // Another workspace's app took the port meanwhile.
      if (entry && entry.workspaceId !== wid) return;
      const updated = { ...(entry ?? previewData), url, loading: false, reloadToken: token };
      previewMapRef.current.set(port, updated);
      if (activePreviewPortRef.current === port) setPreviewData(updated);
    } catch (e) {
      console.error('Failed to refresh preview:', e);
      const entry = previewMapRef.current.get(port);
      if (entry && entry.workspaceId !== wid) return;
      const updated = { ...(entry ?? previewData), loading: false, error: true };
      previewMapRef.current.set(port, updated);
      if (activePreviewPortRef.current === port) setPreviewData(updated);
    }
  }, [previewData, workspaceId]);

  // Toggle file panel
  const handleToggleFilePanel = useCallback(() => {
    if (rightPanelType === 'file') {
      leaveFiles(() => {
        setRightPanelType(null);
        popPanelHistory();
      });
    } else {
      // The header's toggle is the view's own files; a sibling shown before
      // the panel closed was reached through a link, and the closed panel
      // holds no draft to lose.
      if (filePanelWorkspaceId) setFilePanelWorkspaceId(null);
      applyPanelWidth(DEFAULT_PANEL_WIDTH);
      setRightPanelType('file');
      pushPanelHistory();
    }
  }, [rightPanelType, applyPanelWidth, pushPanelHistory, popPanelHistory, leaveFiles, filePanelWorkspaceId, setFilePanelWorkspaceId]);

  return {
    activeTabKind,
    panelTarget,
    handleTargetHandled,
    handleTargetMemoryHandled,
    handleTargetMemoHandled,
    rightPanelType,
    setRightPanelType,
    rightPanelWidth,
    panelExpanded,
    handleTogglePanelExpanded,
    previewData,
    panelWrapperRef,
    isDragging,
    dragJustEnded,
    handleDividerMouseDown,
    popPanelHistory,
    handleOpenFileFromChat,
    handleOpenSourcesFromChat,
    handleOpenStatusFromChat,
    handleToolCallDetailClick,
    handleCloseDetailPanel,
    handleClosePreview,
    handleRefreshPreview,
    handleToggleFilePanel,
    handleFilesLeaveGuardChange,
    handleActiveTabKindChange,
    leaveFiles,
    handleOpenPreview,
    handleOpenShareLink,
    handleOpenChart,
    handleOpenInMarketView,
    detailToolCall,
    transcript,
    getRecentWritePaths,
  };
}
