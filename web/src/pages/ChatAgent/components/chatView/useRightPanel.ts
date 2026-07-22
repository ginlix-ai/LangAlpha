import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { Dispatch, SetStateAction } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { getPreviewUrl } from '../../utils/api';
import { computeAgentArtifactRouting } from '../../utils/agentPaths';
import { isValidUuid } from '../../utils/uuid';
import { clampPanelWidth as clampPanelWidthUtil } from '@/lib/panelUtils';
import type { PanelTarget } from '../RightPanel';
import type { PreviewData } from '../../hooks/utils/types';
import type { ProvenanceRecord } from '@/types/chat';
import type { PlanData, ToolCallProcessRecord } from './types';

/** Append an optional path suffix to a base URL (e.g. "/timeline.html"). */
function appendPathSuffix(baseUrl: string, path?: string): string {
  if (!path) return baseUrl;
  try {
    const parsed = new URL(baseUrl);
    parsed.pathname = parsed.pathname.replace(/\/+$/, '') + path;
    return parsed.toString();
  } catch {
    return baseUrl;
  }
}

/** Right-panel controller (carved out of ChatView, 5.9c): panel type/width,
 * target routing, tool-call/plan detail, multi-port preview resolution,
 * divider drag, sources provenance, and mobile back-gesture integration. */
export function useRightPanel({
  isMobile,
  workspaceId,
  isActive,
  containerRef,
  setFilePanelWorkspaceId,
  messages,
}: {
  isMobile: boolean;
  workspaceId: string;
  isActive: boolean;
  containerRef: React.RefObject<HTMLDivElement | null>;
  setFilePanelWorkspaceId: Dispatch<SetStateAction<string | null>>;
  messages: unknown[];
}) {
  const location = useLocation();
  const navigate = useNavigate();

  // Guards one-shot consumption of the ?file= deep link (report share / copy link).
  const fileDeepLinkConsumedRef = useRef(false);

  // Single source of truth for what the RightPanel is pointed at. Exactly one
  // target is ever set (file/memory/memo/sources/status); the panel derives the
  // active tab, tab visibility, and snap-back from `.kind`. Sources/status stay
  // set while their tab is open (tab chrome persistence) and are cleared on
  // panel close by the effect below; file/memory/memo self-clear once the child
  // panel consumes the pre-select (the handled callbacks).
  const [panelTarget, setPanelTarget] = useState<PanelTarget | null>(null);
  // Stable handlers — these land in useEffect deps in MemoryPanel/MemoPanel/
  // FilePanel. Inline arrows would create a new identity on every ChatView
  // render, re-triggering those effects on every streaming chunk (the
  // `targetKey == null` guard makes them no-ops, but the wakeup is wasted).
  // Each clears the target only if it still matches the kind it consumed, so a
  // fast follow-up open of a different kind isn't wiped by a late callback.
  const handleTargetFileHandled = useCallback(() => setPanelTarget((pt) => (pt?.kind === 'file' ? null : pt)), []);
  const handleTargetDirHandled = useCallback(() => setPanelTarget((pt) => (pt?.kind === 'file' ? null : pt)), []);
  const handleTargetMemoryHandled = useCallback(() => setPanelTarget((pt) => (pt?.kind === 'memory' ? null : pt)), []);
  const handleTargetMemoHandled = useCallback(() => setPanelTarget((pt) => (pt?.kind === 'memo' ? null : pt)), []);

  const isDraggingRef = useRef(false);
  const [isDragging, setIsDragging] = useState(false);
  // True for exactly one render after drag ends — forces transition duration:0
  // so Framer Motion jumps to the final width instead of animating from pre-drag.
  const dragJustEndedRef = useRef(false);

  // Right panel management - can show 'file', 'detail', 'preview', or null (closed)
  const [rightPanelType, setRightPanelType] = useState<'file' | 'detail' | 'preview' | null>(null);
  const [rightPanelWidth, setRightPanelWidth] = useState(750);
  // Multi-port preview state: Map keyed by port lives in a ref (non-active updates don't re-render).
  // activePreviewPort + derived previewData drive the panel render.
  const previewMapRef = useRef<Map<number, PreviewData>>(new Map());
  const activePreviewPortRef = useRef<number | null>(null);
  const reloadCounterRef = useRef(0);
  const [previewData, setPreviewData] = useState<PreviewData | null>(null);
  const panelWrapperRef = useRef<HTMLDivElement>(null);

  // Clear the drag-just-ended flag after each render so future transitions animate normally.
  useEffect(() => { dragJustEndedRef.current = false; });

  // Clear preview cache and cross-workspace state when workspace changes to avoid leaking old workspace data.
  useEffect(() => {
    previewMapRef.current.clear();
    activePreviewPortRef.current = null;
    setPreviewData(null);
    setFilePanelWorkspaceId(null);
  }, [workspaceId, setFilePanelWorkspaceId]);

  // Tool call detail panel state
  const [detailToolCall, setDetailToolCall] = useState<ToolCallProcessRecord | null>(null);
  // Plan detail panel state
  const [detailPlanData, setDetailPlanData] = useState<PlanData | null>(null);

  const clampPanelWidth = useCallback(
    (desired: number) => clampPanelWidthUtil(desired, containerRef.current?.offsetWidth || window.innerWidth),
    [containerRef],
  );

  // Handle drag panel width — direct DOM manipulation for smooth, jank-free resize.
  // React state is only updated once on mouseup; during drag we bypass React/Framer.
  const PREVIEW_MAX_RATIO = 0.92;
  const handleDividerMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    isDraggingRef.current = true;
    setIsDragging(true);
    const startX = e.clientX;
    const startWidth = rightPanelWidth;
    const containerW = containerRef.current?.offsetWidth || window.innerWidth;
    const maxRatio = rightPanelType === 'preview' ? PREVIEW_MAX_RATIO : undefined;

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
      currentWidth = clampPanelWidthUtil(startWidth + delta, containerW, maxRatio);
      if (wrapperEl) wrapperEl.style.width = `${currentWidth}px`;
      if (innerEl) innerEl.style.width = `${currentWidth}px`;
    };

    const onMouseUp = () => {
      isDraggingRef.current = false;
      // Flag ensures the next render uses duration:0 so Framer doesn't
      // animate from the stale pre-drag width to the final width.
      dragJustEndedRef.current = true;
      setIsDragging(false);
      setRightPanelWidth(currentWidth);
      iframes?.forEach(iframe => { (iframe as HTMLIFrameElement).style.pointerEvents = ''; });
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('mouseup', onMouseUp);
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
    };

    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
  }, [rightPanelWidth, rightPanelType, containerRef]);

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
        setDetailToolCall(null);
        setDetailPlanData(null);
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
   * Routes a click on a tool-call artifact to the right panel tab that owns
   * its domain. The pure decision is computed by computeAgentArtifactRouting;
   * we apply the result atomically (clear everything, then set).
   */
  const handleOpenAgentArtifactFromChat = useCallback((rawPath: string, targetWorkspaceId?: string) => {
    const r = computeAgentArtifactRouting(rawPath, targetWorkspaceId);
    if (r.setWorkspaceId && !isValidUuid(r.setWorkspaceId)) {
      console.warn('[ChatView] ignoring artifact ref with invalid workspace id', r.setWorkspaceId);
      return;
    }

    // The routing result carries exactly one non-null target field; map it to
    // the matching panel kind. `targetMemoKey` may legitimately be '' (memo
    // index → LIST view), so test for null rather than truthiness.
    let target: PanelTarget;
    if (r.targetMemoryKey != null && r.targetMemoryTier != null) {
      target = { kind: 'memory', key: r.targetMemoryKey, tier: r.targetMemoryTier };
    } else if (r.targetMemoKey != null) {
      target = { kind: 'memo', key: r.targetMemoKey };
    } else {
      target = { kind: 'file', path: r.targetFile };
    }
    setPanelTarget(target);
    if (r.clearWorkspaceId) {
      setFilePanelWorkspaceId(null);
    } else if (r.setWorkspaceId) {
      setFilePanelWorkspaceId(r.setWorkspaceId);
    }

    setRightPanelWidth(clampPanelWidth(850));
    setRightPanelType('file');
    pushPanelHistory();
  }, [clampPanelWidth, pushPanelHistory, setFilePanelWorkspaceId]);

  // Alias kept for the existing callers (tool-call rows, ws:// flash links,
  // file-panel handoffs) that still use the older name. Pure identity — the
  // unified router does the path-aware classification on every call.
  const handleOpenFileFromChat = handleOpenAgentArtifactFromChat;

  // Opens the Sources tab for a turn by pinning the message id — the single
  // target replaces any prior file/memory/memo/status one, so the panel snaps
  // to Sources; it resolves live records from `messages`.
  const handleOpenSourcesFromChat = useCallback((messageId: string) => {
    setPanelTarget({ kind: 'sources', messageId });
    setRightPanelWidth(clampPanelWidth(850));
    setRightPanelType('file');
    pushPanelHistory();
  }, [clampPanelWidth, pushPanelHistory]);

  // Opens the Status tab (live market watch) from the persistent chip. The
  // single 'status' target replaces any prior one, so the panel snaps to Status;
  // it resolves live watch state from `marketWatch`.
  const handleOpenStatusFromChat = useCallback(() => {
    setPanelTarget({ kind: 'status' });
    setRightPanelWidth(clampPanelWidth(850));
    setRightPanelType('file');
    pushPanelHistory();
  }, [clampPanelWidth, pushPanelHistory]);

  // The message id whose provenance the Sources tab shows, when a sources target
  // is active. Drives the two provenance memos below.
  const sourcesMessageId = panelTarget?.kind === 'sources' ? panelTarget.messageId : null;

  // Live provenance for the targeted turn — resolved from `messages` so the
  // Sources panel updates as records stream in (live) or replay re-delivers
  // them (reload). Recomputes on every `messages` change while the tab is open.
  const sourcesRecords = useMemo<Record<string, ProvenanceRecord> | undefined>(() => {
    if (!sourcesMessageId) return undefined;
    const msg = messages.find((m) => (m as { id?: string }).id === sourcesMessageId);
    return (msg as { provenanceRecords?: Record<string, ProvenanceRecord> } | undefined)?.provenanceRecords;
  }, [sourcesMessageId, messages]);

  // Thread-wide provenance: every turn's records merged in chronological order.
  // The Sources panel dedups across turns (first occurrence wins) and offers a
  // "This turn / All sources" switch when this set is larger than the turn's.
  // Gated on an open Sources tab so we don't merge on every unrelated render.
  const allSourcesRecords = useMemo<Record<string, ProvenanceRecord> | undefined>(() => {
    if (!sourcesMessageId) return undefined;
    const merged: Record<string, ProvenanceRecord> = {};
    for (const m of messages) {
      const recs = (m as { provenanceRecords?: Record<string, ProvenanceRecord> }).provenanceRecords;
      if (!recs) continue;
      // First occurrence wins: keep the earliest turn's metadata for a colliding
      // key (Object.assign would let later turns overwrite — last-wins).
      for (const key in recs) {
        if (!(key in merged)) merged[key] = recs[key];
      }
    }
    return merged;
  }, [sourcesMessageId, messages]);

  // Drop a sticky sources/status target whenever the right panel is closed or
  // switches to a non-file view (detail/preview), so a later file/memory click
  // doesn't reopen that tab. These two are the only kinds that persist while
  // their tab is open; file/memory/memo self-clear via the handled callbacks.
  // The many close call sites all funnel through rightPanelType.
  useEffect(() => {
    if (rightPanelType !== 'file' && (panelTarget?.kind === 'sources' || panelTarget?.kind === 'status')) {
      setPanelTarget(null);
    }
  }, [rightPanelType, panelTarget]);

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

  // Open file panel filtered to a specific directory. The single 'file' target
  // (with `dir`) replaces any pending memory/memo/status pre-select, so nothing
  // can snap-back hijack the dir click.
  const handleOpenDirFromChat = useCallback((dirPath: string) => {
    setRightPanelWidth(clampPanelWidth(850));
    setRightPanelType('file');
    setPanelTarget({ kind: 'file', dir: dirPath });
    pushPanelHistory();
  }, [clampPanelWidth, pushPanelHistory]);

  // Determine detail panel width based on content type
  const getDetailPanelWidth = useCallback((toolCallProcess: ToolCallProcessRecord | null) => {
    let desired = 650;
    if (!toolCallProcess) { desired = 550; }
    else {
      const toolName = toolCallProcess.toolName || '';
      const artifactType = toolCallProcess.toolCallResult?.artifact?.type;

      // Wide: file reading, SEC filings, subagent results
      if (artifactType === 'sec_filing') desired = 850;
      else if (toolName === 'Read') desired = 850;
      else if (toolName === 'Task' || toolName === 'task') desired = 750;
      // Medium: charts, search results, default markdown
      else if (artifactType === 'stock_prices' || artifactType === 'market_indices' || artifactType === 'sector_performance') desired = 650;
      else if (toolName === 'WebSearch' || toolName === 'web_search') desired = 650;
      // Slim: compact data cards
      else if (artifactType === 'company_overview') desired = 480;
      else if (artifactType === 'automations') desired = 480;
    }
    return clampPanelWidth(desired);
  }, [clampPanelWidth]);

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

  // Resolve a preview URL and update the Map entry for this port.
  // Only syncs to render state if this port is still active.
  // If the entry has a `path` suffix (e.g. "/timeline.html"), it's appended to the signed URL.
  const resolveAndSetPreview = useCallback((wid: string, port: number, command?: string, pathSuffix?: string) => {
    resolvePreviewUrl(wid, port, command)
      .then((baseUrl: string) => {
        const entry = previewMapRef.current.get(port);
        if (!entry) return;
        const url = appendPathSuffix(baseUrl, pathSuffix ?? entry.path);
        const updated = { ...entry, url, loading: false, error: undefined };
        previewMapRef.current.set(port, updated);
        if (activePreviewPortRef.current === port) setPreviewData(updated);
      })
      .catch(() => {
        const entry = previewMapRef.current.get(port);
        if (!entry) return;
        const updated = { ...entry, url: '', loading: false, error: true };
        previewMapRef.current.set(port, updated);
        if (activePreviewPortRef.current === port) setPreviewData(updated);
      });
  }, [resolvePreviewUrl]);

  // Open preview URL in right panel
  const handleOpenPreview = useCallback((data: PreviewData) => {
    previewMapRef.current.set(data.port, data);
    activePreviewPortRef.current = data.port;
    setPreviewData(data);
    setRightPanelType('preview');
    const containerW = containerRef.current?.offsetWidth || window.innerWidth;
    setRightPanelWidth(clampPanelWidthUtil(850, containerW, PREVIEW_MAX_RATIO));
    pushPanelHistory();
    // If opened with loading state (no URL yet), resolve via authenticated endpoint
    if (data.loading && !data.url && workspaceId) {
      resolveAndSetPreview(workspaceId, data.port, data.command, data.path);
    }
  }, [pushPanelHistory, workspaceId, resolveAndSetPreview, containerRef]);

  // Open tool call detail in right panel (or preview panel for preview_url artifacts)
  const handleToolCallDetailClick = useCallback((toolCallProcess: ToolCallProcessRecord) => {
    const artifact = toolCallProcess.toolCallResult?.artifact as Record<string, unknown> | undefined;
    if (artifact?.type === 'preview_url' && artifact.port && workspaceId) {
      const port = artifact.port as number;
      const title = artifact.title as string | undefined;
      const command = artifact.command as string | undefined;
      const path = artifact.path as string | undefined;
      const token = ++reloadCounterRef.current;
      // Check Map cache (not single state) — show cached URL instantly, then verify in background
      const cached = previewMapRef.current.get(port);
      if (cached?.url) {
        handleOpenPreview({ ...cached, url: '', loading: true, error: undefined, reloadToken: token, path });
        resolveAndSetPreview(workspaceId, port, command, path);
        return;
      }
      // No cache — resolve (restarts server if needed via 503 fallback)
      // handleOpenPreview will trigger resolution since loading=true and url=''
      handleOpenPreview({ url: '', port, title, command, path, loading: true, reloadToken: token });
      return;
    }
    setDetailToolCall(toolCallProcess);
    setDetailPlanData(null);
    setRightPanelWidth(getDetailPanelWidth(toolCallProcess));
    setRightPanelType('detail');
    pushPanelHistory();
  }, [getDetailPanelWidth, pushPanelHistory, workspaceId, handleOpenPreview, resolveAndSetPreview]);

  // Open plan detail in right panel
  const handlePlanDetailClick = useCallback((planData: PlanData) => {
    setDetailPlanData(planData);
    setDetailToolCall(null);
    setRightPanelWidth(clampPanelWidth(550));
    setRightPanelType('detail');
    pushPanelHistory();
  }, [clampPanelWidth, pushPanelHistory]);

  // Close detail panel (shared by MobileBottomSheet + DetailPanel onClose)
  const handleCloseDetailPanel = useCallback(() => {
    setRightPanelType(null);
    setDetailToolCall(null);
    setDetailPlanData(null);
    popPanelHistory();
  }, [popPanelHistory]);

  // Close preview panel (keep Map cache for instant reopen, but stop background state updates)
  const handleClosePreview = useCallback(() => {
    activePreviewPortRef.current = null;
    setRightPanelType(null);
    popPanelHistory();
  }, [popPanelHistory]);

  // Refresh preview: restart process + resolve fresh signed URL (force bypasses cache)
  const handleRefreshPreview = useCallback(async () => {
    if (!previewData || !workspaceId) return;
    // Capture values before async gap to avoid stale closure if user switches ports
    const { port, command, path } = previewData;
    const loadingEntry = { ...previewData, loading: true, error: undefined };
    previewMapRef.current.set(port, loadingEntry);
    setPreviewData(loadingEntry);
    try {
      const result = await getPreviewUrl(workspaceId, port, command, true);
      const token = ++reloadCounterRef.current;
      const url = appendPathSuffix(result.url, path);
      const entry = previewMapRef.current.get(port);
      const updated = { ...(entry ?? previewData), url, loading: false, reloadToken: token };
      previewMapRef.current.set(port, updated);
      if (activePreviewPortRef.current === port) setPreviewData(updated);
    } catch (e) {
      console.error('Failed to refresh preview:', e);
      const entry = previewMapRef.current.get(port);
      const updated = { ...(entry ?? previewData), loading: false, error: true };
      previewMapRef.current.set(port, updated);
      if (activePreviewPortRef.current === port) setPreviewData(updated);
    }
  }, [previewData, workspaceId]);

  // Toggle file panel
  const handleToggleFilePanel = useCallback(() => {
    if (rightPanelType === 'file') {
      setRightPanelType(null);
      popPanelHistory();
    } else {
      setRightPanelWidth(clampPanelWidth(850));
      setRightPanelType('file');
      pushPanelHistory();
    }
  }, [rightPanelType, clampPanelWidth, pushPanelHistory, popPanelHistory]);

  return {
    panelTarget,
    handleTargetFileHandled,
    handleTargetDirHandled,
    handleTargetMemoryHandled,
    handleTargetMemoHandled,
    rightPanelType,
    setRightPanelType,
    rightPanelWidth,
    previewData,
    panelWrapperRef,
    isDragging,
    dragJustEndedRef,
    handleDividerMouseDown,
    popPanelHistory,
    handleOpenFileFromChat,
    handleOpenSourcesFromChat,
    handleOpenStatusFromChat,
    handleOpenDirFromChat,
    handleToolCallDetailClick,
    handlePlanDetailClick,
    handleCloseDetailPanel,
    handleClosePreview,
    handleRefreshPreview,
    handleToggleFilePanel,
    handleOpenPreview,
    detailToolCall,
    detailPlanData,
    sourcesRecords,
    allSourcesRecords,
  };
}
