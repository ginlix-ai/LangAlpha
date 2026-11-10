import React, { useEffect, useEffectEvent, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { ExternalLink, Pencil, RefreshCw } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import type { BridgeOptions } from '@/lib/appBridge/AppBridge';
import { useAppFrame } from '@/lib/appBridge/useAppFrame';
import type { Scope } from '@/lib/appBridge/protocol';
import type { SavedDashboard, SavedDashboardsApi } from '@/lib/dashboards';
import { cn } from '@/lib/utils';
import { HostedFrame } from '../ChatAgent/components/viewers/HostedFrame';
import { shareLinkHref } from '../ChatAgent/utils/api/shareLinks';
import { DashboardAccess } from './components/DashboardAccess';
import { DashboardAwayNotice } from './components/DashboardAwayNotice';
import DashboardHeader from './components/DashboardHeader';
import { DashboardSwitcher } from './components/DashboardSwitcher';
import { useDashboardAgent } from './hooks/useDashboardAgent';
import { useDashboardSource, type DashboardSource } from './hooks/useDashboardSource';
import { TOP, usePageScroll, type ScrollPlace } from './hooks/usePageScroll';

/** How long a cold start runs before the built-in dashboard is offered meanwhile. */
const SLOW_START_MS = 6_000;
/** What a renewed frame gets after its load to ask for its data, before it replaces the one on screen. */
const SETTLE_MS = 800;

interface CustomDashboardProps {
  dashboard: SavedDashboard;
  dashboards: SavedDashboardsApi;
  /** The built-in dashboard, which stands in while this one cannot be shown, with the note saying why. */
  builtIn: (slots: { switcher: React.ReactNode; notice: React.ReactNode }) => React.ReactElement;
}

/**
 * The dashboard the user chose in place of the built-in one: a page kept on
 * the server, or a running app. While it has nothing to show (its machine is
 * asleep, it was removed, the user would rather not wait for a cold start)
 * the built-in dashboard stands in and says why, and this one stays chosen.
 */
export default function CustomDashboard({ dashboard, dashboards, builtIn }: CustomDashboardProps): React.ReactElement {
  const source = useDashboardSource(dashboard);
  // The user chose not to wait for it to start, for this visit.
  const [setAside, setSetAside] = useState(false);
  const away = source.url ? null : source.away;

  const show = () => {
    setSetAside(false);
    if (away) source.refresh();
  };
  const switcher = <DashboardSwitcher dashboards={dashboards} onReselect={show} />;

  if (away || setAside) {
    return builtIn({
      switcher,
      notice: (
        <DashboardAwayNotice
          dashboard={dashboard}
          away={away ?? { kind: 'waiting', ready: !!source.url }}
          busy={source.refreshing}
          onOpen={show}
          onRemove={() => dashboards.remove(dashboard.code)}
        />
      ),
    });
  }
  return <DashboardFrame dashboard={dashboard} source={source} switcher={switcher} onSetAside={() => setSetAside(true)} />;
}

interface DashboardFrameProps {
  dashboard: SavedDashboard;
  source: DashboardSource;
  switcher: React.ReactNode;
  onSetAside: () => void;
}

/** The dashboard's header and its frame, which renews without ever blanking. */
function DashboardFrame({ dashboard, source, switcher, onSetAside }: DashboardFrameProps): React.ReactElement {
  const { t } = useTranslation();
  const agent = useDashboardAgent();
  const [frameKey, setFrameKey] = useState(0);
  // The frame on screen. A new address (a renewed signature, a new version)
  // loads behind it and takes its place once ready. Each keeps the sandbox
  // and the grants that came with its own address: an app whose command
  // changed is another program, and the one still on screen is not it.
  const [front, setFront] = useState<Layer | null>(null);
  const [slow, setSlow] = useState(false);
  // What the document on screen asked to read, and the consent bar's "Not
  // now": both outlive a renewal, which swaps the frame under them.
  const [asked, setAsked] = useState<Scope[] | null>(null);
  // "Not now" is said to one program: an app whose command changed is
  // another, with grants of its own, and asks for itself.
  const [notNowFor, setNotNowFor] = useState<string | null>(null);
  const { url } = source;
  const next: Layer | null = url ? { url, sandbox: source.sandbox, grantKey: source.grantKey } : null;
  // The same address is the same frame, told whatever its source now says.
  const onScreen = next && front && front.url !== next.url ? front : next;
  const grantKey = onScreen?.grantKey ?? null;
  // Where the reader is on the layer shown, which a new version opens at.
  // In pixels from the top: a refresh mostly changes figures, not the
  // sections above them, so an offset lands on the same section where a
  // fraction of a page that grew would drift; the page clamps it. Tagged with
  // the dashboard, so another one the user switches to opens at its top.
  const place = useRef<(ScrollPlace & { code: string }) | null>(null);
  const placeFor = (): ScrollPlace => (place.current?.code === dashboard.code ? place.current : TOP);
  const page = dashboard.kind === 'page';
  const title = source.title || dashboard.title || t('customDashboard.untitled');

  useEffect(() => {
    if (url) return;
    const timer = setTimeout(() => setSlow(true), SLOW_START_MS);
    return () => clearTimeout(timer);
  }, [url]);

  const reloadFrame = () => {
    setFront(null);
    setFrameKey((k) => k + 1);
  };
  const reload = () => {
    // Asking again is asking to be asked again.
    setNotNowFor(null);
    reloadFrame();
    source.refresh();
  };

  // The frame on screen, and the renewed one loading behind it.
  const layers = !next ? [] : onScreen && onScreen !== next ? [onScreen, next] : [next];

  return (
    <div className="flex h-full min-h-0 flex-col" style={{ backgroundColor: 'var(--color-bg-page)' }}>
      <h1 className="sr-only">{title}</h1>
      <DashboardHeader
        actions={
          <>
            {switcher}
            {grantKey && asked && (
              <DashboardAccess
                grantKey={grantKey}
                asked={asked}
                // The bar does not ask for what was just taken back, and the
                // frame starts over without what it may no longer read.
                onRevoked={() => setNotNowFor(grantKey)}
                onDone={reloadFrame}
              />
            )}
            <HeaderButton label={t('customDashboard.edit')} onClick={() => agent.edit(dashboard)}>
              <Pencil size={16} />
            </HeaderButton>
            <HeaderButton label={t('customDashboard.reload')} onClick={reload}>
              <RefreshCw size={16} />
            </HeaderButton>
            {!page && (
              // A page opened on its own has no LangAlpha around it to answer for its data.
              <HeaderButton
                label={t('filePanel.openInBrowser')}
                className="hidden sm:flex"
                onClick={() => window.open(shareLinkHref(dashboard.code, dashboard.path), '_blank', 'noopener,noreferrer')}
              >
                <ExternalLink size={16} />
              </HeaderButton>
            )}
          </>
        }
      />
      <div className="relative min-h-0 flex-1 overflow-hidden">
        {layers.map((layer) => {
          const shown = layer === onScreen;
          return (
            <FrameLayer
              key={`${frameKey}:${layer.url}`}
              url={layer.url}
              sandbox={layer.sandbox}
              title={title}
              shown={shown}
              settle={front !== null}
              onReady={() => setFront(layer)}
              place={placeFor}
              onScroll={(at) => {
                // A layer loading behind is only being put in place.
                if (shown) place.current = { code: dashboard.code, ...at };
              }}
              bridge={{
                grantKey: layer.grantKey,
                subject: page ? 'page' : 'app',
                active: shown,
                consent: {
                  dismissed: notNowFor !== null && notNowFor === layer.grantKey,
                  onDismiss: () => setNotNowFor(layer.grantKey),
                },
                // A frame loading again says first that nothing has asked
                // yet: what the last document asked stands until this one says.
                onAsked: (scopes) => setAsked((held) => (scopes === null && front === null ? held : scopes)),
              }}
            />
          );
        })}
        {front === null && (
          // A cold computer takes a while: the read waits for it to start and
          // for the app's command to answer on its port.
          <div className="absolute inset-0 flex flex-col items-center justify-center gap-3 px-6 text-center">
            <Loader size={20} style={{ color: 'var(--color-accent-primary)' }} />
            <div className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>
              {t(page ? 'customDashboard.startingPage' : 'customDashboard.starting', { name: title })}
            </div>
            {slow && !url && (
              <button
                type="button"
                onClick={onSetAside}
                className="text-xs underline-offset-2 hover:underline"
                style={{ color: 'var(--color-text-tertiary)' }}
              >
                {t('customDashboard.showBuiltIn')}
              </button>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

/** One address the dashboard frames, with what came with it. */
interface Layer {
  url: string;
  sandbox: string;
  grantKey: string | null;
}

interface FrameLayerProps {
  url: string;
  sandbox: string;
  title: string;
  /** On screen; a layer that is not is a renewal loading behind it. */
  shown: boolean;
  /** Wait for the document to ask for its data before taking the screen. */
  settle: boolean;
  onReady: () => void;
  /** Where the document should open. */
  place: () => ScrollPlace;
  onScroll: (at: ScrollPlace) => void;
  bridge: BridgeOptions;
}

/** One address's frame and the bridge answering it. */
function FrameLayer({ url, sandbox, title, shown, settle, onReady, place, onScroll, bridge }: FrameLayerProps): React.ReactElement {
  const app = useAppFrame(url, bridge, sandbox);
  const [loaded, setLoaded] = useState(false);
  const scroll = usePageScroll({ frameRef: app.frame.ref, src: url, sandbox, place, onScroll });
  const ready = useEffectEvent(() => {
    // Asked at its hello too; the reader may have moved since.
    if (!shown) scroll.restore();
    onReady();
  });

  useEffect(() => {
    if (!loaded) return;
    if (!settle) {
      ready();
      return;
    }
    const timer = setTimeout(ready, SETTLE_MS);
    return () => clearTimeout(timer);
  }, [loaded, settle]);

  return (
    <div className={cn('absolute inset-0 flex flex-col', !shown && 'invisible')} aria-hidden={!shown} inert={!shown}>
      {app.bridge}
      <div className="relative min-h-0 flex-1">
        <HostedFrame
          {...app.frame}
          title={title}
          onLoad={() => setLoaded(true)}
          className="block h-full w-full border-0"
          style={{ backgroundColor: 'var(--color-bg-page)' }}
        />
      </div>
    </div>
  );
}

function HeaderButton({
  label,
  onClick,
  className,
  children,
}: {
  label: string;
  onClick: () => void;
  className?: string;
  children: React.ReactNode;
}): React.ReactElement {
  return (
    <button
      type="button"
      onClick={onClick}
      className={cn('tap-target flex h-9 w-9 shrink-0 items-center justify-center rounded-md transition-colors hover:bg-(--color-bg-hover)', className)}
      style={{ color: 'var(--color-text-secondary)' }}
      title={label}
      aria-label={label}
    >
      {children}
    </button>
  );
}
