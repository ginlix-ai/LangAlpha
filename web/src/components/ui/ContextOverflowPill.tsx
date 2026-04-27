import { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router-dom';
import { Sparkles, ArrowUp } from 'lucide-react';
import { ChatInputRegistry, ContextBus } from '@/lib/contextBus';
import type { WidgetContextSnapshot } from '@/pages/Dashboard/widgets/framework/contextSnapshot';
import './ContextOverflowPill.css';

/**
 * Floating pill rendered at the app shell. Surfaces the widget context deck
 * count *only* when zero chat inputs are in the viewport — otherwise the
 * deck is already visible above one of those inputs and showing the pill
 * would be redundant noise.
 *
 * Click behavior:
 *   - If a registered chat input exists, smooth-scroll to the nearest one.
 *   - Otherwise, navigate to the default chat thread with state so the chat
 *     input there can re-seed its deck via `addWidgetSnapshot` on mount.
 */
export function ContextOverflowPill() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const [snapshots, setSnapshots] = useState<WidgetContextSnapshot[]>([]);
  const [anyChatVisible, setAnyChatVisible] = useState(true);

  // Mirror the bus locally so we know when there's anything to surface.
  useEffect(() => {
    const off = ContextBus.subscribe((event) => {
      if (event.type === 'attach') {
        setSnapshots((prev) => {
          if (prev.some((s) => s.widget_id === event.snapshot.widget_id)) return prev;
          return [...prev, event.snapshot];
        });
      } else if (event.type === 'detach') {
        setSnapshots((prev) => prev.filter((s) => s.widget_id !== event.widgetId));
      } else if (event.type === 'clear') {
        setSnapshots([]);
      }
    });
    return off;
  }, []);

  // Track which registered chat inputs are in the viewport. We rebuild the
  // observer when the registry membership changes so newly-mounted inputs
  // start being observed immediately.
  useEffect(() => {
    let observer: IntersectionObserver | null = null;
    const visibilityByEl = new Map<Element, boolean>();
    const recompute = () => {
      const someVisible = [...visibilityByEl.values()].some(Boolean);
      setAnyChatVisible(someVisible);
    };

    const setup = () => {
      observer?.disconnect();
      visibilityByEl.clear();
      const els = ChatInputRegistry.list();
      if (els.length === 0) {
        setAnyChatVisible(false);
        return;
      }
      observer = new IntersectionObserver(
        (entries) => {
          entries.forEach((entry) => {
            visibilityByEl.set(entry.target, entry.isIntersecting);
          });
          recompute();
        },
        { threshold: 0.01 },
      );
      els.forEach((el) => {
        visibilityByEl.set(el, false);
        observer!.observe(el);
      });
    };

    setup();
    const off = ChatInputRegistry.subscribe(setup);
    return () => {
      observer?.disconnect();
      off();
    };
  }, []);

  if (snapshots.length === 0 || anyChatVisible) return null;

  const handleClick = () => {
    const els = ChatInputRegistry.list();
    if (els.length > 0) {
      els[0].scrollIntoView({ behavior: 'smooth', block: 'center' });
      return;
    }
    // Strip image data URLs — chip rendering doesn't need them and a couple of
    // chart captures can blow past Firefox's 640KB structured-clone ceiling.
    const lite = snapshots.map(({ image_jpeg_data_url: _img, ...rest }) => rest);
    navigate('/chat/t/__default__', { state: { widgetSnapshots: lite } });
  };

  return (
    <button
      type="button"
      className="context-overflow-pill"
      onClick={handleClick}
      aria-label={t('chat.widgetContext.overflowAria', {
        count: snapshots.length,
        defaultValue: '{{count}} item(s) in context — open chat',
      })}
    >
      <Sparkles className="h-3.5 w-3.5" />
      <span className="context-overflow-pill__count">{snapshots.length}</span>
      <span className="context-overflow-pill__label">
        {t('chat.widgetContext.overflowLabel', { defaultValue: 'in context' })}
      </span>
      <ArrowUp className="h-3 w-3" />
    </button>
  );
}
