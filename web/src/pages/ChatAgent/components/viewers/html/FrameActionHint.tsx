import { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import './FrameActionHint.css';

/** How long "click again" stays once the click would count. */
const READY_SHOWN_MS = 3000;
const TICK_MS = 250;

/**
 * Says a click in the widget did not count yet, and when one will: a refusal
 * inside the activation window (`retryAfterRefusal`) is otherwise silent, and
 * reads as a broken button. Drawn by the host over the frame's own box, so
 * the widget cannot draw or hide it.
 */
export default function FrameActionHint({ retryAt }: { retryAt: number | null }) {
  // Keyed, so a new refusal starts its own countdown from now.
  return retryAt == null ? null : <Countdown key={retryAt} until={retryAt} />;
}

function Countdown({ until }: { until: number }) {
  const { t } = useTranslation();
  const [now, setNow] = useState(() => performance.now());
  useEffect(() => {
    const timer = setInterval(() => {
      const at = performance.now();
      setNow(at);
      if (at >= until + READY_SHOWN_MS) clearInterval(timer);
    }, TICK_MS);
    return () => clearInterval(timer);
  }, [until]);

  if (now >= until + READY_SHOWN_MS) return null;
  const seconds = Math.ceil((until - now) / 1000);
  return (
    <div className="frame-action-hint">
      <span aria-hidden="true">
        {seconds > 0 ? t('filePanel:widgetClickAgainIn', { count: seconds }) : t('filePanel:widgetClickAgain')}
      </span>
      {/* A stable line for screen readers, so the countdown is not read out each second. */}
      <span className="sr-only" role="status">{t('filePanel:widgetClickRefused')}</span>
    </div>
  );
}
