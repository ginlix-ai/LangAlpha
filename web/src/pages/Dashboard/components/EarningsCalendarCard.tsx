import React, { useEffect, useState, useCallback, useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import i18n from '@/i18n';
import { createPortal } from 'react-dom';
import { ChevronRight, X, Calendar } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import { getEarningsCalendar } from '../utils/api';

interface EarningsEntry {
  symbol: string;
  date: string;
  companyName?: string;
  [key: string]: unknown;
}

interface EarningsPreviewItem extends EarningsEntry {
  _isPast: boolean;
}

interface LogoFallbackProps {
  symbol: string;
}

interface EarningsItemProps {
  item: EarningsEntry;
  index: number;
  isPast?: boolean;
}

interface SectionLabelProps {
  label: string;
}

interface EarningsModalProps {
  earnings: EarningsEntry[];
  onClose: () => void;
}

interface DateGroup {
  date: string;
  items: EarningsEntry[];
}

interface DateTabInfo {
  weekday: string;
  label: string;
}

function LogoFallback({ symbol }: LogoFallbackProps) {
  return (
    <span className="font-bold text-xs" style={{ color: 'var(--color-text-primary)' }}>
      {(symbol || '??').substring(0, 2)}
    </span>
  );
}

function formatDate(dateStr: string | undefined): string {
  if (!dateStr) return '';
  return new Date(dateStr + 'T00:00:00').toLocaleDateString(i18n.language, { month: 'short', day: 'numeric' });
}

function EarningsItem({ item, index: _index, isPast }: EarningsItemProps) {
  const dateStr = formatDate(item.date);

  return (
    <div
      className="group flex items-center justify-between p-3 rounded-xl border border-transparent transition-all cursor-pointer"
      style={{ backgroundColor: 'transparent', opacity: isPast ? 0.6 : 1 }}
      onMouseEnter={(e) => {
        e.currentTarget.style.backgroundColor = 'var(--color-bg-hover)';
        e.currentTarget.style.borderColor = 'var(--color-border-muted)';
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.backgroundColor = 'transparent';
        e.currentTarget.style.borderColor = 'transparent';
      }}
    >
      <div className="flex items-center gap-3">
        <div
          className="w-10 h-10 rounded-lg flex items-center justify-center overflow-hidden border"
          style={{
            backgroundColor: 'var(--color-bg-tag)',
            borderColor: 'var(--color-border-muted)',
          }}
        >
          <LogoFallback symbol={item.symbol} />
        </div>
        <div>
          <div className="font-bold text-sm" style={{ color: 'var(--color-text-primary)' }}>
            {item.symbol}
          </div>
          <div className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
            {item.companyName || item.symbol}
          </div>
        </div>
      </div>

      <div className="text-right">
        <div className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
          {dateStr}
        </div>
      </div>
    </div>
  );
}

function SectionLabel({ label }: SectionLabelProps) {
  return (
    <div
      className="text-[10px] font-bold uppercase tracking-wider px-3 pt-2 pb-1"
      style={{ color: 'var(--color-text-secondary)' }}
    >
      {label}
    </div>
  );
}

function formatDateTab(dateStr: string | undefined): DateTabInfo {
  if (!dateStr) return { weekday: '', label: '' };
  const d = new Date(dateStr + 'T00:00:00');
  const weekday = d.toLocaleDateString(i18n.language, { weekday: 'short' });
  const month = d.toLocaleDateString(i18n.language, { month: 'short' });
  const day = d.getDate();
  return { weekday, label: `${month} ${day}` };
}

function EarningsModal({ earnings, onClose }: EarningsModalProps) {
  const { t } = useTranslation();
  const todayStr = new Date().toISOString().split('T')[0];

  // Group by date, sorted chronologically
  const dateGroups = useMemo((): DateGroup[] => {
    const groups: Record<string, EarningsEntry[]> = {};
    for (const e of earnings) {
      if (!e.date) continue;
      if (!groups[e.date]) groups[e.date] = [];
      groups[e.date].push(e);
    }
    return Object.keys(groups)
      .sort((a, b) => a.localeCompare(b))
      .map((date) => ({ date, items: groups[date] }));
  }, [earnings]);

  // Default to today's tab, or the nearest upcoming date
  const defaultDate = useMemo(() => {
    const todayGroup = dateGroups.find((g) => g.date === todayStr);
    if (todayGroup) return todayStr;
    const upcoming = dateGroups.find((g) => g.date > todayStr);
    if (upcoming) return upcoming.date;
    return dateGroups[dateGroups.length - 1]?.date || '';
  }, [dateGroups, todayStr]);

  const [activeDate, setActiveDate] = useState(defaultDate);

  const activeItems = useMemo(
    () => dateGroups.find((g) => g.date === activeDate)?.items || [],
    [dateGroups, activeDate]
  );

  const isPastDate = activeDate < todayStr;

  useEffect(() => {
    const handleEsc = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', handleEsc);
    return () => window.removeEventListener('keydown', handleEsc);
  }, [onClose]);

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      onClick={onClose}
      className="fixed inset-0 z-[1030] flex items-center justify-center p-4 md:p-8"
      style={{ backgroundColor: 'var(--color-bg-overlay, rgba(0,0,0,0.6))', backdropFilter: 'blur(4px)' }}
    >
      <motion.div
        initial={{ opacity: 0, scale: 0.95, y: 20 }}
        animate={{ opacity: 1, scale: 1, y: 0 }}
        exit={{ opacity: 0, scale: 0.95, y: 20 }}
        onClick={(e) => e.stopPropagation()}
        className="w-full max-w-4xl max-h-[80vh] rounded-3xl overflow-hidden shadow-2xl flex flex-col relative border"
        style={{
          backgroundColor: 'var(--color-bg-elevated)',
          borderColor: 'var(--color-border-muted)',
        }}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-6 pt-6 pb-4 flex-shrink-0">
          <h2 className="text-xl font-bold flex items-center gap-2" style={{ color: 'var(--color-text-primary)' }}>
            <Calendar size={20} style={{ color: 'var(--color-accent-light)' }} />
            {t('dashboard.widgets.earningsCalendar.title')}
          </h2>
          <button
            onClick={onClose}
            className="p-2 rounded-full transition-colors"
            style={{ color: 'var(--color-text-secondary)' }}
            onMouseEnter={(e) => { e.currentTarget.style.backgroundColor = 'var(--color-bg-hover)'; }}
            onMouseLeave={(e) => { e.currentTarget.style.backgroundColor = 'transparent'; }}
            aria-label={t('dashboard.earningsCalendarCard.close')}
          >
            <X size={18} />
          </button>
        </div>

        {/* Date tabs */}
        <div
          className="flex gap-1 px-6 pb-4 overflow-x-auto flex-shrink-0"
          style={{ scrollbarWidth: 'none' }}
        >
          {dateGroups.map((group) => {
            const isActive = group.date === activeDate;
            const isToday = group.date === todayStr;
            const isPast = group.date < todayStr;
            const { weekday, label } = formatDateTab(group.date);
            return (
              <button
                key={group.date}
                onClick={() => setActiveDate(group.date)}
                className="flex flex-col items-center px-4 py-2 rounded-xl text-xs font-medium transition-all flex-shrink-0 min-w-[72px] border"
                style={{
                  backgroundColor: isActive
                    ? 'var(--color-accent-primary)'
                    : 'var(--color-bg-tag)',
                  color: isActive
                    ? '#fff'
                    : isPast
                      ? 'var(--color-text-secondary)'
                      : 'var(--color-text-primary)',
                  borderColor: isActive
                    ? 'var(--color-accent-primary)'
                    : isToday
                      ? 'var(--color-accent-overlay)'
                      : 'transparent',
                  opacity: isPast && !isActive ? 0.7 : 1,
                }}
              >
                <span className="text-[10px] uppercase tracking-wider" style={{ opacity: 0.8 }}>
                  {weekday}
                </span>
                <span className="font-bold">{label}</span>
                <span className="text-[10px] mt-0.5" style={{ opacity: 0.7 }}>
                  {t('dashboard.earningsCalendarCard.stockCount', { count: group.items.length })}
                </span>
              </button>
            );
          })}
        </div>

        {/* Divider */}
        <div className="mx-6 border-b flex-shrink-0" style={{ borderColor: 'var(--color-border-muted)' }} />

        {/* Items grid */}
        <div className="overflow-y-auto flex-1 p-6">
          <AnimatePresence mode="wait">
            {activeItems.length === 0 ? (
              <motion.p
                key="empty"
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{ opacity: 0 }}
                className="text-sm py-8 text-center"
                style={{ color: 'var(--color-text-secondary)' }}
              >
                {t('dashboard.earningsCalendarCard.emptyOnDate')}
              </motion.p>
            ) : (
              <motion.div
                key={activeDate}
                initial={{ opacity: 0, y: 8 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -8 }}
                transition={{ duration: 0.2 }}
                className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3"
              >
                {activeItems.map((item, i) => (
                  <div
                    key={item.symbol + item.date + i}
                    className="flex items-center gap-3 p-3 rounded-xl border transition-all"
                    style={{
                      backgroundColor: 'var(--color-bg-card)',
                      borderColor: 'var(--color-border-muted)',
                      opacity: isPastDate ? 0.7 : 1,
                    }}
                    onMouseEnter={(e) => {
                      e.currentTarget.style.borderColor = 'var(--color-border-default)';
                    }}
                    onMouseLeave={(e) => {
                      e.currentTarget.style.borderColor = 'var(--color-border-muted)';
                    }}
                  >
                    <div
                      className="w-10 h-10 rounded-lg flex items-center justify-center overflow-hidden border flex-shrink-0"
                      style={{
                        backgroundColor: 'var(--color-bg-tag)',
                        borderColor: 'var(--color-border-muted)',
                      }}
                    >
                      <LogoFallback symbol={item.symbol} />
                    </div>
                    <div className="min-w-0">
                      <div className="font-bold text-sm truncate" style={{ color: 'var(--color-text-primary)' }}>
                        {item.symbol}
                      </div>
                      <div className="text-xs truncate" style={{ color: 'var(--color-text-secondary)' }}>
                        {item.companyName || item.symbol}
                      </div>
                    </div>
                  </div>
                ))}
              </motion.div>
            )}
          </AnimatePresence>
        </div>
      </motion.div>
    </motion.div>
  );
}

function EarningsCalendarCard() {
  const { t } = useTranslation();
  const [allEarnings, setAllEarnings] = useState<EarningsEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [modalOpen, setModalOpen] = useState(false);

  const fetchEarnings = useCallback(async () => {
    setLoading(true);
    try {
      const today = new Date();
      const from = new Date(today.getTime() - 5 * 86400000).toISOString().split('T')[0];
      const to = new Date(today.getTime() + 5 * 86400000).toISOString().split('T')[0];
      const result = await getEarningsCalendar({ from, to });
      // Filter to US-market symbols only (no dot-suffix like .BK, .TW, .L, .TO, etc.)
      const usOnly = ((result?.data || []) as EarningsEntry[]).filter((e) => e.symbol && !e.symbol.includes('.'));
      setAllEarnings(usOnly);
    } catch (err: unknown) {
      console.error('[EarningsCalendarCard] fetch failed:', (err as Error)?.message);
      setAllEarnings([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchEarnings();
  }, [fetchEarnings]);

  const todayStr = new Date().toISOString().split('T')[0];

  const { recent, upcoming } = useMemo(() => {
    const r = allEarnings.filter((e) => e.date < todayStr).sort((a, b) => b.date.localeCompare(a.date));
    const u = allEarnings.filter((e) => e.date >= todayStr).sort((a, b) => a.date.localeCompare(b.date));
    return { recent: r, upcoming: u };
  }, [allEarnings, todayStr]);

  // Card preview: show up to 3 upcoming + fill remaining from recent, max 6 total
  const previewItems = useMemo((): EarningsPreviewItem[] => {
    const upSlice = upcoming.slice(0, 3);
    const remaining = 6 - upSlice.length;
    const recSlice = recent.slice(0, remaining);
    return [
      ...recSlice.map((e) => ({ ...e, _isPast: true })),
      ...upSlice.map((e) => ({ ...e, _isPast: false })),
    ];
  }, [recent, upcoming]);

  return (
    <>
      <div className="dashboard-glass-card p-6 flex flex-col">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-xl font-bold" style={{ color: 'var(--color-text-primary)' }}>
            {t('dashboard.widgets.earningsCalendar.title')}
          </h2>
          <button
            onClick={() => setModalOpen(true)}
            className="text-xs flex items-center gap-1 transition-colors"
            style={{ color: 'var(--color-text-secondary)' }}
            onMouseEnter={(e) => (e.currentTarget.style.color = 'var(--color-text-primary)')}
            onMouseLeave={(e) => (e.currentTarget.style.color = 'var(--color-text-secondary)')}
          >
            {t('dashboard.earningsCalendarCard.viewAll')} <ChevronRight size={12} />
          </button>
        </div>

        <div className="flex flex-col gap-1">
          {loading ? (
            Array.from({ length: 4 }).map((_, i) => (
              <div key={i} className="flex items-center gap-3 p-3 animate-pulse">
                <div
                  className="w-10 h-10 rounded-lg"
                  style={{ backgroundColor: 'var(--color-border-default)' }}
                />
                <div className="flex-1">
                  <div
                    className="h-4 rounded mb-1"
                    style={{ backgroundColor: 'var(--color-border-default)', width: '50%' }}
                  />
                  <div
                    className="h-3 rounded"
                    style={{ backgroundColor: 'var(--color-border-default)', width: '70%' }}
                  />
                </div>
              </div>
            ))
          ) : previewItems.length === 0 ? (
            <p className="text-sm py-4 text-center" style={{ color: 'var(--color-text-secondary)' }}>
              {t('dashboard.earningsCalendarCard.emptyPeriod')}
            </p>
          ) : (
            <>
              {previewItems.some((e) => e._isPast) && <SectionLabel label={t('dashboard.earningsCalendarCard.sectionRecent')} />}
              {previewItems.filter((e) => e._isPast).map((item, i) => (
                <EarningsItem key={item.symbol + item.date + i} item={item} index={i} isPast />
              ))}
              {previewItems.some((e) => !e._isPast) && <SectionLabel label={t('dashboard.earningsCalendarCard.sectionUpcoming')} />}
              {previewItems.filter((e) => !e._isPast).map((item, i) => (
                <EarningsItem key={item.symbol + item.date + i} item={item} index={i} />
              ))}
            </>
          )}
        </div>
      </div>

      {createPortal(
        <AnimatePresence>
          {modalOpen && (
            <EarningsModal earnings={allEarnings} onClose={() => setModalOpen(false)} />
          )}
        </AnimatePresence>,
        document.body
      )}
    </>
  );
}

export default EarningsCalendarCard;
