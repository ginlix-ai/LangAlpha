import React, { useRef, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { AlignEndHorizontal, Clock, Menu, Tag } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import i18n from '@/i18n';
import { Card, CardContent, CardHeader, CardTitle } from '../../../components/ui/card';

interface PopularItem {
  indexNumber?: string;
  title: string;
  description?: string;
  event_timestamp?: string;
  duration?: string;
  tags?: string[];
}

interface PopularCardProps {
  items?: PopularItem[];
  loading?: boolean;
  hasMore?: boolean;
  onLoadMore?: () => void;
}

function formatRelativeTime(timestamp: string): string {
  if (!timestamp) return '';
  const now = new Date();
  const then = new Date(timestamp);
  const diffMs = now.getTime() - then.getTime();
  const diffMin = Math.floor(diffMs / 60000);
  if (diffMin < 1) return i18n.t('dashboard.widgets.common.relativeNow');
  if (diffMin < 60) return i18n.t('dashboard.widgets.common.relativePast', { when: `${diffMin}m` });
  const diffHr = Math.floor(diffMin / 60);
  if (diffHr < 24) return i18n.t('dashboard.widgets.common.relativePast', { when: `${diffHr}h` });
  const diffDay = Math.floor(diffHr / 24);
  return i18n.t('dashboard.widgets.common.relativePast', { when: `${diffDay}d` });
}

function PopularCard({ items = [], loading = false, hasMore = false, onLoadMore }: PopularCardProps) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const loadingMore = useRef(false);

  const handleCardClick = (item: PopularItem) => {
    if (item.indexNumber) {
      navigate(`/detail/${item.indexNumber}`);
    }
  };

  const handleScroll = useCallback((e: React.UIEvent<HTMLDivElement>) => {
    if (!hasMore || !onLoadMore || loadingMore.current) return;
    const el = e.target as HTMLDivElement;
    if (el.scrollLeft + el.clientWidth >= el.scrollWidth - 100) {
      loadingMore.current = true;
      onLoadMore();
      setTimeout(() => { loadingMore.current = false; }, 1000);
    }
  }, [hasMore, onLoadMore]);

  return (
    <Card
      className="flex-shrink-0"
      style={{ background: 'var(--color-accent-gradient)', border: '1px solid var(--color-bg-card-border)', boxShadow: 'var(--shadow-card)', borderRadius: '4px' }}
    >
      <CardHeader className="px-5 py-4" style={{ paddingLeft: '20px', paddingRight: '24px', paddingTop: '16px', paddingBottom: '16px' }}>
        <div className="flex items-center justify-between">
          <CardTitle className="title-font text-base font-semibold" style={{ color: 'var(--color-text-primary)', letterSpacing: '0.15px', lineHeight: '24px' }}>
            {t('dashboard.popularCard.title')}
          </CardTitle>
          <Menu className="h-4 w-4 cursor-pointer transition-colors" style={{ color: 'var(--color-text-primary)' }} />
        </div>
      </CardHeader>
      <CardContent className="px-5 pt-0 pb-0" style={{ paddingLeft: '20px', paddingRight: '20px', paddingBottom: '20px' }}>
        <div
          className="flex gap-2.5 overflow-x-auto popular-scroll-hide"
          onScroll={handleScroll}
        >
          {loading
            ? Array.from({ length: 4 }).map((_, idx) => (
                <Card
                  key={idx}
                  className="flex-shrink-0"
                  style={{ backgroundColor: 'var(--color-bg-card)', border: '1px solid var(--color-border-default)', borderRadius: '6px', width: '220px' }}
                >
                  <CardContent className="p-3">
                    <div className="flex flex-col gap-2 animate-pulse">
                      <div className="h-4 rounded" style={{ backgroundColor: 'var(--color-border-default)', width: '60%' }} />
                      <div className="h-3 rounded" style={{ backgroundColor: 'var(--color-border-default)', width: '90%', marginTop: '8px' }} />
                      <div className="h-3 rounded" style={{ backgroundColor: 'var(--color-border-default)', width: '40%', marginTop: '8px' }} />
                    </div>
                  </CardContent>
                </Card>
              ))
            : items.map((item, idx) => (
                <Card
                  key={item.indexNumber || idx}
                  className="flex-shrink-0 cursor-pointer dashboard-popular-item overflow-hidden"
                  style={{
                    border: '1px solid var(--color-border-default)',
                    borderRadius: '6px',
                    boxSizing: 'border-box',
                    width: '220px',
                  }}
                  onClick={() => handleCardClick(item)}
                >
                  <CardContent className="p-3" style={{ backgroundColor: 'var(--color-bg-card)' }}>
                    <div className="flex flex-col gap-2">
                      <div className="flex items-center gap-2 min-w-0">
                        <div className="w-4 h-4 flex items-center justify-center flex-shrink-0">
                          <AlignEndHorizontal className="w-4 h-4" style={{ color: 'var(--color-text-primary)' }} />
                        </div>
                        <div className="flex items-center gap-2.5 flex-1 min-w-0">
                          <h3
                            className="font-semibold text-sm flex-1 min-w-0 truncate"
                            style={{ color: 'var(--color-text-primary)', letterSpacing: '0.1px', lineHeight: '20px' }}
                            title={item.title}
                          >
                            {item.title}
                          </h3>
                          <svg className="w-4 h-4 flex-shrink-0" style={{ color: 'var(--color-text-primary)', opacity: 0.65 }} fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                          </svg>
                        </div>
                      </div>
                      <div className="flex flex-col gap-2.5" style={{ paddingTop: '8px' }}>
                        <p
                          className="text-xs line-clamp-2 min-w-0"
                          style={{ color: 'var(--color-text-primary)', opacity: 0.65, letterSpacing: '0.4px', lineHeight: '16px' }}
                          title={item.description}
                        >
                          {item.description}
                        </p>
                        <div className="flex items-center gap-2 flex-wrap" style={{ paddingTop: '4px' }}>
                          {item.event_timestamp && (
                            <div className="flex items-center gap-1 px-2 py-0.5 rounded-md flex-shrink-0" style={{ backgroundColor: 'var(--color-bg-input, rgba(255,255,255,0.06))' }}>
                              <Clock className="w-3 h-3" style={{ color: 'var(--color-text-secondary)' }} />
                              <span className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>{formatRelativeTime(item.event_timestamp)}</span>
                            </div>
                          )}
                          {item.duration && !item.event_timestamp && (
                            <div className="flex items-center gap-1 px-2 py-0.5 rounded-md flex-shrink-0" style={{ backgroundColor: 'var(--color-bg-input, rgba(255,255,255,0.06))' }}>
                              <Clock className="w-3 h-3" style={{ color: 'var(--color-text-secondary)' }} />
                              <span className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>{item.duration}</span>
                            </div>
                          )}
                          {item.tags && item.tags.length > 0 && item.tags.slice(0, 2).map((tag, i) => (
                            <div key={i} className="flex items-center gap-1 px-2 py-0.5 rounded-md flex-shrink-0" style={{ backgroundColor: 'var(--color-bg-input, rgba(255,255,255,0.06))' }}>
                              <Tag className="w-3 h-3" style={{ color: 'var(--color-text-secondary)' }} />
                              <span className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>{tag}</span>
                            </div>
                          ))}
                        </div>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              ))}
        </div>
      </CardContent>
    </Card>
  );
}

export default PopularCard;
