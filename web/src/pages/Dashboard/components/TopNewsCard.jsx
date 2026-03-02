import React from 'react';
import { useNavigate } from 'react-router-dom';
import { Menu } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '../../../components/ui/card';
import { ScrollArea } from '../../../components/ui/scroll-area';
import { navigateToNewsItem } from '../utils/navigation';
import { getSourceColor } from '../utils/sourceColor';

/**
 * Top News list card (General News).
 * Data via props: items = [{ id, title, time, isHot, source?, favicon? }].
 */
function TopNewsCard({ items = [], loading = false }) {
  const navigate = useNavigate();

  const handleItemClick = (item) => navigateToNewsItem(navigate, item);
  return (
    <Card className="fin-card flex flex-col h-full min-h-0 overflow-hidden">
      <CardHeader
        className="px-6 py-4 flex-shrink-0"
        style={{ borderBottom: '1px solid var(--color-border-muted)' }}
      >
        <div className="flex items-center justify-between">
          <CardTitle
            className="dashboard-title-font text-base font-semibold"
            style={{ color: 'var(--color-text-primary)', letterSpacing: '0.15px' }}
          >
            Market
          </CardTitle>
          <Menu
            className="h-4 w-4 cursor-pointer transition-colors"
            style={{ color: 'var(--color-text-primary)' }}
          />
        </div>
      </CardHeader>
      <CardContent
        className="px-6 pt-0 pb-0 flex-1 min-h-0 overflow-hidden"
        style={{ display: 'flex', flexDirection: 'column' }}
      >
        <ScrollArea className="w-full flex-1 min-h-0">
          <div className="space-y-0">
            {loading
              ? Array.from({ length: 6 }).map((_, idx) => (
                  <div
                    key={idx}
                    className="flex items-center py-3 animate-pulse"
                    style={{ borderBottom: '1px solid var(--color-border-subtle)' }}
                  >
                    <div className="w-6 h-6 rounded flex-shrink-0 mr-2.5" style={{ backgroundColor: 'var(--color-border-default)' }} />
                    <div className="flex-1 min-w-0">
                      <div className="h-4 rounded" style={{ backgroundColor: 'var(--color-border-default)', width: `${60 + (idx % 3) * 15}%` }} />
                    </div>
                    <div className="h-3 rounded flex-shrink-0 ml-2.5" style={{ backgroundColor: 'var(--color-border-default)', width: '60px' }} />
                  </div>
                ))
              : items.map((item, idx) => {
                  const sourceColor = getSourceColor(item.source);
                  return (
                    <div
                      key={item.id || item.indexNumber || idx}
                      className={`flex items-center py-3 cursor-pointer dashboard-feed-row ${item.isHot ? 'dashboard-hot-accent' : ''}`}
                      style={{ borderBottom: '1px solid var(--color-border-subtle)' }}
                      onClick={() => handleItemClick(item)}
                    >
                      {/* Source badge */}
                      <div className="flex-shrink-0 mr-2.5" style={{ paddingLeft: item.isHot ? '8px' : '0' }}>
                        {item.favicon ? (
                          <div
                            className="dashboard-source-badge"
                            style={{ backgroundColor: 'var(--color-bg-tag)' }}
                          >
                            <img
                              src={item.favicon}
                              alt=""
                              className="w-3.5 h-3.5"
                              onError={(e) => {
                                e.target.style.display = 'none';
                                e.target.parentElement.textContent = (item.source || '?')[0];
                              }}
                            />
                          </div>
                        ) : (
                          <div
                            className="dashboard-source-badge"
                            style={{ backgroundColor: sourceColor.bg, color: sourceColor.color }}
                          >
                            {(item.source || '?')[0]}
                          </div>
                        )}
                      </div>

                      {/* Title */}
                      <div className="flex-1 min-w-0">
                        <p
                          className="text-sm font-medium text-left"
                          style={{
                            color: 'var(--color-text-primary)',
                            overflow: 'hidden',
                            textOverflow: 'ellipsis',
                            whiteSpace: 'nowrap',
                            display: 'block',
                          }}
                          title={item.title}
                        >
                          {item.title}
                        </p>
                      </div>

                      {/* Time */}
                      <p
                        className="dashboard-mono text-xs text-right flex-shrink-0 whitespace-nowrap ml-2.5"
                        style={{ color: 'var(--color-text-secondary)' }}
                      >
                        {item.time}
                      </p>
                    </div>
                  );
                })}
          </div>
        </ScrollArea>
      </CardContent>
    </Card>
  );
}

export default TopNewsCard;
