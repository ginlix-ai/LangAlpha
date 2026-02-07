import React from 'react';
import { useNavigate } from 'react-router-dom';
import { Menu, Sparkles } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '../../../components/ui/card';
import { ScrollArea } from '../../../components/ui/scroll-area';

/**
 * Top News list card. Data via props: items = [{ indexNumber, title, time, isHot }].
 */
function TopNewsCard({ items = [], loading = false }) {
  const navigate = useNavigate();

  const handleItemClick = (item) => {
    if (item.indexNumber) {
      navigate(`/detail/${item.indexNumber}`);
    }
  };
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
                    <div className="flex-1 min-w-0">
                      <div className="h-4 rounded" style={{ backgroundColor: 'var(--color-border-default)', width: `${60 + (idx % 3) * 15}%` }} />
                    </div>
                    <div className="h-3 rounded flex-shrink-0 ml-2.5" style={{ backgroundColor: 'var(--color-border-default)', width: '60px' }} />
                  </div>
                ))
              : items.map((item, idx) => (
              <div
                key={item.indexNumber || idx}
                className="flex items-center py-3 cursor-pointer transition-colors"
                style={{ borderBottom: '1px solid var(--color-border-subtle)' }}
                onClick={() => handleItemClick(item)}
              >
                {item.isHot ? (
                  <>
                    <div
                      className="flex items-center justify-center mr-2 ml-1 flex-shrink-0"
                      style={{
                        padding: '2px 5px',
                        gap: '4px',
                        width: '22px',
                        height: '22px',
                        background:
                          'linear-gradient(0deg, var(--color-accent-overlay), var(--color-accent-overlay)), var(--color-bg-tag)',
                        borderRadius: '6px',
                      }}
                    >
                      <Sparkles className="w-3 h-3" style={{ color: 'var(--color-text-primary)' }} />
                    </div>
                    <div className="flex-1 min-w-0">
                      <p
                        className="text-sm font-normal text-left"
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
                  </>
                ) : (
                  <div className="flex-1 min-w-0">
                    <p
                      className="text-sm font-normal text-left"
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
                )}
                <p
                  className="text-sm font-normal text-right flex-shrink-0 whitespace-nowrap ml-2.5"
                  style={{ color: 'var(--color-text-secondary)' }}
                >
                  {item.time}
                </p>
              </div>
            ))}
          </div>
        </ScrollArea>
      </CardContent>
    </Card>
  );
}

export default TopNewsCard;
