import React from 'react';
import { Menu, Zap } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import i18n from '@/i18n';

interface NewsItem {
  title: string;
  time: string;
  isHot: boolean;
}

const TopNews: React.FC = () => {
  const { t } = useTranslation();
  const newsItems: NewsItem[] = [
    { title: t('dashboard.topNews.sample1'), time: i18n.t('dashboard.widgets.common.relativePast', { when: '10m' }), isHot: true },
    { title: t('dashboard.topNews.sample2'), time: i18n.t('dashboard.widgets.common.relativePast', { when: '2m' }), isHot: false },
    { title: t('dashboard.topNews.sample1'), time: i18n.t('dashboard.widgets.common.relativePast', { when: '10m' }), isHot: true },
    { title: t('dashboard.topNews.sample1'), time: i18n.t('dashboard.widgets.common.relativePast', { when: '10m' }), isHot: true },
    { title: t('dashboard.topNews.sample3'), time: i18n.t('dashboard.widgets.common.relativePast', { when: '12h' }), isHot: false },
    { title: t('dashboard.topNews.sample4'), time: i18n.t('dashboard.widgets.common.relativePast', { when: '22h' }), isHot: false },
  ];

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-semibold">{t('dashboard.topNews.title')}</h2>
        <Menu className="h-5 w-5 text-muted-foreground cursor-pointer" />
      </div>
      <div className="space-y-2">
        {newsItems.map((item, idx) => (
          <div 
            key={idx} 
            className="flex items-center justify-between p-3 rounded-lg hover:bg-accent cursor-pointer transition-colors"
          >
            <div className="flex items-center space-x-3 flex-1">
              {item.isHot && <Zap className="h-4 w-4 text-primary" />}
              <div className="flex-1">
                <p className="text-sm font-medium">{item.title}</p>
                <p className="text-xs text-muted-foreground">{item.time}</p>
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

export default TopNews;
