import React from 'react';
import { Menu, Zap } from 'lucide-react';

const TopNews = () => {
  const newsItems = [
    { title: 'Retail Sales Slump Takes Toll on Market,...', time: '10 min ago', isHot: true },
    { title: "Tech Giant's Earnings Soar, Stock Hits All-Ti...", time: '2 min ago', isHot: false },
    { title: 'Retail Sales Slump Takes Toll on Market,...', time: '10 min ago', isHot: true },
    { title: 'Retail Sales Slump Takes Toll on Market,...', time: '10 min ago', isHot: true },
    { title: 'High-Profile IPO Falls Short of Expectations...', time: '12 hrs ago', isHot: false },
    { title: 'Electric Vehicle Stocks Skyrocket as Deman...', time: '22 hrs ago', isHot: false },
  ];

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-semibold">Top News</h2>
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
