import React from 'react';
import { Menu } from 'lucide-react';

const TopResearch = () => {
  const researchItems = [
    { title: 'Retail Sales Slump Takes Toll on Market, Stocks Dip', time: '10 min ago' },
    { title: 'Retail Sales Slump Takes Toll on Market, Stocks Dip', time: '10 min ago' },
    { title: 'Retail Sales Slump Takes Toll on Market, Stocks Dip', time: '10 min ago' },
    { title: 'Retail Sales Slump Takes Toll on Market, Stocks Dip', time: '10 min ago' },
  ];

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-semibold">Top Research</h2>
        <Menu className="h-5 w-5 text-muted-foreground cursor-pointer" />
      </div>
      <div className="space-y-3">
        {researchItems.map((item, idx) => (
          <div 
            key={idx} 
            className="flex items-center space-x-4 p-3 rounded-lg hover:bg-accent cursor-pointer transition-colors"
          >
            <div className="w-16 h-16 bg-gradient-to-br from-primary/20 via-blue-500/20 to-pink-500/20 rounded-lg flex items-center justify-center flex-shrink-0">
              <div className="w-12 h-12 border-2 border-primary/50 rounded flex items-center justify-center">
                <div className="w-8 h-8 border border-primary/30 rounded"></div>
              </div>
            </div>
            <div className="flex-1">
              <p className="text-sm font-medium">{item.title}</p>
              <p className="text-xs text-muted-foreground mt-1">{item.time}</p>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

export default TopResearch;
