import React from 'react';
import { Card, CardContent } from '../../../components/ui/card';
import { Menu } from 'lucide-react';

const WhatsPopular = () => {
  const popularItems = [
    { title: 'Comparison Report', description: 'A comprehensive analysis comparing industries.', duration: '20-30min', isHighlighted: true },
    { title: 'Comparison Report', description: 'A comprehensive analysis comparing industries.', duration: '20-30min', isHighlighted: false },
    { title: 'Comparison Report', description: 'A comprehensive analysis comparing industries.', duration: '20-30min', isHighlighted: false },
    { title: 'Comparison Report', description: 'A comprehensive analysis comparing industries.', duration: '20-30min', isHighlighted: false },
  ];

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-semibold">What's Popular</h2>
        <Menu className="h-5 w-5 text-muted-foreground cursor-pointer" />
      </div>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {popularItems.map((item, idx) => (
          <Card 
            key={idx} 
            className={`bg-card border-border cursor-pointer transition-all hover:shadow-lg ${
              item.isHighlighted ? 'border-primary border-2' : ''
            }`}
          >
            <CardContent className="p-4">
              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <div className="flex items-center space-x-2">
                    <div className="w-8 h-8 bg-primary/20 rounded flex items-center justify-center">
                      <div className="w-4 h-4 border-2 border-primary rounded"></div>
                    </div>
                    <h3 className="font-semibold">{item.title}</h3>
                  </div>
                  <svg className="w-5 h-5 text-muted-foreground" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                  </svg>
                </div>
                <p className="text-sm text-muted-foreground">{item.description}</p>
                <div className="flex items-center space-x-2 text-sm text-muted-foreground">
                  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                  <span>{item.duration}</span>
                </div>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  );
};

export default WhatsPopular;
