import React from 'react';
import IndexCard from './IndexCard';

const IndexMovement = () => {
  const indices = [
    { name: 'S&P 500 ETF', value: '509.90', change: '-3.05', changePercent: '-0.40', isPositive: false },
    { name: 'S&P 500 ETF', value: '509.90', change: '3.05', changePercent: '0.40', isPositive: true },
    { name: 'S&P 500 ETF', value: '509.90', change: '-3.05', changePercent: '-0.40', isPositive: false },
    { name: 'S&P 500 ETF', value: '509.90', change: '-3.05', changePercent: '-0.40', isPositive: false },
    { name: 'S&P 500 ETF', value: '509.90', change: '-3.05', changePercent: '-0.40', isPositive: false },
  ];

  return (
    <div className="space-y-4">
      <div>
        <h2 className="text-2xl font-semibold">Index Movement</h2>
        <p className="text-sm text-muted-foreground">Some summary words</p>
      </div>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
        {indices.map((index, idx) => (
          <IndexCard key={idx} {...index} />
        ))}
      </div>
    </div>
  );
};

export default IndexMovement;
