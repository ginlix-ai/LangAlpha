import React from 'react';
import { Plus } from 'lucide-react';

const Watchlist = () => {
  const watchlistItems = [
    { symbol: 'AAPL', name: 'Apple', price: '125', change: '6.36%', isPositive: true },
    { symbol: 'UBER', name: 'Uber', price: '80', change: '-3.84%', isPositive: false },
    { symbol: 'AAPL', name: 'Apple', price: '125', change: '6.36%', isPositive: true },
    { symbol: 'UBER', name: 'Uber', price: '80', change: '-3.84%', isPositive: false },
    { symbol: 'UBER', name: 'Uber', price: '80', change: '-3.84%', isPositive: false },
    { symbol: 'UBER', name: 'Uber', price: '80', change: '-3.84%', isPositive: false },
    { symbol: 'AAPL', name: 'Apple', price: '125', change: '6.36%', isPositive: true },
    { symbol: 'AAPL', name: 'Apple', price: '125', change: '6.36%', isPositive: true },
  ];

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold">Create watchlist</h2>
        <Plus className="h-5 w-5 text-muted-foreground cursor-pointer hover:text-foreground" />
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border">
              <th className="text-left py-2 px-2 text-muted-foreground font-medium">Symbol</th>
              <th className="text-left py-2 px-2 text-muted-foreground font-medium">Name</th>
              <th className="text-left py-2 px-2 text-muted-foreground font-medium">Price</th>
              <th className="text-left py-2 px-2 text-muted-foreground font-medium">Change</th>
            </tr>
          </thead>
          <tbody>
            {watchlistItems.map((item, idx) => (
              <tr key={idx} className="border-b border-border hover:bg-accent/50 cursor-pointer transition-colors">
                <td className="py-2 px-2 font-medium">{item.symbol}</td>
                <td className="py-2 px-2 text-muted-foreground">{item.name}</td>
                <td className="py-2 px-2">{item.price}</td>
                <td className={`py-2 px-2 ${item.isPositive ? 'text-green-500' : 'text-red-500'}`}>
                  {item.isPositive ? '+' : ''}{item.change}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default Watchlist;
