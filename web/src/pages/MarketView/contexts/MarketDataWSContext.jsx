import React, { createContext, useContext } from 'react';
import useMarketDataWS from '../hooks/useMarketDataWS';

const MarketDataWSContext = createContext(null);

export function MarketDataWSProvider({ children }) {
  const ws = useMarketDataWS();
  return (
    <MarketDataWSContext.Provider value={ws}>
      {children}
    </MarketDataWSContext.Provider>
  );
}

export function useMarketDataWSContext() {
  const ctx = useContext(MarketDataWSContext);
  if (!ctx) {
    throw new Error('useMarketDataWSContext must be used within <MarketDataWSProvider>');
  }
  return ctx;
}
