/**
 * TradingCenter API utilities
 * All backend endpoints used by the TradingCenter page
 */
import { api, headers, DEFAULT_USER_ID } from '@/api/client';

/**
 * Fetch stock historical data for charting
 * Uses backend API endpoint: GET /api/v1/market-data/intraday/stocks/{symbol}
 * 
 * @param {string} symbol - Stock symbol (e.g., 'AAPL', 'MSFT')
 * @param {string} interval - Data interval (default: '1hour' for daily-like view, supports: 1min, 5min, 15min, 30min, 1hour, 4hour)
 * @returns {Promise<{data: Array, isReal: boolean, error?: string}>} Chart data in lightweight-charts format
 */
export async function fetchStockData(symbol, interval = '1hour') {
  if (!symbol || !symbol.trim()) {
    return { data: [], isReal: false, error: 'Symbol is required' };
  }

  const symbolUpper = symbol.trim().toUpperCase();
  
  try {
    // Use backend API endpoint for intraday data
    // For daily-like view, we use 1hour interval and fetch recent data
    const { data } = await api.get(`/api/v1/market-data/intraday/stocks/${encodeURIComponent(symbolUpper)}`, {
      params: {
        interval: interval === '1day' ? '1hour' : interval, // Map 1day to 1hour for backend
      },
    });

    const dataPoints = data?.data || [];
    
    if (!Array.isArray(dataPoints) || dataPoints.length === 0) {
      return { data: [], isReal: false, error: 'No data available' };
    }

    // Convert backend format to lightweight-charts format
    // Backend returns: { date: "YYYY-MM-DD HH:MM:SS", open, high, low, close, volume }
    // Chart needs: { time: unix_timestamp, open, high, low, close }
    const chartData = dataPoints.map((point) => {
      const date = new Date(point.date);
      return {
        time: Math.floor(date.getTime() / 1000), // Convert to unix timestamp
        open: parseFloat(point.open) || 0,
        high: parseFloat(point.high) || 0,
        low: parseFloat(point.low) || 0,
        close: parseFloat(point.close) || 0,
      };
    }).filter(item => 
      !isNaN(item.open) && 
      !isNaN(item.high) && 
      !isNaN(item.low) && 
      !isNaN(item.close) &&
      item.time > 0
    ).sort((a, b) => a.time - b.time);

    if (chartData.length === 0) {
      return { data: [], isReal: false, error: 'Data conversion failed' };
    }

    return { data: chartData, isReal: true };
  } catch (error) {
    console.error('Error fetching stock data from backend:', error);
    const errorMsg = error?.response?.data?.detail || error?.message || 'Failed to fetch stock data';
    
    // Return mock data as fallback
    const mockData = generateMockData(symbolUpper);
    return { data: mockData, isReal: false, error: errorMsg };
  }
}

/**
 * Fetch real-time stock price and quote information
 * Uses backend API endpoint: POST /api/v1/market-data/intraday/stocks (batch endpoint)
 * 
 * @param {string} symbol - Stock symbol
 * @returns {Promise<{price: number, change: number, changePercent: string, open: number, high: number, low: number}>}
 */
export async function fetchRealTimePrice(symbol) {
  if (!symbol || !symbol.trim()) {
    throw new Error('Symbol is required');
  }

  const symbolUpper = symbol.trim().toUpperCase();
  
  try {
    // Use batch endpoint to get latest price
    const { data } = await api.post('/api/v1/market-data/intraday/stocks', {
      symbols: [symbolUpper],
      interval: '1min',
    });

    const results = data?.results || {};
    const points = results[symbolUpper];
    
    if (!Array.isArray(points) || points.length === 0) {
      throw new Error('No price data available');
    }

    // Get first and last data points to calculate change
    const first = points[0];
    const last = points[points.length - 1];
    const open = parseFloat(first?.open || 0);
    const close = parseFloat(last?.close || 0);
    const high = parseFloat(last?.high || close);
    const low = parseFloat(last?.low || close);
    const change = close - open;
    const changePercent = open ? ((change / open) * 100).toFixed(2) + '%' : '0.00%';

    return {
      symbol: symbolUpper,
      price: Math.round(close * 100) / 100,
      open: Math.round(open * 100) / 100,
      high: Math.round(high * 100) / 100,
      low: Math.round(low * 100) / 100,
      change: Math.round(change * 100) / 100,
      changePercent,
    };
  } catch (error) {
    console.error('Error fetching real-time price:', error);
    throw error;
  }
}

/**
 * Fetch stock profile/company information
 * Note: This endpoint may need to be implemented in the backend
 * For now, returns basic info from quote data
 * 
 * @param {string} symbol - Stock symbol
 * @returns {Promise<Object>} Stock profile information
 */
export async function fetchStockInfo(symbol) {
  if (!symbol || !symbol.trim()) {
    throw new Error('Symbol is required');
  }

  const symbolUpper = symbol.trim().toUpperCase();
  
  try {
    // Use intraday endpoint to get basic info
    // In a full implementation, this would call a dedicated profile endpoint
    const { data } = await api.post('/api/v1/market-data/intraday/stocks', {
      symbols: [symbolUpper],
      interval: '1min',
    });

    const results = data?.results || {};
    const points = results[symbolUpper];
    
    if (!Array.isArray(points) || points.length === 0) {
      // Return default structure
      return {
        Symbol: symbolUpper,
        Name: `${symbolUpper} Corp`,
        Exchange: 'NASDAQ',
        Price: 0,
        Open: 0,
        High: 0,
        Low: 0,
        '52WeekHigh': 0,
        '52WeekLow': 0,
        AverageVolume: 0,
        SharesOutstanding: 0,
        MarketCapitalization: 0,
        DividendYield: 0,
      };
    }

    const last = points[points.length - 1];
    const first = points[0];
    
    // Extract basic info (full profile would come from a dedicated endpoint)
    return {
      Symbol: symbolUpper,
      Name: `${symbolUpper} Corp`, // Would come from profile endpoint
      Exchange: 'NASDAQ', // Would come from profile endpoint
      Price: parseFloat(last?.close || 0),
      Open: parseFloat(first?.open || 0),
      High: parseFloat(last?.high || 0),
      Low: parseFloat(last?.low || 0),
      '52WeekHigh': 0, // Would come from profile endpoint
      '52WeekLow': 0, // Would come from profile endpoint
      AverageVolume: 0, // Would come from profile endpoint
      SharesOutstanding: 0, // Would come from profile endpoint
      MarketCapitalization: 0, // Would come from profile endpoint
      DividendYield: 0, // Would come from profile endpoint
    };
  } catch (error) {
    console.error('Error fetching stock info:', error);
    // Return default structure on error
    return {
      Symbol: symbolUpper,
      Name: `${symbolUpper} Corp`,
      Exchange: 'NASDAQ',
      Price: 0,
      Open: 0,
      High: 0,
      Low: 0,
      '52WeekHigh': 0,
      '52WeekLow': 0,
      AverageVolume: 0,
      SharesOutstanding: 0,
      MarketCapitalization: 0,
      DividendYield: 0,
    };
  }
}

/**
 * Generate mock data for fallback when API fails
 * @param {string} symbol - Stock symbol
 * @returns {Array} Mock chart data
 */
function generateMockData(symbol) {
  const data = [];
  const basePrice = 100 + Math.random() * 50;
  let currentPrice = basePrice;
  const today = new Date();

  // Generate 90 days of mock data
  for (let i = 90; i >= 0; i--) {
    const date = new Date(today);
    date.setDate(date.getDate() - i);
    const timestamp = Math.floor(date.getTime() / 1000);

    const change = (Math.random() - 0.5) * 4;
    const open = currentPrice;
    const close = open + change;
    const high = Math.max(open, close) + Math.random() * 2;
    const low = Math.min(open, close) - Math.random() * 2;

    currentPrice = close;

    data.push({
      time: timestamp,
      open: parseFloat(open.toFixed(2)),
      high: parseFloat(high.toFixed(2)),
      low: parseFloat(low.toFixed(2)),
      close: parseFloat(close.toFixed(2)),
    });
  }

  return data;
}
