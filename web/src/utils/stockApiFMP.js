import axios from 'axios';

// Request cache to avoid duplicate calls
const requestCache = new Map();
const lastRequestTime = {};
const pendingRequests = new Map(); // Prevent duplicate requests

// Delay helper
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Vite env variable
const getApiKey = () => import.meta.env.VITE_FMP_API_KEY;

/**
 * Fetch stock OHLC data - uses Financial Modeling Prep API (free tier)
 * Free tier uses /stable/ endpoint
 * @param {string} symbol - Stock symbol (e.g. MSFT, AAPL)
 * @param {string} interval - Time interval (FMP free tier mainly supports daily)
 */
export const fetchStockData = async (symbol, interval = '1day') => {
  try {
    const API_KEY = getApiKey();

    console.log('FMP API Key check:', API_KEY ? 'configured' : 'not configured');

    if (!API_KEY || API_KEY === 'your_api_key_here') {
      throw new Error('FMP API key not configured. Please set VITE_FMP_API_KEY in .env');
    }

    // Check cache (valid for 5 minutes)
    const cacheKey = `${symbol}_${interval}`;
    const cached = requestCache.get(cacheKey);
    if (cached && Date.now() - cached.timestamp < 5 * 60 * 1000) {
      console.log('Using cached data:', symbol);
      if (Array.isArray(cached.data)) {
        return { data: cached.data, isReal: true };
      }
      return cached.data;
    }

    if (pendingRequests.has(cacheKey)) {
      console.log('Waiting for in-progress request:', symbol);
      const pendingResult = await pendingRequests.get(cacheKey);
      if (Array.isArray(pendingResult)) {
        return { data: pendingResult, isReal: true };
      }
      return pendingResult;
    }

    const now = Date.now();
    const lastTime = lastRequestTime['_global'] || 0;
    const timeSinceLastRequest = now - lastTime;

    if (timeSinceLastRequest < 1000) {
      const waitTime = 1000 - timeSinceLastRequest;
      console.log(`Waiting ${waitTime}ms to avoid rate limit...`);
      await delay(waitTime);
    }

    const url = `https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=${symbol}&apikey=${API_KEY}`;

    console.log('Requesting stock data (FMP):', symbol);

    const requestPromise = (async () => {
      try {
        lastRequestTime['_global'] = Date.now();
        const response = await axios.get(url);
        const raw = response.data;
        console.log('FMP API response type:', Array.isArray(raw) ? `array(len=${raw.length})` : typeof raw);

        if (raw && !Array.isArray(raw) && raw['Error Message']) {
          const errorMsg = raw['Error Message'];
          console.error('FMP API error:', errorMsg);
          throw new Error(`FMP API error: ${errorMsg}`);
        }

        const historicalData = Array.isArray(raw) ? raw : raw?.historical;
        if (!historicalData || !Array.isArray(historicalData) || historicalData.length === 0) {
          throw new Error('Failed to fetch stock data (FMP returned empty or unexpected format)');
        }

        const data = historicalData
          .map((dayData) => {
            const dateObj = new Date(dayData.date);
            return {
              time: Math.floor(dateObj.getTime() / 1000),
              open: parseFloat(dayData.open),
              high: parseFloat(dayData.high),
              low: parseFloat(dayData.low),
              close: parseFloat(dayData.close),
            };
          })
          .filter(item =>
            !isNaN(item.open) && !isNaN(item.high) && !isNaN(item.low) && !isNaN(item.close)
          )
          .sort((a, b) => a.time - b.time);

        if (data.length === 0) {
          throw new Error('Data conversion failed, no valid data points');
        }

        const result = { data, isReal: true };
        requestCache.set(cacheKey, {
          data: result,
          timestamp: Date.now()
        });

        console.log(`[Real data] Fetched ${data.length} data points for ${symbol} (FMP API)`);
        return { data, isReal: true };
      } finally {
        pendingRequests.delete(cacheKey);
      }
    })();

    pendingRequests.set(cacheKey, requestPromise);
    return await requestPromise;
  } catch (error) {
    console.error('Failed to fetch stock data (FMP):', error);

    const cacheKey = `${symbol}_${interval}`;
    pendingRequests.delete(cacheKey);

    console.warn('[Mock data] FMP API failed, using mock data as fallback. Error:', error.message || error);
    const mockData = generateMockData(symbol);
    return { data: mockData, isReal: false, error: error.message };
  }
};

/**
 * Generate mock OHLC data (for demo or API failure fallback)
 */
const generateMockData = (symbol) => {
  const data = [];
  const basePrice = 100 + Math.random() * 50;
  let currentPrice = basePrice;
  const today = new Date();

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
};

/**
 * Fetch real-time stock price (for live updates)
 */
export const fetchRealTimePrice = async (symbol) => {
  try {
    const API_KEY = getApiKey();
    if (!API_KEY || API_KEY === 'your_api_key_here') {
      throw new Error('FMP API key not configured');
    }

    const cacheKey = `quote_${symbol}`;
    const cached = requestCache.get(cacheKey);
    if (cached && Date.now() - cached.timestamp < 60 * 1000) {
      console.log('Using cached real-time price:', symbol);
      return cached.data;
    }

    if (pendingRequests.has(cacheKey)) {
      console.log('Waiting for in-progress real-time price request:', symbol);
      return await pendingRequests.get(cacheKey);
    }

    const now = Date.now();
    const lastTime = lastRequestTime['_global'] || 0;
    const timeSinceLastRequest = now - lastTime;

    if (timeSinceLastRequest < 1000) {
      const waitTime = 1000 - timeSinceLastRequest;
      await delay(waitTime);
    }

    const url = `https://financialmodelingprep.com/stable/quote?symbol=${symbol}&apikey=${API_KEY}`;

    const requestPromise = (async () => {
      try {
        lastRequestTime['_global'] = Date.now();
        const response = await axios.get(url);

        if (response.data['Error Message']) {
          throw new Error(`FMP API error: ${response.data['Error Message']}`);
        }

        const quote = Array.isArray(response.data) ? response.data[0] : response.data;

        if (!quote || !quote.price) {
          throw new Error('Failed to fetch real-time price');
        }

        const result = {
          symbol: quote.symbol,
          price: parseFloat(quote.price),
          open: parseFloat(quote.open),
          high: parseFloat(quote.dayHigh),
          low: parseFloat(quote.dayLow),
          volume: parseFloat(quote.volume),
          previousClose: parseFloat(quote.previousClose),
          change: parseFloat(quote.change),
          changePercent: typeof quote.changesPercentage === 'number' ? `${quote.changesPercentage.toFixed(2)}%` : '0.00%',
          latestTradingDay: quote.timestamp ? new Date(quote.timestamp * 1000).toISOString().split('T')[0] : new Date().toISOString().split('T')[0],
        };

        requestCache.set(cacheKey, {
          data: result,
          timestamp: Date.now()
        });

        return result;
      } finally {
        pendingRequests.delete(cacheKey);
      }
    })();

    pendingRequests.set(cacheKey, requestPromise);
    return await requestPromise;
  } catch (error) {
    console.error('Failed to fetch real-time price (FMP):', error);
    pendingRequests.delete(`quote_${symbol}`);
    throw error;
  }
};

/**
 * Fetch stock profile info
 */
export const fetchStockInfo = async (symbol) => {
  try {
    const API_KEY = getApiKey();
    if (!API_KEY || API_KEY === 'your_api_key_here') {
      throw new Error('FMP API key not configured');
    }

    const cacheKey = `profile_${symbol}`;
    const cached = requestCache.get(cacheKey);
    if (cached && Date.now() - cached.timestamp < 10 * 60 * 1000) {
      return cached.data;
    }

    if (pendingRequests.has(cacheKey)) {
      return await pendingRequests.get(cacheKey);
    }

    const now = Date.now();
    const lastTime = lastRequestTime['_global'] || 0;
    if (now - lastTime < 1000) {
      await delay(1000 - (now - lastTime));
    }

    const url = `https://financialmodelingprep.com/stable/profile?symbol=${symbol}&apikey=${API_KEY}`;

    const requestPromise = (async () => {
      try {
        lastRequestTime['_global'] = Date.now();
        const response = await axios.get(url);

        if (response.data['Error Message']) {
          throw new Error(`FMP API error: ${response.data['Error Message']}`);
        }

        const profile = Array.isArray(response.data) ? response.data[0] : response.data;

        if (!profile) {
          throw new Error('Failed to fetch stock info');
        }

        const result = {
          Symbol: profile.symbol,
          Name: profile.companyName,
          Exchange: profile.exchangeShortName || profile.exchange,
          Price: profile.price || 0,
          Open: profile.open || 0,
          High: profile.dayHigh || profile.high || 0,
          Low: profile.dayLow || profile.low || 0,
          '52WeekHigh': profile.range?.split('-')?.[1] || profile.yearHigh || 0,
          '52WeekLow': profile.range?.split('-')?.[0] || profile.yearLow || 0,
          AverageVolume: profile.volAvg || 0,
          SharesOutstanding: profile.sharesOutstanding || 0,
          MarketCapitalization: profile.mktCap || 0,
          DividendYield: profile.lastDiv ? (profile.lastDiv / profile.price) : 0,
        };

        requestCache.set(cacheKey, {
          data: result,
          timestamp: Date.now()
        });

        return result;
      } finally {
        pendingRequests.delete(cacheKey);
      }
    })();

    pendingRequests.set(cacheKey, requestPromise);
    return await requestPromise;
  } catch (error) {
    console.error('Failed to fetch stock info (FMP):', error);
    pendingRequests.delete(`profile_${symbol}`);
    throw error;
  }
};
