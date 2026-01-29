import axios from 'axios';

// 请求缓存，避免重复调用
const requestCache = new Map();
const lastRequestTime = {};
const pendingRequests = new Map(); // 防止重复请求

// 延迟函数
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Vite 环境变量
const getApiKey = () => import.meta.env.VITE_FMP_API_KEY;

/**
 * 获取股票K线数据 - 使用Financial Modeling Prep API（免费版）
 * 免费版使用 /stable/ 端点
 * @param {string} symbol - 股票代码（如：MSFT, AAPL等）
 * @param {string} interval - 时间间隔（FMP免费版主要支持日线）
 */
export const fetchStockData = async (symbol, interval = '1day') => {
  try {
    const API_KEY = getApiKey();

    console.log('FMP API Key检查:', API_KEY ? '已配置' : '未配置');

    if (!API_KEY || API_KEY === '你的FMP_API_KEY' || API_KEY === 'your_api_key_here') {
      throw new Error('FMP API key未配置，请在.env文件中设置VITE_FMP_API_KEY');
    }

    // 检查缓存（5分钟内有效）
    const cacheKey = `${symbol}_${interval}`;
    const cached = requestCache.get(cacheKey);
    if (cached && Date.now() - cached.timestamp < 5 * 60 * 1000) {
      console.log('使用缓存数据:', symbol);
      if (Array.isArray(cached.data)) {
        return { data: cached.data, isReal: true };
      }
      return cached.data;
    }

    if (pendingRequests.has(cacheKey)) {
      console.log('等待正在进行的请求:', symbol);
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
      console.log(`等待 ${waitTime}ms 以避免频率限制...`);
      await delay(waitTime);
    }

    const url = `https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=${symbol}&apikey=${API_KEY}`;

    console.log('请求股票数据 (FMP):', symbol);

    const requestPromise = (async () => {
      try {
        lastRequestTime['_global'] = Date.now();
        const response = await axios.get(url);
        const raw = response.data;
        console.log('FMP API响应类型:', Array.isArray(raw) ? `array(len=${raw.length})` : typeof raw);

        if (raw && !Array.isArray(raw) && raw['Error Message']) {
          const errorMsg = raw['Error Message'];
          console.error('FMP API错误:', errorMsg);
          throw new Error(`FMP API错误: ${errorMsg}`);
        }

        const historicalData = Array.isArray(raw) ? raw : raw?.historical;
        if (!historicalData || !Array.isArray(historicalData) || historicalData.length === 0) {
          throw new Error('无法获取股票数据（FMP返回空数据或格式不符合预期）');
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
          throw new Error('数据转换失败，没有有效的数据点');
        }

        const result = { data, isReal: true };
        requestCache.set(cacheKey, {
          data: result,
          timestamp: Date.now()
        });

        console.log(`✅ [真实数据] 成功获取 ${symbol} 的 ${data.length} 个数据点 (FMP API)`);
        return { data, isReal: true };
      } finally {
        pendingRequests.delete(cacheKey);
      }
    })();

    pendingRequests.set(cacheKey, requestPromise);
    return await requestPromise;
  } catch (error) {
    console.error('获取股票数据失败 (FMP):', error);

    const cacheKey = `${symbol}_${interval}`;
    pendingRequests.delete(cacheKey);

    console.warn('❌ [模拟数据] FMP API失败，使用模拟数据作为fallback。错误信息:', error.message || error);
    const mockData = generateMockData(symbol);
    return { data: mockData, isReal: false, error: error.message };
  }
};

/**
 * 生成模拟K线数据（用于演示或API失败时的fallback）
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
 * 获取实时股票价格（用于实时更新）
 */
export const fetchRealTimePrice = async (symbol) => {
  try {
    const API_KEY = getApiKey();
    if (!API_KEY || API_KEY === '你的FMP_API_KEY' || API_KEY === 'your_api_key_here') {
      throw new Error('FMP API key未配置');
    }

    const cacheKey = `quote_${symbol}`;
    const cached = requestCache.get(cacheKey);
    if (cached && Date.now() - cached.timestamp < 60 * 1000) {
      console.log('使用缓存的实时价格:', symbol);
      return cached.data;
    }

    if (pendingRequests.has(cacheKey)) {
      console.log('等待正在进行的实时价格请求:', symbol);
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
          throw new Error(`FMP API错误: ${response.data['Error Message']}`);
        }

        const quote = Array.isArray(response.data) ? response.data[0] : response.data;

        if (!quote || !quote.price) {
          throw new Error('无法获取实时价格');
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
    console.error('获取实时价格失败 (FMP):', error);
    pendingRequests.delete(`quote_${symbol}`);
    throw error;
  }
};

/**
 * 获取股票基本信息
 */
export const fetchStockInfo = async (symbol) => {
  try {
    const API_KEY = getApiKey();
    if (!API_KEY || API_KEY === '你的FMP_API_KEY' || API_KEY === 'your_api_key_here') {
      throw new Error('FMP API key未配置');
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
          throw new Error(`FMP API错误: ${response.data['Error Message']}`);
        }

        const profile = Array.isArray(response.data) ? response.data[0] : response.data;

        if (!profile) {
          throw new Error('无法获取股票信息');
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
    console.error('获取股票信息失败 (FMP):', error);
    pendingRequests.delete(`profile_${symbol}`);
    throw error;
  }
};
