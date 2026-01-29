import React, { useEffect, useRef, useState, useImperativeHandle, forwardRef } from 'react';
import { createChart, ColorType } from 'lightweight-charts';
import html2canvas from 'html2canvas';
import './TradingChart.css';
import { fetchStockData } from '../../../utils/stockApiFMP';

const TradingChart = forwardRef(({ symbol, onCapture }, ref) => {
  const chartContainerRef = useRef();
  const rsiChartContainerRef = useRef();
  const chartRef = useRef();
  const rsiChartRef = useRef();
  const candlestickSeriesRef = useRef();
  const ma50SeriesRef = useRef();
  const ma200SeriesRef = useRef();
  const rsiSeriesRef = useRef();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [lastUpdateTime, setLastUpdateTime] = useState(null);
  const [rsiValue, setRsiValue] = useState(null);
  const [ma50Value, setMa50Value] = useState(null);
  const [ma200Value, setMa200Value] = useState(null);
  const [isMockData, setIsMockData] = useState(false);

  const calculateMA = (data, period) => {
    const result = [];
    for (let i = 0; i < data.length; i++) {
      if (i >= period - 1) {
        let sum = 0;
        for (let j = i - period + 1; j <= i; j++) {
          sum += data[j].close;
        }
        const maValue = sum / period;
        if (!isNaN(maValue) && isFinite(maValue)) {
          result.push({
            time: data[i].time,
            value: maValue
          });
        }
      }
    }
    return result;
  };

  const calculateRSI = (data, period = 14) => {
    const result = [];
    const gains = [];
    const losses = [];

    for (let i = 1; i < data.length; i++) {
      const change = data[i].close - data[i - 1].close;
      gains.push(change > 0 ? change : 0);
      losses.push(change < 0 ? -change : 0);
    }

    for (let i = 0; i < data.length; i++) {
      if (i >= period) {
        const avgGain = gains.slice(i - period, i).reduce((a, b) => a + b, 0) / period;
        const avgLoss = losses.slice(i - period, i).reduce((a, b) => a + b, 0) / period;
        const rs = avgLoss === 0 ? 100 : avgGain / avgLoss;
        const rsi = 100 - (100 / (1 + rs));
        if (!isNaN(rsi) && isFinite(rsi)) {
          result.push({
            time: data[i].time,
            value: rsi
          });
        }
      }
    }
    return result;
  };

  useImperativeHandle(ref, () => ({
    captureChart: async () => {
      if (!chartContainerRef.current) return null;
      try {
        const canvas = await html2canvas(chartContainerRef.current, {
          backgroundColor: '#0f1422',
          scale: 2,
          logging: false,
        });
        return new Promise((resolve) => {
          canvas.toBlob((blob) => resolve(blob), 'image/png');
        });
      } catch (err) {
        console.error('截图失败:', err);
        return null;
      }
    }
  }));

  const loadData = async () => {
    setLoading(true);
    setError(null);
    setIsMockData(false);
    try {
      const result = await fetchStockData(symbol, '1day');
      const data = result && result.data ? result.data : result;
      const isReal = !(result && result.data && result.isReal === false);
      const errorMsg = result && result.data ? result.error : null;

      setIsMockData(!isReal);
      if (!isReal) {
        console.warn('⚠️ [模拟数据] 当前显示的是模拟数据。原因:', errorMsg || 'FMP API 调用失败');
      } else {
        console.log('✅ [真实数据] 当前显示的是真实数据 (FMP API)');
      }

      if (data && Array.isArray(data) && data.length > 0) {
        candlestickSeriesRef.current.setData(data);

        const ma50Data = calculateMA(data, 50);
        ma50SeriesRef.current.setData(ma50Data);
        const lastMa50 = ma50Data[ma50Data.length - 1]?.value;
        if (lastMa50) {
          setMa50Value(lastMa50.toFixed(2));
        }

        const ma200Data = calculateMA(data, 200);
        ma200SeriesRef.current.setData(ma200Data);
        const lastMa200 = ma200Data[ma200Data.length - 1]?.value;
        if (lastMa200) {
          setMa200Value(lastMa200.toFixed(2));
        }

        const rsiData = calculateRSI(data, 14);
        if (rsiData.length > 0 && rsiSeriesRef.current) {
          rsiSeriesRef.current.setData(rsiData);
          const lastRsi = rsiData[rsiData.length - 1]?.value;
          if (lastRsi !== undefined && lastRsi !== null) {
            setRsiValue(lastRsi.toFixed(0));
          }
          if (rsiChartRef.current) {
            rsiChartRef.current.timeScale().fitContent();
          }
        }

        chartRef.current.timeScale().fitContent();
        setLastUpdateTime(new Date());
        setError(null);
      } else {
        setError('未找到股票数据');
      }
    } catch (err) {
      console.error('加载股票数据失败:', err);
      setError('加载数据失败，请检查FMP API配置。如果API失败，将使用模拟数据。');
      setIsMockData(true);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (!chartContainerRef.current) return;

    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: '#0f1422' },
        textColor: '#8b8fa3',
      },
      width: chartContainerRef.current.clientWidth,
      height: 500,
      grid: {
        vertLines: { color: '#1a1f35' },
        horzLines: { color: '#1a1f35' },
      },
      crosshair: { mode: 1 },
      rightPriceScale: {
        borderColor: '#1a1f35',
        scaleMargins: {
          top: 0.1,
          bottom: 0.1,
        },
      },
      timeScale: {
        borderColor: '#1a1f35',
        timeVisible: true,
        secondsVisible: false,
      },
    });

    chartRef.current = chart;

    const candlestickSeries = chart.addCandlestickSeries({
      upColor: '#10b981',
      downColor: '#ef4444',
      borderVisible: false,
      wickUpColor: '#10b981',
      wickDownColor: '#ef4444',
    });
    candlestickSeriesRef.current = candlestickSeries;

    const ma50Series = chart.addLineSeries({
      color: '#3b82f6',
      lineWidth: 2,
      title: 'MA50',
    });
    ma50SeriesRef.current = ma50Series;

    const ma200Series = chart.addLineSeries({
      color: '#f59e0b',
      lineWidth: 2,
      title: 'MA200',
    });
    ma200SeriesRef.current = ma200Series;

    setTimeout(() => {
      if (rsiChartContainerRef.current && !rsiChartRef.current) {
        const rsiChart = createChart(rsiChartContainerRef.current, {
          layout: {
            background: { type: ColorType.Solid, color: '#0f1422' },
            textColor: '#8b8fa3',
          },
          width: rsiChartContainerRef.current.clientWidth,
          height: 150,
          grid: {
            vertLines: { color: '#1a1f35' },
            horzLines: { color: '#1a1f35' },
          },
          rightPriceScale: {
            borderColor: '#1a1f35',
            visible: true,
            scaleMargins: {
              top: 0.1,
              bottom: 0.1,
            },
          },
          timeScale: {
            borderColor: '#1a1f35',
            timeVisible: true,
            secondsVisible: false,
          },
        });

        rsiChartRef.current = rsiChart;

        const rsiSeries = rsiChart.addLineSeries({
          color: '#667eea',
          lineWidth: 2,
          priceFormat: {
            type: 'price',
            precision: 0,
            minMove: 1,
          },
        });
        rsiSeriesRef.current = rsiSeries;
      }
    }, 100);

    loadData();
    const handleResize = () => {
      if (chartContainerRef.current) {
        chart.applyOptions({
          width: chartContainerRef.current.clientWidth,
        });
      }
    };

    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      if (chartRef.current) {
        chartRef.current.remove();
      }
      if (rsiChartRef.current) {
        rsiChartRef.current.remove();
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [symbol]);

  return (
    <div className="trading-chart-container">
      {isMockData && (
        <div className="trading-chart-mock-warning">
          ⚠️ 当前显示的是模拟数据（FMP API 调用失败）。请检查 <code>.env</code> 中 <code>VITE_FMP_API_KEY</code> 是否已填写真实 Key，并重启 <code>npm run dev</code>。
        </div>
      )}
      <div className="chart-header">
        <div className="chart-info">
          <span className="chart-label">Daily Summary & Indicators</span>
          {lastUpdateTime && (
            <span className="update-time">
              Last update: {lastUpdateTime.toLocaleTimeString()}
            </span>
          )}
        </div>
        <div className="chart-indicators">
          <span className="indicator-item">
            <span className="indicator-color" style={{ backgroundColor: '#3b82f6' }}></span>
            MA50: {ma50Value || 'N/A'}
          </span>
          <span className="indicator-item">
            <span className="indicator-color" style={{ backgroundColor: '#f59e0b' }}></span>
            MA200: {ma200Value || 'N/A'}
          </span>
          <span className="indicator-item">
            <span className="indicator-color" style={{ backgroundColor: '#667eea' }}></span>
            RSI (14): {rsiValue || 'N/A'}
          </span>
        </div>
      </div>
      <div className="chart-tools">
        <button type="button" className="chart-tool-btn">+</button>
        <button type="button" className="chart-tool-btn">−</button>
        <button type="button" className="chart-tool-btn">✎</button>
        <button type="button" className="chart-tool-btn">T</button>
      </div>
      <div className="charts-container">
        <div
          ref={chartContainerRef}
          className="chart-wrapper"
        />
        <div className="rsi-container">
          <div className="rsi-label">RSI (6,14,24) RSI 14: {rsiValue || 'N/A'}</div>
          <div className="rsi-chart-wrapper" ref={rsiChartContainerRef}></div>
        </div>
      </div>
      {loading && (
        <div className="chart-loading">
          <div>加载中...</div>
          <div className="chart-loading-hint">
            如果长时间无响应，可能是API调用频率限制，请等待1秒后刷新
          </div>
        </div>
      )}
      {error && (
        <div className="chart-error">
          <div className="chart-error-title">⚠️ 数据加载失败</div>
          <div>{error}</div>
        </div>
      )}
    </div>
  );
});

TradingChart.displayName = 'TradingChart';

export default TradingChart;
