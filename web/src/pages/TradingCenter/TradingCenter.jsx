import React, { useState, useRef, useEffect } from 'react';
import './TradingCenter.css';
import TopBar from './components/TopBar';
import StockHeader from './components/StockHeader';
import TradingChart from './components/TradingChart';
import TradingPanel from './components/TradingPanel';
import { fetchRealTimePrice, fetchStockInfo } from './utils/api';

function TradingCenter() {
  const [selectedStock, setSelectedStock] = useState('MSFT');
  const [stockInfo, setStockInfo] = useState(null);
  const [realTimePrice, setRealTimePrice] = useState(null);
  const chartRef = useRef();

  // Fetch stock info and real-time price when selected stock changes
  useEffect(() => {
    if (!selectedStock) return;

    const loadStockData = async () => {
      try {
        // Fetch stock info and real-time price in parallel
        const [info, price] = await Promise.all([
          fetchStockInfo(selectedStock),
          fetchRealTimePrice(selectedStock).catch(() => null), // Don't fail if price fetch fails
        ]);
        
        setStockInfo(info);
        if (price) {
          setRealTimePrice(price);
        }
      } catch (error) {
        console.error('Error loading stock data:', error);
        // Set basic info on error
        setStockInfo({
          Symbol: selectedStock,
          Name: `${selectedStock} Corp`,
          Exchange: 'NASDAQ',
        });
      }
    };

    loadStockData();

    // Set up interval to refresh real-time price every minute
    const priceInterval = setInterval(async () => {
      try {
        const price = await fetchRealTimePrice(selectedStock);
        setRealTimePrice(price);
      } catch (error) {
        console.error('Error refreshing real-time price:', error);
      }
    }, 60000); // Refresh every minute

    return () => {
      clearInterval(priceInterval);
    };
  }, [selectedStock]);

  const handleCaptureChart = async () => {
    if (!chartRef.current) return;
    try {
      const blob = await chartRef.current.captureChart();
      if (blob) {
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = `${selectedStock}_chart_${new Date().getTime()}.png`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
      }
    } catch (error) {
      console.error('截图失败:', error);
    }
  };

  return (
    <div className="trading-center-container">
      <TopBar onStockSearch={setSelectedStock} />
      <div className="trading-content-wrapper">
        <div className="trading-left-panel">
          <StockHeader
            symbol={selectedStock}
            stockInfo={stockInfo}
            realTimePrice={realTimePrice}
          />
          <TradingChart
            ref={chartRef}
            symbol={selectedStock}
            onCapture={handleCaptureChart}
          />
        </div>
        <TradingPanel symbol={selectedStock} realTimePrice={realTimePrice} />
      </div>
    </div>
  );
}

export default TradingCenter;
