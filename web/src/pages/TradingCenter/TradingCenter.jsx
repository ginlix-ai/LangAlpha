import React, { useState, useRef } from 'react';
import './TradingCenter.css';
import TopBar from './components/TopBar';
import StockHeader from './components/StockHeader';
import TradingChart from './components/TradingChart';
import TradingPanel from './components/TradingPanel';

function TradingCenter() {
  const [selectedStock, setSelectedStock] = useState('MSFT');
  const [stockInfo, setStockInfo] = useState(null);
  const [realTimePrice, setRealTimePrice] = useState(null);
  const chartRef = useRef();

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
