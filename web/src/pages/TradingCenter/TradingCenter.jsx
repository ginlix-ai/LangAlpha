import React, { useState, useRef, useEffect } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { useToast } from '@/components/ui/use-toast';
import './TradingCenter.css';
import DashboardHeader from '../Dashboard/components/DashboardHeader';
import StockHeader from './components/StockHeader';
import TradingChart from './components/TradingChart';
import TradingChatInput from './components/TradingChatInput';
import TradingPanel from './components/TradingPanel';
import { fetchRealTimePrice, fetchStockInfo } from './utils/api';
import { useTradingChat } from './hooks/useTradingChat';
import { deleteFlashWorkspaces } from './utils/api';
import { findOrCreateDefaultWorkspace } from '../Dashboard/utils/workspace';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from '../../components/ui/dialog';
import { Loader2 } from 'lucide-react';

function TradingCenter() {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const { toast } = useToast();
  const [selectedStock, setSelectedStock] = useState('GOOGL');
  const [selectedStockDisplay, setSelectedStockDisplay] = useState(null);
  const [stockInfo, setStockInfo] = useState(null);
  const [realTimePrice, setRealTimePrice] = useState(null);
  const [chartMeta, setChartMeta] = useState(null);
  const chartRef = useRef();
  const [isCreatingWorkspace, setIsCreatingWorkspace] = useState(false);

  // Trading chat hook for flash mode conversations
  const { messages, isLoading, error, handleSendMessage: handleFastModeSend } = useTradingChat();

  // Handle URL parameter symbol (for navigation from Dashboard search)
  useEffect(() => {
    const symbolParam = searchParams.get('symbol');
    if (symbolParam) {
      const symbol = symbolParam.trim().toUpperCase();
      if (symbol && symbol !== selectedStock) {
        setSelectedStock(symbol);
        setSelectedStockDisplay(null);
        setChartMeta(null);
      }
      // Clear the URL parameter after applying it
      setSearchParams({});
    }
  }, [searchParams, selectedStock, setSearchParams]);

  // Cleanup: Delete flash workspaces when component unmounts (navigation away or refresh)
  useEffect(() => {
    return () => {
      // Delete all flash workspaces on unmount
      deleteFlashWorkspaces('test_user_001').catch((err) => {
        console.warn('[TradingCenter] Error deleting flash workspaces on unmount:', err);
      });
    };
  }, []);

  const handleStockSearch = (symbol, searchResult) => {
    setSelectedStock(symbol);
    setSelectedStockDisplay(
      searchResult
        ? {
            name: searchResult.name || searchResult.symbol,
            exchange: searchResult.exchangeShortName || searchResult.stockExchange || '',
          }
        : null
    );
    setChartMeta(null);
  };

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
      console.error('Chart capture failed:', error);
    }
  };

  const handleSendMessage = async (message, mode) => {
    if (mode === 'fast') {
      // Fast mode: use current flash API behavior
      handleFastModeSend(message);
    } else {
      // Deep mode: navigate to ChatAgent with initial message
      try {
        setIsCreatingWorkspace(true);

        // Find or create "Stealth Agent" workspace
        const workspaceId = await findOrCreateDefaultWorkspace(
          () => {}, // onCreating - already showing loading state
          () => {}  // onCreated
        );

        // Close dialog before navigation (component will unmount on navigation)
        setIsCreatingWorkspace(false);

        // Navigate to ChatAgent with initial message
        navigate(`/chat/${workspaceId}/__default__`, {
          state: {
            initialMessage: message,
            planMode: false,
          },
        });
      } catch (error) {
        console.error('Error setting up deep mode:', error);
        toast({
          variant: 'destructive',
          title: 'Error',
          description: 'Failed to set up deep mode. Please try again.',
        });
        setIsCreatingWorkspace(false);
      }
    }
  };

  return (
    <div className="trading-center-container">
      <DashboardHeader title="Market View" onStockSearch={handleStockSearch} />
      <div className="trading-content-wrapper">
        <div className="trading-left-panel">
          <StockHeader
            symbol={selectedStock}
            stockInfo={stockInfo}
            realTimePrice={realTimePrice}
            chartMeta={chartMeta}
            displayOverride={selectedStockDisplay}
          />
          <TradingChart
            ref={chartRef}
            symbol={selectedStock}
            onCapture={handleCaptureChart}
            onStockMeta={setChartMeta}
          />
        </div>
        <div className="trading-right-panel">
          <div className="trading-right-panel-inner">
            <TradingChatInput onSend={handleSendMessage} isLoading={isLoading} />
            <TradingPanel
              messages={messages}
              isLoading={isLoading}
              error={error}
            />
          </div>
        </div>
      </div>
      {isCreatingWorkspace && (
        <Dialog open={isCreatingWorkspace} onOpenChange={() => {}}>
          <DialogContent className="sm:max-w-md text-white border" style={{ backgroundColor: 'var(--color-bg-elevated)', borderColor: 'var(--color-border-elevated)' }}>
            <DialogHeader>
              <DialogTitle className="dashboard-title-font" style={{ color: 'var(--color-text-primary)' }}>
                Setting up Deep Mode
              </DialogTitle>
              <DialogDescription style={{ color: 'var(--color-text-secondary)' }}>
                Please wait while we set up your workspace...
              </DialogDescription>
            </DialogHeader>
            <div className="flex items-center justify-center py-4">
              <Loader2 className="h-6 w-6 animate-spin" style={{ color: 'var(--color-accent-primary)' }} />
            </div>
          </DialogContent>
        </Dialog>
      )}
    </div>
  );
}

export default TradingCenter;
