import React, { useState, useEffect } from 'react';
import { Camera, Globe, Send, Zap, Loader2 } from 'lucide-react';
import { Input } from '@/components/ui/input';
import './TradingChatInput.css';

/**
 * Chat input for TradingCenter right panel.
 * Same layout as Dashboard: Plus, input "What would you like to know?", Agent, Plan Mode, Tool, Send.
 * 
 * @param {Object} props
 * @param {Function} props.onSend - Callback when message is sent (message, mode)
 * @param {boolean} props.isLoading - Whether a message is currently loading
 */
function TradingChatInput({ onSend, isLoading = false, onCaptureChart, chartImage, onRemoveChartImage, prefillMessage, onClearPrefill }) {
  const [message, setMessage] = useState('');
  const [planMode, setPlanMode] = useState(false);
  const [mode, setMode] = useState('fast'); // 'fast' or 'deep'

  useEffect(() => {
    if (prefillMessage) {
      setMessage(prefillMessage);
      onClearPrefill?.();
    }
  }, [prefillMessage, onClearPrefill]);

  const handleSend = () => {
    if (!message.trim() || isLoading) {
      return;
    }
    if (onSend) {
      onSend(message.trim(), mode, chartImage || null);
      setMessage('');
    }
  };

  const handleKeyPress = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  const handleModeToggle = () => {
    setMode(prev => prev === 'fast' ? 'deep' : 'fast');
  };

  return (
    <>
      <div className="trading-chat-card">
        {chartImage && (
          <div className="chart-attachment-preview">
            <img src={chartImage} alt="Chart" className="chart-attachment-thumb" />
            <button className="chart-attachment-remove" onClick={onRemoveChartImage}>&times;</button>
          </div>
        )}
        <div className="trading-chat-row">
          <Input
            className="trading-chat-input"
            placeholder="What would you like to know?"
            value={message}
            onChange={(e) => setMessage(e.target.value)}
            onKeyDown={handleKeyPress}
            disabled={isLoading}
          />
          <div className="trading-chat-actions">
            <button
              type="button"
              className="trading-chat-btn trading-chat-btn-pill"
              onClick={onCaptureChart}
              title="Attach chart screenshot"
            >
              <Camera className="trading-chat-icon-sm" />
            </button>
            <button
              type="button"
              className={`trading-chat-btn trading-chat-btn-pill ${mode === 'deep' ? 'active' : ''}`}
              onClick={handleModeToggle}
              title={mode === 'fast' ? 'Fast Mode - Click to switch to Deep Mode' : 'Deep Mode - Click to switch to Fast Mode'}
            >
              <Globe className="trading-chat-icon-sm" />
              <span>{mode === 'fast' ? 'Fast Mode' : 'Deep Mode'}</span>
            </button>
            <button
              type="button"
              className={`trading-chat-btn trading-chat-btn-pill ${planMode ? 'active' : ''}`}
              onClick={() => setPlanMode(!planMode)}
            >
              <Zap className="trading-chat-icon-sm" />
              <span>Plan Mode</span>
            </button>
            <button
              type="button"
              className="trading-chat-send"
              onClick={handleSend}
              disabled={isLoading || !message.trim()}
              aria-label="Send"
            >
              {isLoading ? (
                <Loader2 className="trading-chat-send-icon spinning" />
              ) : (
                <Send className="trading-chat-send-icon" />
              )}
            </button>
          </div>
        </div>
      </div>
    </>
  );
}

export default React.memo(TradingChatInput);
