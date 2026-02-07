import React, { useState } from 'react';
import { ChevronDown, Globe, Plus, Send, Zap, Loader2 } from 'lucide-react';
import { Input } from '@/components/ui/input';
import './TradingChatInput.css';

/**
 * Chat input for TradingCenter right panel.
 * Same layout as Dashboard: Plus, input "What would you like to know?", Agent, Plan Mode, Tool, Send.
 * 
 * @param {Object} props
 * @param {Function} props.onSend - Callback when message is sent
 * @param {boolean} props.isLoading - Whether a message is currently loading
 */
function TradingChatInput({ onSend, isLoading = false }) {
  const [message, setMessage] = useState('');
  const [planMode, setPlanMode] = useState(false);

  const handleSend = () => {
    if (!message.trim() || isLoading) {
      return;
    }
    if (onSend) {
      onSend(message.trim());
      setMessage(''); // Clear input after sending
    }
  };

  const handleKeyPress = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  return (
    <>
      <div className="trading-chat-card">
        <div className="trading-chat-row">
          <button
            type="button"
            className="trading-chat-btn trading-chat-btn-icon"
            aria-label="Add"
          >
            <Plus className="trading-chat-icon" />
          </button>
          <Input
            className="trading-chat-input"
            placeholder="What would you like to know?"
            value={message}
            onChange={(e) => setMessage(e.target.value)}
            onKeyDown={handleKeyPress}
            disabled={isLoading}
          />
          <div className="trading-chat-actions">
            <button type="button" className="trading-chat-btn trading-chat-btn-pill">
              <Globe className="trading-chat-icon-sm" />
              <span>Agent</span>
            </button>
            <button
              type="button"
              className={`trading-chat-btn trading-chat-btn-pill ${planMode ? 'active' : ''}`}
              onClick={() => setPlanMode(!planMode)}
            >
              <Zap className="trading-chat-icon-sm" />
              <span>Plan Mode</span>
            </button>
            <button type="button" className="trading-chat-btn trading-chat-btn-pill trading-chat-btn-tool">
              <span>Tool</span>
              <ChevronDown className="trading-chat-icon-sm" />
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

export default TradingChatInput;
