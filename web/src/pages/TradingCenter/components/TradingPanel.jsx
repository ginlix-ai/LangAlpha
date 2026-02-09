import React, { useRef, useEffect } from 'react';
import { Bot, Loader2, User } from 'lucide-react';
import MessageList from '../../ChatAgent/components/MessageList';
import './TradingPanel.css';

/**
 * TradingPanel Component
 * 
 * Displays chat messages in the right panel of TradingCenter.
 * Reuses ChatAgent message components for consistent rendering.
 * 
 * @param {Object} props
 * @param {Array} props.messages - Array of chat messages
 * @param {boolean} props.isLoading - Whether a message is currently loading
 * @param {string} props.error - Error message if any
 */
const TradingPanel = ({ messages = [], isLoading = false, error = null }) => {
  const messagesEndRef = useRef(null);
  const messagesContainerRef = useRef(null);

  // Auto-scroll to bottom when messages change or when streaming
  useEffect(() => {
    const scrollToBottom = () => {
      if (messagesContainerRef.current) {
        messagesContainerRef.current.scrollTo({
          top: messagesContainerRef.current.scrollHeight,
          behavior: 'smooth',
        });
      }
    };

    // Scroll when messages change
    if (messages.length > 0) {
      // Use setTimeout to ensure DOM has updated
      const timeoutId = setTimeout(scrollToBottom, 100);
      return () => clearTimeout(timeoutId);
    }
  }, [messages]);

  // Also scroll when a message is streaming (content updates)
  useEffect(() => {
    const hasStreamingMessage = messages.some((msg) => msg.isStreaming);
    if (hasStreamingMessage && messagesContainerRef.current) {
      const timeoutId = setTimeout(() => {
        if (messagesContainerRef.current) {
          messagesContainerRef.current.scrollTo({
            top: messagesContainerRef.current.scrollHeight,
            behavior: 'smooth',
          });
        }
      }, 50);
      return () => clearTimeout(timeoutId);
    }
  }, [messages]);

  return (
    <div className="trading-panel">
      <div 
        ref={messagesContainerRef}
        style={{ 
          flex: 1,
          minHeight: 0,
          overflowY: 'auto',
          overflowX: 'hidden',
        }}
      >
        <div style={{ padding: '16px 24px', maxWidth: '100%' }}>
          <MessageList 
            messages={messages.map(msg => ({
              ...msg,
              error: error && msg.id === messages[messages.length - 1]?.id ? error : msg.error
            }))} 
            onOpenSubagentTask={() => {}}
            onOpenFile={() => {}}
          />
          {error && messages.length === 0 && (
            <div style={{ color: '#ef4444', padding: '12px', fontSize: '14px' }}>
              Error: {error}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default TradingPanel;
