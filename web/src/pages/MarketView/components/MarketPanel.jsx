import React, { useRef, useEffect } from 'react';
import MessageList from '../../ChatAgent/components/MessageList';
import LogoLoading from '../../../components/ui/logo-loading';
import './MarketPanel.css';

/**
 * MarketPanel Component
 *
 * Displays chat messages in the right panel of MarketView.
 * Reuses ChatAgent message components for consistent rendering.
 *
 * @param {Object} props
 * @param {Array} props.messages - Array of chat messages
 * @param {boolean} props.isLoading - Whether a message is currently loading
 * @param {string} props.error - Error message if any
 */
const MarketPanel = ({ messages = [], isLoading = false, error = null }) => {
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
    <div className="market-panel">
      <div 
        ref={messagesContainerRef}
        style={{ 
          flex: 1,
          minHeight: 0,
          overflowY: 'auto',
          overflowX: 'hidden',
        }}
      >
        {messages.length === 0 ? (
          <div className="market-chat-empty-state" style={{ height: '100%' }}>
            <LogoLoading size={60} color="var(--color-accent-overlay)" />
            <p className="market-chat-empty-text" style={{ marginTop: 16 }}>
              Start a conversation by typing a message below
            </p>
            {error && (
              <div style={{ color: 'var(--color-loss)', padding: '12px', fontSize: '14px' }}>
                Error: {error}
              </div>
            )}
          </div>
        ) : (
          <div style={{ padding: '16px 24px', maxWidth: '100%' }}>
            <MessageList
              messages={messages}
              hideAvatar
              compactToolCalls
              onOpenSubagentTask={() => {}}
              onOpenFile={() => {}}
            />
            {error && (
              <div style={{
                margin: '8px 0',
                padding: '10px 14px',
                borderRadius: '8px',
                background: 'var(--color-loss-soft)',
                border: '1px solid var(--color-border-loss)',
                color: 'var(--color-loss)',
                fontSize: '13px',
                lineHeight: '1.5',
              }}>
                {error}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

export default React.memo(MarketPanel);
