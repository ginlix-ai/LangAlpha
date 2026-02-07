import React, { useRef, useEffect } from 'react';
import { Bot, User, Loader2 } from 'lucide-react';
import TextMessageContent from '../../ChatAgent/components/TextMessageContent';
import ReasoningMessageContent from '../../ChatAgent/components/ReasoningMessageContent';
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
      {/* Chat Messages Section */}
      <div className="trading-chat-messages">
        <div className="trading-chat-messages-header">
          <h3>Chat</h3>
        </div>
        <div 
          ref={messagesContainerRef}
          className="trading-chat-messages-content"
        >
          {messages.length === 0 ? (
            <div className="trading-chat-empty-state">
              <Bot className="trading-chat-empty-icon" />
              <p className="trading-chat-empty-text">Start a conversation by typing a message above</p>
            </div>
          ) : (
            <div className="trading-chat-messages-list">
              {messages.map((message) => (
                <TradingMessageBubble key={message.id} message={message} />
              ))}
              {error && (
                <div className="trading-chat-error">
                  <span>Error: {error}</span>
                </div>
              )}
              <div ref={messagesEndRef} />
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

/**
 * TradingMessageBubble Component
 * Renders a single message bubble
 */
function TradingMessageBubble({ message }) {
  const isUser = message.role === 'user';
  const isAssistant = message.role === 'assistant';

  return (
    <div className={`trading-message-bubble ${isUser ? 'trading-message-user' : 'trading-message-assistant'}`}>
      {isAssistant && (
        <div className="trading-message-avatar">
          <Bot className="trading-message-avatar-icon" />
        </div>
      )}
      <div className={`trading-message-content ${message.error ? 'trading-message-error' : ''}`}>
        {message.error ? (
          <div className="trading-message-error-text">
            <strong>Error:</strong> {message.error}
          </div>
        ) : (
          <>
            {message.contentSegments && message.contentSegments.length > 0 ? (
              <TradingMessageContentSegments
                segments={message.contentSegments}
                reasoningProcesses={message.reasoningProcesses || {}}
                isStreaming={message.isStreaming}
                hasError={message.error}
              />
            ) : (
              <>
                <TextMessageContent
                  content={message.content || ''}
                  isStreaming={message.isStreaming}
                  hasError={message.error}
                />
                {message.isStreaming && (
                  <Loader2 className="trading-message-streaming-icon" />
                )}
              </>
            )}
          </>
        )}
      </div>
      {isUser && (
        <div className="trading-message-avatar">
          <User className="trading-message-avatar-icon" />
        </div>
      )}
    </div>
  );
}

/**
 * TradingMessageContentSegments Component
 * Renders content segments in chronological order
 */
function TradingMessageContentSegments({ segments, reasoningProcesses, isStreaming, hasError }) {
  const sortedSegments = [...segments].sort((a, b) => a.order - b.order);
  
  // Group consecutive text segments
  const groupedSegments = [];
  let currentTextGroup = null;
  
  for (const segment of sortedSegments) {
    if (segment.type === 'text') {
      if (currentTextGroup) {
        currentTextGroup.content += segment.content;
        currentTextGroup.lastOrder = segment.order;
      } else {
        currentTextGroup = {
          type: 'text',
          content: segment.content,
          order: segment.order,
          lastOrder: segment.order,
        };
        groupedSegments.push(currentTextGroup);
      }
    } else {
      currentTextGroup = null;
      groupedSegments.push(segment);
    }
  }
  
  return (
    <div className="trading-message-segments">
      {groupedSegments.map((segment, index) => {
        if (segment.type === 'text') {
          const isLastSegment = index === groupedSegments.length - 1;
          return (
            <div key={`text-${segment.order}-${index}`}>
              <TextMessageContent
                content={segment.content}
                isStreaming={isStreaming && isLastSegment}
                hasError={hasError}
              />
            </div>
          );
        } else if (segment.type === 'reasoning') {
          const reasoningProcess = reasoningProcesses[segment.reasoningId];
          if (reasoningProcess) {
            return (
              <ReasoningMessageContent
                key={`reasoning-${segment.reasoningId}`}
                reasoningContent={reasoningProcess.content || ''}
                isReasoning={reasoningProcess.isReasoning || false}
                reasoningComplete={reasoningProcess.reasoningComplete || false}
              />
            );
          }
        }
        return null;
      })}
      {isStreaming && (
        <Loader2 className="trading-message-streaming-icon" />
      )}
    </div>
  );
}

export default TradingPanel;
