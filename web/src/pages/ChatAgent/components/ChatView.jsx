import React, { useEffect, useRef } from 'react';
import { ArrowLeft } from 'lucide-react';
import { ScrollArea } from '../../../components/ui/scroll-area';
import ChatInput from './ChatInput';
import MessageList from './MessageList';
import { useChatMessages } from '../hooks/useChatMessages';

/**
 * ChatView Component
 * 
 * Displays the chat interface for a specific workspace.
 * Handles:
 * - Message display and streaming
 * - Auto-scrolling
 * - Navigation back to gallery
 * 
 * @param {string} workspaceId - The workspace ID to chat in
 * @param {Function} onBack - Callback to navigate back to gallery
 */
function ChatView({ workspaceId, onBack }) {
  const scrollAreaRef = useRef(null);
  const { messages, isLoading, isLoadingHistory, messageError, handleSendMessage } = useChatMessages(workspaceId);

  // Auto-scroll to bottom when new messages arrive
  useEffect(() => {
    if (scrollAreaRef.current) {
      // ScrollArea component has a nested structure with overflow-auto
      const scrollContainer = scrollAreaRef.current.querySelector('.overflow-auto') ||
                             scrollAreaRef.current.querySelector('[data-radix-scroll-area-viewport]') ||
                             scrollAreaRef.current;
      if (scrollContainer) {
        // Use setTimeout to ensure DOM is updated
        setTimeout(() => {
          scrollContainer.scrollTop = scrollContainer.scrollHeight;
        }, 0);
      }
    }
  }, [messages]);

  return (
    <div className="chat-agent-container" style={{ backgroundColor: '#1B1D25' }}>
      {/* Header */}
      <div
        className="flex items-center justify-between px-6 py-4 flex-shrink-0"
        style={{ borderBottom: '1px solid rgba(255, 255, 255, 0.1)' }}
      >
        <div className="flex items-center gap-4">
          <button
            onClick={onBack}
            className="p-2 rounded-md transition-colors hover:bg-white/10"
            style={{ color: '#FFFFFF' }}
            title="Back to workspaces"
          >
            <ArrowLeft className="h-5 w-5" />
          </button>
          <h1 className="text-lg font-semibold" style={{ color: '#FFFFFF' }}>
            Chat Agent
          </h1>
        </div>
        {messageError && (
          <p className="text-xs" style={{ color: '#FF383C' }}>
            {messageError}
          </p>
        )}
      </div>

      {/* Messages Area - Fixed height, scrollable */}
      <div 
        className="flex-1 overflow-hidden"
        style={{ 
          minHeight: 0,
          height: 0, // Force flex-1 to work properly
        }}
      >
        <ScrollArea ref={scrollAreaRef} className="h-full w-full">
          <div className="px-6 py-4">
            <MessageList messages={messages} />
          </div>
        </ScrollArea>
      </div>

      {/* Input Area */}
      <div className="flex-shrink-0 p-4" style={{ borderTop: '1px solid rgba(255, 255, 255, 0.1)' }}>
        <ChatInput onSend={handleSendMessage} disabled={isLoading || isLoadingHistory || !workspaceId} />
      </div>
    </div>
  );
}

export default ChatView;
