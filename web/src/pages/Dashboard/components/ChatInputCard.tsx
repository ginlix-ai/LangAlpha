import React, { useState, useRef } from 'react';
import ChatInput, { type ChatInputHandle } from '../../../components/ui/chat-input';
import { useChatInput } from '../hooks/useChatInput';
import { useIsMobile } from '@/hooks/useIsMobile';
import { MobileFabChat } from '@/components/ui/mobile-fab-chat';

const SUGGESTION_CHIPS: string[] = [
  "Summarize Apple's earnings",
  'Compare TSLA vs BYD',
  'Predict market volatility',
  'Analyze my portfolio risk',
];

/**
 * Floating chat input wrapper for dashboard.
 * Renders as a fixed pill at the bottom of the viewport.
 * On mobile: collapses to a floating logo FAB by default.
 */
function ChatInputCard() {
  const {
    mode,
    setMode,
    isLoading,
    handleSend,
    workspaces,
    selectedWorkspaceId,
    setSelectedWorkspaceId,
  } = useChatInput();

  const [focused, setFocused] = useState(false);
  const chatInputRef = useRef<ChatInputHandle>(null);
  const isMobile = useIsMobile();
  const [chatExpanded, setChatExpanded] = useState(false);

  const handleMobileSend = (...args: Parameters<typeof handleSend>) => {
    handleSend(...args);
    setChatExpanded(false);
  };

  if (isMobile) {
    return (
      <MobileFabChat
        expanded={chatExpanded}
        onExpand={() => setChatExpanded(true)}
        onCollapse={() => setChatExpanded(false)}
        className="fixed left-0 right-0 z-40 px-3"
        style={{ bottom: 'calc(var(--bottom-tab-height, 0px) + 8px)' }}
      >
        <div className="dashboard-floating-chat">
          <ChatInput
            ref={chatInputRef}
            onSend={handleMobileSend}
            disabled={isLoading}
            mode={mode}
            onModeChange={setMode}
            workspaces={workspaces}
            selectedWorkspaceId={selectedWorkspaceId}
            onWorkspaceChange={setSelectedWorkspaceId}
            placeholder="Ask AI about market trends..."
            minRows={2}
          />
        </div>
      </MobileFabChat>
    );
  }

  return (
    <div className="dashboard-floating-chat-wrapper fixed bottom-8 left-0 right-0 z-40 flex justify-center pointer-events-none">
      <div className="pointer-events-auto w-full max-w-2xl px-4">
        {/* Suggestion bubbles — above the input, outside focus container */}
        <div className={`dashboard-suggestion-bubbles ${focused ? 'visible' : ''}`}>
          {SUGGESTION_CHIPS.map((label, i) => (
            <button
              key={label}
              type="button"
              className="dashboard-suggestion-bubble"
              style={{ transitionDelay: `${i * 60}ms` }}
              onMouseDown={(e) => e.preventDefault()}
              onClick={() => chatInputRef.current?.setValue(label)}
            >
              {label}
            </button>
          ))}
        </div>

        <div
          className="dashboard-floating-chat"
          onFocus={() => setFocused(true)}
          onBlur={(e) => {
            if (!e.currentTarget.contains(e.relatedTarget)) setFocused(false);
          }}
        >
          <ChatInput
            ref={chatInputRef}
            onSend={handleSend}
            disabled={isLoading}
            mode={mode}
            onModeChange={setMode}
            workspaces={workspaces}
            selectedWorkspaceId={selectedWorkspaceId}
            onWorkspaceChange={setSelectedWorkspaceId}
            placeholder="Ask AI about market trends, specific stocks, or portfolio analysis..."
            minRows={2}
          />
        </div>
      </div>
    </div>
  );
}

export default ChatInputCard;
