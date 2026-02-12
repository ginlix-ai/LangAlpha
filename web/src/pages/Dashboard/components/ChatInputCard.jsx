import React from 'react';
import { Loader2 } from 'lucide-react';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from '../../../components/ui/dialog';
import ChatInput from '../../../components/ui/chat-input';
import { useChatInput } from '../hooks/useChatInput';

/**
 * Chat input strip matching ChatAgent input bar.
 * When user sends a message, navigates to ChatAgent page with selected workspace.
 * Creates the workspace if it doesn't exist.
 */
function ChatInputCard() {
  const {
    mode,
    setMode,
    isLoading,
    showCreatingDialog,
    handleSend,
    workspaces,
    selectedWorkspaceId,
    setSelectedWorkspaceId,
  } = useChatInput();

  return (
    <>
      <ChatInput
        onSend={handleSend}
        disabled={isLoading}
        mode={mode}
        onModeChange={setMode}
        workspaces={workspaces}
        selectedWorkspaceId={selectedWorkspaceId}
        onWorkspaceChange={setSelectedWorkspaceId}
        placeholder="What would you like to know?"
      />

      {/* Creating Workspace Dialog */}
      <Dialog open={showCreatingDialog} onOpenChange={() => {}}>
        <DialogContent className="sm:max-w-md text-white border" style={{ backgroundColor: 'var(--color-bg-elevated)', borderColor: 'var(--color-border-elevated)' }}>
          <DialogHeader>
            <DialogTitle className="dashboard-title-font" style={{ color: 'var(--color-text-primary)' }}>
              Creating Workspace
            </DialogTitle>
            <DialogDescription style={{ color: 'var(--color-text-secondary)' }}>
              Creating your default "LangAlpha" workspace. Please wait...
            </DialogDescription>
          </DialogHeader>
          <div className="flex items-center justify-center py-4">
            <Loader2 className="h-6 w-6 animate-spin" style={{ color: 'var(--color-accent-primary)' }} />
          </div>
        </DialogContent>
      </Dialog>
    </>
  );
}

export default ChatInputCard;
