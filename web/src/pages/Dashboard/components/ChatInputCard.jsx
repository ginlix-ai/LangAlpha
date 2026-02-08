import React from 'react';
import { Card, CardContent } from '../../../components/ui/card';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from '../../../components/ui/dialog';
import ChatInput from '../../../components/ui/ChatInput';
import { useChatInput } from '../hooks/useChatInput';
import LogoLoading from '../../../components/LogoLoading';

/**
 * Chat input strip matching ChatAgent input bar.
 * When user sends a message, navigates to ChatAgent page with "LangAlpha" workspace.
 * Creates the workspace if it doesn't exist.
 */
function ChatInputCard() {
  const {
    message,
    setMessage,
    planMode,
    setPlanMode,
    isLoading,
    showCreatingDialog,
    handleSend,
  } = useChatInput();

  // Wrapper for onSend - useChatInput's handleSend uses internal state,
  // but since we're using controlled mode, the state is already updated
  // when onSend is called, so we can just call handleSend directly
  const handleSendWrapper = () => {
    handleSend();
  };

  return (
    <>
    <Card
      className="fin-card flex-shrink-0"
      style={{ borderColor: 'var(--color-accent-primary)', borderWidth: '1.5px' }}
    >
      <CardContent className="p-3">
        <ChatInput
          onSend={handleSendWrapper}
          disabled={isLoading}
          variant="dashboard"
          message={message}
          setMessage={setMessage}
          planMode={planMode}
          setPlanMode={setPlanMode}
        />
      </CardContent>
    </Card>

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
            <LogoLoading size={24} color="var(--color-accent-primary)" />
          </div>
        </DialogContent>
      </Dialog>
    </>
  );
}

export default ChatInputCard;
