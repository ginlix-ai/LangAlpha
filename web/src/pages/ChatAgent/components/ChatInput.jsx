import ChatInput from '../../../components/ui/ChatInput';

/**
 * ChatInput wrapper for ChatAgent page
 * Uses uncontrolled mode - component manages its own state
 */
const ChatInputWrapper = ({ onSend, disabled = false, onStop, isLoading }) => {
  return (
    <ChatInput
      onSend={onSend}
      disabled={disabled}
      variant="chat"
      onStop={onStop}
      isLoading={isLoading}
    />
  );
};

export default ChatInputWrapper;
