import { Send, Loader2, Square, Zap } from 'lucide-react';
import { useState, useEffect, useCallback } from 'react';
import { Input } from './input';

/**
 * Universal ChatInput Component
 * 
 * Supports both controlled and uncontrolled modes:
 * - Controlled: Pass message, planMode, setMessage, setPlanMode from parent
 * - Uncontrolled: Component manages its own state
 * 
 * @param {Object} props
 * @param {Function} props.onSend - Callback when message is sent: (message, planMode) => void
 * @param {boolean} props.disabled - Whether input is disabled
 * @param {string} props.placeholder - Input placeholder text
 * @param {string} props.variant - Style variant: 'chat' (default) or 'dashboard'
 * @param {string} props.message - Controlled: message value
 * @param {Function} props.setMessage - Controlled: message setter
 * @param {boolean} props.planMode - Controlled: planMode value
 * @param {Function} props.setPlanMode - Controlled: planMode setter
 * @param {React.ReactNode} props.extraButtons - Optional extra buttons to render before Plan Mode button
 */
const ChatInput = ({
  onSend,
  disabled = false,
  placeholder = 'What would you like to know?',
  variant = 'chat', // 'chat' or 'dashboard'
  // Controlled mode props
  message: controlledMessage,
  setMessage: setControlledMessage,
  planMode: controlledPlanMode,
  setPlanMode: setControlledPlanMode,
  // Extra buttons
  extraButtons,
  // Stop button props
  onStop,
  isLoading = false,
}) => {
  // Internal state for uncontrolled mode
  const [internalMessage, setInternalMessage] = useState('');
  const [internalPlanMode, setInternalPlanMode] = useState(false);

  // Track whether stop has been requested to prevent repeated clicks
  const [isStopping, setIsStopping] = useState(false);

  // Reset isStopping when loading finishes
  useEffect(() => {
    if (!isLoading) setIsStopping(false);
  }, [isLoading]);

  const handleStop = useCallback(() => {
    if (isStopping) return;
    setIsStopping(true);
    onStop?.();
  }, [isStopping, onStop]);

  // Determine if controlled or uncontrolled
  const isControlled = controlledMessage !== undefined;
  const message = isControlled ? controlledMessage : internalMessage;
  const planMode = isControlled ? controlledPlanMode : internalPlanMode;
  const setMessage = isControlled ? setControlledMessage : setInternalMessage;
  const setPlanMode = isControlled ? setControlledPlanMode : setInternalPlanMode;

  const handleSend = () => {
    if (!message.trim() || disabled) {
      return;
    }

    onSend(message, planMode);
    
    // Clear input after sending (only in uncontrolled mode)
    if (!isControlled) {
      setInternalMessage('');
    }
  };

  const handleKeyPress = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  // Style variants
  const containerStyles = variant === 'dashboard' 
    ? {
        backgroundColor: 'transparent',
        border: 'none',
      }
    : {
        backgroundColor: 'rgba(0, 0, 0, 0.3)',
        border: '1.5px solid hsl(var(--primary))',
      };

  const inputStyles = variant === 'dashboard'
    ? {
        backgroundColor: 'transparent',
        border: 'none',
        color: 'var(--color-text-muted)',
        fontSize: '14px',
        outline: 'none',
      }
    : {
        backgroundColor: 'transparent',
        border: 'none',
        color: '#BBBBBB',
        fontSize: '14px',
      };

  const buttonColor = variant === 'dashboard' 
    ? 'var(--color-text-muted)' 
    : '#BBBBBB';

  const sendButtonStyle = variant === 'dashboard'
    ? {
        backgroundColor: (disabled || !message.trim()) ? 'rgba(97, 85, 245, 0.5)' : 'var(--color-accent-primary)',
        color: 'var(--color-text-on-accent)',
      }
    : {
        backgroundColor: disabled ? 'rgba(97, 85, 245, 0.5)' : '#6155F5',
        color: '#FFFFFF',
      };

  return (
    <div
      className={`flex items-center ${variant === 'dashboard' ? 'gap-1' : 'gap-2 p-3 rounded-lg'}`}
      style={containerStyles}
    >
      <Input 
        placeholder={placeholder}
        className="flex-1 h-9 rounded-md text-sm focus-visible:ring-0 focus-visible:ring-offset-0 focus:outline-none"
        style={inputStyles}
        value={message}
        onChange={(e) => setMessage(e.target.value)}
        onKeyPress={handleKeyPress}
        disabled={disabled}
      />
      {extraButtons}
      <button
        className="inline-flex items-center rounded-full border-none cursor-pointer"
        style={{
          gap: '6px',
          padding: '6px 10px',
          fontSize: '13px',
          fontWeight: 500,
          background: planMode ? 'rgba(255, 255, 255, 0.12)' : 'transparent',
          color: planMode ? '#e2e8f0' : 'var(--color-text-muted, #8b8fa3)',
          transition: 'background 0.2s, color 0.2s',
        }}
        onClick={() => setPlanMode(!planMode)}
        onMouseEnter={(e) => {
          if (!planMode) {
            e.currentTarget.style.background = 'rgba(255, 255, 255, 0.05)';
          }
        }}
        onMouseLeave={(e) => {
          if (!planMode) {
            e.currentTarget.style.background = 'transparent';
          }
        }}
      >
        <Zap className="h-4 w-4" />
        <span>Plan Mode</span>
      </button>
      {isLoading && onStop ? (
        <button
          className="w-8 h-9 rounded-md flex items-center justify-center transition-colors disabled:opacity-60 disabled:cursor-not-allowed"
          style={{ backgroundColor: isStopping ? '#991b1b' : '#dc2626', color: '#FFFFFF' }}
          onClick={handleStop}
          disabled={isStopping}
          title={isStopping ? 'Stopping...' : 'Stop'}
        >
          {isStopping ? (
            <Loader2 className="h-3.5 w-3.5 animate-spin" />
          ) : (
            <Square className="h-3.5 w-3.5" fill="currentColor" />
          )}
        </button>
      ) : (
        <button
          className="w-8 h-9 rounded-md flex items-center justify-center transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
          style={sendButtonStyle}
          onClick={handleSend}
          disabled={disabled || !message.trim()}
        >
          <Send className="h-4 w-4" />
        </button>
      )}
    </div>
  );
};

export default ChatInput;
