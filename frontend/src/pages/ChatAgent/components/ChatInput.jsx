import React, { useState } from 'react';
import { Input } from '../../../components/ui/input';
import { Button } from '../../../components/ui/button';
import { Plus, Globe, Zap, ChevronDown, Send } from 'lucide-react';
import { sendChatMessage } from '../utils/api';

const ChatInput = () => {
  const [message, setMessage] = useState('');
  const [loading, setLoading] = useState(false);

  const handleSend = async () => {
    if (!message.trim()) {
      return;
    }

    setLoading(true);
    try {
      const response = await sendChatMessage(message, false);
      console.log('Chat response:', response);
      // TODO: Handle the response (e.g., display in chat history)
      setMessage(''); // Clear input after sending
    } catch (error) {
      console.error('Error sending message:', error);
      // TODO: Show error message to user
    } finally {
      setLoading(false);
    }
  };

  const handleKeyPress = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  return (
    <div className="flex items-center space-x-2 p-4 bg-card rounded-lg border border-border">
      <Plus className="h-5 w-5 text-muted-foreground cursor-pointer hover:text-foreground" />
      <Input 
        placeholder="What would you like to know?" 
        className="flex-1 bg-background border-border"
        value={message}
        onChange={(e) => setMessage(e.target.value)}
        onKeyPress={handleKeyPress}
        disabled={loading}
      />
      <div className="flex items-center space-x-2">
        <Button variant="ghost" size="sm" className="h-9">
          <Globe className="h-4 w-4 mr-2" />
          Agent
        </Button>
        <Button variant="ghost" size="sm" className="h-9">
          <Zap className="h-4 w-4 mr-2" />
          Fast
        </Button>
        <Button variant="ghost" size="sm" className="h-9">
          Tool
          <ChevronDown className="h-4 w-4 ml-2" />
        </Button>
        <Button 
          size="icon" 
          className="h-10 w-10 rounded-full bg-primary hover:bg-primary/90"
          onClick={handleSend}
          disabled={loading || !message.trim()}
        >
          <Send className="h-4 w-4" />
        </Button>
      </div>
    </div>
  );
};

export default ChatInput;
