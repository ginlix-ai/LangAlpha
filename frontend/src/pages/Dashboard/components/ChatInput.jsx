import React from 'react';
import { Input } from '../../../components/ui/input';
import { Button } from '../../../components/ui/button';
import { Plus, Globe, Zap, ChevronDown, Send } from 'lucide-react';

const ChatInput = () => {
  return (
    <div className="flex items-center space-x-2 p-4 bg-card rounded-lg border border-border">
      <Plus className="h-5 w-5 text-muted-foreground cursor-pointer hover:text-foreground" />
      <Input 
        placeholder="What would you like to know?" 
        className="flex-1 bg-background border-border"
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
        <Button size="icon" className="h-10 w-10 rounded-full bg-primary hover:bg-primary/90">
          <Send className="h-4 w-4" />
        </Button>
      </div>
    </div>
  );
};

export default ChatInput;
