import React from 'react';
import ChatInput from './components/ChatInput';
import './ChatAgent.css';

function ChatAgent() {
  return (
    <div className="chat-agent-container">
      <h1>Chat Agent</h1>
      <ChatInput />
    </div>
  );
}

export default ChatAgent;
