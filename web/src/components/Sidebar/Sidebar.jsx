import { ChartCandlestick, LayoutDashboard, MessageSquareText } from 'lucide-react';
import React from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import logo from '../../assets/img/logo.svg';
import { getChatSession } from '../../pages/ChatAgent/hooks/utils/chatSessionRestore';
import './Sidebar.css';

function Sidebar() {
  const navigate = useNavigate();
  const location = useLocation();

  const menuItems = [
    {
      key: '/dashboard',
      icon: LayoutDashboard,
      label: 'Dashboard',
    },
    {
      key: '/chat',
      icon: MessageSquareText,
      label: 'Chat Agent',
    },
    {
      key: '/trading',
      icon: ChartCandlestick,
      label: 'Trading Center',
    },
  ];

  const handleItemClick = (path) => {
    if (path === '/chat') {
      const session = getChatSession();
      if (session) {
        navigate(`/chat/${session.workspaceId}/${session.threadId}`);
        return;
      }
    }
    navigate(path);
  };

  return (
    <aside className="sidebar">
      {/* Logo */}
      <div className="sidebar-logo" onClick={() => navigate('/dashboard')} style={{ cursor: 'pointer' }}>
        <img src={logo} alt="Logo" style={{ width: '40px', height: '40px', objectFit: 'contain' }} />
      </div>

      {/* Navigation Items */}
      <nav className="sidebar-nav">
        {menuItems.map((item) => {
          const Icon = item.icon;
          // For chat route, check if pathname starts with '/chat' to include workspace routes
          // For other routes, use exact match
          const isActive = item.key === '/chat' 
            ? location.pathname.startsWith('/chat')
            : location.pathname === item.key;
          
          return (
            <button
              key={item.key}
              className={`sidebar-nav-item ${isActive ? 'active' : ''}`}
              onClick={() => handleItemClick(item.key)}
              aria-label={item.label}
              title={item.label}
            >
              <Icon className="sidebar-nav-icon" />
            </button>
          );
        })}
      </nav>
    </aside>
  );
}

export default Sidebar;
