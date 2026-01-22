import { LayoutDashboard, MessageSquareText, ShoppingCart, User } from 'lucide-react';
import React from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
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
      key: '/home',
      icon: User,
      label: 'Personal Home',
    },
    {
      key: '/trading',
      icon: ShoppingCart,
      label: 'Trading Center',
    },
  ];

  const handleItemClick = (path) => {
    navigate(path);
  };

  return (
    <aside className="sidebar">
      {/* Logo Placeholder */}
      <div className="sidebar-logo">
        <div className="sidebar-logo-gradient"></div>
      </div>

      {/* Navigation Items */}
      <nav className="sidebar-nav">
        {menuItems.map((item) => {
          const Icon = item.icon;
          const isActive = location.pathname === item.key;
          
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
