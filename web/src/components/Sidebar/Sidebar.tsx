import { ChartCandlestick, LayoutDashboard, MessageSquareText, Timer } from 'lucide-react';
import React from 'react';
import { useTranslation } from 'react-i18next';
import { useLocation, useNavigate } from 'react-router-dom';
import logoLight from '../../assets/img/logo.svg';
import logoDark from '../../assets/img/logo-dark.svg';
import { useTheme } from '../../contexts/ThemeContext';
import AccountMenu from './AccountMenu';
import './Sidebar.css';

function Sidebar() {
  const navigate = useNavigate();
  const location = useLocation();
  const { t } = useTranslation();
  const { theme } = useTheme();
  const logo = theme === 'light' ? logoDark : logoLight;

  const menuItems = [
    {
      key: '/dashboard',
      icon: LayoutDashboard,
      label: t('sidebar.dashboard'),
    },
    {
      key: '/chat',
      icon: MessageSquareText,
      label: t('sidebar.chatAgent'),
    },
    {
      key: '/market',
      icon: ChartCandlestick,
      label: t('sidebar.marketView'),
    },
    {
      key: '/automations',
      icon: Timer,
      label: t('sidebar.automations'),
    },
  ];

  const handleItemClick = (path: string) => {
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

      {/* Account menu — pinned to bottom */}
      <div className="sidebar-bottom">
        <AccountMenu />
      </div>
    </aside>
  );
}

export default Sidebar;
