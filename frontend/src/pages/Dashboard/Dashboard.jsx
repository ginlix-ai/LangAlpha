import { AlignEndHorizontal, Bell, ChevronDown, Clock, Globe, HelpCircle, Menu, Plus, Search, Send, Sparkles, User, Zap } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '../../components/ui/card';
import { Input } from '../../components/ui/input';
import { ScrollArea } from '../../components/ui/scroll-area';
import './Dashboard.css';

function Dashboard() {
  // Mock data - Real financial indices
  const indices = [
    { symbol: 'SPX', name: 'S&P 500', value: '5090.25', change: '-3.05', changePercent: '-0.40', isPositive: false },
    { symbol: 'NDX', name: 'NASDAQ 100', value: '18452.80', change: '12.35', changePercent: '0.40', isPositive: true },
    { symbol: 'DJI', name: 'Dow Jones', value: '38450.20', change: '-45.20', changePercent: '-0.12', isPositive: false },
    { symbol: 'RUT', name: 'Russell 2000', value: '2156.40', change: '-8.75', changePercent: '-0.40', isPositive: false },
  ];

  const popularItems = [
    { title: 'Comparison Report', description: 'A comprehensive analysis comparing industries.', duration: '20-30min', isHighlighted: true },
    { title: 'Comparison Report', description: 'A comprehensive analysis comparing industries.', duration: '20-30min', isHighlighted: false },
    { title: 'Comparison Report', description: 'A comprehensive analysis comparing industries.', duration: '20-30min', isHighlighted: false },
    { title: 'Comparison Report', description: 'A comprehensive analysis comparing industries.', duration: '20-30min', isHighlighted: false },
  ];

  const newsItems = [
    { title: 'Federal Reserve Signals Potential Rate Cuts Amid Economic Uncertainty', time: '5 min ago', isHot: true },
    { title: 'Tech Stocks Rally as AI Companies Report Record Quarterly Earnings', time: '12 min ago', isHot: false },
    { title: 'Oil Prices Surge Following OPEC Production Cut Announcement', time: '18 min ago', isHot: true },
    { title: 'Cryptocurrency Market Volatility Increases as Regulatory News Emerges', time: '25 min ago', isHot: false },
    { title: 'Global Supply Chain Disruptions Impact Manufacturing Sector Performance', time: '1 hr ago', isHot: true },
    { title: 'Housing Market Shows Signs of Cooling as Mortgage Rates Climb', time: '2 hrs ago', isHot: false },
    { title: 'European Central Bank Maintains Current Interest Rate Policy', time: '3 hrs ago', isHot: false },
    { title: 'Renewable Energy Investments Reach All-Time High in Q4', time: '5 hrs ago', isHot: true },
  ];

  const researchItems = [
    { title: 'Retail Sales Slump Takes Toll on Market, Stocks Dip', time: '10 min ago' },
    { title: 'Retail Sales Slump Takes Toll on Market, Stocks Dip', time: '10 min ago' },
    { title: 'Retail Sales Slump Takes Toll on Market, Stocks Dip', time: '10 min ago' },
    { title: 'Retail Sales Slump Takes Toll on Market, Stocks Dip', time: '10 min ago' },
  ];

  const watchlistItems = [
    { symbol: 'AAPL', name: 'Apple', price: '125', change: '6.36%', isPositive: true },
    { symbol: 'UBER', name: 'Uber', price: '80', change: '-3.84%', isPositive: false },
    { symbol: 'AAPL', name: 'Apple', price: '125', change: '6.36%', isPositive: true },
    { symbol: 'UBER', name: 'Uber', price: '80', change: '-3.84%', isPositive: false },
    { symbol: 'UBER', name: 'Uber', price: '80', change: '-3.84%', isPositive: false },
    { symbol: 'UBER', name: 'Uber', price: '80', change: '-3.84%', isPositive: false },
    { symbol: 'AAPL', name: 'Apple', price: '125', change: '6.36%', isPositive: true },
    { symbol: 'AAPL', name: 'Apple', price: '125', change: '6.36%', isPositive: true },
  ];

  const portfolioItems = [
    { symbol: 'AAPL', name: 'Apple', price: '125', change: '6.36%', isPositive: true },
    { symbol: 'UBER', name: 'Uber', price: '80', change: '-3.84%', isPositive: false },
    { symbol: 'AAPL', name: 'Apple', price: '125', change: '6.36%', isPositive: true },
    { symbol: 'UBER', name: 'Uber', price: '80', change: '-3.84%', isPositive: false },
    { symbol: 'UBER', name: 'Uber', price: '80', change: '-3.84%', isPositive: false },
    { symbol: 'UBER', name: 'Uber', price: '80', change: '-3.84%', isPositive: false },
    { symbol: 'AAPL', name: 'Apple', price: '125', change: '6.36%', isPositive: true },
    { symbol: 'AAPL', name: 'Apple', price: '125', change: '6.36%', isPositive: true },
  ];

  return (
    <div className="dashboard-container min-h-screen" style={{ backgroundColor: '#1B1D25' }}>
      {/* Header */}
      <div className="flex items-center justify-between px-5 py-2.5" style={{ borderBottom: '1px solid rgba(255, 255, 255, 0.1)' }}>
        <h1 className="text-base font-medium" style={{ color: '#FFFFFF', letterSpacing: '0.15px' }}>Main Page</h1>
        <div className="flex items-center gap-4 flex-1 max-w-md mx-8">
          <div className="relative flex-1">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-5 w-5" style={{ color: '#78716C' }} />
            <Input 
              placeholder="Search" 
              className="pl-10 h-10 rounded-md text-sm"
              style={{ 
                backgroundColor: '#1C1917', 
                border: '0.5px solid #44403C',
                color: '#FFFFFF',
                fontSize: '14px'
              }}
            />
          </div>
        </div>
        <div className="flex items-center gap-4">
          <Bell className="h-5 w-5 cursor-pointer transition-colors" style={{ color: '#78716C' }} />
          <HelpCircle className="h-5 w-5 cursor-pointer transition-colors" style={{ color: '#78716C' }} />
          <div className="h-7 w-7 rounded-full flex items-center justify-center cursor-pointer transition-colors" style={{ backgroundColor: 'rgba(97, 85, 245, 0.2)' }}>
            <User className="h-4 w-4" style={{ color: '#6155F5' }} />
          </div>
        </div>
      </div>

      {/* Main Content Grid */}
      <div className="flex-1 p-4 overflow-hidden">
        <div className="grid grid-cols-[1fr_360px] gap-4 h-full">
          {/* Left Column */}
          <div className="flex flex-col gap-4 h-full overflow-hidden">
            {/* Index Movement */}
            <Card className="fin-card flex-shrink-0" style={{ backgroundColor: 'transparent', border: 'none', boxShadow: 'none' }}>
              <div className="flex items-center gap-2.5 p-0">
                {/* Title Section */}
                <div className="flex flex-col gap-3 flex-shrink-0" style={{ width: '200px' }}>
                  <CardTitle className="text-base font-semibold" style={{ color: '#FFFFFF', letterSpacing: '0.15px', lineHeight: '24px' }}>Index Movement</CardTitle>
                  <p className="text-xs" style={{ color: '#FFFFFF', opacity: 0.65, letterSpacing: '0.4px', lineHeight: '16px' }}>Some summary words</p>
                </div>
                
                {/* Index Cards */}
                <div className="flex gap-2.5 flex-1 min-w-0">
                  {indices.map((index, idx) => (
                    <div 
                      key={idx} 
                      className="flex-1 flex flex-col gap-2 p-4 transition-all relative"
                      style={{ 
                        backgroundColor: '#0A0A0A',
                        border: '1px solid #202020',
                        borderRadius: '8px',
                        minWidth: '0'
                      }}
                    >
                      {/* Vertical divider line */}
                      {idx > 0 && (
                        <div 
                          className="absolute left-0 top-1/2 transform -translate-y-1/2 -translate-x-1.25"
                          style={{ 
                            width: '1px',
                            height: '60px',
                            backgroundColor: 'rgba(255, 255, 255, 0.1)'
                          }}
                        />
                      )}
                      
                      <div className="flex flex-col gap-2">
                        <p className="text-sm leading-tight truncate" style={{ color: '#FFFFFF', fontSize: '14px', lineHeight: '18px' }}>{index.name}</p>
                        <p className="text-sm tabular-nums leading-none" style={{ color: '#FFFFFF', opacity: 0.65, fontSize: '14px', lineHeight: '18px' }}>{index.value}</p>
                        <div className="flex items-center gap-2 flex-wrap">
                          <p className={`text-sm font-normal tabular-nums ${index.isPositive ? 'text-up' : 'text-down'}`} style={{ fontSize: '14px', lineHeight: '18px' }}>
                            {index.isPositive ? '+' : ''}{index.change}
                          </p>
                          <p className={`text-sm font-normal tabular-nums ${index.isPositive ? 'text-up' : 'text-down'}`} style={{ fontSize: '14px', lineHeight: '18px' }}>
                            {index.isPositive ? '+' : ''}{index.changePercent}%
                          </p>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </Card>

            {/* What's Popular */}
            <Card 
              className="flex-shrink-0"
              style={{ 
                background: 'linear-gradient(90deg, #6155F5 0%, #8F88EC 28.85%, #39328F 100%)',
                border: 'none',
                boxShadow: 'none',
                borderRadius: '8px'
              }}
            >
              <CardHeader className="px-5 py-4" style={{ paddingLeft: '20px', paddingRight: '24px', paddingTop: '16px', paddingBottom: '16px' }}>
                <div className="flex items-center justify-between">
                  <CardTitle className="text-base font-semibold" style={{ color: '#FFFFFF', letterSpacing: '0.15px', lineHeight: '24px' }}>What's Popular</CardTitle>
                  <Menu className="h-4 w-4 cursor-pointer transition-colors" style={{ color: '#FFFFFF' }} />
                </div>
              </CardHeader>
              <CardContent className="px-5 pt-0 pb-0" style={{ paddingLeft: '20px', paddingRight: '20px', paddingBottom: '20px' }}>
                <ScrollArea className="w-full">
                  <div className="flex gap-2.5">
                    {popularItems.map((item, idx) => (
                      <Card 
                        key={idx} 
                        className="min-w-[240px] cursor-pointer transition-all"
                        style={{ 
                          backgroundColor: '#0A0A0A',
                          border: '1px solid #202020',
                          borderRadius: '8px',
                          boxSizing: 'border-box'
                        }}
                      >
                        <CardContent className="p-3">
                          <div className="flex flex-col gap-2">
                            <div className="flex items-center gap-2" style={{ width: '216px' }}>
                              <div className="w-4 h-4 flex items-center justify-center flex-shrink-0">
                                <AlignEndHorizontal className="w-4 h-4" style={{ color: '#F2F2F7' }} />
                              </div>
                              <div className="flex items-center gap-2.5 flex-1 min-w-0">
                                <h3 className="font-semibold text-sm flex-1" style={{ color: '#F2F2F7', letterSpacing: '0.1px', lineHeight: '20px' }}>{item.title}</h3>
                                <svg className="w-4 h-4 flex-shrink-0" style={{ color: '#F2F2F7', opacity: 0.65 }} fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                                </svg>
                              </div>
                            </div>
                            <div className="flex flex-col gap-2.5" style={{ paddingTop: '8px' }}>
                              <p className="text-xs" style={{ color: '#FFFFFF', opacity: 0.65, letterSpacing: '0.4px', lineHeight: '16px' }}>{item.description}</p>
                              <div className="flex items-center gap-2" style={{ paddingTop: '8px' }}>
                                <div className="flex items-center gap-1 px-2 py-0.5 rounded-md" style={{ backgroundColor: '#27272A' }}>
                                  <Clock className="w-3 h-3" style={{ color: '#F2F2F7', opacity: 0.65 }} />
                                  <span className="text-xs" style={{ color: '#FAFAFA', opacity: 0.65 }}>{item.duration}</span>
                                </div>
                              </div>
                            </div>
                          </div>
                        </CardContent>
                      </Card>
                    ))}
                  </div>
                </ScrollArea>
              </CardContent>
            </Card>

            {/* Top News + Top Research */}
            <div className="grid grid-cols-2 gap-4 flex-1 min-h-0">
              {/* Top News */}
              <Card className="fin-card flex flex-col h-full overflow-hidden">
                <CardHeader className="px-6 py-4 flex-shrink-0" style={{ borderBottom: '1px solid rgba(255, 255, 255, 0.1)' }}>
                  <div className="flex items-center justify-between">
                    <CardTitle className="text-base font-semibold" style={{ color: '#FFFFFF', letterSpacing: '0.15px' }}>Top News</CardTitle>
                    <Menu className="h-4 w-4 cursor-pointer transition-colors" style={{ color: '#FFFFFF' }} />
                  </div>
                </CardHeader>
                <CardContent className="px-6 pt-0 pb-0 flex-1 min-h-0 overflow-hidden" style={{ display: 'flex', flexDirection: 'column' }}>
                  <ScrollArea className="w-full flex-1">
                    <div className="space-y-0">
                      {newsItems.map((item, idx) => (
                        <div 
                          key={idx} 
                          className="flex items-center py-3 cursor-pointer transition-colors"
                          style={{ borderBottom: '1px solid #1F1F1F' }}
                        >
                          {item.isHot ? (
                            <>
                              {/* Icon with left margin */}
                              <div 
                                className="flex items-center justify-center mr-2 ml-1 flex-shrink-0" 
                                style={{ 
                                  padding: '2px 5px',
                                  gap: '4px',
                                  width: '22px',
                                  height: '22px',
                                  background: 'linear-gradient(0deg, rgba(108, 97, 243, 0.5), rgba(108, 97, 243, 0.5)), #27272A',
                                  borderRadius: '6px'
                                }}
                              >
                                <Sparkles className="w-3 h-3" style={{ color: '#FFFFFF' }} />
                              </div>
                              {/* Title after icon */}
                              <div className="flex-1 min-w-0">
                                <p 
                                  className="text-sm font-normal text-left" 
                                  style={{ 
                                    color: '#FFFFFF',
                                    overflow: 'hidden',
                                    textOverflow: 'ellipsis',
                                    whiteSpace: 'nowrap',
                                    display: 'block'
                                  }}
                                  title={item.title}
                                >
                                  {item.title}
                                </p>
                              </div>
                            </>
                          ) : (
                            <>
                              {/* Title without icon - no left margin */}
                              <div className="flex-1 min-w-0">
                                <p 
                                  className="text-sm font-normal text-left" 
                                  style={{ 
                                    color: '#FFFFFF',
                                    overflow: 'hidden',
                                    textOverflow: 'ellipsis',
                                    whiteSpace: 'nowrap',
                                    display: 'block'
                                  }}
                                  title={item.title}
                                >
                                  {item.title}
                                </p>
                              </div>
                            </>
                          )}
                          <p className="text-sm font-normal text-right flex-shrink-0 whitespace-nowrap ml-2.5" style={{ color: '#999999' }}>{item.time}</p>
                        </div>
                      ))}
                    </div>
                  </ScrollArea>
                </CardContent>
              </Card>

              {/* Top Research */}
              <Card className="fin-card flex flex-col h-full overflow-hidden">
                <CardHeader className="px-6 py-4 flex-shrink-0" style={{ borderBottom: '1px solid rgba(255, 255, 255, 0.1)' }}>
                  <div className="flex items-center justify-between">
                    <CardTitle className="text-base font-semibold" style={{ color: '#FFFFFF', letterSpacing: '0.15px' }}>Top Research</CardTitle>
                    <Menu className="h-4 w-4 cursor-pointer transition-colors" style={{ color: '#FFFFFF' }} />
                  </div>
                </CardHeader>
                <CardContent className="px-6 pt-0 pb-0 flex-1 min-h-0 overflow-hidden" style={{ display: 'flex', flexDirection: 'column' }}>
                  <ScrollArea className="w-full flex-1">
                    <div className="space-y-0">
                      {researchItems.map((item, idx) => (
                        <div 
                          key={idx} 
                          className="flex items-center gap-2.5 py-2.5 cursor-pointer transition-colors"
                          style={{ borderBottom: '1px solid #1F1F1F' }}
                        >
                          <div className="w-[90px] h-[54px] flex-shrink-0 rounded" style={{ backgroundColor: '#FFFFFF' }}>
                            {/* Placeholder for chart icon */}
                          </div>
                          <div className="flex-1 min-w-0">
                            <p className="text-sm font-normal" style={{ color: '#FFFFFF' }}>{item.title}</p>
                          </div>
                          <p className="text-sm font-normal text-right flex-shrink-0" style={{ color: '#999999' }}>{item.time}</p>
                        </div>
                      ))}
                    </div>
                  </ScrollArea>
                </CardContent>
              </Card>
            </div>

            {/* Chat Input */}
            <Card 
              className="fin-card flex-shrink-0"
              style={{ borderColor: 'hsl(var(--primary))', borderWidth: '1.5px' }}
            >
              <CardContent className="p-3">
                <div className="flex items-center gap-1">
                  <button className="w-9 h-9 flex items-center justify-center rounded-md transition-colors">
                    <Plus className="h-4 w-4" style={{ color: '#BBBBBB' }} />
                  </button>
                  <Input 
                    placeholder="What would you like to know?" 
                    className="flex-1 h-9 rounded-md text-sm focus-visible:ring-0 focus-visible:ring-offset-0 focus:outline-none"
                    style={{ 
                      backgroundColor: 'transparent',
                      border: 'none',
                      color: '#BBBBBB',
                      fontSize: '14px',
                      outline: 'none'
                    }}
                  />
                  <div className="flex items-center gap-1">
                    <button className="flex items-center gap-1.5 px-2 py-1.5 rounded-full transition-colors">
                      <Globe className="h-4 w-4" style={{ color: '#BBBBBB' }} />
                      <span className="text-sm font-medium" style={{ color: '#BBBBBB' }}>Agent</span>
                    </button>
                    <button className="flex items-center gap-1.5 px-2 py-1.5 rounded-full transition-colors" style={{ backgroundColor: 'rgba(255, 255, 255, 0.1)' }}>
                      <Zap className="h-4 w-4" style={{ color: '#BBBBBB' }} />
                      <span className="text-sm font-medium" style={{ color: '#BBBBBB' }}>Fast</span>
                    </button>
                    <button className="flex items-center gap-1.5 px-3 py-1.5 rounded-md transition-colors">
                      <span className="text-sm font-medium" style={{ color: '#BBBBBB' }}>Tool</span>
                      <ChevronDown className="h-4 w-4" style={{ color: '#BBBBBB' }} />
                    </button>
                    <button className="w-8 h-9 rounded-md flex items-center justify-center transition-colors" style={{ backgroundColor: '#6155F5' }}>
                      <Send className="h-4 w-4" style={{ color: '#FFFFFF' }} />
                    </button>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>

          {/* Right Column */}
          <div className="flex flex-col gap-4 h-full overflow-hidden">
            {/* Watchlist Panel */}
            <Card className="panel flex flex-col flex-1 min-h-0">
              <CardHeader className="px-6 py-4 flex-shrink-0">
                <div className="flex items-center justify-between">
                  <CardTitle className="text-base font-semibold" style={{ color: '#FFFFFF', letterSpacing: '0.15px' }}>Create watchlist</CardTitle>
                  <Plus className="h-4 w-4 cursor-pointer transition-colors" style={{ color: '#FFFFFF' }} />
                </div>
              </CardHeader>
              <CardContent className="px-6 pb-6 pt-0 flex-1 min-h-0">
                <ScrollArea className="h-full">
                  <table className="w-full text-sm">
                    <thead>
                      <tr style={{ borderBottom: '1px solid #1F1F1F' }}>
                        <th className="text-left py-2 px-2 font-normal text-xs" style={{ color: '#999999' }}>Symbol</th>
                        <th className="text-left py-2 px-2 font-normal text-xs" style={{ color: '#999999' }}>Name</th>
                        <th className="text-left py-2 px-2 font-normal text-xs" style={{ color: '#999999' }}>Price</th>
                        <th className="text-left py-2 px-2 font-normal text-xs" style={{ color: '#999999' }}>Change</th>
                      </tr>
                    </thead>
                    <tbody>
                      {watchlistItems.map((item, idx) => (
                        <tr 
                          key={idx} 
                          className="cursor-pointer transition-colors"
                          style={{ borderBottom: '1px solid #1F1F1F' }}
                        >
                          <td className="py-2.5 px-2 font-normal" style={{ color: '#FFFFFF' }}>{item.symbol}</td>
                          <td className="py-2.5 px-2 font-normal" style={{ color: '#FFFFFF' }}>{item.name}</td>
                          <td className="py-2.5 px-2 font-normal tabular-nums" style={{ color: item.isPositive ? '#0FEDBE' : '#FF383C' }}>{item.price}</td>
                          <td className="py-2.5 px-2 font-normal tabular-nums" style={{ color: item.isPositive ? '#0FEDBE' : '#FF383C' }}>
                            {item.isPositive ? '+' : ''}{item.change}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </ScrollArea>
              </CardContent>
            </Card>

            {/* Portfolio Panel */}
            <Card className="panel flex flex-col flex-1 min-h-0">
              <CardHeader className="px-6 py-4 flex-shrink-0">
                <div className="flex items-center justify-between">
                  <CardTitle className="text-base font-semibold" style={{ color: '#FFFFFF', letterSpacing: '0.15px' }}>Create My Portfolio</CardTitle>
                  <Plus className="h-4 w-4 cursor-pointer transition-colors" style={{ color: '#FFFFFF' }} />
                </div>
              </CardHeader>
              <CardContent className="px-6 pb-6 pt-0 flex-1 min-h-0">
                <ScrollArea className="h-full">
                  <table className="w-full text-sm">
                    <thead>
                      <tr style={{ borderBottom: '1px solid #1F1F1F' }}>
                        <th className="text-left py-2 px-2 font-normal text-xs" style={{ color: '#999999' }}>Symbol</th>
                        <th className="text-left py-2 px-2 font-normal text-xs" style={{ color: '#999999' }}>Name</th>
                        <th className="text-left py-2 px-2 font-normal text-xs" style={{ color: '#999999' }}>Price</th>
                        <th className="text-left py-2 px-2 font-normal text-xs" style={{ color: '#999999' }}>Change</th>
                      </tr>
                    </thead>
                    <tbody>
                      {portfolioItems.map((item, idx) => (
                        <tr 
                          key={idx} 
                          className="cursor-pointer transition-colors"
                          style={{ borderBottom: '1px solid #1F1F1F' }}
                        >
                          <td className="py-2.5 px-2 font-normal" style={{ color: '#FFFFFF' }}>{item.symbol}</td>
                          <td className="py-2.5 px-2 font-normal" style={{ color: '#FFFFFF' }}>{item.name}</td>
                          <td className="py-2.5 px-2 font-normal tabular-nums" style={{ color: item.isPositive ? '#0FEDBE' : '#FF383C' }}>{item.price}</td>
                          <td className="py-2.5 px-2 font-normal tabular-nums" style={{ color: item.isPositive ? '#0FEDBE' : '#FF383C' }}>
                            {item.isPositive ? '+' : ''}{item.change}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </ScrollArea>
              </CardContent>
            </Card>
          </div>
        </div>
      </div>
    </div>
  );
}

export default Dashboard;
