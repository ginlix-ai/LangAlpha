import React from 'react';
import { Input } from '../../../components/ui/input';
import { Search, Bell, HelpCircle, User } from 'lucide-react';

const DashboardHeader = () => {
  return (
    <div className="flex items-center justify-between p-4 border-b border-border bg-card">
      <h1 className="text-2xl font-semibold">Main Page</h1>
      <div className="flex items-center space-x-4 flex-1 max-w-md mx-8">
        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <Input 
            placeholder="Search" 
            className="pl-10 bg-background border-border"
          />
        </div>
      </div>
      <div className="flex items-center space-x-4">
        <Bell className="h-5 w-5 text-muted-foreground cursor-pointer hover:text-foreground" />
        <HelpCircle className="h-5 w-5 text-muted-foreground cursor-pointer hover:text-foreground" />
        <div className="h-8 w-8 rounded-full bg-primary/20 flex items-center justify-center cursor-pointer hover:bg-primary/30">
          <User className="h-4 w-4 text-primary" />
        </div>
      </div>
    </div>
  );
};

export default DashboardHeader;
