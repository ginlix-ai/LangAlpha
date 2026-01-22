import React from 'react';
import { Card, CardContent } from '../../../components/ui/card';

const IndexCard = ({ name, value, change, changePercent, isPositive }) => {
  return (
    <Card className="bg-card border-border">
      <CardContent className="p-4">
        <div className="space-y-2">
          <p className="text-sm text-muted-foreground">{name}</p>
          <p className="text-2xl font-semibold">{value}</p>
          <p className={`text-sm font-medium ${isPositive ? 'text-green-500' : 'text-red-500'}`}>
            {isPositive ? '+' : ''}{change} {isPositive ? '+' : ''}{changePercent}%
          </p>
        </div>
      </CardContent>
    </Card>
  );
};

export default IndexCard;
