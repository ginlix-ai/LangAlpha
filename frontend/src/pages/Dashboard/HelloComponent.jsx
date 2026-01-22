import React, { useState } from 'react';
import { Input, Button, message } from 'antd';
import { fetchHello } from './utils/api';
import './HelloComponent.css';

function HelloComponent() {
  const [textValue, setTextValue] = useState('');
  const [loading, setLoading] = useState(false);

  const handleClick = async () => {
    setLoading(true);
    try {
      const response = await fetchHello();
      setTextValue(response);
      message.success('Successfully fetched data from backend');
    } catch (error) {
      message.error('Failed to fetch data from backend');
      setTextValue('');
      console.error('Error:', error);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="hello-component">
      <div className="hello-component-content">
        <Input
          value={textValue}
          placeholder="Response from backend will appear here"
          readOnly
          style={{ marginBottom: '16px' }}
        />
        <Button 
          type="primary" 
          onClick={handleClick}
          loading={loading}
        >
          Call Backend API
        </Button>
      </div>
    </div>
  );
}

export default HelloComponent;
