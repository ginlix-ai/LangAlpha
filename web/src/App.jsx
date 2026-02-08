import React from 'react';
import Sidebar from './components/Sidebar/Sidebar';
import Main from './components/Main/Main';
import AuthGate from './components/AuthGate';
import './App.css';

function App() {
  return (
    <AuthGate>
      <div className="app-layout">
        <Sidebar />
        <main className="app-main">
          <Main />
        </main>
      </div>
    </AuthGate>
  );
}

export default App;
