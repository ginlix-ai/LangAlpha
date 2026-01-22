import React from 'react';
import Sidebar from './components/Sidebar/Sidebar';
import Main from './components/Main/Main';
import './App.css';

function App() {
  return (
    <div className="app-layout">
      <Sidebar />
      <main className="app-main">
        <Main />
      </main>
    </div>
  );
}

export default App;
