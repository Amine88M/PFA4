import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App.jsx'; // ou './App.jsx' si nécessaire
import './index.css'; // important pour le styling de base

const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);