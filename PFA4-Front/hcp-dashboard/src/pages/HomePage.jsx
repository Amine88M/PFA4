import React, { useState } from 'react';
import Sidebar from '../components/Sidebar/Sidebar';
import Dashboard from '../components/Dashboard/Dashboard';
//import './HomePage.css';

const HomePage = () => {
  const [activeTab, setActiveTab] = useState('exploration');
  const [selectedIndicator, setSelectedIndicator] = useState('pib_total');
  const [selectedPeriod, setSelectedPeriod] = useState({ start: 2010, end: 2023 });
  const [selectedSector, setSelectedSector] = useState(null);

  return (
    <div className="home-page">
      <Sidebar 
        activeTab={activeTab}
        setActiveTab={setActiveTab}
        selectedIndicator={selectedIndicator}
        setSelectedIndicator={setSelectedIndicator}
        selectedPeriod={selectedPeriod}
        setSelectedPeriod={setSelectedPeriod}
        selectedSector={selectedSector}
        setSelectedSector={setSelectedSector}
      />
      <Dashboard 
        activeTab={activeTab}
        selectedIndicator={selectedIndicator}
        selectedPeriod={selectedPeriod}
        selectedSector={selectedSector}
      />
    </div>
  );
};

export default HomePage;