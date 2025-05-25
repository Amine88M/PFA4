import React from 'react';
import './Sidebar.css';
import IndicatorSelector from '../../components/DataControls/IndicatorSelector';
import PeriodSelector from '../../components/DataControls/PeriodSelector';
import SectorSelector from '../../components/DataControls/SectorSelector';

const Sidebar = ({
  activeTab,
  setActiveTab,
  selectedIndicator,
  setSelectedIndicator,
  selectedPeriod,
  setSelectedPeriod,
  selectedSector,
  setSelectedSector
}) => {
  return (
    <div className="sidebar">
      <div className="sidebar-header">
        <h2>Dashboard</h2>
        <p>Données économiques du Maroc</p>
      </div>
      
      <div className="sidebar-menu">
        <div 
          className={`menu-item ${activeTab === 'exploration' ? 'active' : ''}`}
          onClick={() => setActiveTab('exploration')}
        >
          Exploration des Données
        </div>
        
        {activeTab === 'exploration' && (
          <div className="sub-menu">
            <IndicatorSelector 
              selectedIndicator={selectedIndicator}
              setSelectedIndicator={setSelectedIndicator}
            />
            
            <PeriodSelector 
              selectedPeriod={selectedPeriod}
              setSelectedPeriod={setSelectedPeriod}
            />
            
            {selectedIndicator === 'pib_secteur' && (
              <SectorSelector 
                selectedSector={selectedSector}
                setSelectedSector={setSelectedSector}
              />
            )}
          </div>
        )}
        
        <div 
          className={`menu-item ${activeTab === 'analysis' ? 'active' : ''}`}
          onClick={() => setActiveTab('analysis')}
        >
          Analyse Avancée
        </div>
      </div>
      
      <div className="sidebar-footer">
        <p>Version 1.0.0</p>
      </div>
    </div>
  );
};

export default Sidebar;