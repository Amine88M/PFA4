import React from 'react';

const SectorSelector = ({ selectedSector, setSelectedSector }) => {
  const sectors = [
    { value: 'agriculture', label: 'Agriculture' },
    { value: 'industrie', label: 'Industrie' },
    { value: 'services', label: 'Services' }
  ];

  return (
    <div className="control-group">
      <label>Secteur</label>
      <select 
        value={selectedSector || ''}
        onChange={(e) => setSelectedSector(e.target.value)}
      >
        <option value="">Tous les secteurs</option>
        {sectors.map(sector => (
          <option key={sector.value} value={sector.value}>
            {sector.label}
          </option>
        ))}
      </select>
    </div>
  );
};

export default SectorSelector;