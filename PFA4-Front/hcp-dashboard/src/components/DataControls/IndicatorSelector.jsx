import React from 'react';

const IndicatorSelector = ({ selectedIndicator, setSelectedIndicator }) => {
  const indicators = [
    { value: 'pib_total', label: 'PIB Total' },
    {value: 'dette_publique', label: 'Dette Publique'},
    { value: 'taux_croissance', label: 'Taux de Croissance' },
    { value: 'inflation', label: 'Inflation' },
    { value: 'taux_chomage', label: 'Taux de Chômage' },
    { value: 'pib_secteur', label: 'PIB par Secteur' },
    { value: 'commerce_exterieur', label: 'Commerce Extérieur' },
    { value: 'secteur_bancaire', label: 'Secteur Bancaire' }
  ];

  return (
    <div className="control-group">
      <label>Indicateur</label>
      <select 
        value={selectedIndicator}
        onChange={(e) => setSelectedIndicator(e.target.value)}
      >
        {indicators.map(indicator => (
          <option key={indicator.value} value={indicator.value}>
            {indicator.label}
          </option>
        ))}
      </select>
    </div>
  );
};

export default IndicatorSelector;