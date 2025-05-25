import React, { useEffect, useState } from 'react';

const PeriodSelector = ({ selectedPeriod, setSelectedPeriod, selectedIndicator, rawData }) => {
  const [minYear, setMinYear] = useState(null);
  const [maxYear, setMaxYear] = useState(null);

  // Calculer les années min et max en fonction de l'indicateur sélectionné
  useEffect(() => {
    if (rawData && selectedIndicator) {
      let years = [];

      // Récupérer les années minimales et maximales en fonction de l'indicateur sélectionné
      if (selectedIndicator === 'dette_publique') {
        years = rawData.dette_publique.map(item => item.annee);
      } else if (selectedIndicator === 'pib_total') {
        years = rawData.pib_total.map(item => item.annee);
      } else if (selectedIndicator === 'pib_secteur') {
        years = rawData.pib_secteur.map(item => item.annee);
      }

      if (years.length > 0) {
        setMinYear(Math.min(...years)); // L'année minimale
        setMaxYear(Math.max(...years)); // L'année maximale
      }
    }
  }, [rawData, selectedIndicator]);

  // Si minYear ou maxYear ne sont pas encore définis, afficher un message de chargement
  if (minYear === null || maxYear === null) {
    return <div>Chargement des années...</div>;
  }

  // Générer la liste des années entre minYear et maxYear
  const years = Array.from({ length: maxYear - minYear + 1 }, (_, i) => minYear + i);

  return (
    <div className="control-group">
      <label>Période</label>
      <div className="period-selectors">
        <select
          value={selectedPeriod.start}
          onChange={(e) => setSelectedPeriod({
            ...selectedPeriod,
            start: parseInt(e.target.value)
          })}
        >
          {years.map(year => (
            <option key={`start-${year}`} value={year}>
              {year}
            </option>
          ))}
        </select>
        <span>à</span>
        <select
          value={selectedPeriod.end}
          onChange={(e) => setSelectedPeriod({
            ...selectedPeriod,
            end: Math.min(parseInt(e.target.value), maxYear) // Limiter la sélection à maxYear
          })}
        >
          {years.map(year => (
            <option key={`end-${year}`} value={year}>
              {year}
            </option>
          ))}
        </select>
      </div>
    </div>
  );
};

export default PeriodSelector;
