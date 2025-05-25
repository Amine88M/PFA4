import React, { useEffect, useState } from 'react';
import './Dashboard.css';
import LineChart from '../Charts/LineChart';
import PieChart from '../Charts/PieChart';

const Dashboard = ({ activeTab, selectedIndicator, selectedPeriod, selectedSector }) => {
  const [rawData, setRawData] = useState(null);

  useEffect(() => {
    // Test du fetch pour vérifier la réponse
    fetch('/data/indicateurs_hcp.json')
      .then(res => {
        console.log('Status de la réponse :', res.status); // Affiche le statut de la réponse
        return res.text(); // Utilise .text() pour afficher le contenu brut
      })
      .then(data => {
        console.log('Données récupérées :', data); // Affiche les données reçues (en texte brut)
        // Si la réponse est correcte, on passe ensuite à JSON
        try {
          const jsonData = JSON.parse(data);
          setRawData(jsonData);
        } catch (error) {
          console.error('Erreur de parsing JSON :', error);
        }
      })
      .catch(err => console.error('Erreur chargement données:', err));

  }, []);

  const getChartData = () => {
    if (!rawData || !selectedIndicator || !selectedPeriod) {
      console.warn('⛔ Données ou paramètres manquants');
      return null;
    }

    if (selectedIndicator === 'pib_total') {
      const dataset = rawData.pib_total.filter(item =>
        item.annee >= selectedPeriod.start && item.annee <= selectedPeriod.end
      );

      if (dataset.length === 0) {
        console.warn('⚠️ Aucune donnée trouvée pour la période sélectionnée');
        return null;
      }

      return {
        labels: dataset.map(d => d.annee),
        datasets: [{
          label: 'PIB Total (USD)',
          data: dataset.map(d => d.valeur),
          borderColor: 'rgb(75, 192, 192)',
          tension: 0.1
        }]
      };
    }

    if (selectedIndicator === 'pib_secteur') {
      // Vérification des données secteur pour l'année sélectionnée
      const secteurData = rawData.pib_secteur.filter(item => item.annee === selectedPeriod.end)[0];
      
      if (!secteurData) {
        console.warn('⚠️ Données secteur indisponibles pour cette année');
        return null;
      }

      return {
        labels: Object.keys(secteurData).filter(key => key !== 'annee'), // Exclude "annee" key
        datasets: [{
          data: Object.values(secteurData).filter(value => typeof value === 'number'), // Exclude non-numeric values (like "annee")
          backgroundColor: [
            'rgb(255, 99, 132)',
            'rgb(54, 162, 235)',
            'rgb(255, 205, 86)',
            'rgb(75, 192, 192)',
            'rgb(153, 102, 255)'
          ]
        }]
      };
    }

    if (selectedIndicator === 'dette_publique') {
        // Filtrage des données pour la période sélectionnée
        const filteredData = rawData.dette_publique.filter(item => 
          item.annee >= selectedPeriod.start && item.annee <= selectedPeriod.end
        );
      
        if (filteredData.length === 0) {
          console.warn('⚠️ Aucune donnée trouvée pour la période sélectionnée');
          return null;
        }
      
        // Séparer les données en deux groupes : "Devise locale" et "% du PIB"
        const pibData = filteredData.filter(item => item.unite === "% du PIB");
        const deviseData = filteredData.filter(item => item.unite === "Devise locale");
      
        // Regrouper les données par année pour éviter les duplications
        const pibGrouped = pibData.reduce((acc, curr) => {
          if (!acc[curr.annee]) {
            acc[curr.annee] = [];
          }
          acc[curr.annee].push(curr.valeur); // Ajouter la valeur en % du PIB
          return acc;
        }, {});
      
        const deviseGrouped = deviseData.reduce((acc, curr) => {
          if (!acc[curr.annee]) {
            acc[curr.annee] = [];
          }
          acc[curr.annee].push(curr.valeur); // Ajouter la valeur en devise locale
          return acc;
        }, {});
      
        // Calculer la moyenne des valeurs pour chaque année
        const pibDataset = Object.keys(pibGrouped).map(year => {
          const values = pibGrouped[year];
          const averageValue = values.reduce((sum, val) => sum + val, 0) / values.length;
          return {
            annee: year,
            valeur: averageValue
          };
        });
      
        const deviseDataset = Object.keys(deviseGrouped).map(year => {
          const values = deviseGrouped[year];
          const averageValue = values.reduce((sum, val) => sum + val, 0) / values.length;
          return {
            annee: year,
            valeur: averageValue
          };
        });
      
        // Trier les années dans l'ordre croissant
        pibDataset.sort((a, b) => a.annee - b.annee);
        deviseDataset.sort((a, b) => a.annee - b.annee);
      
        // Retourner les données sous forme de graphique
        return {
          labels: pibDataset.map(d => d.annee), // Utiliser les années communes
          datasets: [
            {
              label: 'Dette publique (% du PIB)',
              data: pibDataset.map(d => d.valeur),
              borderColor: 'rgb(255, 159, 64)',
              tension: 0.1
            },
            {
              label: 'Dette publique (Devise locale)',
              data: deviseDataset.map(d => d.valeur),
              borderColor: 'rgb(75, 192, 192)',
              tension: 0.1
            }
          ]
        };
      }
      
      

    return null;
  };

  const renderChart = () => {
    const data = getChartData();
    if (!data) return <div>Sélectionnez un indicateur pour voir les données</div>;

    switch (selectedIndicator) {
      case 'pib_total':
      case 'taux_croissance':
      case 'inflation':
      case 'taux_chomage':
        case 'dette_publique':
        return <LineChart data={data} />;
      case 'pib_secteur':
        return <PieChart data={data} />;
      default:
        return <div>Visualisation non disponible pour cet indicateur</div>;
    }
  };

  return (
    <div className="dashboard">
      <div className="dashboard-header">
        <h1>
          {activeTab === 'exploration' ? 'Exploration des Données' : 'Analyse Avancée'}
        </h1>
        <div className="dashboard-filters">
          <span>{selectedIndicator?.replace('_', ' ')}</span>
          {selectedPeriod && (
            <span>{selectedPeriod.start} - {selectedPeriod.end}</span>
          )}
          {selectedSector && <span>{selectedSector}</span>}
        </div>
      </div>
      <div className="dashboard-content">
        {renderChart()}
      </div>
    </div>
  );
};

export default Dashboard;
