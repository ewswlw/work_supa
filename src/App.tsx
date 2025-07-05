import React, { useState, useEffect } from 'react';
import ExcelTable from './components/ExcelTable';
import './App.css';

interface BondData {
  [key: string]: any;
}

function App() {
  const [bondData, setBondData] = useState<BondData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const loadBondData = async () => {
      try {
        setLoading(true);
        const response = await fetch('/bond_data.json');
        if (!response.ok) {
          throw new Error('Failed to load bond data');
        }
        const data = await response.json();
        setBondData(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load data');
      } finally {
        setLoading(false);
      }
    };

    loadBondData();
  }, []);

  const handleDataChange = (newData: BondData[]) => {
    setBondData(newData);
  };

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-100">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500 mx-auto mb-4"></div>
          <p className="text-gray-600">Loading bond data...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-100">
        <div className="text-center">
          <div className="text-red-500 text-xl mb-4">⚠️</div>
          <h2 className="text-xl font-semibold text-gray-800 mb-2">Error Loading Data</h2>
          <p className="text-gray-600 mb-4">{error}</p>
          <button 
            onClick={() => window.location.reload()}
            className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="h-screen flex flex-col bg-gray-100">
      <header className="bg-white shadow-sm border-b">
        <div className="px-6 py-4">
          <h1 className="text-2xl font-bold text-gray-800">Bond Z-Spread Analysis</h1>
          <p className="text-gray-600">Excel-like interface for bond spread data analysis</p>
        </div>
      </header>
      
      <main className="flex-1 p-4">
        <div className="bg-white rounded-lg shadow-sm h-full">
          <ExcelTable 
            data={bondData} 
            onDataChange={handleDataChange}
          />
        </div>
      </main>
    </div>
  );
}

export default App;