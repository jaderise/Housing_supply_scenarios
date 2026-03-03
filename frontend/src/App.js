import React, { useState } from 'react';
import { useApi } from './hooks/useApi';
import Panel1_NationalPicture from './components/Panel1_NationalPicture';
import Panel2_MetroExplorer from './components/Panel2_MetroExplorer';
import Panel3_ScenarioBuilder from './components/Panel3_ScenarioBuilder';

function App() {
  const [activePanel, setActivePanel] = useState(1);
  const { data: metros } = useApi('/api/metros');
  const { data: metadata } = useApi('/api/metadata');
  const { data: dqSummary } = useApi('/api/dq/summary');

  const panels = [
    { id: 1, label: 'National Picture' },
    { id: 2, label: 'Metro Explorer' },
    { id: 3, label: 'Scenario Builder' },
  ];

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 px-6 py-4">
        <div className="max-w-7xl mx-auto flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold text-gray-900">
              Housing Market Analysis
            </h1>
            <p className="text-sm text-gray-500 mt-1">
              U.S. Housing Supply Scenario Tool
            </p>
          </div>
          <div className="flex items-center gap-4 text-sm text-gray-500">
            {metadata && metadata.latest_data_year && (
              <span>Data through {metadata.latest_data_year}</span>
            )}
            {metadata && metadata.total_metros > 0 && (
              <span>{metadata.total_metros} metros</span>
            )}
            {dqSummary && dqSummary.total_errors > 0 && (
              <span className="text-amber-600" title="Data quality issues detected">
                {dqSummary.total_errors} DQ issues
              </span>
            )}
          </div>
        </div>
      </header>

      {/* Panel Navigation */}
      <nav className="bg-white border-b border-gray-200 px-6">
        <div className="max-w-7xl mx-auto flex gap-0">
          {panels.map(panel => (
            <button
              key={panel.id}
              onClick={() => setActivePanel(panel.id)}
              className={`px-6 py-3 text-sm font-medium border-b-2 transition-colors ${
                activePanel === panel.id
                  ? 'border-blue-600 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700'
              }`}
            >
              {panel.label}
            </button>
          ))}
        </div>
      </nav>

      {/* Panel Content */}
      <main className="max-w-7xl mx-auto px-6 py-6">
        {activePanel === 1 && <Panel1_NationalPicture />}
        {activePanel === 2 && <Panel2_MetroExplorer metros={metros || []} />}
        {activePanel === 3 && <Panel3_ScenarioBuilder metros={metros || []} />}
      </main>

      {/* Footer */}
      <footer className="bg-white border-t border-gray-200 px-6 py-4 mt-8">
        <div className="max-w-7xl mx-auto text-sm text-gray-400">
          <p>
            Data sources: Census Bureau (Building Permits, Population Estimates, ACS),
            HUD (USPS Vacancy, Fair Market Rents), FRED (Federal Reserve Economic Data).
          </p>
          <p className="mt-1">
            Methodology and assumptions documented in project specification files.
          </p>
        </div>
      </footer>
    </div>
  );
}

export default App;
