import React, { useState, useEffect, useCallback } from 'react';
import { useApi, useStreamingApi } from '../hooks/useApi';

const SLIDER_LABELS = {
  hh_formation: {
    values: ['low', 'baseline', 'high'],
    labels: ['Low (-10% vs. trend)', 'Baseline (2010-2019)', 'High (+10% vs. trend)'],
  },
  demolition: {
    values: ['low', 'baseline', 'high'],
    labels: ['Low (0.15%/yr)', 'Baseline (0.25%/yr)', 'High (0.35%/yr)'],
  },
  migration: {
    values: ['reverting', 'flat', 'continuing'],
    labels: ['Reverting (pre-2020)', 'Flat (recent levels)', 'Continuing (pandemic pace)'],
  },
  horizon: {
    values: [1, 2, 3],
    labels: ['1 Year', '2 Years', '3 Years'],
  },
};

const DEFAULT_QUESTION =
  'Given this scenario, what are the investment implications for homebuilder exposure (DHI, LEN, NVR) and regional bank credit risk in this market?';

function Panel3_ScenarioBuilder({ metros }) {
  const [scenarioMetro, setScenarioMetro] = useState('38060');
  const [params, setParams] = useState({
    hh_formation: 'baseline',
    demolition: 'baseline',
    migration: 'flat',
    horizon: 3,
  });
  const [userQuestion, setUserQuestion] = useState(DEFAULT_QUESTION);
  const [compareMetro, setCompareMetro] = useState('');
  const [showCompare, setShowCompare] = useState(false);

  const endpoint = scenarioMetro
    ? `/api/scenario/${scenarioMetro}?hh_formation=${params.hh_formation}&demolition=${params.demolition}&migration=${params.migration}&horizon=${params.horizon}`
    : null;

  const { data: scenario } = useApi(endpoint, [scenarioMetro, params.hh_formation, params.demolition, params.migration, params.horizon]);

  const compareEndpoint = showCompare && compareMetro
    ? `/api/scenario/${compareMetro}?hh_formation=${params.hh_formation}&demolition=${params.demolition}&migration=${params.migration}&horizon=${params.horizon}`
    : null;

  const { data: compareScenario } = useApi(compareEndpoint, [compareMetro, params.hh_formation, params.demolition, params.migration, params.horizon]);

  const { data: metroLatest } = useApi(
    scenarioMetro ? `/api/metro/${scenarioMetro}/latest` : null,
    [scenarioMetro]
  );
  const { data: nationalTs } = useApi('/api/national/timeseries');

  const { response: claudeResponse, isStreaming, stream } = useStreamingApi();

  const handleSliderChange = useCallback((key, idx) => {
    const values = SLIDER_LABELS[key].values;
    setParams(prev => ({ ...prev, [key]: values[idx] }));
  }, []);

  const handleInterpret = useCallback(() => {
    if (!scenario || !metroLatest) return;

    const nationalDeficit = nationalTs?.cumulative_deficit_baseline?.[nationalTs.cumulative_deficit_baseline.length - 1] || 0;
    const mortgageRate = nationalTs?.mortgage_rate_annual_avg?.[nationalTs.mortgage_rate_annual_avg.length - 1] || 0;

    stream({
      cbsa_code: scenarioMetro,
      cbsa_name: scenario.cbsa_name,
      scenario_params: params,
      scenario_output: {
        current_deficit: scenario.current_deficit_baseline,
        projected_surplus_deficit: scenario.projected_surplus_deficit,
        end_state_deficit: scenario.end_state_deficit,
      },
      metro_context: {
        vacancy_rate: metroLatest.vacancy_rate || 0,
        permits_vs_national_avg: metroLatest.permits_vs_national_avg_ratio || 1,
        mortgage_pct_income: metroLatest.mortgage_pct_median_income || 0,
        sun_belt: metros.find(m => m.cbsa_code === scenarioMetro)?.sun_belt || false,
      },
      national_context: {
        national_deficit: nationalDeficit,
        mortgage_rate: mortgageRate,
      },
      user_question: userQuestion,
    });
  }, [scenario, metroLatest, nationalTs, scenarioMetro, params, userQuestion, metros, stream]);

  return (
    <div className="space-y-6">
      {/* Metro Selector + Compare Toggle */}
      <div className="bg-white rounded-lg border border-gray-200 p-4">
        <div className="flex items-center gap-4 flex-wrap">
          <select
            value={scenarioMetro}
            onChange={e => setScenarioMetro(e.target.value)}
            className="flex-1 min-w-[200px] border border-gray-300 rounded px-3 py-2 text-sm"
          >
            {(metros || []).map(m => (
              <option key={m.cbsa_code} value={m.cbsa_code}>
                {m.cbsa_name}
              </option>
            ))}
          </select>

          <label className="flex items-center gap-2 text-sm text-gray-600">
            <input
              type="checkbox"
              checked={showCompare}
              onChange={e => setShowCompare(e.target.checked)}
            />
            Compare
          </label>

          {showCompare && (
            <select
              value={compareMetro}
              onChange={e => setCompareMetro(e.target.value)}
              className="flex-1 min-w-[200px] border border-gray-300 rounded px-3 py-2 text-sm"
            >
              <option value="">Select metro...</option>
              {(metros || []).filter(m => m.cbsa_code !== scenarioMetro).map(m => (
                <option key={m.cbsa_code} value={m.cbsa_code}>
                  {m.cbsa_name}
                </option>
              ))}
            </select>
          )}
        </div>
      </div>

      {/* Scenario Sliders */}
      <div className="bg-white rounded-lg border border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-800 mb-4">Scenario Parameters</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {Object.entries(SLIDER_LABELS).map(([key, config]) => {
            const currentIdx = config.values.indexOf(params[key]);
            return (
              <div key={key}>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  {key === 'hh_formation' ? 'Household Formation' :
                   key === 'demolition' ? 'Demolition/Obsolescence' :
                   key === 'migration' ? 'Migration Trend' : 'Time Horizon'}
                </label>
                <input
                  type="range"
                  min={0}
                  max={2}
                  step={1}
                  value={currentIdx}
                  onChange={e => handleSliderChange(key, parseInt(e.target.value))}
                  className="w-full"
                />
                <div className="flex justify-between text-xs text-gray-500 mt-1">
                  {config.labels.map((label, i) => (
                    <span
                      key={i}
                      className={i === currentIdx ? 'font-bold text-blue-600' : ''}
                    >
                      {label}
                    </span>
                  ))}
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* Scenario Output */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <ScenarioCard title="Primary Metro" scenario={scenario} />
        {showCompare && compareScenario && (
          <ScenarioCard title="Comparison Metro" scenario={compareScenario} />
        )}
      </div>

      {/* Claude Interpretation */}
      <div className="bg-white rounded-lg border border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-800 mb-4">Claude Interpretation</h3>

        <textarea
          value={userQuestion}
          onChange={e => setUserQuestion(e.target.value)}
          rows={3}
          className="w-full border border-gray-300 rounded px-3 py-2 text-sm mb-3"
          placeholder="Ask about investment implications..."
        />

        <button
          onClick={handleInterpret}
          disabled={isStreaming || !scenario}
          className={`px-4 py-2 rounded text-sm font-medium ${
            isStreaming
              ? 'bg-gray-300 text-gray-500 cursor-not-allowed'
              : 'bg-blue-600 text-white hover:bg-blue-700'
          }`}
        >
          {isStreaming ? 'Analyzing...' : 'Interpret with Claude'}
        </button>

        {(claudeResponse || isStreaming) && (
          <div className="mt-4 p-4 bg-gray-50 rounded border border-gray-200">
            <div className="prose prose-sm max-w-none whitespace-pre-wrap text-gray-800">
              {claudeResponse}
              {isStreaming && <span className="animate-pulse">|</span>}
            </div>
            <p className="text-xs text-gray-400 mt-3">
              Analysis generated by Claude based on precalculated scenario data
            </p>
          </div>
        )}
      </div>
    </div>
  );
}

function ScenarioCard({ title, scenario }) {
  if (!scenario) return null;

  const deficitColor = (scenario.end_state_deficit || 0) < 0 ? 'text-red-600' : 'text-green-600';

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-6">
      <h4 className="text-sm font-medium text-gray-500 uppercase tracking-wide">{title}</h4>
      <p className="text-sm text-gray-700 mt-1">{scenario.cbsa_name}</p>

      <div className={`text-4xl font-bold mt-3 ${deficitColor}`}>
        {scenario.end_state_deficit != null
          ? `${scenario.end_state_deficit < 0 ? '' : '+'}${scenario.end_state_deficit.toLocaleString()}`
          : '--'
        }
      </div>
      <p className="text-sm text-gray-500">projected end-state deficit</p>

      <div className="mt-4 grid grid-cols-2 gap-3 text-sm">
        <div>
          <span className="text-gray-500">Projected New HH:</span>
          <span className="ml-1 font-medium">{scenario.projected_new_households?.toLocaleString()}</span>
        </div>
        <div>
          <span className="text-gray-500">Projected Completions:</span>
          <span className="ml-1 font-medium">{scenario.projected_completions?.toLocaleString()}</span>
        </div>
        <div>
          <span className="text-gray-500">Surplus/Deficit:</span>
          <span className="ml-1 font-medium">{scenario.projected_surplus_deficit?.toLocaleString()}</span>
        </div>
        <div>
          <span className="text-gray-500">Current Baseline:</span>
          <span className="ml-1 font-medium">{scenario.current_deficit_baseline?.toLocaleString()}</span>
        </div>
      </div>

      {scenario.scenario_label && (
        <p className="text-xs text-gray-400 mt-3">{scenario.scenario_label}</p>
      )}
    </div>
  );
}

export default Panel3_ScenarioBuilder;
