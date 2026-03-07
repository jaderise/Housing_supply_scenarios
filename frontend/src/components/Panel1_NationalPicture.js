import React, { useState, useMemo } from 'react';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  Legend, ResponsiveContainer
} from 'recharts';
import { useApi } from '../hooks/useApi';

const SCENARIO_LEVELS = ['low', 'baseline', 'high'];

function Panel1_NationalPicture() {
  const [hhFormation, setHhFormation] = useState('baseline');
  const [demolition, setDemolition] = useState('baseline');

  const { data: timeseries, loading: tsLoading } = useApi('/api/national/timeseries');
  const { data: scenario } = useApi(
    `/api/national/scenario?hh_formation=${hhFormation}&demolition=${demolition}`,
    [hhFormation, demolition]
  );

  const chartData = useMemo(() => {
    if (!timeseries) return [];
    return timeseries.years.map((year, i) => ({
      year,
      completions: timeseries.total_completions[i],
      households: timeseries.hh_formation_rate[i],
      deficit: timeseries.cumulative_deficit_baseline[i],
      mortgageRate: timeseries.mortgage_rate_annual_avg[i]
        ? (timeseries.mortgage_rate_annual_avg[i] * 100).toFixed(1)
        : null,
    }));
  }, [timeseries]);

  const currentDeficit = scenario?.current_deficit;
  const deficitColor = currentDeficit && currentDeficit < 0 ? 'text-red-600' : 'text-green-600';

  if (tsLoading) {
    return <div className="text-center py-12 text-gray-500">Loading national data...</div>;
  }

  return (
    <div className="space-y-6">
      {/* Deficit Counter */}
      <div className="bg-white rounded-lg border border-gray-200 p-6 text-center">
        <h2 className="text-sm font-medium text-gray-500 uppercase tracking-wide">
          Cumulative Housing Deficit (Baseline)
        </h2>
        <p className={`text-5xl font-bold mt-2 ${deficitColor}`}>
          {currentDeficit != null
            ? `${currentDeficit < 0 ? '' : '+'}${currentDeficit.toLocaleString()}`
            : '--'
          }
        </p>
        <p className="text-sm text-gray-400 mt-1">units since 2008</p>

        {scenario && (
          <div className="mt-4 grid grid-cols-3 gap-4 text-sm">
            <div>
              <div className="text-gray-500">1-Year Projection</div>
              <div className="font-semibold">
                {scenario.end_state_deficit_1yr?.toLocaleString() ?? '--'}
              </div>
            </div>
            <div>
              <div className="text-gray-500">2-Year Projection</div>
              <div className="font-semibold">
                {scenario.end_state_deficit_2yr?.toLocaleString() ?? '--'}
              </div>
            </div>
            <div>
              <div className="text-gray-500">3-Year Projection</div>
              <div className="font-semibold">
                {scenario.end_state_deficit_3yr?.toLocaleString() ?? '--'}
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Scenario Toggle */}
      <div className="bg-white rounded-lg border border-gray-200 p-4">
        <div className="flex flex-wrap gap-6">
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">
              Household Formation
            </label>
            <div className="flex gap-1">
              {SCENARIO_LEVELS.map(level => (
                <button
                  key={level}
                  onClick={() => setHhFormation(level)}
                  className={`px-3 py-1 text-sm rounded ${
                    hhFormation === level
                      ? 'bg-blue-600 text-white'
                      : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                  }`}
                >
                  {level.charAt(0).toUpperCase() + level.slice(1)}
                </button>
              ))}
            </div>
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-500 mb-1">
              Demolition Rate
            </label>
            <div className="flex gap-1">
              {SCENARIO_LEVELS.map(level => (
                <button
                  key={level}
                  onClick={() => setDemolition(level)}
                  className={`px-3 py-1 text-sm rounded ${
                    demolition === level
                      ? 'bg-blue-600 text-white'
                      : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                  }`}
                >
                  {level.charAt(0).toUpperCase() + level.slice(1)}
                </button>
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* Main Chart: Formation vs Completions */}
      <div className="bg-white rounded-lg border border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-800 mb-4">
          Household Formation vs. Housing Completions
        </h3>
        <ResponsiveContainer width="100%" height={400}>
          <LineChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="year" />
            <YAxis yAxisId="left" label={{ value: 'Thousands', angle: -90, position: 'insideLeft' }} />
            <YAxis yAxisId="right" orientation="right" label={{ value: 'Cumulative Deficit', angle: 90, position: 'insideRight' }} />
            <Tooltip />
            <Legend />
            <Line
              yAxisId="left"
              type="monotone"
              dataKey="completions"
              stroke="#3b82f6"
              name="Completions"
              dot={false}
              strokeWidth={2}
            />
            <Line
              yAxisId="left"
              type="monotone"
              dataKey="households"
              stroke="#f97316"
              name="New Households"
              dot={false}
              strokeWidth={2}
            />
            <Line
              yAxisId="right"
              type="monotone"
              dataKey="deficit"
              stroke="#9ca3af"
              name="Cumulative Deficit"
              dot={false}
              strokeWidth={1}
              strokeDasharray="5 5"
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

export default Panel1_NationalPicture;
