import React, { useState, useMemo } from 'react';
import {
  BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, ReferenceLine, ReferenceArea
} from 'recharts';
import { useApi } from '../hooks/useApi';

const SIGNAL_COLORS = {
  red: { bg: 'bg-red-100', text: 'text-red-700', label: 'Oversupply Signal' },
  yellow: { bg: 'bg-yellow-100', text: 'text-yellow-700', label: 'Mixed' },
  green: { bg: 'bg-green-100', text: 'text-green-700', label: 'Undersupply Signal' },
};

function Panel2_MetroExplorer({ metros }) {
  const [selectedMetro, setSelectedMetro] = useState('19100');

  const { data: summary, loading: summaryLoading } = useApi(
    selectedMetro ? `/api/metro/${selectedMetro}/summary` : null,
    [selectedMetro]
  );
  const { data: latest } = useApi(
    selectedMetro ? `/api/metro/${selectedMetro}/latest` : null,
    [selectedMetro]
  );

  const permitsChartData = useMemo(() => {
    if (!summary) return [];
    const startIdx = Math.max(0, summary.years.length - 10);
    return summary.years.slice(startIdx).map((year, i) => ({
      year,
      permits: summary.permits_per_1000[startIdx + i],
    }));
  }, [summary]);

  const vacancyChartData = useMemo(() => {
    if (!summary) return [];
    return summary.years
      .map((year, i) => ({
        year,
        vacancy: summary.vacancy_rate[i] != null ? (summary.vacancy_rate[i] * 100) : null,
      }))
      .filter(d => d.year >= 2015 && d.vacancy != null);
  }, [summary]);

  const signal = latest?.oversupply_signal || 'yellow';
  const signalStyle = SIGNAL_COLORS[signal];

  return (
    <div className="space-y-6">
      {/* Metro Selector */}
      <div className="bg-white rounded-lg border border-gray-200 p-4">
        <div className="flex items-center gap-4">
          <select
            value={selectedMetro}
            onChange={e => setSelectedMetro(e.target.value)}
            className="flex-1 border border-gray-300 rounded px-3 py-2 text-sm"
          >
            {(metros || []).map(m => (
              <option key={m.cbsa_code} value={m.cbsa_code}>
                {m.cbsa_name} ({m.cbsa_code})
              </option>
            ))}
          </select>

          {latest && (
            <span className={`px-3 py-1 rounded text-sm font-medium ${signalStyle.bg} ${signalStyle.text}`}>
              {signalStyle.label}
            </span>
          )}
        </div>
      </div>

      {summaryLoading ? (
        <div className="text-center py-12 text-gray-500">Loading metro data...</div>
      ) : (
        <>
          {/* Key Stats Grid */}
          {latest && (
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              <StatCard
                label="Permits per 1K"
                value={latest.permits_per_1000_residents?.toFixed(1)}
                dqFlag={latest.dq_flag}
              />
              <StatCard
                label="vs. National Avg"
                value={latest.permits_vs_national_avg_ratio
                  ? `${latest.permits_vs_national_avg_ratio.toFixed(2)}x`
                  : null}
              />
              <StatCard
                label="Vacancy Rate"
                value={latest.vacancy_rate
                  ? `${(latest.vacancy_rate * 100).toFixed(1)}%`
                  : null}
              />
              <StatCard
                label="Cumulative Deficit"
                value={latest.cumulative_deficit_since_2008?.toLocaleString()}
                highlight={latest.cumulative_deficit_since_2008 < 0}
              />
            </div>
          )}

          {/* Permits Bar Chart */}
          <div className="bg-white rounded-lg border border-gray-200 p-6">
            <h3 className="text-lg font-semibold text-gray-800 mb-4">
              Permits per 1,000 Residents (Last 10 Years)
            </h3>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={permitsChartData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="year" />
                <YAxis />
                <Tooltip />
                <Bar dataKey="permits" fill="#3b82f6" name="Permits per 1K" />
                {latest?.permits_vs_national_avg_ratio && (
                  <ReferenceLine
                    y={latest.permits_per_1000_residents / latest.permits_vs_national_avg_ratio}
                    stroke="#ef4444"
                    strokeDasharray="5 5"
                    label="National Avg"
                  />
                )}
              </BarChart>
            </ResponsiveContainer>
          </div>

          {/* Vacancy Trend */}
          <div className="bg-white rounded-lg border border-gray-200 p-6">
            <h3 className="text-lg font-semibold text-gray-800 mb-4">
              Vacancy Rate Trend (2015-Present)
            </h3>
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={vacancyChartData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="year" />
                <YAxis domain={[0, 'auto']} unit="%" />
                <Tooltip formatter={(val) => `${val.toFixed(1)}%`} />
                <ReferenceArea y1={5} y2={8} fill="#e5e7eb" fillOpacity={0.5} />
                <Line
                  type="monotone"
                  dataKey="vacancy"
                  stroke="#3b82f6"
                  name="Vacancy Rate"
                  dot={false}
                  strokeWidth={2}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>

          {/* Affordability Gauges */}
          {latest && (
            <div className="bg-white rounded-lg border border-gray-200 p-6">
              <h3 className="text-lg font-semibold text-gray-800 mb-4">Affordability</h3>
              <div className="space-y-4">
                <AffordabilityGauge
                  label="Mortgage as % of Income"
                  value={latest.mortgage_pct_median_income}
                />
                <AffordabilityGauge
                  label="Fair Market Rent as % of Income"
                  value={latest.fmr_pct_median_income}
                />
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}

function StatCard({ label, value, highlight, dqFlag }) {
  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4">
      <div className="text-xs font-medium text-gray-500 uppercase tracking-wide flex items-center gap-1">
        {label}
        {dqFlag && <span title={dqFlag} className="text-amber-500 cursor-help">&#9888;</span>}
      </div>
      <div className={`text-2xl font-bold mt-1 ${highlight ? 'text-red-600' : 'text-gray-900'}`}>
        {value ?? '--'}
      </div>
    </div>
  );
}

function AffordabilityGauge({ label, value }) {
  if (value == null) return null;
  const pct = Math.min(value * 100, 60);
  const color = pct < 30 ? 'bg-green-500' : pct < 40 ? 'bg-yellow-500' : 'bg-red-500';

  return (
    <div>
      <div className="flex justify-between text-sm mb-1">
        <span className="text-gray-600">{label}</span>
        <span className="font-medium">{(value * 100).toFixed(1)}%</span>
      </div>
      <div className="w-full bg-gray-200 rounded-full h-3 relative">
        <div
          className={`${color} h-3 rounded-full transition-all`}
          style={{ width: `${(pct / 60) * 100}%` }}
        />
        <div
          className="absolute top-0 bottom-0 w-0.5 bg-gray-800"
          style={{ left: `${(30 / 60) * 100}%` }}
          title="30% affordability threshold"
        />
      </div>
    </div>
  );
}

export default Panel2_MetroExplorer;
