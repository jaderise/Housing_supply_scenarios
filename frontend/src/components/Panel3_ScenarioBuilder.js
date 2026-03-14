import React, { useState, useCallback, useMemo } from 'react';
import { useApi, useStreamingApi } from '../hooks/useApi';

/* ── Scenario parameter adjustments (mirrors scenario_params.yaml) ── */
const PARAM_ADJUSTMENTS = {
  hh_formation: {
    very_low: 0.90, low: 0.95, baseline: 1.00, high: 1.05, very_high: 1.10,
  },
  demolition: {
    very_low: 0.0010, low: 0.0015, baseline: 0.0025, high: 0.0030, very_high: 0.0035,
  },
  migration: {
    strong_decline: 0.50, moderate_decline: 0.75, flat: 1.00,
    moderate_growth: 1.25, strong_growth: 1.50,
  },
  income_growth: {
    negative: -0.02, stagnant: 0.00, baseline: 0.02, strong: 0.04, very_strong: 0.06,
  },
  borrowing: {
    very_tight: 0.015, tight: 0.0075, baseline: 0.0, loose: -0.0075, very_loose: -0.015,
  },
  demographic: {
    shrinking: 0.85, slow: 0.92, baseline: 1.00, growing: 1.08, surging: 1.15,
  },
};

const SLIDER_LABELS = {
  hh_formation: {
    title: 'Household Formation',
    subtitle: 'New households forming locally',
    values: ['very_low', 'low', 'baseline', 'high', 'very_high'],
    labels: ['-10%/yr', '-5%/yr', 'Trend', '+5%/yr', '+10%/yr'],
    trendKey: 'hh_formation_growth',
    unit: 'growth rate',
    format: v => `${(v * 100).toFixed(1)}%`,
  },
  demolition: {
    title: 'Vacancy Rate',
    subtitle: 'Housing stock loss rate',
    values: ['very_low', 'low', 'baseline', 'high', 'very_high'],
    labels: ['0.10%/yr', '0.15%/yr', '0.25%/yr', '0.30%/yr', '0.35%/yr'],
    trendKey: 'vacancy_rate',
    unit: 'vacancy %',
    format: v => `${(v * 100).toFixed(1)}%`,
  },
  migration: {
    title: 'Net Domestic Migration',
    subtitle: 'People relocating to/from this metro',
    values: ['strong_decline', 'moderate_decline', 'flat', 'moderate_growth', 'strong_growth'],
    labels: ['-50%', '-25%', 'Current', '+25%', '+50%'],
    trendKey: 'migration',
    unit: 'people/yr',
    format: v => {
      const sign = v >= 0 ? '+' : '';
      return Math.abs(v) >= 1000
        ? `${sign}${(v / 1000).toFixed(1)}k`
        : `${sign}${Math.round(v).toLocaleString()}`;
    },
  },
  income_growth: {
    title: 'Median Income',
    subtitle: 'Real household income growth',
    values: ['negative', 'stagnant', 'baseline', 'strong', 'very_strong'],
    labels: ['-2%/yr', '0%/yr', '+2%/yr', '+4%/yr', '+6%/yr'],
    trendKey: 'income',
    unit: 'median HH$',
    format: v => `$${(v / 1000).toFixed(0)}k`,
  },
  borrowing: {
    title: '30-Year Mortgage Rate',
    subtitle: 'Rate change vs. current level',
    values: ['very_tight', 'tight', 'baseline', 'loose', 'very_loose'],
    labels: ['+150bp', '+75bp', 'Current', '-75bp', '-150bp'],
    trendKey: 'mortgage_rate',
    unit: '30yr rate',
    format: v => `${(v * 100).toFixed(1)}%`,
  },
  demographic: {
    title: 'Prime Homebuyer Cohort (25-44)',
    subtitle: 'Growth of prime-age homebuyers',
    values: ['shrinking', 'slow', 'baseline', 'growing', 'surging'],
    labels: ['-4%/yr', '-2%/yr', 'Trend', '+2%/yr', '+4%/yr'],
    trendKey: 'population_growth',
    unit: 'pop growth',
    format: v => `${(v * 100).toFixed(2)}%`,
  },
};

const HORIZON_VALUES = [1, 2, 3, 5, 7, 10];
const HORIZON_LABELS = ['1yr', '2yr', '3yr', '5yr', '7yr', '10yr'];

const DEFAULT_QUESTION =
  'Given this scenario, what are the investment implications for homebuilder exposure (DHI, LEN, NVR) and regional bank credit risk in this market?';

/* ── Region ordering for metro grouping ─────────────────── */
const REGION_ORDER = ['Northeast', 'Midwest', 'South', 'West'];

function groupMetrosByRegion(metros) {
  const groups = {};
  for (const region of REGION_ORDER) {
    groups[region] = [];
  }
  for (const m of (metros || [])) {
    const region = m.region || 'Other';
    if (!groups[region]) groups[region] = [];
    groups[region].push(m);
  }
  // Sort alphabetically within each region
  for (const region of Object.keys(groups)) {
    groups[region].sort((a, b) => a.cbsa_name.localeCompare(b.cbsa_name));
  }
  return groups;
}

/* ── Projection functions per slider ─────────────────────── */
function projectTrend(sliderKey, sliderValue, historicalData, horizon, allParams) {
  if (!historicalData || historicalData.length < 2) return [];

  const last = historicalData[historicalData.length - 1];
  const prev = historicalData[historicalData.length - 2];
  const lastYear = last.year;
  const lastVal = last.value;
  const projected = [];

  for (let yr = 1; yr <= horizon; yr++) {
    const futureYear = lastYear + yr;
    let projectedVal;

    switch (sliderKey) {
      case 'hh_formation': {
        // Sparkline shows growth rate — project the CAGR the user chose
        // CAGR mapping: very_low=-0.10, low=-0.05, baseline=0, high=+0.05, very_high=+0.10
        const cagrMap = { very_low: -0.10, low: -0.05, baseline: 0, high: 0.05, very_high: 0.10 };
        projectedVal = cagrMap[sliderValue] ?? 0;
        break;
      }
      case 'demolition': {
        // Vacancy rate shifts based on demolition rate delta vs baseline
        const demoRate = PARAM_ADJUSTMENTS.demolition[sliderValue];
        const baseRate = PARAM_ADJUSTMENTS.demolition.baseline;
        const rateDiff = demoRate - baseRate;
        projectedVal = lastVal + rateDiff * yr * 0.8;
        projectedVal = Math.max(0, projectedVal);
        break;
      }
      case 'migration': {
        // Migration multiplier on recent level
        const migAdj = PARAM_ADJUSTMENTS.migration[sliderValue];
        projectedVal = lastVal * migAdj;
        break;
      }
      case 'income_growth': {
        // Compound growth at selected rate
        const rate = PARAM_ADJUSTMENTS.income_growth[sliderValue];
        projectedVal = lastVal * Math.pow(1 + rate, yr);
        break;
      }
      case 'borrowing': {
        // Rate shock as level shift
        const shock = PARAM_ADJUSTMENTS.borrowing[sliderValue];
        projectedVal = lastVal + shock;
        projectedVal = Math.max(0.01, projectedVal);
        break;
      }
      case 'demographic': {
        // Population growth rate scaled by demographic multiplier
        const demoAdj = PARAM_ADJUSTMENTS.demographic[sliderValue];
        // Project the growth rate itself, adjusted by the demographic factor
        projectedVal = lastVal * demoAdj;
        break;
      }
      default:
        projectedVal = lastVal;
    }

    projected.push({ year: futureYear, value: projectedVal });
  }

  return projected;
}

/* ── Sparkline with projection (SVG) ─────────────────────── */
function Sparkline({ historical, projected, color = '#3b82f6', width = 140, height = 36 }) {
  if (!historical || historical.length < 2) return null;

  const allData = [...historical, ...(projected || [])];
  const histLen = historical.length;
  const values = allData.map(d => d.value);
  const minVal = Math.min(...values);
  const maxVal = Math.max(...values);
  const range = maxVal - minVal || 1;

  const padding = 3;
  const chartW = width - padding * 2;
  const chartH = height - padding * 2;

  const allPoints = values.map((v, i) => ({
    x: padding + (i / (allData.length - 1)) * chartW,
    y: padding + chartH - ((v - minVal) / range) * chartH,
  }));

  const histPoints = allPoints.slice(0, histLen);
  const histStr = histPoints.map(p => `${p.x},${p.y}`).join(' ');
  const projPoints = allPoints.slice(histLen - 1);
  const projStr = projPoints.map(p => `${p.x},${p.y}`).join(' ');

  const firstX = histPoints[0].x;
  const lastHistX = histPoints[histLen - 1].x;
  const bottom = height - padding;

  const gradId = `grad-${color.replace('#', '')}-${width}`;
  const projGradId = `pgrad-${color.replace('#', '')}-${width}`;

  return (
    <svg width={width} height={height} className="inline-block flex-shrink-0">
      <defs>
        <linearGradient id={gradId} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={color} stopOpacity="0.12" />
          <stop offset="100%" stopColor={color} stopOpacity="0.01" />
        </linearGradient>
        <linearGradient id={projGradId} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={color} stopOpacity="0.08" />
          <stop offset="100%" stopColor={color} stopOpacity="0.01" />
        </linearGradient>
      </defs>

      {/* Historical area */}
      <polygon
        points={`${firstX},${bottom} ${histStr} ${lastHistX},${bottom}`}
        fill={`url(#${gradId})`}
      />

      {/* Projected area */}
      {projPoints.length > 1 && (() => {
        const pFirstX = projPoints[0].x;
        const pLastX = projPoints[projPoints.length - 1].x;
        return (
          <polygon
            points={`${pFirstX},${bottom} ${projStr} ${pLastX},${bottom}`}
            fill={`url(#${projGradId})`}
          />
        );
      })()}

      {/* Historical line (solid) */}
      <polyline
        points={histStr}
        fill="none"
        stroke={color}
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />

      {/* Projected line (dashed) */}
      {projPoints.length > 1 && (
        <polyline
          points={projStr}
          fill="none"
          stroke={color}
          strokeWidth="1.5"
          strokeDasharray="3,2"
          strokeLinecap="round"
          strokeLinejoin="round"
          opacity="0.7"
        />
      )}

      {/* Junction dot */}
      <circle
        cx={histPoints[histLen - 1].x}
        cy={histPoints[histLen - 1].y}
        r="2"
        fill="white"
        stroke={color}
        strokeWidth="1.5"
      />

      {/* End-of-projection dot */}
      {projPoints.length > 1 && (
        <circle
          cx={allPoints[allPoints.length - 1].x}
          cy={allPoints[allPoints.length - 1].y}
          r="2.5"
          fill={color}
          opacity="0.7"
        />
      )}
    </svg>
  );
}

/* ── Trend badge ─────────────────────────────────────────── */
function TrendBadge({ historical, projected, format, unit }) {
  if (!historical || historical.length < 1) return null;

  const hasProjection = projected && projected.length > 0;
  const latest = hasProjection
    ? projected[projected.length - 1]
    : historical[historical.length - 1];
  const base = historical[historical.length - 1];
  const latestVal = format(latest.value);

  let arrow = '';
  let arrowColor = 'text-gray-400';
  const diff = latest.value - base.value;
  const pctChange = base.value !== 0 ? Math.abs(diff / base.value) : 0;

  if (hasProjection && diff !== 0) {
    arrow = diff > 0 ? '\u2191' : '\u2193';
    arrowColor = pctChange > 0.05
      ? (diff > 0 ? 'text-green-600' : 'text-red-600')
      : (diff > 0 ? 'text-green-400' : 'text-red-400');
  } else if (!hasProjection && historical.length >= 2) {
    const prev = historical[historical.length - 2];
    const d = latest.value - prev.value;
    if (d > 0) { arrow = '\u2191'; arrowColor = 'text-green-400'; }
    else if (d < 0) { arrow = '\u2193'; arrowColor = 'text-red-400'; }
    else { arrow = '\u2192'; }
  }

  return (
    <div className="flex items-center gap-1 text-xs min-w-0">
      <span className="font-semibold text-gray-700 truncate">{latestVal}</span>
      {arrow && <span className={`font-bold ${arrowColor}`}>{arrow}</span>}
      <span className="text-gray-400 truncate">{unit}</span>
      <span className="text-gray-300">
        ({hasProjection ? `${latest.year}e` : latest.year})
      </span>
    </div>
  );
}

/* ── Main Component ──────────────────────────────────────── */
function Panel3_ScenarioBuilder({ metros }) {
  const [scenarioMetro, setScenarioMetro] = useState('35620');
  const [params, setParams] = useState({
    hh_formation: 'baseline',
    demolition: 'baseline',
    migration: 'flat',
    income_growth: 'baseline',
    borrowing: 'baseline',
    demographic: 'baseline',
    horizon: 3,
  });
  const [userQuestion, setUserQuestion] = useState(DEFAULT_QUESTION);
  const [compareMetro, setCompareMetro] = useState('');
  const [showCompare, setShowCompare] = useState(false);

  const qs = `hh_formation=${params.hh_formation}&demolition=${params.demolition}&migration=${params.migration}&income_growth=${params.income_growth}&borrowing=${params.borrowing}&demographic=${params.demographic}&horizon=${params.horizon}`;

  const endpoint = scenarioMetro ? `/api/scenario/${scenarioMetro}?${qs}` : null;
  const { data: scenario } = useApi(endpoint, [scenarioMetro, ...Object.values(params)]);

  const compareEndpoint = showCompare && compareMetro
    ? `/api/scenario/${compareMetro}?${qs}` : null;
  const { data: compareScenario } = useApi(compareEndpoint, [compareMetro, ...Object.values(params)]);

  const { data: metroLatest } = useApi(
    scenarioMetro ? `/api/metro/${scenarioMetro}/latest` : null, [scenarioMetro]
  );
  const { data: nationalTs } = useApi('/api/national/timeseries');
  const { data: trends } = useApi(
    scenarioMetro ? `/api/metro/${scenarioMetro}/trends` : null, [scenarioMetro]
  );

  const { response: claudeResponse, isStreaming, stream } = useStreamingApi();

  // Group metros by region
  const metroGroups = useMemo(() => groupMetrosByRegion(metros), [metros]);

  // Compute projections
  const projections = useMemo(() => {
    if (!trends) return {};
    const result = {};
    for (const key of Object.keys(SLIDER_LABELS)) {
      const config = SLIDER_LABELS[key];
      const hist = trends[config.trendKey];
      result[key] = projectTrend(key, params[key], hist, params.horizon, params);
    }
    return result;
  }, [trends, params]);

  const handleSliderChange = useCallback((key, idx) => {
    const values = SLIDER_LABELS[key].values;
    setParams(prev => ({ ...prev, [key]: values[idx] }));
  }, []);

  const handleHorizonChange = useCallback((idx) => {
    setParams(prev => ({ ...prev, horizon: HORIZON_VALUES[idx] }));
  }, []);

  const handleInterpret = useCallback(() => {
    if (!scenario || !metroLatest) return;

    const nationalDeficit = nationalTs?.cumulative_deficit_baseline?.[
      nationalTs.cumulative_deficit_baseline.length - 1
    ] || 0;
    const mortgageRate = nationalTs?.mortgage_rate_annual_avg?.[
      nationalTs.mortgage_rate_annual_avg.length - 1
    ] || 0;

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
      national_context: { national_deficit: nationalDeficit, mortgage_rate: mortgageRate },
      user_question: userQuestion,
    });
  }, [scenario, metroLatest, nationalTs, scenarioMetro, params, userQuestion, metros, stream]);

  const horizonIdx = HORIZON_VALUES.indexOf(params.horizon);

  const sparkColors = {
    hh_formation: '#6366f1',
    demolition: '#f59e0b',
    migration: '#10b981',
    income_growth: '#3b82f6',
    borrowing: '#ef4444',
    demographic: '#8b5cf6',
  };

  const renderSliderWithSparkline = (key) => {
    const config = SLIDER_LABELS[key];
    const currentIdx = config.values.indexOf(params[key]);
    const trendData = trends?.[config.trendKey] || [];
    const projData = projections[key] || [];
    const color = sparkColors[key];

    return (
      <div key={key} className="bg-gray-50 rounded-lg p-3">
        <div className="mb-1">
          <label className="block text-sm font-medium text-gray-700">
            {config.title}
          </label>
          {config.subtitle && (
            <span className="text-xs text-gray-400">{config.subtitle}</span>
          )}
        </div>

        {/* Sparkline + value */}
        {trendData.length >= 2 ? (
          <div className="flex items-center gap-2 mb-2">
            <Sparkline
              historical={trendData}
              projected={projData}
              color={color}
              width={130}
              height={32}
            />
            <TrendBadge
              historical={trendData}
              projected={projData}
              format={config.format}
              unit={config.unit}
            />
          </div>
        ) : (
          <div className="text-xs text-gray-300 mb-2 h-8 flex items-center">No trend data</div>
        )}

        {/* 5-stop slider */}
        <input
          type="range"
          min={0}
          max={4}
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
              style={{ fontSize: '0.65rem' }}
            >
              {label}
            </span>
          ))}
        </div>
      </div>
    );
  };

  const renderMetroSelect = (value, onChange, excludeCode) => (
    <select
      value={value}
      onChange={onChange}
      className="flex-1 min-w-[200px] border border-gray-300 rounded px-3 py-2 text-sm"
    >
      {!value && <option value="">Select metro...</option>}
      {REGION_ORDER.map(region => {
        const regionMetros = (metroGroups[region] || []).filter(
          m => !excludeCode || m.cbsa_code !== excludeCode
        );
        if (regionMetros.length === 0) return null;
        return (
          <optgroup key={region} label={region}>
            {regionMetros.map(m => (
              <option key={m.cbsa_code} value={m.cbsa_code}>
                {m.cbsa_name}
              </option>
            ))}
          </optgroup>
        );
      })}
    </select>
  );

  return (
    <div className="space-y-6">
      {/* Metro Selector + Compare */}
      <div className="bg-white rounded-lg border border-gray-200 p-4">
        <div className="flex items-center gap-4 flex-wrap">
          {renderMetroSelect(scenarioMetro, e => setScenarioMetro(e.target.value), null)}

          <label className="flex items-center gap-2 text-sm text-gray-600">
            <input
              type="checkbox"
              checked={showCompare}
              onChange={e => setShowCompare(e.target.checked)}
            />
            Compare
          </label>

          {showCompare && renderMetroSelect(
            compareMetro, e => setCompareMetro(e.target.value), scenarioMetro
          )}
        </div>
      </div>

      {/* Scenario Sliders */}
      <div className="bg-white rounded-lg border border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-800 mb-4">Scenario Parameters</h3>

        <p className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">
          Supply & Demand
        </p>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
          {['hh_formation', 'demolition', 'migration'].map(renderSliderWithSparkline)}
        </div>

        <p className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">
          Economic & Demographic
        </p>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
          {['income_growth', 'borrowing', 'demographic'].map(renderSliderWithSparkline)}
        </div>

        <p className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">
          Projection Horizon
        </p>
        <div>
          <input
            type="range"
            min={0}
            max={HORIZON_VALUES.length - 1}
            step={1}
            value={horizonIdx >= 0 ? horizonIdx : 2}
            onChange={e => handleHorizonChange(parseInt(e.target.value))}
            className="w-full max-w-md"
          />
          <div className="flex justify-between text-xs text-gray-500 mt-1 max-w-md">
            {HORIZON_LABELS.map((label, i) => (
              <span key={i} className={i === horizonIdx ? 'font-bold text-blue-600' : ''}>
                {label}
              </span>
            ))}
          </div>
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
              Analysis generated by Claude based on real-time scenario calculation
            </p>
          </div>
        )}
      </div>
    </div>
  );
}

function ScenarioCard({ title, scenario }) {
  if (!scenario) return null;

  const deficit = scenario.end_state_deficit || 0;
  const deficitColor = deficit < 0 ? 'text-red-600' : 'text-green-600';
  const signal = deficit > 0 ? 'OVERSUPPLY' : 'UNDERSUPPLY';
  const signalColor = deficit > 0 ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800';

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-6">
      <div className="flex items-center justify-between">
        <div>
          <h4 className="text-sm font-medium text-gray-500 uppercase tracking-wide">{title}</h4>
          <p className="text-sm text-gray-700 mt-1">{scenario.cbsa_name}</p>
        </div>
        <span className={`text-xs font-bold px-2 py-1 rounded ${signalColor}`}>
          {signal}
        </span>
      </div>

      <div className={`text-4xl font-bold mt-3 ${deficitColor}`}>
        {scenario.end_state_deficit != null
          ? `${deficit < 0 ? '' : '+'}${deficit.toLocaleString()}`
          : '--'
        }
      </div>
      <p className="text-sm text-gray-500">projected end-state deficit ({scenario.horizon_years}yr)</p>

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
          <span className={`ml-1 font-medium ${(scenario.projected_surplus_deficit || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
            {scenario.projected_surplus_deficit?.toLocaleString()}
          </span>
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
