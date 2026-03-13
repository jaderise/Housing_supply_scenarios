import React, { useState, useCallback, useMemo } from 'react';
import { useApi, useStreamingApi } from '../hooks/useApi';

/* ── Scenario parameter adjustments (mirrors scenario_params.yaml) ── */
const PARAM_ADJUSTMENTS = {
  hh_formation: { low: 0.90, baseline: 1.00, high: 1.10 },
  demolition:   { low: 0.0015, baseline: 0.0025, high: 0.0035 },
  migration:    { reverting: 0.50, flat: 1.00, continuing: 1.25 },
  income_growth:{ stagnant: 0.00, baseline: 0.02, strong: 0.04 },
  borrowing:    { tight: 0.015, baseline: 0.0, loose: -0.015 },  // rate_shock
  demographic:  { aging: 0.92, baseline: 1.00, millennial_surge: 1.12 },
};

const SLIDER_LABELS = {
  hh_formation: {
    title: 'Household Formation',
    values: ['low', 'baseline', 'high'],
    labels: ['Low (-10%)', 'Baseline', 'High (+10%)'],
    trendKey: 'hh_formation',
    unit: 'HH/yr',
    format: v => v >= 1000 ? `${(v / 1000).toFixed(1)}k` : Math.round(v).toLocaleString(),
  },
  demolition: {
    title: 'Demolition Rate',
    values: ['low', 'baseline', 'high'],
    labels: ['Low (0.15%/yr)', 'Baseline (0.25%)', 'High (0.35%)'],
    trendKey: 'vacancy_rate',
    unit: 'vacancy %',
    format: v => `${(v * 100).toFixed(1)}%`,
  },
  migration: {
    title: 'Migration Trend',
    values: ['reverting', 'flat', 'continuing'],
    labels: ['Reverting', 'Flat', 'Continuing'],
    trendKey: 'migration',
    unit: 'pop \u0394/yr',
    format: v => {
      const sign = v >= 0 ? '+' : '';
      return Math.abs(v) >= 1000
        ? `${sign}${(v / 1000).toFixed(1)}k`
        : `${sign}${Math.round(v).toLocaleString()}`;
    },
  },
  income_growth: {
    title: 'Income Growth',
    values: ['stagnant', 'baseline', 'strong'],
    labels: ['Stagnant (0%)', 'Baseline (+2%)', 'Strong (+4%)'],
    trendKey: 'income',
    unit: 'median HH$',
    format: v => `$${(v / 1000).toFixed(0)}k`,
  },
  borrowing: {
    title: 'Borrowing Environment',
    values: ['tight', 'baseline', 'loose'],
    labels: ['Tight (+150bp)', 'Baseline', 'Loose (-150bp)'],
    trendKey: 'mortgage_rate',
    unit: '30yr rate',
    format: v => `${(v * 100).toFixed(1)}%`,
  },
  demographic: {
    title: 'Demographic Shift',
    values: ['aging', 'baseline', 'millennial_surge'],
    labels: ['Aging (slower)', 'Baseline', 'Millennial surge'],
    trendKey: 'population',
    unit: 'population',
    format: v => v >= 1e6 ? `${(v / 1e6).toFixed(2)}M` : `${(v / 1000).toFixed(0)}k`,
  },
};

const HORIZON_VALUES = [1, 2, 3, 5, 7, 10];
const HORIZON_LABELS = ['1yr', '2yr', '3yr', '5yr', '7yr', '10yr'];

const DEFAULT_QUESTION =
  'Given this scenario, what are the investment implications for homebuilder exposure (DHI, LEN, NVR) and regional bank credit risk in this market?';

/* ── Projection functions per slider ─────────────────────── */
function projectTrend(sliderKey, sliderValue, historicalData, horizon, allParams) {
  if (!historicalData || historicalData.length < 2) return [];

  const last = historicalData[historicalData.length - 1];
  const prev = historicalData[historicalData.length - 2];
  const lastYear = last.year;
  const lastVal = last.value;
  const projected = [];

  // Compute recent growth rate (3-year average if available)
  const recentN = Math.min(historicalData.length, 4);
  const recentStart = historicalData[historicalData.length - recentN];
  const baseGrowthRate = recentN > 1 && recentStart.value !== 0
    ? (lastVal / recentStart.value) ** (1 / (recentN - 1)) - 1
    : 0;

  for (let yr = 1; yr <= horizon; yr++) {
    const futureYear = lastYear + yr;
    let projectedVal;

    switch (sliderKey) {
      case 'hh_formation': {
        // HH formation multiplier applied to last value, with demographic cross-effect
        const hhAdj = PARAM_ADJUSTMENTS.hh_formation[sliderValue];
        const demoAdj = PARAM_ADJUSTMENTS.demographic[allParams.demographic];
        projectedVal = lastVal * hhAdj * demoAdj;
        break;
      }
      case 'demolition': {
        // Vacancy rate shifts proportionally to demolition rate vs baseline
        const demoRate = PARAM_ADJUSTMENTS.demolition[sliderValue];
        const baseRate = PARAM_ADJUSTMENTS.demolition.baseline;
        const rateDiff = demoRate - baseRate;
        // Higher demolition → vacancy rises; ~0.1% vacancy per 0.1% demolition per year
        projectedVal = lastVal + rateDiff * yr * 0.8;
        projectedVal = Math.max(0, projectedVal);
        break;
      }
      case 'migration': {
        // Migration multiplier on recent pop change
        const migAdj = PARAM_ADJUSTMENTS.migration[sliderValue];
        projectedVal = lastVal * migAdj;
        // Reverting trends toward zero over time
        if (sliderValue === 'reverting') {
          projectedVal = lastVal * (1 - (1 - migAdj) * Math.min(yr / horizon, 1));
        }
        break;
      }
      case 'income_growth': {
        // Compound growth at selected rate
        const rate = PARAM_ADJUSTMENTS.income_growth[sliderValue];
        projectedVal = lastVal * Math.pow(1 + rate, yr);
        break;
      }
      case 'borrowing': {
        // Rate shock applied as level shift
        const shock = PARAM_ADJUSTMENTS.borrowing[sliderValue];
        projectedVal = lastVal + shock;
        projectedVal = Math.max(0.01, projectedVal);
        break;
      }
      case 'demographic': {
        // Population growth scaled by demographic multiplier
        const demoAdj = PARAM_ADJUSTMENTS.demographic[sliderValue];
        const recentGrowth = lastVal - prev.value;
        projectedVal = lastVal + recentGrowth * demoAdj * yr;
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

  const toPoint = (v, i) => {
    const x = padding + (i / (allData.length - 1)) * chartW;
    const y = padding + chartH - ((v - minVal) / range) * chartH;
    return { x, y };
  };

  const allPoints = values.map((v, i) => toPoint(v, i));

  // Historical portion
  const histPoints = allPoints.slice(0, histLen);
  const histStr = histPoints.map(p => `${p.x},${p.y}`).join(' ');

  // Projected portion (starts from last historical point)
  const projPoints = allPoints.slice(histLen - 1); // overlap at junction
  const projStr = projPoints.map(p => `${p.x},${p.y}`).join(' ');

  // Area under historical
  const firstX = histPoints[0].x;
  const lastHistX = histPoints[histLen - 1].x;
  const bottom = height - padding;

  // Unique gradient ID
  const gradId = `grad-${color.replace('#', '')}-${width}`;
  const projGradId = `projgrad-${color.replace('#', '')}-${width}`;

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

      {/* Historical area fill */}
      <polygon
        points={`${firstX},${bottom} ${histStr} ${lastHistX},${bottom}`}
        fill={`url(#${gradId})`}
      />

      {/* Projected area fill */}
      {projPoints.length > 1 && (() => {
        const projFirstX = projPoints[0].x;
        const projLastX = projPoints[projPoints.length - 1].x;
        return (
          <polygon
            points={`${projFirstX},${bottom} ${projStr} ${projLastX},${bottom}`}
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

      {/* Junction dot (end of historical) */}
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

/* ── Trend badge showing projected end value ─────────────── */
function TrendBadge({ historical, projected, format, unit }) {
  if (!historical || historical.length < 1) return null;

  const hasProjection = projected && projected.length > 0;
  const displayData = hasProjection ? projected : historical;
  const latest = displayData[displayData.length - 1];
  const base = historical[historical.length - 1];
  const latestVal = format(latest.value);

  let arrow = '';
  let arrowColor = 'text-gray-400';
  const diff = latest.value - base.value;
  const pctChange = base.value !== 0 ? Math.abs(diff / base.value) : 0;

  if (hasProjection && diff !== 0) {
    if (diff > 0) {
      arrow = '\u2191';
      arrowColor = pctChange > 0.05 ? 'text-green-600' : 'text-green-400';
    } else {
      arrow = '\u2193';
      arrowColor = pctChange > 0.05 ? 'text-red-600' : 'text-red-400';
    }
  } else if (!hasProjection) {
    const prev = historical.length >= 2 ? historical[historical.length - 2] : null;
    if (prev) {
      const d = latest.value - prev.value;
      const pc = prev.value !== 0 ? Math.abs(d / prev.value) : 0;
      if (d > 0) { arrow = '\u2191'; arrowColor = pc > 0.05 ? 'text-green-600' : 'text-green-400'; }
      else if (d < 0) { arrow = '\u2193'; arrowColor = pc > 0.05 ? 'text-red-600' : 'text-red-400'; }
      else { arrow = '\u2192'; }
    }
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
  const [scenarioMetro, setScenarioMetro] = useState('38060');
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

  const endpoint = scenarioMetro
    ? `/api/scenario/${scenarioMetro}?${qs}`
    : null;

  const { data: scenario } = useApi(endpoint, [scenarioMetro, ...Object.values(params)]);

  const compareEndpoint = showCompare && compareMetro
    ? `/api/scenario/${compareMetro}?${qs}`
    : null;

  const { data: compareScenario } = useApi(compareEndpoint, [compareMetro, ...Object.values(params)]);

  const { data: metroLatest } = useApi(
    scenarioMetro ? `/api/metro/${scenarioMetro}/latest` : null,
    [scenarioMetro]
  );
  const { data: nationalTs } = useApi('/api/national/timeseries');

  // Fetch trend data for sparklines
  const { data: trends } = useApi(
    scenarioMetro ? `/api/metro/${scenarioMetro}/trends` : null,
    [scenarioMetro]
  );

  const { response: claudeResponse, isStreaming, stream } = useStreamingApi();

  // Compute projections for each slider
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

  const horizonIdx = HORIZON_VALUES.indexOf(params.horizon);

  // Sparkline colors per slider category
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
        <label className="block text-sm font-medium text-gray-700 mb-1">
          {config.title}
        </label>

        {/* Sparkline + projected value row */}
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

        {/* Slider */}
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
  };

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

      {/* Scenario Sliders with Sparklines */}
      <div className="bg-white rounded-lg border border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-800 mb-4">Scenario Parameters</h3>

        {/* Supply & Demand section */}
        <p className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">Supply & Demand</p>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
          {['hh_formation', 'demolition', 'migration'].map(renderSliderWithSparkline)}
        </div>

        {/* Economic & Demographic section */}
        <p className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">Economic & Demographic</p>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
          {['income_growth', 'borrowing', 'demographic'].map(renderSliderWithSparkline)}
        </div>

        {/* Time Horizon */}
        <p className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-3">Projection Horizon</p>
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
              <span
                key={i}
                className={i === horizonIdx ? 'font-bold text-blue-600' : ''}
              >
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
