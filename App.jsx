// src/App.jsx
import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { Line } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler,
} from 'chart.js';

// We are removing the time scale and date-fns adapter entirely to prevent environment-specific errors.
// Charts will use a 'category' axis with manually formatted labels.
ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler
);

// --- Constants ---
const CONSTANTS = {
    AWS_REGION: 'us-west-1',
    BUCKET_NAME: 'ledger-prediction-charting-008971633421',
    REFRESH_INTERVAL: 3600000, 
    DEFAULT_PLANNED_RATE: 65,
    DEFAULT_TARGET_TPH: 60,
};

// Default structure for VIZ.json
const DEFAULT_VIZ_DATA = {
    time: "N/A",
    current_day: { date: "N/A", sarima_predictions: [], network_prediction: 0, previous_year_data: [], current_day_data: [] },
    extended_predictions: { predictions: [] },
    Ledger_Information: {
        timePoints: [],
        metrics: { APU: [], Eligible: [], IPTM: [0], IPTNW: [1], CurrWork: [0], SSF: [0], DOBL: [0] }
    },
    prophet_performance_metrics: {},
    historical_context: { daily_summary_trends: {}, overall_summary: {}, three_hour_block_trends: {} },
};


// --- Helper Functions & Hooks ---
const Logger = {
    log: (message, data) => console.log(`[ATHENA LOG] ${message}`, data === undefined ? '' : data),
    error: (message, error) => console.error(`[ATHENA ERROR] ${message}`, error === undefined ? '' : error),
    warn: (message, data) => console.warn(`[ATHENA WARN] ${message}`, data === undefined ? '' : data)
};

function useTheme() {
    const [theme, setThemeState] = useState(() => {
        const savedTheme = localStorage.getItem('athena-app-theme');
        if (savedTheme) return savedTheme;
        return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
    });
    useEffect(() => {
        document.documentElement.setAttribute('data-theme', theme);
        localStorage.setItem('athena-app-theme', theme);
        document.documentElement.classList.toggle('dark', theme === 'dark');
    }, [theme]);
    const toggleTheme = () => setThemeState(prevTheme => prevTheme === 'light' ? 'dark' : 'light');
    return [theme, toggleTheme];
}

function useUpdateTimer(lastUpdateTime) {
    const [countdown, setCountdown] = useState("--:--");
    const [timerFillWidth, setTimerFillWidth] = useState("100%");
    const [timerFillColor, setTimerFillColor] = useState("bg-sky-500");

    const getNextUpdateTimestamp = useCallback(() => {
        const now = new Date();
        let nextUpdateTimestamp = now.getTime() + CONSTANTS.REFRESH_INTERVAL;
        if (lastUpdateTime && lastUpdateTime !== "N/A") {
            const lastUpdateDate = new Date(lastUpdateTime.replace(/-/g, '/').replace(' ', 'T'));
            if (!isNaN(lastUpdateDate.getTime()) && (lastUpdateDate.getTime() + CONSTANTS.REFRESH_INTERVAL > now.getTime())) {
                nextUpdateTimestamp = lastUpdateDate.getTime() + CONSTANTS.REFRESH_INTERVAL;
            }
        }
        return nextUpdateTimestamp;
    }, [lastUpdateTime]);

    useEffect(() => {
        const intervalId = setInterval(() => {
            const now = new Date().getTime();
            const nextUpdateTarget = getNextUpdateTimestamp();
            const timeLeft = nextUpdateTarget - now;

            if (timeLeft <= 0) {
                setCountdown("00:00");
                setTimerFillWidth("0%");
                return;
            }
            const minutes = Math.floor(timeLeft / 60000);
            const seconds = Math.floor((timeLeft % 60000) / 1000);
            setCountdown(`${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`);
            const progress = Math.max(0, (timeLeft / CONSTANTS.REFRESH_INTERVAL) * 100);
            setTimerFillWidth(`${progress}%`);
            if (progress < 25) setTimerFillColor("bg-red-500");
            else if (progress < 50) setTimerFillColor("bg-yellow-500");
            else setTimerFillColor("bg-sky-500");
        }, 1000);
        return () => clearInterval(intervalId);
    }, [lastUpdateTime, getNextUpdateTimestamp]);
    return { countdown, timerFillWidth, timerFillColor };
}


// --- API Service ---
const ApiService = {
    fetchLatestAvailableData: async () => {
        let baseTime = new Date();
        for (let i = 0; i < 48; i++) { // Look back up to 48 hours
            const year = baseTime.getFullYear();
            const month = String(baseTime.getMonth() + 1).padStart(2, '0');
            const day = String(baseTime.getDate()).padStart(2, '0');
            const hour = String(baseTime.getHours()).padStart(2, '0');
            const dateStr = `${year}-${month}-${day}`;
            const s3Url = `https://${CONSTANTS.BUCKET_NAME}.s3.${CONSTANTS.AWS_REGION}.amazonaws.com/predictions/${dateStr}_${hour}/VIZ.json`;

            try {
                const response = await fetch(s3Url, { mode: 'cors' });
                if (response.ok) {
                    const data = await response.json();
                    Logger.log('[ApiService] Success fetching LATEST VIZ.json from:', s3Url);
                    return { vizDataResult: data };
                }
            } catch (error) {
                Logger.warn(`[ApiService] Error fetching ${s3Url}. This might be a network issue or the file doesn't exist yet.`, error);
            }
            baseTime.setHours(baseTime.getHours() - 1);
        }
        Logger.error('[ApiService] Failed to load LATEST VIZ.json after all attempts.');
        return { vizDataResult: DEFAULT_VIZ_DATA };
    },
};

// --- Helper Date & Number Formatters ---
const formatDate = (dateString, options = { month: 'short', day: 'numeric', weekday: 'short' }) => {
    if (!dateString || dateString === "N/A") return "N/A";
    try {
        const date = new Date(dateString.replace(/-/g, '/') + 'T00:00:00');
        if (isNaN(date.getTime())) return dateString;
        const mainPart = date.toLocaleDateString('en-US', { month: options.month, day: options.day });
        const weekDayPart = `(${date.toLocaleDateString('en-US', { weekday: options.weekday })})`;
        return `${mainPart} ${weekDayPart}`;
    } catch (e) { return dateString; }
};

const parseDateTime = (timeStr) => {
    if (!timeStr) return null;
    const date = new Date(timeStr.includes('T') ? timeStr : timeStr.replace(' ', 'T'));
    return isNaN(date.getTime()) ? null : date;
};

const getOffsetDateString = (baseDateStr, dayOffset) => {
    if (!baseDateStr || baseDateStr === "N/A") return "N/A";
    const baseDate = parseDateTime(`${baseDateStr}T00:00:00`);
    if (!baseDate) return "N/A";
    baseDate.setDate(baseDate.getDate() + dayOffset);
    return `${baseDate.getFullYear()}-${String(baseDate.getMonth() + 1).padStart(2, '0')}-${String(baseDate.getDate()).padStart(2, '0')}`;
};

const getPredictionAtTime = (predictions, targetDateTime) => {
    if (!predictions || !targetDateTime) return null;
    const targetISO = targetDateTime.toISOString().substring(0, 16); // Compare yyyy-MM-ddTHH:mm
    return predictions.find(p => p.Time.startsWith(targetISO)) || null;
}

// --- Icon Components ---
const SunIcon = () => ( <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/></svg> );
const MoonIcon = () => ( <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg> );
const ChevronDownIcon = () => ( <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor" className="w-5 h-5 transition-transform duration-300"> <path strokeLinecap="round" strokeLinejoin="round" d="m19.5 8.25-7.5 7.5-7.5-7.5" /> </svg> );
const ChevronUpIcon = () => ( <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor" className="w-5 h-5 transition-transform duration-300"> <path strokeLinecap="round" strokeLinejoin="round" d="m4.5 15.75 7.5-7.5 7.5 7.5" /> </svg> );

// --- UI Components ---
const Header = ({ currentView, setCurrentView, lastUpdateTime, onRefreshData }) => {
    const [theme, toggleTheme] = useTheme();
    const { countdown, timerFillWidth, timerFillColor } = useUpdateTimer(lastUpdateTime, onRefreshData);
    const NavLink = ({ viewName, children }) => ( <a href="#" onClick={(e) => { e.preventDefault(); setCurrentView(viewName); }} className={`px-3 py-2 sm:px-4 rounded-md text-sm font-medium transition-colors ${currentView === viewName ? 'bg-indigo-600 text-white dark:bg-indigo-500' : 'text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-700'}`}>{children}</a> );

    return ( 
        <header className="bg-white dark:bg-slate-800 shadow-lg p-3 sm:p-4 mb-6 sticky top-0 z-50"> 
            <div className="container mx-auto flex flex-col sm:flex-row justify-between items-center pt-3 sm:pt-4">
                 <div className="flex items-center mb-2 sm:mb-0"> <img src={theme === 'dark' ? "https://ledger-prediction-charting-website.s3.us-west-1.amazonaws.com/ATHENALogoD.png" : "https://ledger-prediction-charting-website.s3.us-west-1.amazonaws.com/ATHENAlogo.PNG"} alt="Athena Logo" className="h-10 sm:h-12 mr-2 sm:mr-3" onError={(e) => { e.target.onerror = null; e.target.src="https://placehold.co/150x50/000000/FFFFFF?text=ATHENA"; }}/> </div> 
                 <div className="flex flex-col sm:flex-row items-center space-y-2 sm:space-y-0 sm:space-x-2 md:space-x-4"> 
                    <div className="text-xs text-gray-600 dark:text-gray-400 text-center sm:text-left"> 
                        <div>Data File Time: <span className="font-semibold">{lastUpdateTime || "N/A"}</span></div> 
                        <div className="flex items-center justify-center sm:justify-start mt-1"> 
                            <div className="w-20 sm:w-24 h-1.5 bg-gray-300 dark:bg-gray-600 rounded-full overflow-hidden mr-1.5 sm:mr-2"> 
                                <div className={`h-full rounded-full transition-all duration-1000 ease-linear ${timerFillColor}`} style={{ width: timerFillWidth }}></div> 
                            </div> 
                            <span className="text-xs">Next Update: <span className="font-semibold">{countdown}</span></span> 
                        </div>
                    </div> 
                    <nav className="flex space-x-1 sm:space-x-2"> <NavLink viewName="dashboard">Dashboard</NavLink> <NavLink viewName="pdp">PDP</NavLink> </nav> 
                    <button onClick={toggleTheme} className="p-2 rounded-full hover:bg-gray-200 dark:hover:bg-gray-700 text-gray-700 dark:text-gray-300 transition-colors" aria-label="Toggle theme"> {theme === 'light' ? <MoonIcon /> : <SunIcon />} </button> 
                </div> 
            </div> 
        </header> 
    );
};

const MetricCard = ({ title, value, unit = "units", subtext, size = "default", children, valueColor = "text-indigo-600 dark:text-indigo-400", change, changeColor = 'text-slate-500' }) => { 
    const titleSize = size === 'large' ? 'text-lg sm:text-xl' : 'text-sm'; 
    const valueSize = size === 'large' ? 'text-3xl sm:text-4xl' : 'text-2xl'; 
    const unitSize = size === 'large' ? 'text-xl sm:text-2xl' : 'text-lg';

    let formattedValue = '--';
    if (value !== null && value !== undefined && !isNaN(value)) {
        let options = { maximumFractionDigits: 0 };
        if (unit === "hrs" || unit === "units/hr" || unit === "%" || unit === "days") {
            options.maximumFractionDigits = 1;
        }
        if (Math.abs(value) < 1 && Math.abs(value) > 0 && unit !== "%") {
             options.maximumFractionDigits = 2;
        }
        formattedValue = value.toLocaleString(undefined, options);
    }
    
    return ( 
      <div className={`bg-slate-50 dark:bg-slate-700/80 p-4 rounded-xl shadow-lg hover:shadow-indigo-300/30 dark:hover:shadow-indigo-800/30 transition-shadow flex flex-col justify-between min-h-[120px]`}> 
        <div> 
          <p className={`${titleSize} text-gray-500 dark:text-gray-400 mb-1 truncate`}>{title}</p> 
          <div className="flex items-baseline">
            <p className={`${valueSize} font-bold ${valueColor}`}>{formattedValue}</p>
            {unit && <span className={`${unitSize} font-medium text-gray-600 dark:text-gray-300 ml-1`}>{unit}</span>}
          </div>
          {change && <p className={`text-sm font-semibold ${changeColor}`}>{change}</p>}
          {subtext && <p className="text-xs text-gray-400 dark:text-gray-500 mt-1">{subtext}</p>} 
        </div> 
        {children} 
      </div> 
    );
};

// --- Chart Components ---

// Custom plugin to draw glide path labels
const glidePathLabelsPlugin = (theme) => ({
    id: 'glidePathLabels',
    afterDraw: (chart) => {
        const { ctx, chartArea: { right }, scales: { y } } = chart;
        const datasets = [
            { datasetIndex: 2, color: theme === 'dark' ? '#a5f3c3' : '#16a34a', label: 'Pred' },
            { datasetIndex: 1, color: theme === 'dark' ? '#bae6fd' : '#0ea5e9', label: 'Upper' },
            { datasetIndex: 0, color: theme === 'dark' ? '#bae6fd' : '#0ea5e9', label: 'Lower' }
        ];
        ctx.save();
        ctx.font = 'bold 12px sans-serif';
        ctx.textAlign = 'left';
        ctx.textBaseline = 'middle';
        datasets.forEach(({datasetIndex, color, label}) => {
            const dataset = chart.data.datasets[datasetIndex];
            if (!dataset || !dataset.data || dataset.data.length === 0) return;
            const lastDataPoint = dataset.data[dataset.data.length - 1];
            if (lastDataPoint === undefined || lastDataPoint === null) return;
            const yPos = y.getPixelForValue(lastDataPoint);
            if (yPos && yPos > chart.chartArea.top && yPos < chart.chartArea.bottom) {
                 const text = `${label}: ${Math.round(lastDataPoint).toLocaleString()}`;
                 ctx.fillStyle = color;
                 ctx.fillText(text, right + 8, yPos);
            }
        });
        ctx.restore();
    }
});


const TodayPredictionActualChart = ({ currentDayData, theme }) => { 
    if (!currentDayData || !currentDayData.date || currentDayData.date === "N/A" || !currentDayData.sarima_predictions.length) { 
        return <div className="h-96 bg-slate-100 dark:bg-slate-700 flex items-center justify-center rounded-md text-gray-400 dark:text-slate-500">Today's prediction data not available.</div>; 
    }
    
    const { sarima_predictions, current_day_data = [], previous_year_data = [] } = currentDayData;
    const gridColor = theme === 'dark' ? 'rgba(71, 85, 105, 0.5)' : 'rgba(203, 213, 225, 0.5)'; 
    const textColor = theme === 'dark' ? '#cbd5e1' : '#475569';
    
    const chartData = useMemo(() => {
        const labels = sarima_predictions.map(p => {
            const d = parseDateTime(p.Time);
            return d ? `${String(d.getHours()).padStart(2, '0')}:00` : '';
        });
        const actualsDataMap = new Map(current_day_data.map(p => {
            const d = parseDateTime(p.Time);
            return d ? [`${String(d.getHours()).padStart(2, '0')}:00`, p.Workable] : [null, null];
        }));
        const prevYearDataMap = new Map(previous_year_data.map(p => {
            const d = parseDateTime(p.Time);
            return d ? [`${String(d.getHours()).padStart(2, '0')}:00`, p.Workable] : [null, null];
        }));

        return {
            labels,
            datasets: [ 
                {
                    label: 'Prediction Lower Bound',
                    data: sarima_predictions.map(p => p.Predicted_Workable_Display_Lower),
                    borderColor: 'transparent',
                    pointRadius: 0,
                    fill: false,
                },
                {
                    label: 'Prediction Upper Bound',
                    data: sarima_predictions.map(p => p.Predicted_Workable_Display_Upper),
                    borderColor: 'transparent',
                    backgroundColor: theme === 'dark' ? 'rgba(52, 211, 153, 0.2)' : 'rgba(16, 185, 129, 0.2)',
                    pointRadius: 0,
                    fill: '-1', 
                },
                { 
                    label: 'SARIMA Predictions', 
                    data: sarima_predictions.map(p => p.Predicted_Workable), 
                    borderColor: theme === 'dark' ? '#6ee7b7' : '#10b981', 
                    tension: 0.3, 
                    pointRadius: 2, 
                    pointHoverRadius: 5, 
                    borderWidth: 2.5, 
                },
                { 
                    label: 'Actual Workable Units', 
                    data: labels.map(label => actualsDataMap.get(label) ?? null),
                    borderColor: theme === 'dark' ? '#7dd3fc' : '#0ea5e9', 
                    backgroundColor: 'transparent', 
                    tension: 0.3, 
                    pointRadius: 2, 
                    pointHoverRadius: 5, 
                    borderWidth: 2, 
                    spanGaps: true,
                }, 
                { 
                    label: 'Previous Year Workable', 
                    data: labels.map(label => prevYearDataMap.get(label) ?? null),
                    borderColor: theme === 'dark' ? '#94a3b8' : '#64748b', 
                    backgroundColor: 'transparent', 
                    tension: 0.3, 
                    pointRadius: 1, 
                    pointHoverRadius: 4, 
                    borderWidth: 1.5, 
                    borderDash: [5, 5], 
                    hidden: true, 
                    spanGaps: true,
                }, 
            ], 
        };
    }, [sarima_predictions, current_day_data, previous_year_data, theme]);
    
    const options = useMemo(() => ({ 
        responsive: true, maintainAspectRatio: false, 
        layout: { padding: { right: 100 } },
        scales: { 
            x: { 
                type: 'category',
                title: { display: true, text: `Time (${formatDate(currentDayData.date)})`, color: textColor, font: {weight: 'bold'} }, 
                ticks: { color: textColor, maxRotation: 0, autoSkipPadding: 20 }, 
                grid: { color: gridColor }, 
            }, 
            y: { 
                beginAtZero: false, 
                title: { display: true, text: 'Workable Units', color: textColor, font: {weight: 'bold'} }, 
                ticks: { color: textColor, callback: v => v.toLocaleString() }, 
                grid: { color: gridColor }, 
            }, 
        }, 
        plugins: { 
            legend: { 
                position: 'top', 
                labels: { color: textColor, font: {size: 14}, filter: item => !item.text.includes('Bound') } 
            }, 
            tooltip: { 
                mode: 'index', 
                intersect: false, 
                callbacks: { label: c => `${c.dataset.label}: ${c.parsed.y.toLocaleString()}` } 
            } 
        }, 
    }), [currentDayData.date, textColor, gridColor]);

    return ( <div className="h-72 md:h-96"> <Line key={currentDayData.date} data={chartData} options={options} plugins={[glidePathLabelsPlugin(theme)]}/> </div> );
};

const ExtendedForecastChart = ({ predictions, theme }) => { 
    if (!Array.isArray(predictions) || predictions.length === 0) { return <div className="h-96 bg-slate-100 dark:bg-slate-700 flex items-center justify-center rounded-md text-gray-400 dark:text-slate-500">No extended forecast data available.</div>; }
    
    const gridColor = theme === 'dark' ? 'rgba(71, 85, 105, 0.5)' : 'rgba(203, 213, 225, 0.5)'; 
    const textColor = theme === 'dark' ? '#cbd5e1' : '#475569';

    const chartData = useMemo(() => ({
        labels: predictions.map(p => {
            const d = parseDateTime(p.Time);
            return d ? `${d.toLocaleDateString('en-us',{month:'short', day:'numeric'})} ${String(d.getHours()).padStart(2, '0')}:00` : '';
        }),
        datasets: [ 
            { 
                label: 'Lower Bound', 
                data: predictions.map(p => p.Predicted_Workable_Display_Lower), 
                borderColor: 'transparent', 
                backgroundColor: 'transparent', 
                pointRadius: 0, fill: false 
            },
            { 
                label: 'Upper Bound', 
                data: predictions.map(p => p.Predicted_Workable_Display_Upper), 
                borderColor: 'transparent', 
                backgroundColor: theme === 'dark' ? 'rgba(56, 189, 248, 0.1)' : 'rgba(14, 165, 233, 0.1)', 
                pointRadius: 0, 
                fill: '-1' 
            },
            { 
                label: 'Predicted Workable', 
                data: predictions.map(p => p.Predicted_Workable), 
                borderColor: theme === 'dark' ? '#7dd3fc' : '#0ea5e9', 
                tension: 0.3, 
                pointRadius: 1, 
                pointHoverRadius: 4, 
                borderWidth: 2 
            },
        ], 
    }), [predictions, theme]);
    
    const options = useMemo(() => ({
        responsive: true, maintainAspectRatio: false, 
        scales: { 
            x: { type: 'category', title: { display: true, text: 'Time', color: textColor, font: {weight: 'bold'} }, ticks: { color: textColor, maxRotation: 45, autoSkip: true, maxTicksLimit: 12 }, grid: { color: gridColor } }, 
            y: { beginAtZero: false, title: { display: true, text: 'Cumulative Workable Units (by Day)', color: textColor, font: {weight: 'bold'} }, ticks: { color: textColor, callback: v => v.toLocaleString() }, grid: { color: gridColor } }, 
        }, 
        plugins: { 
            legend: { display: false }, 
            tooltip: { 
                mode: 'index', 
                intersect: false, 
                callbacks: { label: c => `Predicted: ${c.parsed.y.toLocaleString()}` } 
            } 
        }, 
    }), [textColor, gridColor]);
    
    return ( <div className="h-72 md:h-96"> <Line key={predictions[0]?.Time} data={chartData} options={options} /> </div> );
};

// --- PDP Page Component ---
const ALL_SHIFT_DEFINITIONS = {
    currentNight: { key: 'currentNight', name: "Current Night Shift", quarters: [ {id: 'cnq1', label: "18:00-21:00", startHour: 18, endHour: 21, dateKey: 'current_day_date' }, {id: 'cnq2', label: "21:00-00:00", startHour: 21, endHour: 0,  dateKey: 'current_day_date' }, {id: 'cnq3', label: "00:00-03:00", startHour: 0,  endHour: 3,  dateKey: 'next_day_date' }, {id: 'cnq4', label: "03:00-06:00", startHour: 3,  endHour: 6,  dateKey: 'next_day_date' } ] },
    nextDay: { key: 'nextDay', name: "Next Day Shift", quarters: [ {id: 'ndq1', label: "06:00-09:00", startHour: 6,  endHour: 9, dateKey: 'next_day_date' }, {id: 'ndq2', label: "09:00-12:00", startHour: 9, endHour: 12, dateKey: 'next_day_date' }, {id: 'ndq3', label: "12:00-15:00", startHour: 12, endHour: 15, dateKey: 'next_day_date' }, {id: 'ndq4', label: "15:00-18:00", startHour: 15, endHour: 18, dateKey: 'next_day_date' } ] },
    nextNight: { key: 'nextNight', name: "Next Night Shift", quarters: [ {id: 'nnq1', label: "18:00-21:00", startHour: 18, endHour: 21, dateKey: 'next_day_date' }, {id: 'nnq2', label: "21:00-00:00", startHour: 21, endHour: 0,  dateKey: 'next_day_date' }, {id: 'nnq3', label: "00:00-03:00", startHour: 0,  endHour: 3,  dateKey: 'day_after_next_date' }, {id: 'nnq4', label: "03:00-06:00", startHour: 3,  endHour: 6,  dateKey: 'day_after_next_date' } ] },
};
const ORDERED_SHIFT_KEYS = ['currentNight', 'nextDay', 'nextNight'];

const calculateQuarterVolumeUtil = (quarter, data, shiftDates) => {
    let qActualDateStr = shiftDates[quarter.dateKey];
    if (!qActualDateStr) return 0;
    
    const predictionsSource = data.extended_predictions?.predictions || [];
    if (!predictionsSource.length) return 0;

    let startDateStr = qActualDateStr;
    let endDateStr = qActualDateStr;
    
    if (quarter.startHour > quarter.endHour && quarter.endHour !== 0) { 
        endDateStr = getOffsetDateString(startDateStr, 1);
    }
    
    const endHourForLookup = quarter.endHour === 0 ? 23 : quarter.endHour - 1;
    let startHourForLookup = quarter.startHour === 0 ? -1 : quarter.startHour - 1;

    const endPredEntry = getPredictionAtTime(predictionsSource, parseDateTime(`${endDateStr}T${String(endHourForLookup).padStart(2,'0')}:00`));
    const startPredEntry = startHourForLookup >= 0 ? getPredictionAtTime(predictionsSource, parseDateTime(`${startDateStr}T${String(startHourForLookup).padStart(2,'0')}:00`)) : { Predicted_Workable: 0 };

    if (!endPredEntry || !startPredEntry) return 0;
    
    const volAtEnd = endPredEntry.Predicted_Workable;
    const volAtStart = startPredEntry.Predicted_Workable;

    // Handle night shift quarters that cross midnight
    if (quarter.startHour > quarter.endHour && quarter.endHour !== 0) {
        const volAt23 = getPredictionAtTime(predictionsSource, parseDateTime(`${startDateStr}T23:00`))?.Predicted_Workable || 0;
        return (volAt23 - volAtStart) + volAtEnd;
    }
    
    return Math.max(0, volAtEnd - volAtStart);
};


const ShiftQuarterPlannerCard = ({ shiftDefinition, data, quarterlyInputs, handleInputChange, targetTPH, shiftDates }) => {
    const [isExpanded, setIsExpanded] = useState(true);
    let shiftDisplayDateStr = shiftDefinition.key === 'currentNight' ? shiftDates.current_day_date : shiftDates.next_day_date;

    const calculateQuarterVolume = useCallback((quarter) => calculateQuarterVolumeUtil(quarter, data, shiftDates), [data, shiftDates]);

    const shiftAggregates = useMemo(() => {
        let totalShiftExVol = 0, totalShiftPlannedHours = 0, totalShiftHoursToSolve = 0, totalShiftPlannedVCap = 0;
        shiftDefinition.quarters.forEach(quarter => {
            const exVolForQuarter = calculateQuarterVolume(quarter);
            totalShiftExVol += exVolForQuarter;
            const inputs = quarterlyInputs[quarter.id] || {};
            const plannedHours = parseFloat(inputs.plannedHours) || 0;
            const plannedRate = parseFloat(inputs.plannedRate) || 0;
            totalShiftPlannedHours += plannedHours;
            totalShiftHoursToSolve += targetTPH > 0 ? exVolForQuarter / targetTPH : 0;
            totalShiftPlannedVCap += plannedHours * plannedRate;
        });
        const averageShiftTPH = totalShiftPlannedHours > 0 ? totalShiftExVol / totalShiftPlannedHours : 0;
        return { totalShiftExVol, averageShiftTPH, totalShiftPlannedHours, totalShiftPlannedVCap, totalShiftHoursToSolve };
    }, [shiftDefinition, quarterlyInputs, targetTPH, calculateQuarterVolume]);

    const plannedHoursColor = shiftAggregates.totalShiftPlannedHours >= shiftAggregates.totalShiftHoursToSolve ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400';

    return (
        <div className="bg-white dark:bg-slate-800 shadow-xl rounded-lg p-6">
            <div className="flex justify-between items-center border-b pb-3 border-slate-200 dark:border-slate-700 mb-4">
                <div>
                    <h3 className="text-xl font-semibold text-gray-700 dark:text-white">{shiftDefinition.name}</h3>
                    <p className="text-sm text-indigo-500 dark:text-indigo-400 font-medium">Shift Date: {formatDate(shiftDisplayDateStr)}</p>
                </div>
                <button onClick={() => setIsExpanded(!isExpanded)} className="p-2 rounded-md hover:bg-slate-100 dark:hover:bg-slate-700" aria-label="Toggle expand">
                    {isExpanded ? <ChevronUpIcon /> : <ChevronDownIcon />}
                </button>
            </div>
            <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3 mb-6">
                <MetricCard title="Expected Vol." value={shiftAggregates.totalShiftExVol} size="small"/>
                <MetricCard title="Planned VCap" value={shiftAggregates.totalShiftPlannedVCap} unit="VCap" size="small"/>
                <MetricCard title="Avg. Planned TPH" value={shiftAggregates.averageShiftTPH} unit="units/hr" size="small"/>
                <MetricCard title="Required Hrs" value={shiftAggregates.totalShiftHoursToSolve} unit="hrs" size="small" />
                <MetricCard title="Planned Hrs" value={shiftAggregates.totalShiftPlannedHours} unit="hrs" size="small" valueColor={plannedHoursColor} />
            </div>
            {isExpanded && (
                <div className="space-y-6">
                    {shiftDefinition.quarters.map((quarter) => {
                        const inputs = quarterlyInputs[quarter.id] || { plannedHours: "", plannedRate: CONSTANTS.DEFAULT_PLANNED_RATE };
                        const exVolForQuarter = calculateQuarterVolume(quarter);
                        const hoursToSolve = targetTPH > 0 ? exVolForQuarter / targetTPH : 0;
                        const discrepancy = (parseFloat(inputs.plannedHours) || 0) - hoursToSolve;
                        return (
                            <div key={quarter.id} className="p-4 bg-slate-50 dark:bg-slate-700 rounded-lg shadow-md space-y-3">
                                <h4 className="font-semibold text-md text-indigo-700 dark:text-indigo-400">{quarter.label}</h4>
                                <div className="grid grid-cols-2 gap-x-4 gap-y-2 text-sm">
                                    <span>Expected Vol:</span> <span className="font-medium">{exVolForQuarter.toLocaleString(undefined, {maximumFractionDigits:0})}</span>
                                    <label htmlFor={`${quarter.id}_plannedHours`}>Planned Hrs:</label> <input type="number" id={`${quarter.id}_plannedHours`} value={inputs.plannedHours} onChange={e => handleInputChange(quarter.id, 'plannedHours', e.target.value)} className="w-full p-1.5 border rounded-md bg-white dark:bg-slate-800 text-sm"/>
                                    <label htmlFor={`${quarter.id}_plannedRate`}>Planned Rate:</label> <input type="number" id={`${quarter.id}_plannedRate`} value={inputs.plannedRate} onChange={e => handleInputChange(quarter.id, 'plannedRate', e.target.value)} className="w-full p-1.5 border rounded-md bg-white dark:bg-slate-800 text-sm"/>
                                    <span>Hrs to Solve:</span> <span className="font-medium">{hoursToSolve.toLocaleString(undefined, {maximumFractionDigits:1})}</span>
                                    <span>Discrepancy:</span> <span className={`font-medium ${discrepancy >= 0 ? 'text-green-500' : 'text-red-500'}`}>{discrepancy.toLocaleString(undefined, {maximumFractionDigits:1})} hrs</span>
                                </div>
                            </div>
                        );
                    })}
                </div>
            )}
        </div>
    );
};

const PDPPage = ({ data, quarterlyInputs, setQuarterlyInputs, targetTPH, setTargetTPH }) => {
    const [triggerAutoBalance, setTriggerAutoBalance] = useState(true);

    const shiftDates = useMemo(() => ({
        current_day_date: data.current_day?.date || "N/A",
        next_day_date: getOffsetDateString(data.current_day?.date, 1),
        day_after_next_date: getOffsetDateString(data.current_day?.date, 2),
    }), [data.current_day]);

    useEffect(() => {
        if (triggerAutoBalance && data.extended_predictions?.predictions?.length > 0 && targetTPH > 0) {
            const newQuarterlyData = {};
            ORDERED_SHIFT_KEYS.forEach(key => {
                const shiftDef = ALL_SHIFT_DEFINITIONS[key];
                shiftDef.quarters.forEach(quarter => {
                    const exVolForQuarter = calculateQuarterVolumeUtil(quarter, data, shiftDates);
                    const hoursToSolve = targetTPH > 0 ? exVolForQuarter / targetTPH : 0;
                    newQuarterlyData[quarter.id] = {
                        plannedHours: hoursToSolve > 0.05 ? parseFloat(hoursToSolve.toFixed(1)) : 0,
                        plannedRate: (quarterlyInputs && quarterlyInputs[quarter.id]?.plannedRate) ? quarterlyInputs[quarter.id].plannedRate : CONSTANTS.DEFAULT_PLANNED_RATE,
                    };
                });
            });
            setQuarterlyInputs(newQuarterlyData);
            setTriggerAutoBalance(false);
        }
    }, [data, targetTPH, shiftDates, triggerAutoBalance, setQuarterlyInputs, quarterlyInputs]);

    const handleInputChange = (quarterId, field, value) => {
        const numericValue = parseFloat(value);
        setQuarterlyInputs(prev => ({ ...prev, [quarterId]: { ...prev[quarterId], [field]: isNaN(numericValue) ? '' : numericValue } }));
    };

    if (!data || data.time === "N/A") {
        return <div className="text-center py-10"><div className="animate-spin rounded-full h-12 w-12 border-b-2 border-indigo-500 mx-auto"></div><p>Loading PDP...</p></div>;
    }

    return (
        <div className="container mx-auto px-2 sm:px-4">
            <h2 className="text-2xl sm:text-3xl font-bold text-indigo-700 dark:text-indigo-400 mb-6">Production Daily Plan</h2>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
                <div className="p-4 bg-white dark:bg-slate-800 shadow-lg rounded-lg">
                    <label htmlFor="targetTPHInput" className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Set Overall Target TPH:</label>
                    <div className="flex items-center">
                        <input type="number" id="targetTPHInput" value={targetTPH} onChange={(e) => setTargetTPH(parseFloat(e.target.value) || 0)} className="w-full p-2 border rounded-md"/>
                        <button onClick={() => setTriggerAutoBalance(true)} className="ml-3 px-3 py-2 bg-emerald-600 hover:bg-emerald-700 text-white font-semibold rounded-lg">Apply Optimal Hrs</button>
                    </div>
                </div>
            </div>
            <div className="space-y-8 mt-6">
                {ORDERED_SHIFT_KEYS.map(key => (
                    <ShiftQuarterPlannerCard key={key} shiftDefinition={ALL_SHIFT_DEFINITIONS[key]} data={data} quarterlyInputs={quarterlyInputs} handleInputChange={handleInputChange} targetTPH={targetTPH} shiftDates={shiftDates} />
                ))}
            </div>
        </div>
    );
};

// --- New Dashboard Components ---
const ExecutiveSummaryCard = ({ data }) => {
    if (!data || !data.current_day || !data.prophet_performance_metrics) return null;
    const { current_day, prophet_performance_metrics, Ledger_Information } = data;
    const todayEODSarima = current_day.sarima_predictions?.[current_day.sarima_predictions.length - 1]?.Predicted_Workable || 0;
    const networkTarget = prophet_performance_metrics.network_prediction_target || 0;
    const deviation = networkTarget > 0 ? ((todayEODSarima - networkTarget) / networkTarget) * 100 : 0;
    
    const lastYearEOD = current_day.previous_year_data?.[current_day.previous_year_data.length - 1]?.Workable || 0;
    const yoyChange = lastYearEOD > 0 ? ((todayEODSarima - lastYearEOD) / lastYearEOD) * 100 : 0;
    
    const actuals = current_day.current_day_data || [];
    const latestActual = actuals.length > 0 ? actuals[actuals.length-1] : {Time: null, Workable: 0};
    const ssf = Ledger_Information?.metrics?.SSF?.slice(-1)[0] || 0;

    return (
        <div className="bg-white dark:bg-slate-800 shadow-xl rounded-lg p-6 mb-8">
            <h3 className="text-xl font-semibold text-gray-700 dark:text-white mb-4">Executive Summary</h3>
            <ul className="space-y-3 text-sm text-slate-600 dark:text-slate-300">
                <li className="flex items-start"><span className="text-indigo-500 mr-2 mt-1">&#9656;</span><span>Forecast is <strong>{Math.abs(deviation).toFixed(1)}% {deviation >= 0 ? 'above' : 'below'}</strong> Network Plan for today. (Pred: {todayEODSarima.toLocaleString(undefined, {maximumFractionDigits:0})}, Plan: {networkTarget.toLocaleString(undefined, {maximumFractionDigits:0})})</span></li>
                <li className="flex items-start"><span className="text-indigo-500 mr-2 mt-1">&#9656;</span><span>Today's predicted EOD volume is <strong>{yoyChange >= 0 ? 'up' : 'down'} {Math.abs(yoyChange).toFixed(1)}%</strong> YoY.</span></li>
                {latestActual.Time && <li className="flex items-start"><span className="text-indigo-500 mr-2 mt-1">&#9656;</span><span>As of {parseDateTime(latestActual.Time)?.toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'})}, actual volume is <strong>{latestActual.Workable.toLocaleString(undefined, {maximumFractionDigits:0})}</strong> units.</span></li>}
                <li className="flex items-start"><span className="text-indigo-500 mr-2 mt-1">&#9656;</span><span>Shipped So Far (SSF): <strong>{ssf.toLocaleString(undefined, {maximumFractionDigits:0})}</strong>.</span></li>
            </ul>
        </div>
    )
}

const LedgerInsightsCard = ({ ledgerInfo }) => {
    if (!ledgerInfo || !ledgerInfo.metrics) return null;
    const { APU, Eligible, CurrWork, SSF, DOBL } = ledgerInfo.metrics;
    
    const latestAPU = APU?.slice(-1)[0] ?? 0;
    const latestEligible = Eligible?.slice(-1)[0] ?? 0;
    const latestCurrWork = CurrWork?.slice(-1)[0] ?? 0;
    const latestSSF = SSF?.slice(-1)[0] ?? 0;
    const latestDOBL = DOBL?.slice(-1)[0] ?? 0;

    const totalWorkable = latestCurrWork + latestEligible;
    const processingEfficiency = latestAPU > 0 ? (latestSSF / latestAPU) * 100 : 0;
    
    return(
        <div className="bg-white dark:bg-slate-800 shadow-xl rounded-lg p-6">
            <h3 className="text-xl font-semibold text-gray-700 dark:text-white mb-4">Key Ledger Metrics</h3>
             <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
                 <MetricCard title="Total Workable" value={totalWorkable} subtext="CurrWork + Eligible" size="small"/>
                 <MetricCard title="Available to Pick (APU)" value={latestAPU} size="small"/>
                 <MetricCard title="Shipped So Far (SSF)" value={latestSSF} size="small"/>
                 <MetricCard title="Days of Backlog" value={latestDOBL} unit="days" size="small"/>
                 <MetricCard title="Processing Efficiency" value={processingEfficiency} unit="%" subtext="SSF / APU" size="small"/>
             </div>
        </div>
    )
}

const getTrendIndicatorInfo = (percentageChange) => {
    if (percentageChange === null || percentageChange === undefined || isNaN(parseFloat(percentageChange))) {
        return { arrow: "", colorClass: "text-slate-500", percentageText: "N/A" };
    }
    const change = parseFloat(percentageChange);
    let arrow = "↔️"; let colorClass = "text-slate-500 dark:text-slate-400";
    if (change > 5) { arrow = "⬆️⬆️"; colorClass = "text-green-600 dark:text-green-400"; } 
    else if (change > 1) { arrow = "⬆️"; colorClass = "text-green-500 dark:text-green-500"; } 
    else if (change < -5) { arrow = "⬇️⬇️"; colorClass = "text-red-600 dark:text-red-400"; } 
    else if (change < -1) { arrow = "⬇️"; colorClass = "text-red-500 dark:text-red-500"; }
    return { arrow, colorClass, percentageText: `${change > 0 ? '+' : ''}${change.toFixed(1)}%` };
};

const HistoricalDailySummaryCard = ({ historicalContext }) => {
    const [isExpanded, setIsExpanded] = useState(false);
    if (!historicalContext || !historicalContext.daily_summary_trends || !historicalContext.overall_summary) return null;

    const daysOfWeekOrder = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
    const overall7DayAvg = historicalContext.overall_summary?.avg_daily_volume_rolling_7_days || 0;
    const trendPeriodDays = historicalContext.trend_period_days || 45;
    const longTermOccurrences = historicalContext.num_weeks_for_avg || 6;
    const shortTermOccurrences = historicalContext.short_term_ma_occurrences || 3;

    return (
        <div className="mb-6 p-4 bg-white dark:bg-slate-800 shadow-lg rounded-lg">
            <button onClick={() => setIsExpanded(!isExpanded)} className="w-full flex justify-between items-center text-left text-lg font-semibold text-indigo-700 dark:text-indigo-400 mb-2">
                <span>Daily Historical Trends (Last {trendPeriodDays} Days)</span>
                {isExpanded ? <ChevronUpIcon /> : <ChevronDownIcon />}
            </button>
            {isExpanded && (
                <div className="mt-3 space-y-4">
                    <div className="text-center pb-2 border-b border-slate-200 dark:border-slate-700">
                        <p className="text-md font-semibold text-slate-700 dark:text-slate-200">
                            Overall 7-Day Rolling Avg: <span className="text-indigo-600 dark:text-indigo-400 ml-1">{overall7DayAvg.toLocaleString(undefined, {maximumFractionDigits:0})} units</span>
                        </p>
                    </div>
                    <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-7 gap-3">
                        {daysOfWeekOrder.map(day => {
                            const dayData = historicalContext.daily_summary_trends[day] || {};
                            const longTermAvg = dayData[`avg_total_daily_volume_last_${longTermOccurrences}_occurrences`] || 0;
                            const shortTermAvg = dayData[`avg_total_daily_volume_last_${shortTermOccurrences}_occurrences`] || 0;
                            const lastOccurrenceTotal = dayData.last_occurrence_total_daily_volume || 0; 
                            const trendInfo = getTrendIndicatorInfo(dayData.trend_direction_pct_change);
                            return (
                                <div key={day} className="p-3 bg-slate-50 dark:bg-slate-700/60 rounded-lg shadow">
                                    <p className="font-bold text-md text-slate-800 dark:text-slate-100 mb-1.5 text-center">{day}</p>
                                    <div className="text-xs space-y-1 text-slate-600 dark:text-slate-300">
                                        <p>Last: <span className="font-medium float-right">{lastOccurrenceTotal.toLocaleString(undefined, {maximumFractionDigits:0})}</span></p>
                                        <p>Avg ({shortTermOccurrences}w): <span className="font-medium float-right">{shortTermAvg.toLocaleString(undefined, {maximumFractionDigits:0})}</span></p>
                                        <p>Avg ({longTermOccurrences}w): <span className="font-medium float-right">{longTermAvg.toLocaleString(undefined, {maximumFractionDigits:0})}</span></p>
                                        <p>Trend: <span className={`font-semibold float-right ${trendInfo.colorClass}`}>{trendInfo.arrow} {trendInfo.percentageText}</span></p>
                                    </div>
                                </div>
                            );
                        })}
                    </div>
                </div>
            )}
        </div>
    );
};

const BacklogTrajectoryCard = ({ data, quarterlyInputs }) => {
    const [isExpanded, setIsExpanded] = useState(false);
    
    const trajectoryData = useMemo(() => {
        if (!data.Ledger_Information || !data.extended_predictions) return { currentBacklog: 0, trajectory: [] };

        const { APU, Eligible } = data.Ledger_Information.metrics;
        const currentBacklog = (APU?.slice(-1)[0] || 0) + (Eligible?.slice(-1)[0] || 0);

        const shiftDates = {
            current_day_date: data.current_day?.date || "N/A",
            next_day_date: getOffsetDateString(data.current_day?.date, 1),
            day_after_next_date: getOffsetDateString(data.current_day?.date, 2),
        };

        let accumulatingBacklog = currentBacklog;
        const trajectory = [];

        ORDERED_SHIFT_KEYS.forEach(shiftKey => {
            const shiftDef = ALL_SHIFT_DEFINITIONS[shiftKey];
            if (!shiftDef) return;

            let totalExVol = 0;
            let totalPlannedVCap = 0;

            shiftDef.quarters.forEach(quarter => {
                totalExVol += calculateQuarterVolumeUtil(quarter, data, shiftDates);
                const inputs = quarterlyInputs[quarter.id] || {};
                totalPlannedVCap += (parseFloat(inputs.plannedRate) || CONSTANTS.DEFAULT_PLANNED_RATE) * (parseFloat(inputs.plannedHours) || 0);
            });
            
            const endOfShiftBacklog = Math.max(0, accumulatingBacklog + totalExVol - totalPlannedVCap);

            trajectory.push({
                shiftName: shiftDef.name,
                startBacklog: accumulatingBacklog,
                expectedVol: totalExVol,
                plannedVCap: totalPlannedVCap,
                endBacklog: endOfShiftBacklog,
            });

            accumulatingBacklog = endOfShiftBacklog;
        });
        
        return { currentBacklog, trajectory };
    }, [data, quarterlyInputs]);


    return (
        <div className="mb-6 p-4 bg-white dark:bg-slate-800 shadow-lg rounded-lg">
            <button onClick={() => setIsExpanded(!isExpanded)} className="w-full flex justify-between items-center text-left text-lg font-semibold text-indigo-700 dark:text-indigo-400 mb-2">
                <span>Backlog Handoff Trajectory</span>
                {isExpanded ? <ChevronUpIcon /> : <ChevronDownIcon />}
            </button>
            {isExpanded && (
                <div className="mt-3 space-y-4">
                    <div className="text-center pb-2 border-b border-slate-200 dark:border-slate-700">
                         <p className="text-md font-semibold text-slate-700 dark:text-slate-200">
                            Current Backlog (APU + Eligible): <span className="text-indigo-600 dark:text-indigo-400 ml-1">{trajectoryData.currentBacklog.toLocaleString(undefined, {maximumFractionDigits:0})} units</span>
                        </p>
                    </div>
                    <div className="space-y-3">
                       {trajectoryData.trajectory.map(step => (
                           <div key={step.shiftName} className="p-3 bg-slate-50 dark:bg-slate-700/60 rounded-lg shadow-sm">
                               <p className="font-bold text-md text-slate-800 dark:text-slate-100 mb-1.5">{step.shiftName}</p>
                               <div className="grid grid-cols-2 gap-x-4 text-xs">
                                   <span className="text-slate-500">Start of Shift Backlog:</span><span className="font-medium text-right">{step.startBacklog.toLocaleString(0)}</span>
                                   <span className="text-green-600 dark:text-green-400">+ Expected Volume:</span><span className="font-medium text-right text-green-600 dark:text-green-400">{step.expectedVol.toLocaleString(0)}</span>
                                   <span className="text-red-600 dark:text-red-500">- Planned VCap:</span><span className="font-medium text-right text-red-600 dark:text-red-500">{step.plannedVCap.toLocaleString(0)}</span>
                                   <hr className="col-span-2 my-1 border-slate-300 dark:border-slate-600"/>
                                   <span className="font-semibold">End of Shift Backlog:</span><span className="font-semibold text-right">{step.endBacklog.toLocaleString(0)}</span>
                               </div>
                           </div>
                       ))}
                    </div>
                </div>
            )}
        </div>
    );
};

const ShiftOutlookCard = ({ data }) => {
    const [upcomingShifts, setUpcomingShifts] = useState([]);

    useEffect(() => {
        const now = parseDateTime(data.time);
        const baseDate = parseDateTime(data.current_day.date + 'T00:00:00');
        if(!now || !baseDate || !data.extended_predictions.predictions.length) return;

        const allPotentialShifts = [
            { name: 'Today Night', type: 'Night', date: baseDate },
            { name: 'Tomorrow Day', type: 'Day', date: new Date(baseDate.getTime() + 24 * 3600 * 1000) },
            { name: 'Tomorrow Night', type: 'Night', date: new Date(baseDate.getTime() + 24 * 3600 * 1000) },
            { name: 'Day After Day', type: 'Day', date: new Date(baseDate.getTime() + 48 * 3600 * 1000) },
        ];

        const getShiftTimestamps = (shift) => {
            const shiftStart = new Date(shift.date);
            const shiftEnd = new Date(shift.date);
            if(shift.type === 'Day') {
                shiftStart.setHours(6,0,0,0);
                shiftEnd.setHours(18,0,0,0);
            } else {
                shiftStart.setHours(18,0,0,0);
                shiftEnd.setDate(shiftEnd.getDate() + 1);
                shiftEnd.setHours(6,0,0,0);
            }
            return { shiftStart, shiftEnd };
        };

        const calculatedShifts = allPotentialShifts
            .map(shift => {
                const { shiftStart, shiftEnd } = getShiftTimestamps(shift);
                if (now > shiftEnd) return null; // Shift is in the past

                let charge = 0;
                let range = 'N/A';
                
                if (shift.type === 'Night') {
                    const pred1800 = getPredictionAtTime(data.extended_predictions.predictions, shiftStart);
                    const pred2300 = getPredictionAtTime(data.extended_predictions.predictions, new Date(shiftStart.getTime() + 5 * 3600000));
                    const pred0600 = getPredictionAtTime(data.extended_predictions.predictions, shiftEnd);
                    if(pred1800 && pred2300 && pred0600) {
                        charge = (pred2300.Predicted_Workable - pred1800.Predicted_Workable) + pred0600.Predicted_Workable;
                        const lower = (pred2300.Predicted_Workable_Display_Lower - pred1800.Predicted_Workable_Display_Lower) + pred0600.Predicted_Workable_Display_Lower;
                        const upper = (pred2300.Predicted_Workable_Display_Upper - pred1800.Predicted_Workable_Display_Upper) + pred0600.Predicted_Workable_Display_Upper;
                        range = `${Math.round(lower).toLocaleString()} - ${Math.round(upper).toLocaleString()}`;
                    }
                } else { // Day
                    const pred0600 = getPredictionAtTime(data.extended_predictions.predictions, shiftStart);
                    const pred1800 = getPredictionAtTime(data.extended_predictions.predictions, shiftEnd);
                     if(pred0600 && pred1800) {
                        charge = pred1800.Predicted_Workable - pred0600.Predicted_Workable;
                        const lower = pred1800.Predicted_Workable_Display_Lower - pred0600.Predicted_Workable_Display_Lower;
                        const upper = pred1800.Predicted_Workable_Display_Upper - pred0600.Predicted_Workable_Display_Upper;
                        range = `${Math.round(lower).toLocaleString()} - ${Math.round(upper).toLocaleString()}`;
                    }
                }

                return { ...shift, charge, range, shiftStart };
            })
            .filter(Boolean) // Remove past shifts
            .sort((a,b) => a.shiftStart - b.shiftStart); // Sort by start time
        
        setUpcomingShifts(calculatedShifts.slice(0,2)); // Take the next 2
        
    }, [data.time, data.current_day.date, data.extended_predictions.predictions]);

    return(
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
            {upcomingShifts.map((shift, index) => (
                 <MetricCard 
                    key={index}
                    title={`${shift.name} Charge`} 
                    value={shift.charge}
                    size="large"
                    subtext={`Range: ${shift.range}`}
                />
            ))}
             {upcomingShifts.length < 2 && Array(2 - upcomingShifts.length).fill(null).map((_, index) => (
                  <MetricCard 
                    key={`placeholder-${index}`}
                    title="Upcoming Shift Charge"
                    value={"N/A"}
                    size="large"
                    subtext="Not enough future data"
                />
            ))}
        </div>
    );
};


const DashboardPage = ({ data, theme, quarterlyInputs }) => {
    const { current_day, prophet_performance_metrics, Ledger_Information, extended_predictions, historical_context } = data; 
    
    if (!data || data.time === "N/A" || !current_day || !prophet_performance_metrics || !Ledger_Information) {
        return ( <div className="text-center py-10"><div className="animate-spin rounded-full h-12 w-12 border-b-2 border-indigo-500 mx-auto"></div><p>Loading dashboard...</p></div> );
    }

    const todayEODSarima = current_day.sarima_predictions?.[current_day.sarima_predictions.length - 1]?.Predicted_Workable || 0;
    const alpsTargetToday = prophet_performance_metrics.network_prediction_target || 0;
    const deviationVsAlps = alpsTargetToday > 0 ? ((todayEODSarima - alpsTargetToday) / alpsTargetToday) * 100 : 0;
    const lastYearEOD = current_day.previous_year_data?.[current_day.previous_year_data.length - 1]?.Workable || 0;
    const yoyChange = lastYearEOD > 0 ? ((todayEODSarima - lastYearEOD) / lastYearEOD) * 100 : 0;
    const yoyChangeColor = yoyChange > 0 ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400';

    const nextDayDateStr = getOffsetDateString(current_day.date, 1);
    const nextDayEodEntry = extended_predictions?.predictions.find(p => p.Time.startsWith(nextDayDateStr) && p.Time.includes('T23:00'));

    return (
        <div className="container mx-auto px-2 sm:px-4">
            <h2 className="text-2xl sm:text-3xl font-bold text-indigo-700 dark:text-indigo-400 mb-6">Operations Command Center</h2>
            <ExecutiveSummaryCard data={data} />
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
                <MetricCard title="EOD Prediction (Today)" value={todayEODSarima} subtext={`Plan: ${alpsTargetToday.toLocaleString(0)}`} size="large"/>
                <MetricCard title="Deviation vs. Plan" value={deviationVsAlps} unit="%" valueColor={deviationVsAlps >= 0 ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'} size="large" change={`${yoyChange.toFixed(1)}% vs LY`} changeColor={yoyChangeColor} />
                <MetricCard title={`EOD Prediction (${formatDate(nextDayDateStr, {month:'short', day:'numeric'})})`} value={nextDayEodEntry?.Predicted_Workable} subtext={`Range: ${nextDayEodEntry?.Predicted_Workable_Display_Lower?.toLocaleString(0)} - ${nextDayEodEntry?.Predicted_Workable_Display_Upper?.toLocaleString(0)}`} size="large"/>
            </div>
            
            <ShiftOutlookCard data={data} />
            <HistoricalDailySummaryCard historicalContext={historical_context} />
            <BacklogTrajectoryCard data={data} quarterlyInputs={quarterlyInputs} />

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mb-8">
                <LedgerInsightsCard ledgerInfo={Ledger_Information} />
                <div className="bg-white dark:bg-slate-800 shadow-xl rounded-lg p-6">
                    <h3 className="text-xl font-semibold text-gray-700 dark:text-white mb-4">Today's Hourly Performance</h3>
                    <TodayPredictionActualChart currentDayData={current_day} theme={theme} />
                </div>
            </div>
            <div className="bg-white dark:bg-slate-800 shadow-xl rounded-lg p-6">
                <h3 className="text-xl font-semibold text-gray-700 dark:text-white mb-4">Extended Demand Forecast (48hr+ Rolling)</h3>
                <ExtendedForecastChart predictions={extended_predictions?.predictions || []} theme={theme} />
            </div>
        </div>
    );
};


// --- Main App Component ---
function App() {
    const [currentView, setCurrentView] = useState('dashboard');
    const [vizData, setVizData] = useState(DEFAULT_VIZ_DATA);
    const [isLoading, setIsLoading] = useState(true);
    const [theme] = useTheme();

    const [pdpState, setPdpState] = useState({
        quarterlyInputs: {},
        targetTPH: CONSTANTS.DEFAULT_TARGET_TPH,
    });
    
    const setQuarterlyInputs = useCallback((value) => {
        setPdpState(prev => ({...prev, quarterlyInputs: typeof value === 'function' ? value(prev.quarterlyInputs) : value }));
    }, []);
    
    const setTargetTPH = useCallback((value) => {
       setPdpState(prev => ({...prev, targetTPH: typeof value === 'function' ? value(prev.targetTPH) : value }));
    }, []);


    const loadData = useCallback(async (isAutoRefresh = false) => {
        if (!isAutoRefresh) setIsLoading(true); 
        const { vizDataResult } = await ApiService.fetchLatestAvailableData();
        setVizData(vizDataResult || DEFAULT_VIZ_DATA);
        if (!isAutoRefresh) setIsLoading(false);
    }, []); 

    useEffect(() => { loadData(); }, [loadData]); 

    useEffect(() => { 
        const intervalId = setInterval(() => {
            if (currentView === 'dashboard') {
                Logger.log("Auto-refreshing dashboard data...");
                loadData(true);
            }
        }, CONSTANTS.REFRESH_INTERVAL);
        return () => clearInterval(intervalId);
    }, [loadData, currentView]); 
    
    return ( 
        <div className="min-h-screen bg-slate-100 dark:bg-slate-900 text-slate-900 dark:text-slate-100 transition-colors duration-300 font-sans"> 
            <Header currentView={currentView} setCurrentView={setCurrentView} lastUpdateTime={vizData?.time} onRefreshData={loadData} /> 
            <main className="pt-4 pb-8">  
                {isLoading ? ( 
                    <div className="text-center py-20"> 
                        <div className="animate-spin rounded-full h-16 w-16 border-t-2 border-b-2 border-indigo-500 mx-auto mb-6"></div> 
                        <p className="text-xl text-gray-600 dark:text-gray-400">Initializing & Fetching Data...</p> 
                    </div> 
                ) : (
                    <>
                        {/* FIX: Keep both pages mounted but hide one with CSS. This preserves state and fixes navigation bugs. */}
                        <div style={{ display: currentView === 'dashboard' ? 'block' : 'none' }}>
                           <DashboardPage data={vizData} theme={theme} quarterlyInputs={pdpState.quarterlyInputs} />
                        </div>
                        <div style={{ display: currentView === 'pdp' ? 'block' : 'none' }}>
                           <PDPPage 
                                data={vizData} 
                                quarterlyInputs={pdpState.quarterlyInputs} 
                                setQuarterlyInputs={setQuarterlyInputs}
                                targetTPH={pdpState.targetTPH}
                                setTargetTPH={setTargetTPH}
                            />
                        </div>
                    </>
                )} 
            </main> 
            <footer className="text-center py-6 sm:py-8 mt-8 sm:mt-10 border-t border-slate-200 dark:border-slate-700"> 
                <p className="text-xs sm:text-sm text-slate-500 dark:text-slate-400">&copy; {new Date().getFullYear()} ATHENA Predictive Analytics. For internal use only.</p> 
            </footer> 
        </div> 
    );
}

export default App;
