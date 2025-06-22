// src/App.jsx
import React, { useState, useEffect, useCallback, useMemo } from 'react';
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
  TimeScale,
  Filler,
} from 'chart.js';
import 'chartjs-adapter-date-fns';

// Register Chart.js components
ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  TimeScale,
  Filler
);

// --- Constants ---
const CONSTANTS = {
    AWS_REGION: 'us-west-1',
    BUCKET_NAME: 'ledger-prediction-charting-008971633421',
    REFRESH_INTERVAL: 3600000, 
    DEFAULT_PLANNED_RATE: 65,
    DEFAULT_PLANNED_HOURS: 3, 
    DEFAULT_TARGET_TPH: 60,
};

// Default structure for VIZ.json
const DEFAULT_VIZ_DATA = {
    time: "N/A",
    current_day: { date: "N/A", sarima_predictions: [], network_prediction: 0, previous_year_data: [], current_day_data: [] },
    next_day: { date: "N/A", sarima_predictions: [], previous_year_data: [] },
    extended_predictions: { predictions: [] },
    Ledger_Information: {
        timePoints: [],
        metrics: { APU: [], Eligible: [], IPTM: [0], IPTNW: [1], CurrWork: [0], SSF: [0] }
    }
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
        if (theme === 'dark') {
            document.documentElement.classList.add('dark');
        } else {
            document.documentElement.classList.remove('dark');
        }
    }, [theme]);
    const toggleTheme = () => setThemeState(prevTheme => prevTheme === 'light' ? 'dark' : 'light');
    return [theme, toggleTheme];
}

function useUpdateTimer(lastUpdateTime, onRefreshData) {
    const [countdown, setCountdown] = useState("--:--");
    const [timerFillWidth, setTimerFillWidth] = useState("100%");
    const [timerFillColor, setTimerFillColor] = useState("bg-sky-500");

    const getNextUpdateTimestamp = useCallback(() => {
        const now = new Date();
        let baseTimeForNextUpdate = new Date(now);

        if (lastUpdateTime && lastUpdateTime !== "N/A") {
            const lastUpdateDate = new Date(lastUpdateTime.replace(/-/g, '/').replace(' ', 'T'));
            if (!isNaN(lastUpdateDate.getTime())) {
                if (now.getTime() - lastUpdateDate.getTime() > CONSTANTS.REFRESH_INTERVAL) {
                    baseTimeForNextUpdate = new Date(now);
                } else {
                    baseTimeForNextUpdate = new Date(lastUpdateDate);
                }
            }
        }
        
        let nextUpdateTimestamp = baseTimeForNextUpdate.getTime() + CONSTANTS.REFRESH_INTERVAL;
        if (CONSTANTS.REFRESH_INTERVAL === 3600000) { 
            const nextPossibleHour = new Date(now);
            nextPossibleHour.setHours(now.getHours() + 1);
            nextPossibleHour.setMinutes(0);
            nextPossibleHour.setSeconds(0);
            nextPossibleHour.setMilliseconds(0);

            if (lastUpdateTime && lastUpdateTime !== "N/A") {
                const lastUpdateDate = new Date(lastUpdateTime.replace(/-/g, '/').replace(' ', 'T'));
                 if (!isNaN(lastUpdateDate.getTime()) && (lastUpdateDate.getTime() + CONSTANTS.REFRESH_INTERVAL > nextPossibleHour.getTime())) {
                     nextUpdateTimestamp = lastUpdateDate.getTime() + CONSTANTS.REFRESH_INTERVAL;
                 } else {
                    nextUpdateTimestamp = nextPossibleHour.getTime();
                 }
            } else {
                 nextUpdateTimestamp = nextPossibleHour.getTime();
            }
        }
         if (nextUpdateTimestamp <= now.getTime()) { 
             nextUpdateTimestamp = now.getTime() + CONSTANTS.REFRESH_INTERVAL;
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
            const totalDurationForBar = CONSTANTS.REFRESH_INTERVAL;
            const progress = Math.max(0, (timeLeft / totalDurationForBar) * 100);
            setTimerFillWidth(`${progress}%`);
            if (progress < 25) setTimerFillColor("bg-red-500");
            else if (progress < 50) setTimerFillColor("bg-yellow-500");
            else setTimerFillColor("bg-sky-500");
        }, 1000);
        return () => clearInterval(intervalId);
    }, [lastUpdateTime, getNextUpdateTimestamp, onRefreshData]);
    return { countdown, timerFillWidth, timerFillColor };
}

// --- API Service ---
const ApiService = {
    getCorrectDateTimeForS3: () => new Date(),
    fetchLatestAvailableData: async () => {
        let baseTime = ApiService.getCorrectDateTimeForS3();
        let attempts = 0;
        const maxAttempts = 48;
        while (attempts < maxAttempts) {
            const year = baseTime.getFullYear();
            const month = String(baseTime.getMonth() + 1).padStart(2, '0');
            const day = String(baseTime.getDate()).padStart(2, '0');
            const hour = String(baseTime.getHours()).padStart(2, '0');
            const dateStr = `${year}-${month}-${day}`;
            const s3Url = `https://${CONSTANTS.BUCKET_NAME}.s3.${CONSTANTS.AWS_REGION}.amazonaws.com/predictions/${dateStr}_${hour}/VIZ.json`;
            try {
                const response = await fetch(s3Url, { method: 'GET', mode: 'cors', headers: { 'Accept': 'application/json' } });
                if (response.ok) {
                    const data = await response.json();
                    Logger.log('[ApiService] Successfully fetched LATEST VIZ.json data from:', s3Url);
                    if (data && data.current_day && data.current_day.date && data.current_day.date !== "N/A" &&
                        data.next_day && data.next_day.date && data.next_day.date !== "N/A" &&
                        data.Ledger_Information && data.Ledger_Information.metrics &&
                        data.extended_predictions && Array.isArray(data.extended_predictions.predictions)
                    ) {
                        // Ensure nested arrays exist
                        data.Ledger_Information.metrics.APU = data.Ledger_Information.metrics.APU || [];
                        data.Ledger_Information.metrics.Eligible = data.Ledger_Information.metrics.Eligible || [];
                        data.Ledger_Information.metrics.IPTM = data.Ledger_Information.metrics.IPTM || [0];
                        data.Ledger_Information.metrics.IPTNW = data.Ledger_Information.metrics.IPTNW || [1];
                        data.current_day.sarima_predictions = data.current_day.sarima_predictions || [];
                        data.next_day.sarima_predictions = data.next_day.sarima_predictions || [];
                        return { vizDataResult: data, fetchedDate: data.current_day.date }; // Return data and its effective date
                    }
                    Logger.warn('[ApiService] LATEST Fetched data is missing some expected structures. Trying previous hour.', data);
                } else if (response.status !== 404 && response.status !== 403) {
                    Logger.warn(`[ApiService] Non-404/403 error fetching LATEST ${s3Url}: ${response.status}`);
                }
            } catch (error) {
                Logger.error(`[ApiService] Network error or JSON parsing error fetching LATEST ${s3Url}:`, error);
            }
            baseTime.setHours(baseTime.getHours() - 1);
            attempts++;
        }
        Logger.error('[ApiService] Unable to load LATEST VIZ.json data after all attempts.');
        return { vizDataResult: DEFAULT_VIZ_DATA, fetchedDate: null };
    },
    fetchDataForSpecificDay: async (targetDateStr /* YYYY-MM-DD */) => {
        if (!targetDateStr || targetDateStr === "N/A") {
            Logger.warn("[ApiService] fetchDataForSpecificDay: Invalid targetDateStr provided.");
            return null;
        }
        // Try to find the VIZ.json for this specific targetDateStr, starting from hour 23
        for (let hourAttempt = 23; hourAttempt >= 0; hourAttempt--) {
            const currentAttemptHour = String(hourAttempt).padStart(2, '0');
            const s3Url = `https://${CONSTANTS.BUCKET_NAME}.s3.${CONSTANTS.AWS_REGION}.amazonaws.com/predictions/${targetDateStr}_${currentAttemptHour}/VIZ.json`;
            try {
                // Logger.log(`[ApiService] fetchDataForSpecificDay: Attempting ${s3Url}`);
                const response = await fetch(s3Url, { method: 'GET', mode: 'cors', headers: { 'Accept': 'application/json' } });
                if (response.ok) {
                    const data = await response.json();
                     if (data && data.current_day && data.current_day.date === targetDateStr &&
                        data.next_day && data.next_day.date && // Ensure next_day also has a valid date relative to current_day
                        data.Ledger_Information && data.Ledger_Information.metrics &&
                        data.extended_predictions && Array.isArray(data.extended_predictions.predictions)
                     ) {
                        Logger.log(`[ApiService] fetchDataForSpecificDay: Success for ${targetDateStr} at hour ${currentAttemptHour}`);
                        // Ensure nested arrays exist
                        data.Ledger_Information.metrics.APU = data.Ledger_Information.metrics.APU || [];
                        data.Ledger_Information.metrics.Eligible = data.Ledger_Information.metrics.Eligible || [];
                        data.Ledger_Information.metrics.IPTM = data.Ledger_Information.metrics.IPTM || [0];
                        data.Ledger_Information.metrics.IPTNW = data.Ledger_Information.metrics.IPTNW || [1];
                        data.current_day.sarima_predictions = data.current_day.sarima_predictions || [];
                        data.next_day.sarima_predictions = data.next_day.sarima_predictions || [];
                        return data; 
                    }
                     Logger.warn(`[ApiService] fetchDataForSpecificDay: Data for ${s3Url} is invalid or not for target date ${targetDateStr}.`);
                }
            } catch (error) {
                // Logger.error(`[ApiService] fetchDataForSpecificDay: Network error for ${s3Url}`, error);
            }
        }
        Logger.warn(`[ApiService] fetchDataForSpecificDay: No valid data found for targetDateStr: ${targetDateStr} after checking all hours.`);
        return null; 
    }
};


// --- Components ---
const Header = ({ currentView, setCurrentView, lastUpdateTime, onRefreshData, activeDataSource, dateOfPriorDayView }) => { // Added activeDataSource and dateOfPriorDayView
    const [theme, toggleTheme] = useTheme();
    const { countdown, timerFillWidth, timerFillColor } = useUpdateTimer(lastUpdateTime, onRefreshData);
    const SunIcon = () => ( <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/></svg> );
    const MoonIcon = () => ( <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg> );
    const NavLink = ({ viewName, children }) => ( <a href="#" onClick={(e) => { e.preventDefault(); setCurrentView(viewName); }} className={`px-3 py-2 sm:px-4 rounded-md text-sm font-medium transition-colors ${currentView === viewName ? 'bg-indigo-600 text-white dark:bg-indigo-500' : 'text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-700'}`}>{children}</a> );
    
    let dataStatusMessage = "";
    if (activeDataSource === 'priorDay' && dateOfPriorDayView) {
        dataStatusMessage = `Viewing Prior Day Data: ${formatDate(dateOfPriorDayView)}`;
    }

    return ( 
        <header className="bg-white dark:bg-slate-800 shadow-lg p-3 sm:p-4 mb-6 sticky top-0 z-50"> 
            <div className="bg-gradient-to-r from-blue-600 to-indigo-700 text-white text-center py-2 text-xs sm:text-sm font-semibold mb-1 rounded-md shadow"> </div> 
            {dataStatusMessage && (
                <div className="bg-amber-500 text-white text-center py-1 text-xs font-semibold rounded-b-md shadow-sm">
                    {dataStatusMessage}
                </div>
            )}
            <div className={`container mx-auto flex flex-col sm:flex-row justify-between items-center ${dataStatusMessage ? 'pt-2' : 'pt-3 sm:pt-4'}`}> {/* Adjust padding if message is shown */}
                 <div className="flex items-center mb-2 sm:mb-0"> <img src={theme === 'dark' ? "https://ledger-prediction-charting-website.s3.us-west-1.amazonaws.com/ATHENALogoD.png" : "https://ledger-prediction-charting-website.s3.us-west-1.amazonaws.com/ATHENAlogo.PNG"} alt="Athena Logo" className="h-10 sm:h-12 mr-2 sm:mr-3" onError={(e) => { e.target.onerror = null; e.target.src="https://placehold.co/150x50/000000/FFFFFF?text=ATHENA"; }}/> </div> 
                 <div className="flex flex-col sm:flex-row items-center space-y-2 sm:space-y-0 sm:space-x-2 md:space-x-4"> 
                    <div className="text-xs text-gray-600 dark:text-gray-400 text-center sm:text-left"> 
                        <div>Data File Time: <span className="font-semibold">{lastUpdateTime || "N/A"}</span></div> 
                        {activeDataSource === 'latest' && ( // Only show countdown for latest data
                            <div className="flex items-center justify-center sm:justify-start mt-1"> 
                                <div className="w-20 sm:w-24 h-1.5 bg-gray-300 dark:bg-gray-600 rounded-full overflow-hidden mr-1.5 sm:mr-2"> 
                                    <div className={`h-full rounded-full transition-all duration-1000 ease-linear ${timerFillColor}`} style={{ width: timerFillWidth }}></div> 
                                </div> 
                                <span className="text-xs">Next Update: <span className="font-semibold">{countdown}</span></span> 
                            </div>
                        )}
                    </div> 
                    <nav className="flex space-x-1 sm:space-x-2"> <NavLink viewName="dashboard">Dashboard</NavLink> <NavLink viewName="pdp">PDP</NavLink> </nav> 
                    <button onClick={toggleTheme} className="p-2 rounded-full hover:bg-gray-200 dark:hover:bg-gray-700 text-gray-700 dark:text-gray-300 transition-colors" aria-label="Toggle theme"> {theme === 'light' ? <MoonIcon /> : <SunIcon />} </button> 
                </div> 
            </div> 
        </header> 
    );
};

const formatDate = (dateString) => {
    if (!dateString || dateString === "N/A") return "N/A";
    try {
        let date = new Date(dateString.includes('T') ? dateString : dateString.replace(/-/g, '/').replace(' ', 'T'));
        if (isNaN(date.getTime())) date = new Date(dateString.replace(/-/g, '/') + "T00:00:00"); 
        if (isNaN(date.getTime())) return dateString;
        return `${date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })} (${date.toLocaleDateString('en-US', { weekday: 'short' })})`;
    } catch (e) { Logger.error("Error formatting date:", {dateString, error: e}); return dateString; }
};

const parseDateTime = (timeStr) => {
    if (!timeStr) return null;
    try {
        const normalizedTimeStr = timeStr.replace(' ', 'T');
        let date = new Date(normalizedTimeStr);
        if (isNaN(date.getTime())) {
            const fallbackDate = new Date(timeStr.replace(/-/g, '/'));
            if (isNaN(fallbackDate.getTime())) return null;
            return fallbackDate;
        }
        return date;
    } catch (e) { Logger.error("Error in parseDateTime:", { timeStr, error: e }); return null; }
};

const getOffsetDateString = (baseDateStr, dayOffset) => {
    if (!baseDateStr || baseDateStr === "N/A") return "N/A";
    try {
        const localBaseDate = parseDateTime(baseDateStr + "T00:00:00"); 
        if (!localBaseDate || isNaN(localBaseDate.getTime())) {
            Logger.warn("getOffsetDateString: Invalid baseDateStr provided:", baseDateStr);
            return "N/A";
        }
        const newDate = new Date(localBaseDate.valueOf());
        newDate.setDate(newDate.getDate() + dayOffset); 
        return `${newDate.getFullYear()}-${String(newDate.getMonth() + 1).padStart(2, '0')}-${String(newDate.getDate()).padStart(2, '0')}`;
    } catch (e) { Logger.error("Error in getOffsetDateString:", {baseDateStr, dayOffset, error: e}); return "N/A"; }
};

const getCumulativeVolumeAtSpecificDateTime = (predictionsArray, targetDateStr, targetHour) => {
    if (!Array.isArray(predictionsArray) || predictionsArray.length === 0 || !targetDateStr || targetDateStr === "N/A" || targetHour === undefined) {
        return 0;
    }
    if (targetHour < 0) return 0;

    const entry = predictionsArray.find(p => {
        if (!p || !p.Time) return false;
        const pDatePart = p.Time.substring(0, 10); 
        if (pDatePart !== targetDateStr) return false;
        const d = parseDateTime(p.Time); 
        if (!d) return false;
        return d.getHours() === targetHour;
    });
    return entry ? (entry.Predicted_Workable || entry.Workable || 0) : 0;
};

// --- Chart Components ---
const ExtendedForecastChart = ({ predictions, theme }) => { 
    if (!Array.isArray(predictions) || predictions.length === 0) { return <div className="h-64 bg-slate-100 dark:bg-slate-700 flex items-center justify-center rounded-md text-gray-400 dark:text-slate-500">No extended forecast data available for chart.</div>; }
    const chartLineColor = theme === 'dark' ? 'rgba(56, 189, 248, 1)' : 'rgba(14, 165, 233, 1)';  const chartFillColor = theme === 'dark' ? 'rgba(56, 189, 248, 0.1)' : 'rgba(14, 165, 233, 0.1)'; const gridColor = theme === 'dark' ? 'rgba(71, 85, 105, 0.5)' : 'rgba(203, 213, 225, 0.5)'; const textColor = theme === 'dark' ? '#cbd5e1' : '#475569';
    const chartData = { labels: predictions.map(p => parseDateTime(p.Time)).filter(d => d !== null), datasets: [ { label: 'Predicted Workable Units (Cumulative by Day)', data: predictions.map(p => ({ x: parseDateTime(p.Time), y: p.Predicted_Workable || 0 })), borderColor: chartLineColor, backgroundColor: chartFillColor, tension: 0.3, fill: true, pointRadius: 1, pointHoverRadius: 4, borderWidth: 2, }, ], };
    const options = { responsive: true, maintainAspectRatio: false, scales: { x: { type: 'time', time: { unit: 'hour', tooltipFormat: 'MMM d, HH:mm', displayFormats: { hour: 'HH:mm', day: 'MMM d' }, }, title: { display: true, text: 'Time', color: textColor, font: {weight: 'bold'} }, ticks: { color: textColor, maxRotation: 0, autoSkipPadding: 20, source: 'auto' }, grid: { color: gridColor } }, y: { beginAtZero: false, title: { display: true, text: 'Cumulative Workable Units (by Day)', color: textColor, font: {weight: 'bold'} }, ticks: { color: textColor, callback: function(value) { return value.toLocaleString(); } }, grid: { color: gridColor } }, }, plugins: { legend: { position: 'top', labels: { color: textColor, font: {size: 14} } }, tooltip: { mode: 'index', intersect: false, titleFont: {weight: 'bold'}, bodyFont: {size: 13}, callbacks: { label: function(context) { let label = context.dataset.label || ''; if (label) { label += ': '; } if (context.parsed.y !== null) { label += context.parsed.y.toLocaleString(); } return label; } } }, }, };
    return ( <div className="h-72 md:h-96"> <Line data={chartData} options={options} /> </div> );
};
const TodayPredictionActualChart = ({ currentDayData, theme }) => { 
    if (!currentDayData || currentDayData.date === "N/A" || !currentDayData.sarima_predictions || !currentDayData.current_day_data || !currentDayData.previous_year_data) { return <div className="h-64 bg-slate-100 dark:bg-slate-700 flex items-center justify-center rounded-md text-gray-400 dark:text-slate-500">Today's prediction data is not fully loaded.</div>; }
    const chartLineColorSarima = theme === 'dark' ? 'rgba(165, 243, 195, 1)' : 'rgba(34, 197, 94, 1)'; const chartLineColorActuals = theme === 'dark' ? 'rgba(56, 189, 248, 1)' : 'rgba(14, 165, 233, 1)'; const chartLineColorPreviousYear = theme === 'dark' ? 'rgba(156, 163, 175, 0.7)' : 'rgba(107, 114, 128, 0.7)'; const gridColor = theme === 'dark' ? 'rgba(71, 85, 105, 0.5)' : 'rgba(203, 213, 225, 0.5)'; const textColor = theme === 'dark' ? '#cbd5e1' : '#475569'; const currentDayDateObj = parseDateTime(currentDayData.date + "T00:00:00");
    const normalizeTimeToCurrentDayDisplay = (dataArray, valueKey = 'Workable') => { if (!Array.isArray(dataArray) || !currentDayDateObj) return []; return dataArray.map(p => { const originalDate = parseDateTime(p.Time); if (!originalDate) return null; const newDate = new Date( currentDayDateObj.getFullYear(), currentDayDateObj.getMonth(), currentDayDateObj.getDate(), originalDate.getHours(), originalDate.getMinutes(), originalDate.getSeconds() ); return { x: newDate, y: p[valueKey] || 0 }; }).filter(p => p !== null && p.x !== null && !isNaN(p.x.getTime())); };
    const sarimaData = normalizeTimeToCurrentDayDisplay(currentDayData.predictions_no_same_day, 'Predicted_Workable_No_Same_Day'); const actualsData = normalizeTimeToCurrentDayDisplay(currentDayData.current_day_data, 'Workable'); const previousYearData = normalizeTimeToCurrentDayDisplay(currentDayData.previous_year_data, 'Workable');
    const chartData = { datasets: [ { label: 'SARIMA Predictions', data: sarimaData, borderColor: chartLineColorSarima, backgroundColor: 'transparent', tension: 0.3, pointRadius: 2, pointHoverRadius: 5, borderWidth: 2.5, }, { label: 'Actual Workable Units', data: actualsData, borderColor: chartLineColorActuals, backgroundColor: 'transparent', tension: 0.3, pointRadius: 2, pointHoverRadius: 5, borderWidth: 2, }, { label: 'Previous Year Workable', data: previousYearData, borderColor: chartLineColorPreviousYear, backgroundColor: 'transparent', tension: 0.3, pointRadius: 1, pointHoverRadius: 4, borderWidth: 1.5, borderDash: [5, 5], hidden: true, }, ], };
    const options = { responsive: true, maintainAspectRatio: false, scales: { x: { type: 'time', time: { unit: 'hour', tooltipFormat: 'MMM d, HH:mm', displayFormats: { hour: 'HH:mm' }, }, title: { display: true, text: `Time (${formatDate(currentDayData.date)})`, color: textColor, font: {weight: 'bold'} }, ticks: { color: textColor, maxRotation: 0, autoSkipPadding: 20, source: 'auto' }, grid: { color: gridColor }, }, y: { beginAtZero: false, title: { display: true, text: 'Workable Units', color: textColor, font: {weight: 'bold'} }, ticks: { color: textColor, callback: function(value) { return value.toLocaleString(); } }, grid: { color: gridColor }, }, }, plugins: { legend: { position: 'top', labels: { color: textColor, font: {size: 14} } }, tooltip: { mode: 'index', intersect: false, titleFont: {weight: 'bold'}, bodyFont: {size: 13}, callbacks: { label: function(context) { let label = context.dataset.label || ''; if (label) { label += ': '; } if (context.parsed.y !== null) { label += context.parsed.y.toLocaleString(); } return label; } } }, }, };
    return ( <div className="h-72 md:h-96"> <Line data={chartData} options={options} /> </div> );
};
const BacklogTrendChart = ({ ledgerInfo, theme, currentDayDate }) => { 
    if (!ledgerInfo || !ledgerInfo.metrics || !Array.isArray(ledgerInfo.timePoints) || ledgerInfo.timePoints.length === 0) { return <div className="h-64 bg-slate-100 dark:bg-slate-700 flex items-center justify-center rounded-md text-gray-400 dark:text-slate-500">Ledger information for backlog trend is not available.</div>; }
    const baseDateForLedger = currentDayDate && currentDayDate !== "N/A" ? currentDayDate : null; if (!baseDateForLedger) { return <div className="h-64 bg-slate-100 dark:bg-slate-700 flex items-center justify-center rounded-md text-gray-400 dark:text-slate-500">Backlog trend chart requires a valid current day's date.</div>;}
    const apuDataRaw = ledgerInfo.metrics.APU || []; const eligibleDataRaw = ledgerInfo.metrics.Eligible || []; const timePointsRaw = ledgerInfo.timePoints;
    const chartLineColorAPU = theme === 'dark' ? 'rgba(250, 204, 21, 1)' : 'rgba(234, 179, 8, 1)'; const chartLineColorEligible = theme === 'dark' ? 'rgba(248, 113, 113, 1)' : 'rgba(239, 68, 68, 1)'; const gridColor = theme === 'dark' ? 'rgba(71, 85, 105, 0.5)' : 'rgba(203, 213, 225, 0.5)'; const textColor = theme === 'dark' ? '#cbd5e1' : '#475569';
    const apuData = timePointsRaw.map((tp, index) => { const dt = parseDateTime(`${baseDateForLedger}T${tp}`); return dt ? { x: dt, y: apuDataRaw[index] || 0 } : null; }).filter(d => d !== null);
    const eligibleData = timePointsRaw.map((tp, index) => { const dt = parseDateTime(`${baseDateForLedger}T${tp}`); return dt ? { x: dt, y: eligibleDataRaw[index] || 0 } : null; }).filter(d => d !== null);
    const chartData = { datasets: [ { label: 'APU', data: apuData, borderColor: chartLineColorAPU, backgroundColor: 'transparent', tension: 0.3, pointRadius: 2, pointHoverRadius: 4, borderWidth: 2, }, { label: 'Eligible', data: eligibleData, borderColor: chartLineColorEligible, backgroundColor: 'transparent', tension: 0.3, pointRadius: 2, pointHoverRadius: 4, borderWidth: 2, }, ], };
    const options = { responsive: true, maintainAspectRatio: false, scales: { x: { type: 'time', time: { unit: 'hour', tooltipFormat: 'MMM d, HH:mm', displayFormats: { hour: 'HH:mm', day: 'MMM d, HH:mm' }, }, title: { display: true, text: `Time (${formatDate(baseDateForLedger)})`, color: textColor, font: {weight: 'bold'} }, ticks: { color: textColor, maxRotation: 0, autoSkipPadding: 20, source: 'auto'}, grid: { color: gridColor }, }, y: { beginAtZero: true, title: { display: true, text: 'Units', color: textColor, font: {weight: 'bold'} }, ticks: { color: textColor, callback: function(value) { return value.toLocaleString(); } }, grid: { color: gridColor }, }, }, plugins: { legend: { position: 'top', labels: { color: textColor, font: {size: 14} } }, tooltip: { mode: 'index', intersect: false, titleFont: {weight: 'bold'}, bodyFont: {size: 13}, callbacks: { label: function(context) { let label = context.dataset.label || ''; if (label) { label += ': '; } if (context.parsed.y !== null) { label += context.parsed.y.toLocaleString(); } return label; } } }, }, };
    return ( <div className="h-72 md:h-96"> <Line data={chartData} options={options} /> </div> );
};


// --- PDP Page Component ---
const ALL_SHIFT_DEFINITIONS = {
    currentNight: {
        key: 'currentNight', name: "Current Night Shift",
        anchorDateKey: 'current_day', 
        startHourDef: 18, endHourDef: 6, 
        quarters: [
            {id: 'cnq1', label: "18:00-21:00", startHour: 18, endHour: 21, dateKeyForQuarter: 'current_day_date' },
            {id: 'cnq2', label: "21:00-00:00", startHour: 21, endHour: 0,  dateKeyForQuarter: 'current_day_date' },
            {id: 'cnq3', label: "00:00-03:00", startHour: 0,  endHour: 3,  dateKeyForQuarter: 'next_day_date' },
            {id: 'cnq4', label: "03:00-06:00", startHour: 3,  endHour: 6,  dateKeyForQuarter: 'next_day_date' }
        ]
    },
    nextDay: {
        key: 'nextDay', name: "Next Day Shift",
        anchorDateKey: 'next_day',
        startHourDef: 6, endHourDef: 18,
        quarters: [
            {id: 'ndq1', label: "06:00-09:00", startHour: 6,  endHour: 9, dateKeyForQuarter: 'next_day_date' },
            {id: 'ndq2', label: "09:00-12:00", startHour: 9, endHour: 12, dateKeyForQuarter: 'next_day_date' },
            {id: 'ndq3', label: "12:00-15:00", startHour: 12, endHour: 15, dateKeyForQuarter: 'next_day_date' },
            {id: 'ndq4', label: "15:00-18:00", startHour: 15, endHour: 18, dateKeyForQuarter: 'next_day_date' }
        ]
    },
    nextNight: {
        key: 'nextNight', name: "Next Night Shift",
        anchorDateKey: 'next_day', 
        startHourDef: 18, endHourDef: 6, 
        quarters: [
            {id: 'nnq1', label: "18:00-21:00", startHour: 18, endHour: 21, dateKeyForQuarter: 'next_day_date' },
            {id: 'nnq2', label: "21:00-00:00", startHour: 21, endHour: 0,  dateKeyForQuarter: 'next_day_date' },
            {id: 'nnq3', label: "00:00-03:00", startHour: 0,  endHour: 3,  dateKeyForQuarter: 'day_after_next_date' },
            {id: 'nnq4', label: "03:00-06:00", startHour: 3,  endHour: 6,  dateKeyForQuarter: 'day_after_next_date' }
        ]
    },
    dayAfterNextDay: {
        key: 'dayAfterNextDay', name: "Day After Next - Day Shift",
        anchorDateKey: 'day_after_next', 
        startHourDef: 6, endHourDef: 18,
        quarters: [
            {id: 'dandq1', label: "06:00-09:00", startHour: 6,  endHour: 9,  dateKeyForQuarter: 'day_after_next_date' },
            {id: 'dandq2', label: "09:00-12:00", startHour: 9,  endHour: 12, dateKeyForQuarter: 'day_after_next_date' },
            {id: 'dandq3', label: "12:00-15:00", startHour: 12, endHour: 15, dateKeyForQuarter: 'day_after_next_date' },
            {id: 'dandq4', label: "15:00-18:00", startHour: 15, endHour: 18, dateKeyForQuarter: 'day_after_next_date' }
        ]
    },
    dayAfterNextNight: {
        key: 'dayAfterNextNight', name: "Day After Next - Night Shift",
        anchorDateKey: 'day_after_next', 
        startHourDef: 18, endHourDef: 6,
        quarters: [
            {id: 'dannq1', label: "18:00-21:00", startHour: 18, endHour: 21, dateKeyForQuarter: 'day_after_next_date' },
            {id: 'dannq2', label: "21:00-00:00", startHour: 21, endHour: 0,  dateKeyForQuarter: 'day_after_next_date' },
            {id: 'dannq3', label: "00:00-03:00", startHour: 0,  endHour: 3,  dateKeyForQuarter: 'day_after_next_plus_1_date' },
            {id: 'dannq4', label: "03:00-06:00", startHour: 3,  endHour: 6,  dateKeyForQuarter: 'day_after_next_plus_1_date' }
        ]
    }
};
const ORDERED_SHIFT_KEYS = ['currentNight', 'nextDay', 'nextNight', 'dayAfterNextDay', 'dayAfterNextNight'];

const MetricCard = ({ title, value, unit = "units", subtext, size = "default", children, valueColor = "text-indigo-600 dark:text-indigo-400" }) => { 
    const titleSize = size === 'large' ? 'text-lg sm:text-xl' : 'text-sm'; const valueSize = size === 'large' ? 'text-3xl sm:text-4xl' : 'text-2xl'; const unitSize = size === 'large' ? 'text-xl sm:text-2xl' : 'text-lg';
    return ( <div className={`bg-slate-50 dark:bg-slate-700/80 p-4 rounded-xl shadow-lg hover:shadow-indigo-300/30 dark:hover:shadow-indigo-800/30 transition-shadow flex flex-col justify-between min-h-[110px]`}> <div> <p className={`${titleSize} text-gray-500 dark:text-gray-400 mb-1 truncate`}>{title}</p> <p className={`${valueSize} font-bold ${valueColor}`}> {value?.toLocaleString(undefined, {maximumFractionDigits: (unit === "hrs" || unit === "units/hr" || unit === "VCap") ? 1 : 0}) || '--'} {unit && <span className={`${unitSize} font-medium text-gray-600 dark:text-gray-300 ml-1`}>{unit}</span>} </p> {subtext && <p className="text-xs text-gray-400 dark:text-gray-500 mt-1">{subtext}</p>} </div> {children} </div> );
};
const ChevronDownIcon = () => ( <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor" className="w-5 h-5 transition-transform duration-300"> <path strokeLinecap="round" strokeLinejoin="round" d="m19.5 8.25-7.5 7.5-7.5-7.5" /> </svg> );
const ChevronUpIcon = () => ( <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor" className="w-5 h-5 transition-transform duration-300"> <path strokeLinecap="round" strokeLinejoin="round" d="m4.5 15.75 7.5-7.5 7.5 7.5" /> </svg> );

// Utility function to calculate quarter volume, callable from PDPPage and ShiftQuarterPlannerCard
const calculateQuarterVolumeUtil = (quarter, data, shiftDates) => {
    let qActualDateStr = "N/A";
    switch (quarter.dateKeyForQuarter) {
        case 'current_day_date': qActualDateStr = shiftDates.currentDayDateStr; break;
        case 'next_day_date': qActualDateStr = shiftDates.nextDayDateStr; break;
        case 'day_after_next_date': qActualDateStr = shiftDates.dayAfterNextDateStr; break;
        case 'day_after_next_plus_1_date': qActualDateStr = shiftDates.dayAfterNextPlus1DateStr; break;
        default: Logger.warn(`[Util] Unknown dateKeyForQuarter: ${quarter.dateKeyForQuarter}`); qActualDateStr = "N/A";
    }

    // Logger.log(`[Util CQV] Q_ID: ${quarter.id}, Q_Label: ${quarter.label}, dateKey: ${quarter.dateKeyForQuarter} => qActualDateStr: ${qActualDateStr}`);

    if (qActualDateStr === "N/A" || qActualDateStr === undefined) {
        // Logger.warn(`[Util CQV] qActualDateStr is N/A for quarter ${quarter.id}. Returning 0 volume.`);
        return 0;
    }

    const predictionsSource = data.extended_predictions?.predictions || [];
    // Logger.log(`  [Util CQV] Using extended_predictions (length: ${predictionsSource.length}) for date: ${qActualDateStr}`);

    const endLookupHour = quarter.endHour === 0 ? 23 : quarter.endHour - 1;
    const volAtEndOfQuarter = getCumulativeVolumeAtSpecificDateTime(predictionsSource, qActualDateStr, endLookupHour);

    let volAtStartOfQuarter;
    if (quarter.startHour === 0) {
        volAtStartOfQuarter = 0; 
    } else {
        volAtStartOfQuarter = getCumulativeVolumeAtSpecificDateTime(predictionsSource, qActualDateStr, quarter.startHour - 1);
    }
    
    // Logger.log(`  [Util CQV] volAtStart (for hour ${quarter.startHour-1} on ${qActualDateStr}): ${volAtStartOfQuarter}, volAtEnd (for hour ${endLookupHour} on ${qActualDateStr}): ${volAtEndOfQuarter}`);
    let exVolForQuarter = volAtEndOfQuarter - volAtStartOfQuarter;
    exVolForQuarter = Math.max(0, exVolForQuarter); 
    // Logger.log(`  [Util CQV] exVolForQuarter for ${quarter.id} (${quarter.label}): ${exVolForQuarter}`);
    return exVolForQuarter;
};

const ShiftQuarterPlannerCard = ({ shiftKey, shiftDefinition, data, quarterlyInputs, handleInputChange, targetTPH, shiftDates }) => {
    const [isExpanded, setIsExpanded] = useState(true);
    
    let shiftDisplayDateStr = "N/A";
    if (shiftDefinition.key === 'currentNight' && shiftDates.currentDayDateStr !== "N/A") shiftDisplayDateStr = shiftDates.currentDayDateStr;
    else if ((shiftDefinition.key === 'nextDay' || shiftDefinition.key === 'nextNight') && shiftDates.nextDayDateStr !== "N/A") shiftDisplayDateStr = shiftDates.nextDayDateStr;
    else if ((shiftDefinition.key === 'dayAfterNextDay' || shiftDefinition.key === 'dayAfterNextNight') && shiftDates.dayAfterNextDateStr !== "N/A") shiftDisplayDateStr = shiftDates.dayAfterNextDateStr;

    const calculateQuarterVolume = useCallback((quarter) => {
        return calculateQuarterVolumeUtil(quarter, data, shiftDates);
    }, [data, shiftDates]);

    const shiftAggregates = useMemo(() => {
        let totalShiftExVol = 0; 
        let totalShiftPlannedHours = 0; 
        let totalShiftHoursToSolve = 0;
        let totalShiftPlannedVCap = 0;

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
        // totalShiftDiscrepancy is still useful for internal logic if needed, but not displayed directly as before
        const totalShiftDiscrepancy = totalShiftPlannedHours - totalShiftHoursToSolve; 
        return { 
            totalShiftExVol, 
            averageShiftTPH, 
            totalShiftDiscrepancy, // Keep for potential future use or internal logic
            totalShiftPlannedHours, 
            totalShiftPlannedVCap, 
            totalShiftHoursToSolve 
        };
    }, [shiftDefinition, quarterlyInputs, targetTPH, calculateQuarterVolume]);

    // Determine color for Total Planned Hours card
    const plannedHoursColor = shiftAggregates.totalShiftPlannedHours > shiftAggregates.totalShiftHoursToSolve 
                              ? 'text-red-600 dark:text-red-400' 
                              : 'text-green-600 dark:text-green-400';

    // Helper to get the 3-hour block label suffix (e.g., "0000_0300") from quarter definition
    const getBlockLabelSuffix = (startHour, endHour) => {
        const sH = String(startHour).padStart(2,'0');
        const eH = endHour === 0 ? "00" : String(endHour).padStart(2,'0'); // For midnight end
        if (startHour === 21 && endHour === 0) return "2100_0000"; // Special case for 21:00-00:00
        return `${sH}00_${eH}00`;
    };


    return (
        <div className="bg-white dark:bg-slate-800 shadow-xl rounded-lg p-6">
            <div className="flex justify-between items-center border-b pb-3 border-slate-200 dark:border-slate-700 mb-4">
                <div>
                    <h3 className="text-xl font-semibold text-gray-700 dark:text-white">{shiftDefinition.name}</h3>
                    <p className="text-sm text-indigo-500 dark:text-indigo-400 font-medium">Shift Primary Date: {formatDate(shiftDisplayDateStr)}</p>
                </div>
                <button onClick={() => setIsExpanded(!isExpanded)} className="p-2 rounded-md hover:bg-slate-100 dark:hover:bg-slate-700 transition-colors text-slate-500 dark:text-slate-400" aria-label={isExpanded ? "Collapse shift details" : "Expand shift details"}>
                    {isExpanded ? <ChevronUpIcon /> : <ChevronDownIcon />}
                </button>
            </div>
            <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3 mb-6">
                <MetricCard title="Total Expected Vol." value={shiftAggregates.totalShiftExVol} size="small"/>
                <MetricCard title="Total Planned VCap" value={shiftAggregates.totalShiftPlannedVCap} unit="VCap" size="small"/>
                <MetricCard title="Avg. Planned TPH" value={shiftAggregates.averageShiftTPH} unit="units/hr" size="small"/>
                <MetricCard title="Total Required Hrs" value={shiftAggregates.totalShiftHoursToSolve} unit="hrs" size="small" />
                {/* MODIFIED METRIC CARD FOR PLANNED HOURS */}
                <MetricCard 
                    title="Total Planned Hrs" 
                    value={shiftAggregates.totalShiftPlannedHours} 
                    unit="hrs" 
                    size="small" 
                    valueColor={plannedHoursColor}
                />
            </div>
            {isExpanded && (
                <div className="space-y-6 transition-all duration-500 ease-in-out">
                    {shiftDefinition.quarters.map((quarter) => {
                        const quarterId = quarter.id;
                        const inputs = quarterlyInputs[quarterId] || { plannedHours: "", plannedRate: CONSTANTS.DEFAULT_PLANNED_RATE };
                        const exVolForQuarter = calculateQuarterVolume(quarter);
                        
                        let qDisplayDateStr = "N/A";
                        let qDayName = "N/A";
                        switch (quarter.dateKeyForQuarter) {
                            case 'current_day_date': qDisplayDateStr = shiftDates.currentDayDateStr; break;
                            case 'next_day_date': qDisplayDateStr = shiftDates.nextDayDateStr; break;
                            case 'day_after_next_date': qDisplayDateStr = shiftDates.dayAfterNextDateStr; break;
                            case 'day_after_next_plus_1_date': qDisplayDateStr = shiftDates.dayAfterNextPlus1DateStr; break;
                            default: qDisplayDateStr = "N/A";
                        }
                        if (qDisplayDateStr !== "N/A") {
                            const d = parseDateTime(qDisplayDateStr + "T00:00:00");
                            if (d) qDayName = d.toLocaleDateString('en-US', { weekday: 'long' });
                        }

                        const blockLabelSuffix = getBlockLabelSuffix(quarter.startHour, quarter.endHour);
                        const historicalBlockKey = `${qDayName}_${blockLabelSuffix}`;
                        const blockTrendData = data.historical_context?.three_hour_block_trends?.[historicalBlockKey];
                        
                        let trendInfo = { arrow: "", colorClass: "text-slate-500", percentageText: "N/A" };
                        if(blockTrendData){
                            trendInfo = getTrendIndicatorInfo(blockTrendData.trend_direction_pct_change);
                        }
                        const longTermOccurrences = data.historical_context?.num_weeks_for_avg || 6;
                        const shortTermOccurrences = data.historical_context?.short_term_ma_occurrences || 3;

                        const calculatedTPH = (parseFloat(inputs.plannedHours) || 0) > 0 ? exVolForQuarter / parseFloat(inputs.plannedHours) : 0;
                        const hoursToSolve = targetTPH > 0 ? exVolForQuarter / targetTPH : 0;
                        const discrepancy = (parseFloat(inputs.plannedHours) || 0) - hoursToSolve; // Still calculated for potential internal use or if display changes again
                        const calculatedVCap = (parseFloat(inputs.plannedRate) || 0) * (parseFloat(inputs.plannedHours) || 0);
                        
                        return (
                            <div key={quarterId} className="p-4 bg-slate-50 dark:bg-slate-700 rounded-lg shadow-md space-y-3">
                                <div className="flex justify-between items-start">
                                    <h4 className="font-semibold text-md text-indigo-700 dark:text-indigo-400"> 
                                        {quarter.label} <span className="text-xs text-slate-500 dark:text-slate-400">({formatDate(qDisplayDateStr)})</span> 
                                    </h4>
                                    {blockTrendData && (
                                        <div className="text-right text-xs text-slate-500 dark:text-slate-400 space-y-0.5 pl-2">
                                            <p>Last: <span className="font-medium text-slate-700 dark:text-slate-200">{blockTrendData.last_occurrence_volume?.toLocaleString(undefined, {maximumFractionDigits:0})}</span></p>
                                            <p>Avg ({shortTermOccurrences}o): <span className="font-medium text-slate-700 dark:text-slate-200">{blockTrendData[`avg_volume_last_${shortTermOccurrences}_occurrences`]?.toLocaleString(undefined, {maximumFractionDigits:0})}</span></p>
                                            <p>Avg ({longTermOccurrences}o): <span className="font-medium text-slate-700 dark:text-slate-200">{blockTrendData[`avg_volume_last_${longTermOccurrences}_occurrences`]?.toLocaleString(undefined, {maximumFractionDigits:0})}</span></p>
                                            <p>Trend: <span className={`font-semibold ${trendInfo.colorClass}`}>{trendInfo.arrow} {trendInfo.percentageText}</span></p>
                                        </div>
                                    )}
                                </div>

                                <div className="grid grid-cols-2 gap-x-4 gap-y-2 text-sm pt-1">
                                    <span className="text-slate-600 dark:text-slate-300">Expected Vol:</span> <span className="font-medium text-slate-800 dark:text-slate-100">{exVolForQuarter.toLocaleString(undefined, {maximumFractionDigits:0})}</span>
                                    <label htmlFor={`${quarterId}_plannedHours`} className="text-slate-600 dark:text-slate-300 self-center">Planned Hrs:</label>
                                    <input type="number" id={`${quarterId}_plannedHours`} value={inputs.plannedHours} onChange={e => handleInputChange(quarterId, 'plannedHours', e.target.value)} className="w-full p-1.5 border border-slate-300 dark:border-slate-600 rounded-md bg-white dark:bg-slate-800 text-sm focus:ring-indigo-500 focus:border-indigo-500"/>
                                    <label htmlFor={`${quarterId}_plannedRate`} className="text-slate-600 dark:text-slate-300 self-center">Planned Rate (units/hr):</label>
                                    <input type="number" id={`${quarterId}_plannedRate`} value={inputs.plannedRate} onChange={e => handleInputChange(quarterId, 'plannedRate', e.target.value)} className="w-full p-1.5 border border-slate-300 dark:border-slate-600 rounded-md bg-white dark:bg-slate-800 text-sm focus:ring-indigo-500 focus:border-indigo-500" step="1"/>
                                    <span className="text-slate-600 dark:text-slate-300">Calculated TPH:</span> <span className="font-medium text-slate-800 dark:text-slate-100">{calculatedTPH.toLocaleString(undefined, {maximumFractionDigits:1})}</span>
                                    <span className="text-slate-600 dark:text-slate-300">Calculated VCap:</span> <span className="font-medium text-slate-800 dark:text-slate-100">{calculatedVCap.toLocaleString(undefined, {maximumFractionDigits:0})}</span>
                                    <span className="text-slate-600 dark:text-slate-300">Hrs to Solve ({targetTPH} TPH):</span> <span className="font-medium text-slate-800 dark:text-slate-100">{hoursToSolve.toLocaleString(undefined, {maximumFractionDigits:1})}</span>
                                    <span className="text-slate-600 dark:text-slate-300">Discrepancy:</span>
                                    <span className={`font-medium ${discrepancy >= 0 ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'}`}>{discrepancy.toLocaleString(undefined, {maximumFractionDigits:1})} hrs</span>
                                </div>
                            </div>
                        );
                    })}
                </div>
            )}
        </div>
    );
};

const PDPPage = ({ 
    data, 
    quarterlyInputsGlobal, 
    setQuarterlyInputsGlobal, 
    targetTPHGlobal, 
    setTargetTPHGlobal,
    switchToLatestView, 
    switchToPriorDayView, 
    activeDataSource, 
    dateOfLatestDataLoaded, 
    dateOfPriorDayView 
}) => {
    const [theme] = useTheme();
    const quarterlyInputs = quarterlyInputsGlobal;
    const setQuarterlyInputs = setQuarterlyInputsGlobal;
    const targetTPH = targetTPHGlobal;
    const setTargetTPH = setTargetTPHGlobal;
    
    // State to explicitly trigger auto-balance.
    // True initially to perform the first auto-balance.
    // Also set to true when the "Apply Optimal Hrs" button is clicked.
    const [triggerAutoBalance, setTriggerAutoBalance] = useState(true); 

    const shiftDates = useMemo(() => {
        const cdDate = data.current_day?.date;
        const ndDate = data.next_day?.date;
        return {
            currentDayDateStr: cdDate && cdDate !== "N/A" ? cdDate : "N/A",
            nextDayDateStr: ndDate && ndDate !== "N/A" ? ndDate : "N/A",
            dayAfterNextDateStr: ndDate && ndDate !== "N/A" ? getOffsetDateString(ndDate, 1) : "N/A",
            dayAfterNextPlus1DateStr: ndDate && ndDate !== "N/A" ? getOffsetDateString(ndDate, 2) : "N/A",
        };
    }, [data.current_day, data.next_day]);

    const shiftsToDisplay = useMemo(() => {
        if (shiftDates.currentDayDateStr === "N/A") {
            Logger.warn("[PDPPage shiftsToDisplay] Essential date (current_day.date) is N/A. Cannot determine shifts.");
            return []; 
        }
        let referenceTime;
        if (activeDataSource === 'latest') {
            referenceTime = new Date(); 
        } else if (dateOfPriorDayView && dateOfPriorDayView !== "N/A") {
            referenceTime = parseDateTime(`${dateOfPriorDayView}T12:00:00`);
        } else {
             Logger.warn("[PDPPage shiftsToDisplay] Cannot determine reference time for prior day view or latest.");
            return ORDERED_SHIFT_KEYS.slice(0, 3).map(key => ALL_SHIFT_DEFINITIONS[key]).filter(Boolean); 
        }
        if (!referenceTime || isNaN(referenceTime.getTime())) {
            Logger.warn("[PDPPage shiftsToDisplay] Invalid referenceTime. Defaulting.");
            return ORDERED_SHIFT_KEYS.slice(0, 3).map(key => ALL_SHIFT_DEFINITIONS[key]).filter(Boolean); 
        }
        const upcomingShifts = [];
        for (const shiftKey of ORDERED_SHIFT_KEYS) {
            const shiftDef = ALL_SHIFT_DEFINITIONS[shiftKey]; if (!shiftDef) continue;
            let firstQuarterActualDateStr = "N/A"; const firstQuarter = shiftDef.quarters[0];
            switch (firstQuarter.dateKeyForQuarter) {
                case 'current_day_date': firstQuarterActualDateStr = shiftDates.currentDayDateStr; break;
                case 'next_day_date': firstQuarterActualDateStr = shiftDates.nextDayDateStr; break;
                case 'day_after_next_date': firstQuarterActualDateStr = shiftDates.dayAfterNextDateStr; break;
                case 'day_after_next_plus_1_date': firstQuarterActualDateStr = shiftDates.dayAfterNextPlus1DateStr; break;
                default: break;
            }
            if (firstQuarterActualDateStr === "N/A") continue;
            const shiftStartDateTime = parseDateTime(`${firstQuarterActualDateStr}T${String(firstQuarter.startHour).padStart(2, '0')}:00:00`);
            if (shiftStartDateTime && shiftStartDateTime >= referenceTime) upcomingShifts.push({ key: shiftKey, def: shiftDef, startTime: shiftStartDateTime });
        }
        upcomingShifts.sort((a, b) => a.startTime - b.startTime);
        if (upcomingShifts.length === 0) {
            Logger.warn("[PDPPage shiftsToDisplay] No upcoming shifts found relative to reference time. Showing last available shifts from definitions.");
            const lastThreeKeys = ORDERED_SHIFT_KEYS.slice(-3);
            return lastThreeKeys.map(key => ALL_SHIFT_DEFINITIONS[key]).filter(Boolean);
        }
        const nearestShiftKey = upcomingShifts[0].key; const nearestShiftIndex = ORDERED_SHIFT_KEYS.indexOf(nearestShiftKey);
        if (nearestShiftIndex === -1) {
            Logger.error(`[PDPPage shiftsToDisplay] Critical error: Nearest shift key ${nearestShiftKey} not in ORDERED_SHIFT_KEYS.`);
            return ORDERED_SHIFT_KEYS.slice(0, 3).map(key => ALL_SHIFT_DEFINITIONS[key]).filter(Boolean); 
        }
        const displayKeys = ORDERED_SHIFT_KEYS.slice(nearestShiftIndex, nearestShiftIndex + 3);
        return displayKeys.map(key => ALL_SHIFT_DEFINITIONS[key]).filter(Boolean);
    }, [shiftDates, activeDataSource, dateOfPriorDayView, data.current_day?.date]);

    // Effect for auto-balancing planned hours when `triggerAutoBalance` is true
    useEffect(() => {
        const hasExtendedPredictions = data.extended_predictions?.predictions?.length > 0;
        
        if (triggerAutoBalance && hasExtendedPredictions && shiftsToDisplay.length > 0 && targetTPH > 0) {
            Logger.log("PDPPage: Auto-balancing triggered. Recalculating optimal hours.");
            const newQuarterlyDataForDisplayedShifts = {}; 

            shiftsToDisplay.forEach(shiftDef => {
                if (shiftDef && shiftDef.quarters) {
                    shiftDef.quarters.forEach(quarter => {
                        const exVolForQuarter = calculateQuarterVolumeUtil(quarter, data, shiftDates);
                        const hoursToSolve = targetTPH > 0 ? exVolForQuarter / targetTPH : 0; 
                        const sensibleHoursToSolve = hoursToSolve > 0.05 ? parseFloat(hoursToSolve.toFixed(1)) : 0; 

                        // Preserve existing plannedRate if available, otherwise use default
                        const existingRate = quarterlyInputs[quarter.id]?.plannedRate;
                        newQuarterlyDataForDisplayedShifts[quarter.id] = {
                            plannedHours: sensibleHoursToSolve,
                            plannedRate: existingRate !== undefined ? existingRate : CONSTANTS.DEFAULT_PLANNED_RATE,
                        };
                    });
                }
            });
            
            // Update the global state by merging new data for displayed shifts
            // with existing data for other shifts (if any, though PDP focuses on displayed)
            setQuarterlyInputs(currentGlobalInputs => ({
                ...currentGlobalInputs, // Preserve inputs for shifts not currently displayed
                ...newQuarterlyDataForDisplayedShifts // Overwrite/set for displayed shifts
            }));
            setTriggerAutoBalance(false); // IMPORTANT: Reset trigger after auto-balancing
        }
    }, [
        data, 
        shiftsToDisplay, 
        targetTPH, 
        shiftDates, 
        triggerAutoBalance, // This effect runs when this becomes true
        setQuarterlyInputs, // Stable dispatcher
        quarterlyInputs // Read current rates, but don't cause loop for this effect
    ]); 

    // This effect runs ONCE when shiftsToDisplay changes to ensure initial population
    // if quarterlyInputs doesn't have data for these shifts yet.
    // This is a softer initial population that doesn't use `triggerAutoBalance`
    // to avoid conflict with the button-triggered re-balance.
    useEffect(() => {
        const hasExtendedPredictions = data.extended_predictions?.predictions?.length > 0;
        if (hasExtendedPredictions && shiftsToDisplay.length > 0 && targetTPH > 0) {
            let needsInitialization = false;
            const initialInputs = {};

            shiftsToDisplay.forEach(shiftDef => {
                if (shiftDef && shiftDef.quarters) {
                    shiftDef.quarters.forEach(quarter => {
                        if (!quarterlyInputs[quarter.id]) { // Only if not already set
                            needsInitialization = true;
                            const exVolForQuarter = calculateQuarterVolumeUtil(quarter, data, shiftDates);
                            const hoursToSolve = targetTPH > 0 ? exVolForQuarter / targetTPH : 0;
                            const sensibleHoursToSolve = hoursToSolve > 0.05 ? parseFloat(hoursToSolve.toFixed(1)) : 0;
                            initialInputs[quarter.id] = {
                                plannedHours: sensibleHoursToSolve,
                                plannedRate: CONSTANTS.DEFAULT_PLANNED_RATE,
                            };
                        }
                    });
                }
            });

            if (needsInitialization) {
                Logger.log("PDPPage: Performing one-time initialization for newly displayed shifts.");
                setQuarterlyInputs(currentGlobalInputs => ({
                    ...currentGlobalInputs,
                    ...initialInputs
                }));
            }
        }
    // eslint-disable-next-line react-hooks/exhaustive-deps 
    }, [data.extended_predictions, shiftsToDisplay, targetTPH, shiftDates, setQuarterlyInputs]); // Run if these key data points for calculation change, quarterlyInputs deliberately omitted from direct trigger here

    const handleQuarterlyInputChange = (quarterId, field, value) => {
        const numericValue = parseFloat(value);
        setQuarterlyInputs(prev => ({ 
            ...prev, 
            [quarterId]: { 
                ...prev[quarterId], 
                [field]: isNaN(numericValue) ? '' : (field === 'plannedHours' ? parseFloat(value) : numericValue) // Allow empty string or float for hours
            } 
        }));
    };
    
    const handleReBalanceHours = () => {
        Logger.log("PDPPage: Manual Re-Balance Optimal Hours button clicked.");
        setTriggerAutoBalance(true); 
    };

    const overallTotalPlannedHours = useMemo(() => {
        let total = 0;
        shiftsToDisplay.forEach(shiftDef => {
            if (shiftDef && shiftDef.quarters) {
                shiftDef.quarters.forEach(quarter => {
                    total += parseFloat(quarterlyInputs[quarter.id]?.plannedHours) || 0;
                });
            }
        });
        return total;
    }, [shiftsToDisplay, quarterlyInputs]);


    if (!data || data.time === "N/A" || shiftDates.currentDayDateStr === "N/A") {
        return ( <div className="text-center py-10"><div className="animate-spin rounded-full h-12 w-12 border-b-2 border-indigo-500 mx-auto mb-4"></div><p className="text-gray-600 dark:text-gray-400">Loading PDP data or waiting for valid date information...</p></div> );
    }
    if (shiftsToDisplay.length === 0 && data.time !== "N/A") { 
         return ( <div className="text-center py-10"><p className="text-gray-600 dark:text-gray-400">Could not determine shifts to display. Check VIZ data dates or current time alignment.</p><p className="text-xs text-gray-500 dark:text-gray-400 mt-2">Current VIZ time: {data.time}, Current Day in VIZ: {data.current_day?.date}</p></div> );
    }

    return (
        <div className="container mx-auto px-2 sm:px-4">
            <div className="flex justify-between items-center mb-4">
                <h2 className="text-2xl sm:text-3xl font-bold text-indigo-700 dark:text-indigo-400 tracking-tight">
                    Production Daily Plan (PDP)
                    {activeDataSource === 'priorDay' && dateOfPriorDayView && (
                        <span className="block text-sm text-amber-600 dark:text-amber-400 font-normal">
                            Viewing Plan for: {formatDate(dateOfPriorDayView)}
                        </span>
                    )}
                </h2>
                <div className="flex space-x-2">
                    <button onClick={switchToPriorDayView} disabled={!(activeDataSource === 'latest' ? dateOfLatestDataLoaded : dateOfPriorDayView)} className={`px-3 py-2 text-xs sm:text-sm font-semibold rounded-lg shadow-md transition-colors ${ !(activeDataSource === 'latest' ? dateOfLatestDataLoaded : dateOfPriorDayView) ? 'bg-gray-400 text-gray-700 cursor-not-allowed' : 'bg-sky-600 hover:bg-sky-700 text-white' }`} title={!(activeDataSource === 'latest' ? dateOfLatestDataLoaded : dateOfPriorDayView) ? "Load latest data first or no further prior data available" : "View plan based on data from the previous operational day"}>
                        View Prior Day's Plan
                    </button>
                    <button onClick={switchToLatestView} disabled={activeDataSource === 'latest'} className={`px-3 py-2 text-xs sm:text-sm font-semibold rounded-lg shadow-md transition-colors ${ activeDataSource === 'latest' ? 'bg-gray-400 text-gray-700 cursor-not-allowed' : 'bg-green-600 hover:bg-green-700 text-white' }`} title="Switch to the most recently fetched data">
                        View Latest Plan
                    </button>
                </div>
            </div>
            
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6 items-end"> 
                <div className="p-4 bg-white dark:bg-slate-800 shadow-lg rounded-lg">
                    <label htmlFor="targetTPHInput" className="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Set Overall Target TPH:</label>
                    <div className="flex items-center">
                        <input type="number" id="targetTPHInput" value={targetTPH} onChange={(e) => setTargetTPH(parseFloat(e.target.value) || CONSTANTS.DEFAULT_TARGET_TPH)} className="w-full p-2 border border-slate-300 dark:border-slate-600 rounded-md bg-white dark:bg-slate-700 text-sm focus:ring-indigo-500 focus:border-indigo-500"/>
                        <button onClick={handleReBalanceHours} className="ml-3 px-3 py-2 bg-emerald-600 hover:bg-emerald-700 text-white font-semibold rounded-lg shadow-md transition-colors text-xs sm:text-sm whitespace-nowrap" title="Recalculate and apply optimal planned hours based on current Target TPH and Expected Volume">
                           Apply Optimal Hrs
                        </button>
                    </div>
                </div>
                <MetricCard title="Total Planned Hours (Displayed Shifts)" value={overallTotalPlannedHours} unit="hrs" size="large" />
            </div>

            {data.historical_context && <HistoricalDailySummaryCard historicalContext={data.historical_context} />}

            <div className="space-y-8 mt-6">
                {shiftsToDisplay.map((shiftDef) => (
                    shiftDef ? 
                    <ShiftQuarterPlannerCard
                        key={shiftDef.key}
                        shiftKey={shiftDef.key}
                        shiftDefinition={shiftDef}
                        data={data} 
                        quarterlyInputs={quarterlyInputs}
                        handleInputChange={handleQuarterlyInputChange}
                        targetTPH={targetTPH}
                        shiftDates={shiftDates} 
                    />
                    : null
                ))}
            </div>
            <div className="bg-white dark:bg-slate-800 shadow-xl rounded-lg p-4 sm:p-6 mt-10 mb-6">
                <h3 className="text-lg sm:text-xl font-semibold text-gray-700 dark:text-white mb-4">Extended Hourly Demand Forecast (48hr+ Rolling)</h3>
                <ExtendedForecastChart predictions={data.extended_predictions?.predictions || []} theme={theme} />
            </div>
            <p className="mt-10 text-xs sm:text-sm text-gray-500 dark:text-gray-400 text-center">Note: "Expected Volume" per quarter is derived ONLY from the extended rolling forecast. TPH, VCap, and Discrepancy are calculated based on your inputs.</p>
        </div>
    );
};

const getTrendIndicatorInfo = (percentageChange) => {
    if (percentageChange === null || percentageChange === undefined || isNaN(parseFloat(percentageChange))) {
        return { arrow: "", colorClass: "text-slate-500", percentageText: "N/A" };
    }
    const change = parseFloat(percentageChange);
    let arrow = "↔️"; // Stable
    let colorClass = "text-slate-500 dark:text-slate-400";

    // Define thresholds for trend sensitivity, can be adjusted
    const strongThreshold = 5; // +/- 5% for strong trend
    const moderateThreshold = 1; // +/- 1% for moderate trend

    if (change > strongThreshold) {
        arrow = "⬆️⬆️"; // Strong Up
        colorClass = "text-green-600 dark:text-green-400";
    } else if (change > moderateThreshold) {
        arrow = "⬆️"; // Moderate Up
        colorClass = "text-green-500 dark:text-green-500";
    } else if (change < -strongThreshold) {
        arrow = "⬇️⬇️"; // Strong Down
        colorClass = "text-red-600 dark:text-red-400";
    } else if (change < -moderateThreshold) {
        arrow = "⬇️"; // Moderate Down
        colorClass = "text-red-500 dark:text-red-500";
    }
    
    const percentageText = `${change > 0 ? '+' : ''}${change.toFixed(1)}%`;
    return { arrow, colorClass, percentageText };
};

const HistoricalDailySummaryCard = ({ historicalContext }) => {
    const [isExpanded, setIsExpanded] = useState(false); // Default to collapsed

    // Check if the necessary historical context data is available
    if (!historicalContext || !historicalContext.daily_summary_trends || !historicalContext.overall_summary) {
        return (
            <div className="mb-6 p-4 bg-amber-50 dark:bg-amber-900/30 rounded-lg shadow-md text-sm text-amber-700 dark:text-amber-300">
                Historical trend data is currently unavailable or incomplete.
            </div>
        );
    }

    // Define the order of days for display
    const daysOfWeekOrder = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
    
    // Extract overall summary data with fallbacks
    const overall7DayAvg = historicalContext.overall_summary?.avg_daily_volume_rolling_7_days || 0;
    const trendPeriodDays = historicalContext.trend_period_days || 45; // Default if not in context
    const shortTermOccurrences = historicalContext.short_term_ma_occurrences || 3; // Default if not in context
    const longTermOccurrences = historicalContext.num_weeks_for_avg || 6; // Default if not in context

    return (
        <div className="mb-6 p-4 bg-white dark:bg-slate-800 shadow-lg rounded-lg">
            {/* Button to expand/collapse the historical trends section */}
            <button
                onClick={() => setIsExpanded(!isExpanded)}
                className="w-full flex justify-between items-center text-left text-lg font-semibold text-indigo-700 dark:text-indigo-400 mb-2 hover:text-indigo-500 dark:hover:text-indigo-300 transition-colors"
            >
                <span>Daily Historical Trends (Last {trendPeriodDays} Days)</span>
                {isExpanded ? <ChevronUpIcon /> : <ChevronDownIcon />}
            </button>

            {/* Collapsible content area */}
            {isExpanded && (
                <div className="mt-3 space-y-4">
                    {/* Overall 7-day rolling average display */}
                    <div className="text-center pb-2 border-b border-slate-200 dark:border-slate-700">
                        <p className="text-md font-semibold text-slate-700 dark:text-slate-200">
                            Overall 7-Day Rolling Avg. Daily Volume: 
                            <span className="text-indigo-600 dark:text-indigo-400 ml-1">
                                {overall7DayAvg.toLocaleString(undefined, {maximumFractionDigits:0})} units
                            </span>
                        </p>
                    </div>
                    {/* Grid for displaying trends for each day of the week */}
                    <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-7 gap-3">
                        {daysOfWeekOrder.map(day => {
                            // Get data for the specific day, with fallbacks if data is missing
                            const dayData = historicalContext.daily_summary_trends[day] || {
                                [`avg_total_daily_volume_last_${longTermOccurrences}_occurrences`]: 0,
                                [`avg_total_daily_volume_last_${shortTermOccurrences}_occurrences`]: 0,
                                "last_occurrence_total_daily_volume": 0, 
                                trend_direction_pct_change: 0.0
                            };
                            const longTermAvg = dayData[`avg_total_daily_volume_last_${longTermOccurrences}_occurrences`];
                            const shortTermAvg = dayData[`avg_total_daily_volume_last_${shortTermOccurrences}_occurrences`];
                            const lastOccurrenceTotal = dayData.last_occurrence_total_daily_volume; 
                            const trendInfo = getTrendIndicatorInfo(dayData.trend_direction_pct_change);

                            return (
                                <div key={day} className="p-3 bg-slate-50 dark:bg-slate-700/60 rounded-lg shadow">
                                    <p className="font-bold text-md text-slate-800 dark:text-slate-100 mb-1.5 text-center">{day}</p>
                                    <div className="text-xs space-y-1 text-slate-600 dark:text-slate-300">
                                        <p>Last {day.substring(0,3)} Total: <span className="font-medium float-right">{(lastOccurrenceTotal || 0).toLocaleString(undefined, {maximumFractionDigits:0})}</span></p>
                                        <p>Avg ({shortTermOccurrences} occ): <span className="font-medium float-right">{(shortTermAvg || 0).toLocaleString(undefined, {maximumFractionDigits:0})}</span></p>
                                        <p>Avg ({longTermOccurrences} occ): <span className="font-medium float-right">{(longTermAvg || 0).toLocaleString(undefined, {maximumFractionDigits:0})}</span></p>
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


const DashboardPage = ({ data, quarterlyInputs, targetTPH, setCurrentView }) => {
    const [theme] = useTheme();
    const { current_day, next_day, Ledger_Information, extended_predictions } = data; 

    const multiUnitMix = useMemo(() => { const iptm = Ledger_Information?.metrics?.IPTM?.[0] || 0; const iptnw = Ledger_Information?.metrics?.IPTNW?.[0] || 1; return iptnw > 0 ? (iptm / iptnw) * 100 : 0; }, [Ledger_Information]);
    const todayEODSarima = current_day?.predictions_no_same_day?.[current_day.predictions_no_same_day.length -1]?.Predicted_Workable_No_Same_Day || 0;
    const alpsTargetToday = current_day?.network_prediction || 0;
    const deviationVsAlps = alpsTargetToday > 0 ? ((todayEODSarima - alpsTargetToday) / alpsTargetToday) * 100 : 0;
    const nextDayEODSarima = next_day?.sarima_predictions?.[next_day.sarima_predictions.length -1]?.Predicted_Workable || 0;
    const currentAPU = Ledger_Information?.metrics?.APU?.slice(-1)[0] || 0;
    const currentEligible = Ledger_Information?.metrics?.Eligible?.slice(-1)[0] || 0;
    const currentBacklog = currentAPU + currentEligible;

    const shiftDates = useMemo(() => { 
        const cdDate = data.current_day?.date;
        const ndDate = data.next_day?.date;
        return {
            currentDayDateStr: cdDate && cdDate !== "N/A" ? cdDate : "N/A",
            nextDayDateStr: ndDate && ndDate !== "N/A" ? ndDate : "N/A",
            dayAfterNextDateStr: ndDate && ndDate !== "N/A" ? getOffsetDateString(ndDate, 1) : "N/A",
            dayAfterNextPlus1DateStr: ndDate && ndDate !== "N/A" ? getOffsetDateString(ndDate, 2) : "N/A",
        };
    }, [data.current_day, data.next_day]);

    const getShiftAggregatesForBacklog = useCallback((shiftKeyToCalc, dataForCalc) => {
        const shiftDefinition = ALL_SHIFT_DEFINITIONS[shiftKeyToCalc];
        if (!shiftDefinition) {
            Logger.warn(`[Dashboard Handoff] Shift definition not found for key: ${shiftKeyToCalc}`);
            return { totalExVol: 0, totalPlannedVCap: 0, shiftName: "Unknown Shift" };
        }

        let totalExVol = 0; 
        let totalPlannedVCap = 0;
        
        shiftDefinition.quarters.forEach(quarter => {
            let qActualDateStr = "N/A";
            let predictionsForQuarterData = []; 

            switch (quarter.dateKeyForQuarter) {
                case 'current_day_date': 
                    qActualDateStr = shiftDates.currentDayDateStr;
                    predictionsForQuarterData = dataForCalc.current_day?.sarima_predictions || [];
                    break;
                case 'next_day_date': 
                    qActualDateStr = shiftDates.nextDayDateStr;
                    predictionsForQuarterData = dataForCalc.next_day?.sarima_predictions || [];
                    break;
                case 'day_after_next_date': 
                    qActualDateStr = shiftDates.dayAfterNextDateStr;
                    predictionsForQuarterData = (dataForCalc.extended_predictions?.predictions || []).filter(p => {
                         if (!p || !p.Time) return false; const d = parseDateTime(p.Time);
                         return d && d.toISOString().startsWith(qActualDateStr); 
                    });
                    break;
                 case 'day_after_next_plus_1_date':
                    qActualDateStr = shiftDates.dayAfterNextPlus1DateStr;
                     predictionsForQuarterData = (dataForCalc.extended_predictions?.predictions || []).filter(p => {
                         if (!p || !p.Time) return false; const d = parseDateTime(p.Time);
                         return d && d.toISOString().startsWith(qActualDateStr); 
                    });
                    break;
                default: 
                    qActualDateStr = "N/A";
                    Logger.warn(`[Dashboard Handoff] Unknown dateKeyForQuarter: ${quarter.dateKeyForQuarter} in shift ${shiftDefinition.name}`);
            }

            if (qActualDateStr === "N/A" || qActualDateStr === undefined) {
                Logger.warn(`[Dashboard Handoff] qActualDateStr is N/A for quarter ${quarter.id} in shift ${shiftDefinition.name}. Skipping quarter.`);
                return; 
            }
            
            const endLookupHour = quarter.endHour === 0 ? 23 : quarter.endHour - 1;
            const volAtEndOfQuarter = getCumulativeVolumeAtSpecificDateTime(predictionsForQuarterData, qActualDateStr, endLookupHour);
            
            let volAtStartOfQuarter = (quarter.startHour === 0) ? 0 : getCumulativeVolumeAtSpecificDateTime(predictionsForQuarterData, qActualDateStr, quarter.startHour - 1);
            
            let exVolForQuarter = Math.max(0, volAtEndOfQuarter - volAtStartOfQuarter);
            totalExVol += exVolForQuarter;

            const inputs = quarterlyInputs[quarter.id] || {};
            totalPlannedVCap += (parseFloat(inputs.plannedRate) || 0) * (parseFloat(inputs.plannedHours) || 0);
        });
        return { totalExVol, totalPlannedVCap, shiftName: shiftDefinition.name };
    }, [quarterlyInputs, shiftDates, data]); 

    const handoffTrajectories = useMemo(() => {
        if (shiftDates.currentDayDateStr === "N/A") {
            Logger.warn("[Dashboard Handoff] Current day date is N/A, cannot calculate trajectories.");
            return [];
        }

        const now = new Date();
        let currentActualShiftKey = null;
        for (const shiftKey of ORDERED_SHIFT_KEYS) {
            const shiftDef = ALL_SHIFT_DEFINITIONS[shiftKey];
            let shiftStartDateTime, shiftEndDateTime;
             if (shiftKey === 'currentNight') {
                shiftStartDateTime = parseDateTime(`${shiftDates.currentDayDateStr}T18:00:00`);
                shiftEndDateTime = parseDateTime(`${shiftDates.nextDayDateStr}T06:00:00`);
            } else if (shiftKey === 'nextDay') {
                shiftStartDateTime = parseDateTime(`${shiftDates.nextDayDateStr}T06:00:00`);
                shiftEndDateTime = parseDateTime(`${shiftDates.nextDayDateStr}T18:00:00`);
            } else if (shiftKey === 'nextNight') {
                shiftStartDateTime = parseDateTime(`${shiftDates.nextDayDateStr}T18:00:00`);
                if (shiftDates.dayAfterNextDateStr !== "N/A") shiftEndDateTime = parseDateTime(`${shiftDates.dayAfterNextDateStr}T06:00:00`);
            } else if (shiftKey === 'dayAfterNextDay') {
                if (shiftDates.dayAfterNextDateStr !== "N/A") {
                    shiftStartDateTime = parseDateTime(`${shiftDates.dayAfterNextDateStr}T06:00:00`);
                    shiftEndDateTime = parseDateTime(`${shiftDates.dayAfterNextDateStr}T18:00:00`);
                }
            } else if (shiftKey === 'dayAfterNextNight') {
                 if (shiftDates.dayAfterNextDateStr !== "N/A" && shiftDates.dayAfterNextPlus1DateStr !== "N/A") {
                    shiftStartDateTime = parseDateTime(`${shiftDates.dayAfterNextDateStr}T18:00:00`);
                    shiftEndDateTime = parseDateTime(`${shiftDates.dayAfterNextPlus1DateStr}T06:00:00`);
                }
            }
            if(shiftStartDateTime && shiftEndDateTime && now >= shiftStartDateTime && now < shiftEndDateTime) {
                currentActualShiftKey = shiftKey;
                break;
            }
        }
        
        if (!currentActualShiftKey) { 
            currentActualShiftKey = ORDERED_SHIFT_KEYS[0]; 
            Logger.warn("[Dashboard Handoff] Could not determine current actual shift. Defaulting to " + currentActualShiftKey + " for trajectory start determination.");
        }
        
        const currentShiftIndex = ORDERED_SHIFT_KEYS.indexOf(currentActualShiftKey);
        // Start trajectory calculations from the shift *after* the current actual shift
        const trajectoryStartShiftIndex = currentShiftIndex + 1;
        const trajectoryShiftKeys = ORDERED_SHIFT_KEYS.slice(trajectoryStartShiftIndex, trajectoryStartShiftIndex + 3); 

        let accumulatingBacklog = currentBacklog; // Start with the true current backlog
        const trajectories = [];

        Logger.log(`[Dashboard Handoff] Starting backlog for trajectory: ${accumulatingBacklog}. Current actual shift: ${currentActualShiftKey}. Trajectory shifts: ${trajectoryShiftKeys.join(', ')}`);
        
        for (let i = 0; i < trajectoryShiftKeys.length; i++) {
            const shiftKeyForTrajectory = trajectoryShiftKeys[i];
            if (!shiftKeyForTrajectory) {
                Logger.warn(`[Dashboard Handoff] Not enough future shifts defined for trajectory ${i+1}.`);
                break; 
            }

            const shiftAggs = getShiftAggregatesForBacklog(shiftKeyForTrajectory, data);
            Logger.log(`[Dashboard Handoff] Trajectory ${i+1} (Calculating for Shift: ${shiftAggs.shiftName}): ExVol=${shiftAggs.totalExVol}, PlannedVCap=${shiftAggs.totalPlannedVCap}`);
            
            // The handoff is the backlog *after* this shift completes
            const handoffValue = Math.max(0, accumulatingBacklog + shiftAggs.totalExVol - shiftAggs.totalPlannedVCap);
            
            trajectories.push({
                title: `Handoff Trajectory ${i + 1}`,
                value: handoffValue,
                subtext: `After ${shiftAggs.shiftName}` // This is the shift whose completion results in this handoff value
            });
            accumulatingBacklog = handoffValue; // This handoff becomes the starting backlog for the next trajectory calculation
            Logger.log(`[Dashboard Handoff] Trajectory ${i+1} Result (Backlog after ${shiftAggs.shiftName}): ${handoffValue}`);
        }
        return trajectories;

    }, [data, currentBacklog, getShiftAggregatesForBacklog, shiftDates]);


    if (!data || data.time === "N/A" || !current_day || !next_day || !Ledger_Information) {
        return ( <div className="text-center py-10"><div className="animate-spin rounded-full h-12 w-12 border-b-2 border-indigo-500 mx-auto mb-4"></div><p className="text-gray-600 dark:text-gray-400">Loading dashboard data...</p></div> );
    }
    return (
        <div className="container mx-auto px-2 sm:px-4">
            {/* Header and Top Row Metrics as before */}
            <div className="mb-6 p-4 bg-white dark:bg-slate-800 shadow-lg rounded-lg flex flex-wrap justify-between items-center"><h2 className="text-2xl sm:text-3xl font-semibold text-indigo-700 dark:text-indigo-400">Dashboard Overview</h2><div className="text-sm text-slate-600 dark:text-slate-400">Data Last Updated: <span className="font-semibold">{data.time}</span></div></div>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-6"> <MetricCard title="EOD Prediction (Today)" value={todayEODSarima} subtext={`vs ALPS: ${alpsTargetToday.toLocaleString()}`} size="large"/> <MetricCard title="Deviation (vs ALPS)" value={deviationVsAlps} unit="%" valueColor={deviationVsAlps >= 0 ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'} size="large"/> <MetricCard title="Multi-Unit Mix (Est.)" value={multiUnitMix} unit="%" size="large" subtext="Based on Ledger Info"/> </div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8"> <MetricCard title="EOD Prediction (Next Day)" value={nextDayEODSarima} subtext={formatDate(data.next_day?.date)} size="large" /> <div className="bg-slate-50 dark:bg-slate-700 p-4 rounded-xl shadow-lg flex flex-col items-center justify-center text-center"> <h3 className="text-lg font-semibold text-gray-700 dark:text-white mb-2">Next Day Shift Volumes</h3> <p className="text-xl font-bold text-indigo-600 dark:text-indigo-400"> Day (06-18): <span className="font-mono">{(getShiftAggregatesForBacklog('nextDay', data).totalExVol || 0).toLocaleString()}</span> units </p> <p className="text-xl font-bold text-indigo-600 dark:text-indigo-400 mt-1"> Night (18-06): <span className="font-mono">{(getShiftAggregatesForBacklog('nextNight', data).totalExVol || 0).toLocaleString()}</span> units </p> <button onClick={() => setCurrentView('pdp')} className="mt-4 bg-indigo-600 hover:bg-indigo-700 text-white font-semibold py-2 px-4 rounded-lg shadow-md transition-colors text-sm"> Go to Full PDP Planning </button> </div> </div>
            
            <div className="mb-8">
                <h3 className="text-xl sm:text-2xl font-semibold text-gray-700 dark:text-white mb-4">Backlog Handoff Trajectory</h3>
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                    <MetricCard title="Current Backlog" value={currentBacklog} subtext="(APU + Eligible)" />
                    {handoffTrajectories.map((trajectory, index) => (
                        <MetricCard 
                            key={index}
                            title={trajectory.title} 
                            value={trajectory.value} 
                            subtext={trajectory.subtext} 
                        />
                    ))}
                    {Array(Math.max(0, 3 - handoffTrajectories.length)).fill(null).map((_, index) => (
                         <MetricCard key={`placeholder-${index}`} title={`Handoff Trajectory ${handoffTrajectories.length + index + 1}`} value={"N/A"} subtext="Insufficient future shift data" />
                    ))}
                </div>
            </div>

            {/* Charts as before */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6"> <div className="bg-white dark:bg-slate-800 shadow-xl rounded-lg p-4 sm:p-6"> <h3 className="text-lg sm:text-xl font-semibold text-gray-700 dark:text-white mb-4">Today's Hourly Prediction & Actuals</h3> <TodayPredictionActualChart currentDayData={current_day} theme={theme} /> </div> <div className="bg-white dark:bg-slate-800 shadow-xl rounded-lg p-4 sm:p-6"> <h3 className="text-lg sm:text-xl font-semibold text-gray-700 dark:text-white mb-4">Backlog Trend (APU vs Eligible)</h3> <BacklogTrendChart ledgerInfo={Ledger_Information} theme={theme} currentDayDate={current_day?.date} /> </div> </div>
        </div>
    );
};

// --- Main App Component (Updated State and Data Fetching Logic) ---
function App() {
    const [currentView, setCurrentView] = useState('dashboard');
    const [vizData, setVizData] = useState(DEFAULT_VIZ_DATA);
    const [isLoading, setIsLoading] = useState(true);
    const [quarterlyInputs, setQuarterlyInputs] = useState({});
    const [targetTPH, setTargetTPH] = useState(CONSTANTS.DEFAULT_TARGET_TPH);

    // New state for managing data source view
    const [activeDataSource, setActiveDataSource] = useState('latest'); // 'latest' or 'priorDay'
    const [dateOfLatestDataLoaded, setDateOfLatestDataLoaded] = useState(null); // YYYY-MM-DD of current_day from latest fetch
    const [dateOfPriorDayView, setDateOfPriorDayView] = useState(null); // YYYY-MM-DD of prior day being viewed

    const loadLatestData = useCallback(async (isAutoRefresh = false) => {
        if (!isAutoRefresh) setIsLoading(true); 
        Logger.log("[App] Attempting to load LATEST data...");
        const result = await ApiService.fetchLatestAvailableData();
        if (result && result.vizDataResult && result.vizDataResult.time !== "N/A") {
            setVizData(result.vizDataResult);
            setActiveDataSource('latest');
            setDateOfLatestDataLoaded(result.vizDataResult.current_day?.date); 
            setDateOfPriorDayView(null); 
            Logger.log("[App] LATEST data loaded successfully for date:", result.vizDataResult.current_day?.date);
        } else {
            Logger.warn("[App] Failed to load LATEST data or data was invalid. Keeping existing or default.");
            if(vizData.time === "N/A") setVizData(DEFAULT_VIZ_DATA); 
        }
        if (!isAutoRefresh) setIsLoading(false);
    }, [vizData.time]); 

    const loadPriorDayData = useCallback(async () => {
        setIsLoading(true);
        Logger.log("[App] Attempting to load PRIOR DAY data...");
        let referenceDateForPrior;

        if (activeDataSource === 'latest' && dateOfLatestDataLoaded) {
            referenceDateForPrior = dateOfLatestDataLoaded;
        } else if (activeDataSource === 'priorDay' && dateOfPriorDayView) {
            referenceDateForPrior = dateOfPriorDayView; 
        } else if (dateOfLatestDataLoaded) { 
            referenceDateForPrior = dateOfLatestDataLoaded;
        }
         else {
            Logger.warn("[App] Cannot load prior day data: No valid reference date available.");
            setIsLoading(false);
            return;
        }

        const targetPriorDate = getOffsetDateString(referenceDateForPrior, -1);
        if (targetPriorDate === "N/A") {
            Logger.warn("[App] Cannot load prior day data: Could not calculate target prior date from reference:", referenceDateForPrior);
            setIsLoading(false);
            return;
        }
        
        Logger.log(`[App] Target prior date for fetch: ${targetPriorDate}`);
        const priorData = await ApiService.fetchDataForSpecificDay(targetPriorDate);

        if (priorData && priorData.time !== "N/A") {
            setVizData(priorData);
            setActiveDataSource('priorDay');
            setDateOfPriorDayView(priorData.current_day?.date); 
            Logger.log("[App] PRIOR DAY data loaded successfully for date:", priorData.current_day?.date);
        } else {
            Logger.warn(`[App] No data found for prior day: ${targetPriorDate}. Reverting to LATEST data if possible.`);
            if(dateOfLatestDataLoaded) {
                loadLatestData(); 
            } else {
                Logger.error("[App] No prior day data and no latest data reference. Displaying default.");
                setVizData(DEFAULT_VIZ_DATA); 
                setActiveDataSource('latest'); 
            }
        }
        setIsLoading(false);
    }, [activeDataSource, dateOfLatestDataLoaded, dateOfPriorDayView, loadLatestData]);


    useEffect(() => { 
        loadLatestData();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []); 

    useEffect(() => { 
        const intervalId = setInterval(() => {
            if (activeDataSource === 'latest') {
                Logger.log("[App] Auto-refresh: Fetching latest data...");
                loadLatestData(true); 
            }
        }, CONSTANTS.REFRESH_INTERVAL);
        return () => clearInterval(intervalId);
    }, [activeDataSource, loadLatestData]);
    
    let PageComponentToRender;
    if (currentView === 'dashboard') {
        PageComponentToRender = <DashboardPage data={vizData} quarterlyInputs={quarterlyInputs} targetTPH={targetTPH} setCurrentView={setCurrentView} />;
    } else if (currentView === 'pdp') {
        PageComponentToRender = <PDPPage 
                                    data={vizData} 
                                    quarterlyInputsGlobal={quarterlyInputs} 
                                    setQuarterlyInputsGlobal={setQuarterlyInputs} 
                                    targetTPHGlobal={targetTPH} 
                                    setTargetTPHGlobal={setTargetTPH}
                                    switchToLatestView={loadLatestData}
                                    switchToPriorDayView={loadPriorDayData}
                                    activeDataSource={activeDataSource}
                                    dateOfLatestDataLoaded={dateOfLatestDataLoaded}
                                    dateOfPriorDayView={dateOfPriorDayView}
                                />;
    } else {
        PageComponentToRender = <div className="text-center text-red-500 p-10">Error: Page not found</div>;
    }
    return ( 
        <div className="min-h-screen bg-slate-100 dark:bg-slate-900 text-slate-900 dark:text-slate-100 transition-colors duration-300 font-sans"> 
            <Header 
                currentView={currentView} 
                setCurrentView={setCurrentView} 
                lastUpdateTime={vizData?.time} 
                onRefreshData={activeDataSource === 'latest' ? loadLatestData : () => {} }
                activeDataSource={activeDataSource}
                dateOfPriorDayView={dateOfPriorDayView}
            /> 
            <main className="pt-4 pb-8">  
                {isLoading && vizData.time === "N/A" ? ( 
                    <div className="text-center py-20"> 
                        <div className="animate-spin rounded-full h-16 w-16 border-t-2 border-b-2 border-indigo-500 mx-auto mb-6"></div> 
                        <p className="text-xl text-gray-600 dark:text-gray-400">Initializing ATHENA & Fetching Data...</p> 
                    </div> 
                ) : PageComponentToRender} 
            </main> 
            <footer className="text-center py-6 sm:py-8 mt-8 sm:mt-10 border-t border-slate-200 dark:border-slate-700"> 
                <p className="text-xs sm:text-sm text-slate-500 dark:text-slate-400">&copy; {new Date().getFullYear()} ATHENA Predictive Analytics. For internal use only.</p> 
            </footer> 
        </div> 
    );
}

export default App;