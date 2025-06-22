import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict
import xlwings as xw
import os
from holidays import US as us_holidays
import traceback
import sys
import signal
import json
import boto3
import io
import schedule
import time
from prophet import Prophet
from datetime import datetime, timedelta
from botocore.exceptions import NoCredentialsError

S3_BUCKET_NAME = None

def run_scheduled_task():
    """Executes the main prediction logic on a schedule."""
    try:
        print(f"\nStarting scheduled run at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        main()
        print(f"Completed run at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as e:
        print(f"Error in scheduled run: {str(e)}")
        traceback.print_exc()

def check_environment():
    """
    Checks if all necessary Python versions and packages are installed.
    Raises an exception if any requirement is not met.
    """
    try:
        # Check Python version
        if sys.version_info < (3, 9):
            raise Exception("Python 3.9 or later is required")
            
        # Check required packages
        required_packages = ['pandas', 'numpy', 'xlwings', 'holidays', 'prophet', 'boto3']
        for package in required_packages:
            try:
                __import__(package)
            except ImportError:
                raise Exception(f"Required package '{package}' is not installed")
                
    except Exception as e:
        print(f"Environment Check Failed: {str(e)}")
        print("\nPlease ensure:")
        print("1. Python 3.9+ is installed")
        print("2. All required packages are installed (run install.bat)")
        print("3. Excel file is in the correct location (if applicable for other parts of the system)")
        input("\nPress Enter to exit...")
        sys.exit(1)

class ChargePredictor:
    """
    A class to predict charge levels using a combination of Prophet,
    incorporating external network predictions and historical data.
    """
    def __init__(self):
        try:
            # Initialize S3 client for data retrieval
            self.s3 = boto3.client('s3', region_name='us-west-1')
            self.bucket_name = 'ledger-prediction-charting-008971633421'

            # Fetch and load main data (combine.csv)
            try:
                print("Fetching combine.csv from S3...")
                combine_obj = self.s3.get_object(
                    Bucket=self.bucket_name,
                    Key='userloads/combine.csv'
                )
                combine_data = combine_obj['Body'].read().decode('utf-8')
                self.df = pd.read_csv(io.StringIO(combine_data))
                print("Successfully loaded combine.csv from S3")
            except Exception as e:
                print(f"Error reading combine.csv from S3: {str(e)}")
                raise
            
            # Determine the target date for predictions (today, normalized to midnight)
            self.target_date = pd.to_datetime('today').normalize()
            print(f"Target prediction date: {self.target_date.strftime('%m/%d/%Y')}")
            
            # Basic data sanity check and column renaming
            print("\nOriginal columns:", self.df.columns.tolist())
            if len(self.df.columns) > 2:
                self.df = self.df.iloc[:, [0, 1]].copy()  # Keep only the first two columns
            self.df.columns = ['Time', 'Workable']
            self.df['Time'] = pd.to_datetime(self.df['Time'])
            
            print("\nDataset Information:")
            print(f"Total rows: {len(self.df)}")
            print(f"Date range: {self.df['Time'].min()} to {self.df['Time'].max()}")
            
            # Prepare data, calculate features, and derive 'Hourly_Increase'
            self.prepare_data()
            
            # Initialize model attributes
            self.prophet_model = None
            self.prophet_results = None
            self.network_prediction = 0.0 # Initialize to a safe default
            
            # Train models
            self.train_prophet_model()
            
        except Exception as e:
            print(f"Error in ChargePredictor initialization: {str(e)}")
            print("\nDataFrame shape:", self.df.shape if hasattr(self, 'df') else "Not created")
            print("DataFrame columns:", self.df.columns.tolist() if hasattr(self, 'df') else "Not created")
            traceback.print_exc()
            raise

    def prepare_data(self):
        """
        Prepares and cleans the raw data, extracting time-based, holiday, and seasonal features.
        Crucially, it calculates 'Hourly_Increase', which is the non-decreasing hourly increment
        of 'Workable' volume, resetting to zero at the start of each day. This is the primary
        target for the Prophet model.
        """
        # Feature Engineering for Prophet (implicitly via time index)
        self.df['Hour'] = self.df['Time'].dt.hour
        self.df['DayOfWeek'] = self.df['Time'].dt.day_name()
        self.df['Month'] = self.df['Time'].dt.month
        self.df['Date'] = self.df['Time'].dt.date # Date part only for grouping/filtering
        
        self.df['WeekOfYear'] = self.df['Time'].dt.isocalendar().week.astype(int)
        self.df['IsWeekend'] = (self.df['Time'].dt.weekday >= 5).astype(int)
        # Ensure us_holidays() is called only once for performance
        us_holidays_set = us_holidays()
        self.df['IsHoliday'] = self.df['Time'].dt.date.apply(lambda x: x in us_holidays_set).astype(int)
        self.df['IsNearHoliday'] = self.df.apply(self._is_near_holiday, axis=1).astype(int)
        self.df['Season'] = self.df['Month'].apply(self._get_season)
        self.df['IsPeakHour'] = self.df['Hour'].apply(
            lambda x: 1 if (9 <= x <= 17) or (6 <= x <= 8) else 0
        )
        
        # Drop rows with missing 'Workable' values and sort by time
        self.df = self.df.dropna(subset=['Workable']).sort_values('Time').reset_index(drop=True)
        
        # Calculate 'Hourly_Increase'
        # This function calculates the hourly increase, ensuring it's always non-negative
        # and correctly handles the daily reset (00:00 has 0 increase from previous day).
        def calculate_daily_increase(group):
            # Sort group by time to ensure correct diff calculation
            group = group.sort_values(by='Time')
            
            # Calculate the difference from the previous hour within the group
            increases = group['Workable'].diff()
            
            # For the very first record of each day (00:00), the increase is 0 from the implicit reset.
            increases.iloc[0] = 0 # Ensure the first point of the day has 0 increase
            return increases

        # Apply this function to each day group to get daily hourly increases
        # Use .copy() to avoid SettingWithCopyWarning
        self.df['Hourly_Increase'] = self.df.groupby(self.df['Time'].dt.date).apply(calculate_daily_increase).reset_index(level=0, drop=True).copy()
        
        # Ensure all increases are non-negative, as volume should not decrease within an hour/day
        self.df['Hourly_Increase'] = np.maximum(0, self.df['Hourly_Increase'])

        print(f"Total rows after cleaning: {len(self.df)}")
        print(f"Sample of data with Workable and Hourly_Increase (tail):\n{self.df[['Time', 'Workable', 'Hourly_Increase']].tail()}")
        print(f"Sample of data with Workable and Hourly_Increase (head):\n{self.df[['Time', 'Workable', 'Hourly_Increase']].head()}")


    def _is_near_holiday(self, row):
        """Helper function to check if a date is near a US holiday."""
        date = row['Time'].date()
        for holiday_date in us_holidays().keys():
            if abs((date - holiday_date).days) <= 3:
                return True
        return False

    def _get_season(self, month):
        """Helper function to map a month to a season."""
        if month in [12, 1, 2]: return 'Winter'
        elif month in [3, 4, 5]: return 'Spring'
        elif month in [6, 7, 8]: return 'Summer'
        else: return 'Fall'

    def train_prophet_model(self):
        """
        Trains the Prophet model on the 'Hourly_Increase' data.
        This allows Prophet to learn the hourly growth patterns, which are then cumulated
        for final predictions, respecting the non-decreasing nature and daily resets.
        """
        try:
            # Prepare time series data for Prophet: requires 'ds' (datetime) and 'y' (value)
            prophet_df = self.df[['Time', 'Hourly_Increase']].copy() 
            prophet_df.columns = ['ds', 'y']
            
            # Ensure 'ds' is datetime
            prophet_df['ds'] = pd.to_datetime(prophet_df['ds'])
            
            # Sort by 'ds' and drop duplicates to prevent "cannot reindex" error
            prophet_df = prophet_df.sort_values('ds').drop_duplicates(subset='ds', keep='last')

            # Resample to ensure a consistent hourly frequency, filling missing hours with 0
            # (assuming no increase for missing hours)
            prophet_df = prophet_df.set_index('ds').resample('h').asfreq(fill_value=0).reset_index()

            # Fetch network prediction (IPTNW) from S3 granular.csv
            try:
                print("Fetching granular.csv from S3 for network prediction...")
                granular_obj = self.s3.get_object(
                    Bucket=self.bucket_name,
                    Key='userloads/granular.csv'
                )
                granular_data = granular_obj['Body'].read().decode('utf-8')
                granular_df = pd.read_csv(io.StringIO(granular_data))
                
                # Validate and assign network_prediction, fallback to 0.0 if invalid
                if not granular_df['IPTNW'].empty and pd.notna(granular_df['IPTNW'].iloc[0]):
                    self.network_prediction = float(granular_df['IPTNW'].iloc[0])
                    print(f"Network prediction (IPTNW) loaded: {self.network_prediction}")
                else:
                    self.network_prediction = 0.0
                    print(f"Warning: IPTNW in granular.csv is missing or invalid. Using default network_prediction: {self.network_prediction}")
            except Exception as e:
                self.network_prediction = 0.0
                print(f"Error reading granular.csv from S3: {str(e)}. Using default network_prediction: {self.network_prediction}")

            # Define Prophet model with explicit seasonality components
            # 'additive' seasonality mode is suitable for modeling increments.
            self.prophet_model = Prophet(
                daily_seasonality=True,
                yearly_seasonality=False, # Set to True if annual patterns in 'increases' exist
                weekly_seasonality=True, # Weekly patterns in 'increases' are common
                seasonality_mode='additive', 
                changepoint_prior_scale=0.15 # Adjust for more (higher value) or less (lower value) trend flexibility
            )
            
            print("\nFitting Prophet model on Hourly_Increase...")
            self.prophet_results = self.prophet_model.fit(prophet_df)
            print("Prophet Model training completed on Hourly_Increase")
            
        except Exception as e:
            print(f"Error in train_prophet_model: {str(e)}") 
            raise

    def get_extended_rolling_predictions(self):
        """
        Generates 48-hour rolling predictions starting from the current hour.
        This method stitches together the scaled current day's predictions (from predict_for_target_date)
        and subsequent days' predictions (from predict_next_day_enhanced).
        """
        try:
            current_time = datetime.now().replace(minute=0, second=0, microsecond=0)
            final_predictions_list = []

            # 1. Get current day's predictions (which are already scaled and blend actuals)
            current_day_predictions_df = self.predict_for_target_date()
            if current_day_predictions_df.empty:
                print("Warning: Current day predictions are empty for extended rolling forecast.")
                return pd.DataFrame()

            # Filter current day predictions to include only from the current hour onwards
            current_hour_preds = current_day_predictions_df[
                pd.to_datetime(current_day_predictions_df['Time']) >= current_time
            ].copy()
            final_predictions_list.extend(current_hour_preds.to_dict(orient='records'))
            print(f"Added {len(current_hour_preds)} current day predictions to extended forecast.")

            # 2. Get next day's predictions (which are already scaled)
            next_day_start_dt = (current_time + timedelta(days=1)).replace(hour=0)
            next_day_predictions_df = self.predict_next_day_enhanced(next_day_start_dt)
            if next_day_predictions_df is None or next_day_predictions_df.empty:
                print("Warning: Next day predictions are empty for extended rolling forecast.")
            else:
                final_predictions_list.extend(next_day_predictions_df.to_dict(orient='records'))
                print(f"Added {len(next_day_predictions_df)} next day predictions to extended forecast.")

            # 3. Get the day after next's predictions (to complete 48h or more if needed)
            day_after_next_start_dt = (current_time + timedelta(days=2)).replace(hour=0)
            day_after_next_predictions_df = self.predict_next_day_enhanced(day_after_next_start_dt)
            if day_after_next_predictions_df is None or day_after_next_predictions_df.empty:
                print("Warning: Day after next predictions are empty for extended rolling forecast.")
            else:
                # Calculate how many more hours are needed to reach 48 total from current_time
                combined_df_temp = pd.DataFrame(final_predictions_list)
                hours_needed = 48 - len(combined_df_temp)
                
                if hours_needed > 0:
                    day_after_next_predictions_df_filtered = day_after_next_predictions_df.head(hours_needed)
                    final_predictions_list.extend(day_after_next_predictions_df_filtered.to_dict(orient='records'))
                    print(f"Added {len(day_after_next_predictions_df_filtered)} day after next predictions to extended forecast.")
            
            # Ensure unique times and correct order
            final_df = pd.DataFrame(final_predictions_list)
            if final_df.empty:
                return pd.DataFrame()
            
            final_df['Time_dt'] = pd.to_datetime(final_df['Time'])
            final_df = final_df.sort_values(by='Time_dt').drop_duplicates(subset='Time', keep='first').reset_index(drop=True)
            
            # Re-convert Time_dt to string for final output
            final_df['Time'] = final_df['Time_dt'].dt.strftime('%Y-%m-%dT%H:00')
            final_df = final_df.drop(columns=['Time_dt']) # Drop temporary datetime column

            print(f"Generated extended rolling predictions with {len(final_df)} data points.")
            return final_df

        except Exception as e:
            print(f"Error in get_extended_rolling_predictions: {str(e)}")
            traceback.print_exc()
            return pd.DataFrame()
    
    def predict_for_target_date(self):
        """
        Generates 24-hour predictions for the target date, integrating actuals up to the current hour.
        It uses Prophet to predict hourly increases, which are then cumulatively summed.
        The overall daily total is guided by `self.network_prediction`.
        """
        try:
            pred_range = pd.date_range(
                start=self.target_date.replace(hour=0),
                periods=24,
                freq='h'
            )
            
            future_df = pd.DataFrame({'ds': pred_range})

            # Get raw Prophet predictions for HOURLY INCREASES
            forecast_increases = self.prophet_results.predict(future_df)
            base_hourly_increases = forecast_increases['yhat']
            
            # Ensure hourly increases are non-negative and 0 at midnight
            base_hourly_increases = np.maximum(0, base_hourly_increases)
            if len(base_hourly_increases) > 0:
                base_hourly_increases.iloc[0] = 0 # Midnight increase is zero

            # Calculate Prophet's base daily total (sum of its predicted increases for a full day)
            base_prophet_daily_total = np.sum(base_hourly_increases)
            
            # Get actual data for the current day to blend with predictions
            same_day_data = self.df[self.df['Time'].dt.date == self.target_date.date()].copy()
            last_known_hour = same_day_data['Hour'].max() if not same_day_data.empty else -1
            
            # Calculate overall scaling factor for the day based on network prediction (IPTNW)
            # This adjusts Prophet's natural daily total to align with the external target.
            scaling_factor_for_day = 1.0
            if base_prophet_daily_total > 0:
                scaling_factor_for_day = self.network_prediction / base_prophet_daily_total
            
            adjusted_predictions_cumulative = []
            cumulative_val_from_start = 0

            # Build the cumulative prediction hour by hour
            for hour in range(24):
                if hour <= last_known_hour:
                    # For hours where actual data is available, use the actual values
                    actual_value_row = same_day_data[same_day_data['Hour'] == hour]
                    if not actual_value_row.empty:
                        current_pred = actual_value_row['Workable'].iloc[0]
                    else:
                        # Fallback to previous cumulative if actual data is missing (should ideally be pre-filled)
                        current_pred = cumulative_val_from_start
                    cumulative_val_from_start = current_pred
                else:
                    # For future hours, calculate the predicted increase for this hour from Prophet
                    prophet_inc_for_hour = base_hourly_increases.iloc[hour]
                    
                    # Apply the calculated daily scaling factor to the hourly increase
                    hourly_increase_current_step = prophet_inc_for_hour * scaling_factor_for_day

                    # Add this scaled increase to the cumulative total
                    cumulative_val_from_start += hourly_increase_current_step
                    current_pred = cumulative_val_from_start # This is the new cumulative prediction
                
                adjusted_predictions_cumulative.append(np.round(max(0, current_pred))) # Ensure non-negative and round
            
            results_df = pd.DataFrame({
                'Time': pred_range,
                'Predicted_Workable': adjusted_predictions_cumulative
            })
            
            results_df['Time'] = results_df['Time'].dt.strftime('%Y-%m-%dT%H:00')
            
            print("\nPrediction Weighting Information (Prophet Based, Current Day):")
            print(f"Daily Total Scaled from Prophet (Base Sum: {base_prophet_daily_total:,.0f}) to Network Prediction ({self.network_prediction:,.0f})")
            print(f"Overall Daily Scaling Factor Applied: {scaling_factor_for_day:.3f}")
            print(f"Last known hour from same-day data: {last_known_hour}")
            
            return results_df
            
        except Exception as e:
            print(f"Error in predict_for_target_date (Prophet): {str(e)}")
            traceback.print_exc()
            return pd.DataFrame()
    
    def predict_without_same_day_influence(self, target_date):
        """
        Generates 24-hour predictions for a target date without incorporating same-day actuals.
        This provides a pure Prophet+Network-guided forecast, primarily for comparison.
        Predicts hourly increases, then cumulates them, and applies network scaling.
        """
        try:
            pred_range = pd.date_range(
                start=target_date.replace(hour=0),
                periods=24,
                freq='h'
            )
            
            future_df = pd.DataFrame({'ds': pred_range})

            # Get raw Prophet predictions for HOURLY INCREASES
            forecast_increases = self.prophet_results.predict(future_df)
            base_hourly_increases = forecast_increases['yhat']
            
            # Ensure hourly increases are non-negative and 0 at midnight
            base_hourly_increases = np.maximum(0, base_hourly_increases)
            if len(base_hourly_increases) > 0:
                base_hourly_increases.iloc[0] = 0 # Midnight increase is zero
            
            # Calculate Prophet's base daily total (sum of its predicted increases)
            base_prophet_daily_total = np.sum(base_hourly_increases)

            # Calculate overall scaling factor for the day based on network prediction
            scaling_factor_for_day = 1.0
            if base_prophet_daily_total > 0:
                scaling_factor_for_day = self.network_prediction / base_prophet_daily_total
            
            adjusted_predictions_cumulative = []
            cumulative_val_from_start = 0
            
            for hour in range(24):
                # Get scaled hourly increase
                hourly_increase_current_step = base_hourly_increases.iloc[hour] * scaling_factor_for_day

                # Midnight reset and start cumulative sum for the day
                if hour == 0:
                    hourly_increase_current_step = 0
                    cumulative_val_from_start = 0 
                
                # Add scaled hourly increase to cumulative total
                cumulative_val_from_start += hourly_increase_current_step
                
                adjusted_predictions_cumulative.append(np.round(max(0, cumulative_val_from_start))) # Ensure non-negative and round
            
            results_df = pd.DataFrame({
                'Time': pred_range,
                'Predicted_Workable_No_Same_Day': adjusted_predictions_cumulative
            })
            
            results_df['Time'] = results_df['Time'].dt.strftime('%Y-%m-%dT%H:00')
            
            print("\nPrediction Weighting Information (Prophet Based, No Same Day Influence):")
            print(f"Daily Total Scaled from Prophet (Base Sum: {base_prophet_daily_total:,.0f}) to Network Prediction ({self.network_prediction:,.0f})")
            print(f"Overall Daily Scaling Factor Applied: {scaling_factor_for_day:.3f}")

            return results_df
            
        except Exception as e:
            print(f"Error in predict_without_same_day_influence (Prophet): {str(e)}")
            traceback.print_exc()
            return pd.DataFrame()
    
    def get_alps_data(self):
        """
        Retrieves ALPS data from S3, filters for 'SMF1', and extracts the latest Day/Night shift values.
        """
        try:
            alps_obj = self.s3.get_object(
                Bucket=self.bucket_name,
                Key='userloads/alps.csv'
            )
            alps_data = alps_obj['Body'].read().decode('utf-8')
            alps_df = pd.read_csv(io.StringIO(alps_data))
            
            print("\nOriginal ALPS columns:", alps_df.columns.tolist())
            print("Sample ALPS data:\n", alps_df[['Site', 'Shift', 'Value', 'Date']].head())
            
            alps_df = alps_df[alps_df['Site'] == 'SMF1'].copy() # Use .copy() to avoid SettingWithCopyWarning
            alps_df['Date'] = pd.to_datetime(alps_df['Date'])
            latest_date = alps_df['Date'].max()
            alps_df = alps_df[alps_df['Date'] == latest_date].copy() # Use .copy()
            
            day_shift_value = alps_df[alps_df['Shift'] == 'DAY_SHIFT']['Value'].iloc[0] if not alps_df[alps_df['Shift'] == 'DAY_SHIFT'].empty else 0
            night_shift_value = alps_df[alps_df['Shift'] == 'NIGHT_SHIFT']['Value'].iloc[0] if not alps_df[alps_df['Shift'] == 'NIGHT_SHIFT'].empty else 0
            
            formatted_alps = pd.DataFrame({
                'DAY_SHIFT': [day_shift_value],
                'NIGHT_SHIFT': [night_shift_value]
            })
            
            print("\nALPS Data Retrieved:")
            print(f"Date: {latest_date.strftime('%Y-%m-%d')}")
            print(f"Day Shift: {day_shift_value:,.2f}")
            print(f"Night Shift: {night_shift_value:,.2f}")
            
            return formatted_alps
            
        except Exception as e:
            print(f"Error retrieving ALPS data: {str(e)}")
            traceback.print_exc()
            return None
    
    def get_ledger_information(self):
        """
        Retrieves and formats ledger information (various metrics) from granular.csv.
        """
        try:
            print("Fetching granular.csv from S3 for ledger information...")
            granular_obj = self.s3.get_object(
                Bucket=self.bucket_name,
                Key='userloads/granular.csv'
            )
            granular_data = granular_obj['Body'].read().decode('utf-8')
            df_granular = pd.read_csv(io.StringIO(granular_data)) # Renamed to avoid conflict with self.df
            print("Successfully loaded granular.csv from S3")

            current_hour = datetime.now().hour
            formatted_times = [f"{hour:02d}:00" for hour in range(current_hour + 1)]

            metrics = {}
            for column in df_granular.columns:
                if column != 'Hour':
                    values = []
                    for v in df_granular[column].values:
                        try:
                            if pd.notna(v) and str(v).strip():
                                num = float(v)
                                values.append(int(num) if num.is_integer() else round(num, 2))
                            else:
                                values.append(0)
                        except (ValueError, TypeError):
                            values.append(0)
                    metrics[column] = values[:current_hour + 1]

            ledger_info = {
                "timePoints": formatted_times,
                "metrics": metrics
            }

            print(f"Ledger info created with {len(formatted_times)} time points")
            print(f"Metrics included: {list(metrics.keys())}")

            return ledger_info

        except Exception as e:
            print(f"Error processing ledger information: {str(e)}")
            traceback.print_exc()
            return None
    
    def _get_historical_hourly_shapes(self, days_to_analyze=21):
        """
        Calculates the average hourly increase shape for each day of the week
        over a specified historical window.
        Returns a dictionary: {day_of_week: {hour: avg_increase}}.
        """
        print(f"  [Shape Analysis Debug] Calculating historical hourly shapes for last {days_to_analyze} days.")
        if not hasattr(self, 'df') or self.df.empty:
            print("  [Shape Analysis Debug] Historical dataframe not available for shape analysis.")
            return {}

        end_date = self.target_date - timedelta(days=1)
        start_date = end_date - timedelta(days=days_to_analyze - 1)

        hist_df_window = self.df[
            (self.df['Time'].dt.normalize() >= pd.to_datetime(start_date)) &
            (self.df['Time'].dt.normalize() <= pd.to_datetime(end_date))
        ].copy()

        if hist_df_window.empty:
            print("  [Shape Analysis Debug] No historical data in window for shape analysis.")
            return {}

        # Ensure Hourly_Increase is present and correctly calculated for this window
        def calculate_daily_increase_for_shape(group):
            group = group.sort_values(by='Time')
            increases = group['Workable'].diff()
            increases.iloc[0] = 0 # Reset for start of each day
            return increases

        # Re-calculate Hourly_Increase for the historical window to ensure consistency
        hist_df_window['Hourly_Increase'] = hist_df_window.groupby(hist_df_window['Time'].dt.date).apply(calculate_daily_increase_for_shape).reset_index(level=0, drop=True).copy()
        hist_df_window['Hourly_Increase'] = np.maximum(0, hist_df_window['Hourly_Increase'])
        
        hist_df_window['DayName'] = hist_df_window['Time'].dt.day_name()
        hist_df_window['Hour'] = hist_df_window['Time'].dt.hour

        hourly_shapes = {}
        for day_name in hist_df_window['DayName'].unique():
            hourly_shapes[day_name] = {}
            for hour in range(24):
                avg_increase = hist_df_window[
                    (hist_df_window['DayName'] == day_name) & 
                    (hist_df_window['Hour'] == hour)
                ]['Hourly_Increase'].mean()
                hourly_shapes[day_name][hour] = round(avg_increase, 2) if pd.notna(avg_increase) else 0

        print(f"  [Shape Analysis Debug] Historical hourly shapes computed. Sample (Monday, hour 6): {hourly_shapes.get('Monday', {}).get(6, 'N/A')}")
        return hourly_shapes

    def get_network_scaling_factors(self):
        """
        Calculates network and ALPS-based scaling factors.
        These factors indicate the general trend and will be used as 'guidance'
        for Prophet's total daily volume, not strict scaling.
        """
        try:
            alps_df = self.get_alps_data()
            
            granular_obj = self.s3.get_object(
                Bucket=self.bucket_name,
                Key='userloads/granular.csv'
            )
            granular_data = granular_obj['Body'].read().decode('utf-8')
            granular_df = pd.read_csv(io.StringIO(granular_data))
    
            current_ipt = float(granular_df['IPTNW'].iloc[0])
            three_day_ipt = float(granular_df['IPT3DAY'].iloc[0])
            
            if alps_df is None:
                return self.get_fallback_scaling_factors() # Use fallback if ALPS data is missing

            current_alps_total = float(alps_df['DAY_SHIFT'].iloc[0] + alps_df['NIGHT_SHIFT'].iloc[0])
            
            ipt_trend = three_day_ipt / current_ipt if current_ipt != 0 else 1.0
            alps_trend = current_alps_total / current_ipt if current_ipt != 0 else 1.0
            
            # Weighted trend for overall guidance (ALPS and IPT given higher weight)
            weighted_trend = (ipt_trend * 0.35) + (alps_trend * 0.35) + (1.0 * 0.3)
            
            # Less aggressive adjustment for base and next-day scaling
            # These are general indicators of the overall trend relative to current_ipt,
            # not direct scaling factors for Prophet's output anymore.
            if weighted_trend > 1:
                base_scaling = 1 + (weighted_trend - 1) * 0.5 # Softer increase factor
                next_day_scaling = min(1.3, base_scaling) # Cap the upper limit
            else:
                base_scaling = 1 - (1 - weighted_trend) * 0.5 # Softer decrease factor
                next_day_scaling = max(0.8, base_scaling) # Floor the lower limit

            # Hourly factors defining daily distribution shape (retained for structure)
            hourly_factors = {}
            for hour in range(24):
                if hour < 6: tod_factor = 0.99
                elif 6 <= hour < 12: tod_factor = 1.05
                elif 12 <= hour < 18: tod_factor = 1.03
                else: tod_factor = 1.03
                hourly_factors[hour] = next_day_scaling * tod_factor # Keeping original calculation for dict structure
    
            avg_scaling = sum(hourly_factors.values()) / 24
    
            print("\nScaling Factor Analysis (Guidance Factors):")
            print(f"IPT Trend: {ipt_trend:.3f}")
            print(f"ALPS Trend: {alps_trend:.3f}")
            print(f"Weighted Trend (overall guidance): {weighted_trend:.3f}")
            print(f"Base Scaling (guidance for current day total): {base_scaling:.3f}")
            print(f"Next Day Scaling (guidance for next day total): {next_day_scaling:.3f}")
            print(f"Average Hourly Factor (from internal distribution profile): {avg_scaling:.3f}")
    
            return {
                'base_scaling': base_scaling, 
                'next_day_scaling': next_day_scaling, 
                'hourly_factors': hourly_factors, # These might be used for other purposes or fine-tuning Prophet as regressors
                'metrics': {
                    'current_ipt': current_ipt,
                    'three_day_ipt': three_day_ipt,
                    'alps_total': current_alps_total,
                    'trend_factor': weighted_trend,
                    'avg_scaling': avg_scaling
                }
            }
    
        except Exception as e:
            print(f"Error in get_network_scaling_factors: {str(e)}")
            traceback.print_exc()
            return None
    
    def calculate_historical_trends(self, days_prior=45, num_weeks_for_avg=6, short_term_ma_occurrences=3):
        """
        Calculates historical trends based on data from the last 'days_prior' days.
        Focuses on 3-hour blocks and daily summaries, including moving averages,
        percentage changes for trend direction, and last occurrence totals.
        """
        print(f"\nCalculating historical trends for the last {days_prior} days (long-term avg: {num_weeks_for_avg} occurrences, short-term MA: {short_term_ma_occurrences} occurrences)...")
        if not hasattr(self, 'df') or self.df.empty:
            print("Historical dataframe not available. Skipping trend calculation.")
            return {
                "reference_date_for_trends": self.target_date.strftime("%Y-%m-%d") if hasattr(self, 'target_date') else "N/A",
                "trend_period_days": days_prior,
                "message": "Historical dataframe not available or empty."
            }

        try:
            # Ensure Time and Workable columns are correctly typed
            if not pd.api.types.is_datetime64_any_dtype(self.df['Time']):
                self.df['Time'] = pd.to_datetime(self.df['Time'])
            if not pd.api.types.is_numeric_dtype(self.df['Workable']):
                self.df['Workable'] = pd.to_numeric(self.df['Workable'], errors='coerce').fillna(0)

            # Define the date range for trend calculation
            end_date_for_trends = self.target_date - timedelta(days=1) 
            start_date_for_trends = end_date_for_trends - timedelta(days=days_prior - 1)

            print(f"Historical trend calculation range: {start_date_for_trends.strftime('%Y-%m-%d')} to {end_date_for_trends.strftime('%Y-%m-%d')}")

            # Filter historical data within the specified range
            hist_df = self.df[
                (self.df['Time'].dt.normalize() >= pd.to_datetime(start_date_for_trends)) &
                (self.df['Time'].dt.normalize() <= pd.to_datetime(end_date_for_trends))
            ].copy()

            if hist_df.empty:
                print(f"No historical data found in the last {days_prior} days. Skipping trend calculation.")
                return {
                    "reference_date_for_trends": self.target_date.strftime("%Y-%m-%d"),
                    "trend_period_days": days_prior,
                    "message": "No historical data in the specified period."
                }

            # Add time-based features for grouping
            hist_df['DayName'] = hist_df['Time'].dt.day_name()
            hist_df['DateOnly'] = hist_df['Time'].dt.date
            if 'Hour' not in hist_df.columns:
                 hist_df['Hour'] = hist_df['Time'].dt.hour

            # Define 3-hour blocks for granular trend analysis
            three_hour_blocks = [
                (0, 3, "0000_0300"), (3, 6, "0300_0600"), (6, 9, "0600_0900"),
                (9, 12, "0900_1200"), (12, 15, "1200_1500"), (15, 18, "1500_1800"),
                (18, 21, "1800_2100"), (21, 24, "2100_0000")
            ]

            block_trends = {}
            daily_summary = defaultdict(lambda: {}) 

            all_block_data_points = []
            # Process each day's data to calculate volume within 3-hour blocks
            for date_only_val in sorted(hist_df['DateOnly'].unique()):
                day_df = hist_df[hist_df['DateOnly'] == date_only_val].copy()
                day_df = day_df.set_index('Time').sort_index()
                
                for start_hour, end_hour, block_label_suffix in three_hour_blocks:
                    # Determine start and end cumulative volume for each block
                    vol_at_block_start = 0
                    if start_hour > 0:
                        start_lookup_timestamp = pd.Timestamp(date_only_val).replace(hour=start_hour - 1, minute=59, second=59, microsecond=999999)
                        temp_df_start = day_df.loc[day_df.index <= start_lookup_timestamp]
                        if not temp_df_start.empty:
                            vol_at_block_start = temp_df_start['Workable'].iloc[-1]
                    
                    actual_end_hour_for_lookup = end_hour - 1
                    if end_hour == 24: actual_end_hour_for_lookup = 23
                    
                    end_lookup_timestamp = pd.Timestamp(date_only_val).replace(hour=actual_end_hour_for_lookup, minute=59, second=59, microsecond=999999)
                    temp_df_end = day_df.loc[day_df.index <= end_lookup_timestamp]
                    vol_at_block_end = vol_at_block_start 
                    if not temp_df_end.empty:
                        vol_at_block_end = temp_df_end['Workable'].iloc[-1]
                                        
                    block_volume = max(0, vol_at_block_end - vol_at_block_start)
                    
                    all_block_data_points.append({
                        'DayName': pd.Timestamp(date_only_val).day_name(),
                        'BlockLabelSuffix': block_label_suffix,
                        'DateOnly': date_only_val,
                        'BlockVolume': block_volume
                    })
    
            block_data_df = pd.DataFrame(all_block_data_points)
            days_of_week_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    
            # Calculate trends for each 3-hour block
            if not block_data_df.empty:
                for day_name in days_of_week_order:
                    for _, _, block_label_suffix in three_hour_blocks:
                        block_key = f"{day_name}_{block_label_suffix}"
                        specific_block_occurrences = block_data_df[
                            (block_data_df['DayName'] == day_name) &
                            (block_data_df['BlockLabelSuffix'] == block_label_suffix)
                        ].sort_values(by='DateOnly', ascending=False)
    
                        avg_vol_long, avg_vol_short, last_occurrence_vol, trend_pct_change = 0, 0, 0, 0.0
                        if not specific_block_occurrences.empty:
                            long_term_avg_occurrences = specific_block_occurrences.head(num_weeks_for_avg)
                            short_term_avg_occurrences = specific_block_occurrences.head(short_term_ma_occurrences)
                            avg_vol_long = long_term_avg_occurrences['BlockVolume'].mean() if not long_term_avg_occurrences.empty else 0
                            avg_vol_short = short_term_avg_occurrences['BlockVolume'].mean() if not short_term_avg_occurrences.empty else 0
                            last_occurrence_vol = specific_block_occurrences.iloc[0]['BlockVolume']
                            if avg_vol_long != 0: trend_pct_change = ((avg_vol_short - avg_vol_long) / avg_vol_long) * 100
                            elif avg_vol_short > 0 : trend_pct_change = 9999.0 # Indicate significant positive change from zero
                        
                        block_trends[block_key] = {
                            f"avg_volume_last_{num_weeks_for_avg}_occurrences": round(avg_vol_long, 2),
                            f"avg_volume_last_{short_term_ma_occurrences}_occurrences": round(avg_vol_short, 2),
                            "last_occurrence_volume": round(last_occurrence_vol, 2),
                            "trend_direction_pct_change": round(trend_pct_change, 2)
                        }
            else: # Initialize with zeros if no block data found
                for day_name_iter in days_of_week_order:
                     for _, _, block_label_suffix_iter in three_hour_blocks:
                        block_key_iter = f"{day_name_iter}_{block_label_suffix_iter}"
                        block_trends[block_key_iter] = {
                            f"avg_volume_last_{num_weeks_for_avg}_occurrences": 0,
                            f"avg_volume_last_{short_term_ma_occurrences}_occurrences": 0,
                            "last_occurrence_volume": 0,
                            "trend_direction_pct_change": 0.0
                        }
    
            # Calculate daily total trends
            daily_totals_df = pd.DataFrame()
            if not block_data_df.empty:
                daily_totals_df = block_data_df.groupby(['DateOnly', 'DayName'])['BlockVolume'].sum().reset_index()
            
            for day_name_iter in days_of_week_order:
                daily_summary[day_name_iter] = {
                    f"avg_total_daily_volume_last_{num_weeks_for_avg}_occurrences": 0,
                    f"avg_total_daily_volume_last_{short_term_ma_occurrences}_occurrences": 0,
                    "last_occurrence_total_daily_volume": 0,
                    "trend_direction_pct_change": 0.0
                }
    
            if not daily_totals_df.empty:
                for day_name in daily_totals_df['DayName'].unique():
                    day_specific_totals = daily_totals_df[daily_totals_df['DayName'] == day_name].sort_values(by='DateOnly', ascending=False)
                    if not day_specific_totals.empty:
                        long_term_daily_totals = day_specific_totals.head(num_weeks_for_avg)
                        short_term_daily_totals = day_specific_totals.head(short_term_ma_occurrences)
                        
                        avg_daily_total_long = long_term_daily_totals['BlockVolume'].mean() if not long_term_daily_totals.empty else 0
                        avg_daily_total_short = short_term_daily_totals['BlockVolume'].mean() if not short_term_daily_totals.empty else 0
                        last_occurrence_daily_total = day_specific_totals.iloc[0]['BlockVolume']
    
                        daily_trend_pct_change = 0.0
                        if avg_daily_total_long != 0:
                            daily_trend_pct_change = ((avg_daily_total_short - avg_daily_total_long) / avg_daily_total_long) * 100
                        elif avg_daily_total_short > 0:
                            daily_trend_pct_change = 9999.0
                        
                        daily_summary[day_name].update({
                            f"avg_total_daily_volume_last_{num_weeks_for_avg}_occurrences": round(avg_daily_total_long, 2),
                            f"avg_total_daily_volume_last_{short_term_ma_occurrences}_occurrences": round(avg_daily_total_short, 2),
                            "last_occurrence_total_daily_volume": round(last_occurrence_daily_total, 2),
                            "trend_direction_pct_change": round(daily_trend_pct_change, 2)
                        })
                
                # Calculate overall average daily volumes
                overall_avg_daily_45_days = daily_totals_df['BlockVolume'].mean() if not daily_totals_df.empty else 0
                avg_daily_volume_rolling_7_days = 0
                if not daily_totals_df.empty:
                    daily_totals_sorted_for_rolling = daily_totals_df.sort_values(by='DateOnly')
                    if len(daily_totals_sorted_for_rolling) >= 1:
                        avg_daily_volume_rolling_7_days = daily_totals_sorted_for_rolling['BlockVolume'].rolling(window=7, min_periods=1).mean().iloc[-1]
                    else: 
                         avg_daily_volume_rolling_7_days = 0
    
                print("Historical trend calculation completed.")
                return {
                    "reference_date_for_trends": self.target_date.strftime("%Y-%m-%d"),
                    "trend_period_days": days_prior,
                    "num_weeks_for_avg": num_weeks_for_avg,
                    "short_term_ma_occurrences": short_term_ma_occurrences,
                    "three_hour_block_trends": block_trends,
                    "daily_summary_trends": dict(daily_summary),
                    "overall_summary": {
                        f"avg_daily_volume_last_{days_prior}_days": round(overall_avg_daily_45_days, 2),
                        "avg_daily_volume_rolling_7_days": round(avg_daily_volume_rolling_7_days, 2) 
                    }
                }
    
        except Exception as e:
            print(f"Error in calculate_historical_trends: {str(e)}")
            traceback.print_exc()
            return {
                "reference_date_for_trends": self.target_date.strftime("%Y-%m-%d") if hasattr(self, 'target_date') else "N/A",
                "trend_period_days": days_prior,
                "error_message": str(e)
            }
    
    def log_scaling_metrics(self, scaling_data):
        """Logs the calculated scaling metrics for debugging and analysis."""
        try:
            metrics = scaling_data['metrics']
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            log_entry = {
                'timestamp': timestamp,
                'weighted_trend': metrics.get('weighted_trend', 0),
                'avg_scaling': metrics.get('avg_scaling', 0),
                'base_scaling': scaling_data['base_scaling'],
                'next_day_scaling': scaling_data['next_day_scaling']
            }
            
            print("\nScaling Metrics Log:")
            for key, value in log_entry.items():
                print(f"{key}: {value}")
                
        except Exception as e:
            print(f"Error logging scaling metrics: {str(e)}")
    
    def get_previous_year_data(self, date):
        """Retrieves 'Workable' data for the same day in the previous year."""
        try:
            prev_year_data = self.df[
                (self.df['Time'].dt.month == date.month) & 
                (self.df['Time'].dt.day == date.day) &
                (self.df['Time'].dt.year == date.year)
            ].copy() # Use .copy() to avoid SettingWithCopyWarning
            
            if not prev_year_data.empty:
                return [{
                    "Time": row['Time'].strftime('%Y-%m-%dT%H:00'),
                    "Workable": float(row['Workable'])
                } for _, row in prev_year_data.iterrows()]
            return []
        except Exception as e:
            print(f"Error getting previous year data: {str(e)}")
            return []
    
    def get_current_day_data(self, current_time):
        """Retrieves actual 'Workable' data for the current day up to the current hour."""
        try:
            current_date = current_time.date()
            current_day_data = self.df[
                self.df['Time'].dt.date == current_date
            ].copy() # Use .copy()
            
            # Filter to include only data up to the current hour
            current_day_data = current_day_data[current_day_data['Time'] <= current_time]

            if not current_day_data.empty:
                return [{
                    "Time": row['Time'].strftime('%Y-%m-%dT%H:00'),
                    "Workable": float(row['Workable'])
                } for _, row in current_day_data.iterrows()]
            return []
        except Exception as e:
            print(f"Error getting current day data: {str(e)}")
            return []
    
    def predict_next_day_enhanced(self, target_date):
        """
        Generates 24-hour predictions for the next day, using Prophet's hourly increases
        and then cumulatively summing them. The total daily volume is guided by
        `self.network_prediction` (IPTNW) using a softer blending approach.
        """
        try:
            # Get overall scaling guidance factors
            scaling_data = self.get_network_scaling_factors()
            if not scaling_data:
                print("Warning: No scaling data available for next day enhanced prediction. Using default scaling.")
                # Provide a default minimal scaling_data structure if None
                scaling_data = {'next_day_scaling': 1.0, 'metrics': {'current_ipt': self.network_prediction, 'trend_factor': 1.0}}
    
            # Create a 24-hour prediction range for the target_date
            pred_range = pd.date_range(
                start=target_date.replace(hour=0),
                periods=24,
                freq='h'
            )
            future_df = pd.DataFrame({'ds': pred_range})

            # Get raw Prophet predictions for HOURLY INCREASES
            forecast_increases = self.prophet_results.predict(future_df)
            base_hourly_increases = forecast_increases['yhat']
            
            # Ensure hourly increases are non-negative and 0 at midnight
            base_hourly_increases = np.maximum(0, base_hourly_increases)
            if len(base_hourly_increases) > 0:
                base_hourly_increases.iloc[0] = 0 # Midnight increase is zero, always reset to zero

            # Calculate Prophet's natural total daily volume for this next day
            prophet_base_daily_total = np.sum(base_hourly_increases)

            # Apply a softer blending between Prophet's natural total and the external target
            # This allows Prophet's shape to dominate while still being guided by the target.
            # Blend Prophet's natural daily total with the network target (IPTNW)
            blend_weight_prophet = 0.7 # Give more weight to Prophet's natural forecast
            blend_weight_network = 0.3 # Less aggressive guidance from network prediction

            # Calculate the final target for the day by blending IPTNW and Prophet's total
            final_daily_target = (prophet_base_daily_total * blend_weight_prophet) + \
                                    (self.network_prediction * scaling_data['next_day_scaling'] * blend_weight_network)

            # Recalculate scaling factor to adjust Prophet's hourly increases to meet the `final_daily_target`
            scaling_factor_for_day = 1.0
            if prophet_base_daily_total > 0:
                scaling_factor_for_day = final_daily_target / prophet_base_daily_total
            
            final_predictions_cumulative = []
            cumulative_val_from_start = 0
            
            for hour in range(24):
                hourly_increase_current_step = base_hourly_increases.iloc[hour] * scaling_factor_for_day
                
                # Midnight reset and start cumulative sum for the day
                if hour == 0:
                    hourly_increase_current_step = 0
                    cumulative_val_from_start = 0 
                
                cumulative_val_from_start += hourly_increase_current_step
                
                final_predictions_cumulative.append(np.round(max(0, cumulative_val_from_start)))
    
            results_df = pd.DataFrame({
                'Time': pred_range.strftime('%Y-%m-%dT%H:00'),
                'Predicted_Workable': final_predictions_cumulative
            })
    
            print("\nNetwork Prediction Metrics for Next Day Enhanced (Prophet + Soft Blend):")
            print(f"Target Date: {target_date.strftime('%Y-%m-%d')}")
            print(f"Prophet Base Daily Total: {prophet_base_daily_total:,.0f}")
            print(f"Network Prediction Target (IPTNW): {self.network_prediction:,.0f}")
            print(f"External Next Day Scaling Factor: {scaling_data['next_day_scaling']:.3f}")
            print(f"Blended Final Daily Target: {final_daily_target:,.0f}")
            print(f"Final Scaling Factor Applied to Hourly Increases: {scaling_factor_for_day:.3f}")
    
            return results_df
    
        except Exception as e:
            print(f"Error in enhanced next-day prediction (Prophet): {str(e)}")
            traceback.print_exc()
            return pd.DataFrame()
     
     

def check_aws_credentials():
    """Checks if AWS credentials are configured by attempting to get caller identity."""
    try:
        sts = boto3.client('sts')
        account = sts.get_caller_identity()
        print("AWS Credentials are configured correctly!")
        print(f"Account ID: {account['Account']}")
        print(f"User/Role ARN: {account['Arn']}")
        return True
    except Exception as e:
        print("AWS Credentials check failed:", str(e))
        return False

def setup_s3_bucket():
    """
    Ensures the S3 bucket exists and is configured for versioning.
    Sets the global S3_BUCKET_NAME.
    """
    try:
        s3 = boto3.client('s3', region_name='us-west-1')
        bucket_name = 'ledger-prediction-charting-008971633421'
        
        print(f"Attempting to create/access bucket: {bucket_name}")
        
        try: # Check if bucket exists
            s3.head_bucket(Bucket=bucket_name)
            print(f"Bucket {bucket_name} already exists")
        except: # Create bucket if it doesn't exist
            print(f"Creating bucket {bucket_name}...")
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': 'us-west-1'}
            )
            print("Bucket created, setting up configurations...")
            # Enable versioning
            s3.put_bucket_versioning(
                Bucket=bucket_name,
                VersioningConfiguration={'Status': 'Enabled'}
            )
            print(f"Bucket {bucket_name} created and configured successfully!")
        
        global S3_BUCKET_NAME
        S3_BUCKET_NAME = bucket_name
        
        return True
    except Exception as e:
        print(f"Error setting up S3 bucket: {str(e)}")
        print(f"Full error details: {str(type(e).__name__)}: {str(e)}")
        return False


def check_aws_region():
    """Checks the currently configured AWS region."""
    try:
        session = boto3.Session()
        current_region = session.region_name
        print(f"Current AWS region: {current_region}")
        return current_region
    except Exception as e:
        print(f"Error checking AWS region: {str(e)}")
        return None


def upload_to_s3(local_file, bucket, s3_file):
    """Uploads a specified local file to an S3 bucket."""
    s3_client = boto3.client('s3', region_name='us-west-1')
    try:
        print(f"Attempting to upload {local_file} to {bucket}/{s3_file}")
        s3_client.upload_file(local_file, bucket, s3_file)
        url = f"https://{bucket}.s3.us-west-1.amazonaws.com/{s3_file}"
        print(f"File uploaded successfully to: {url}")
        return True
    except FileNotFoundError:
        print(f"The file {local_file} was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False
    except Exception as e:
        print(f"An error occurred during upload: {str(e)}")
        return False

def main():
    """
    Main function to run the Charge Predictor application.
    Initializes the predictor, generates all forecasts, and saves the output JSON.
    """
    check_environment()
    try:
        # AWS setup and credential checks
        if not check_aws_credentials():
            raise Exception("AWS credentials not properly configured")
        region = check_aws_region()
        if not region:
            raise Exception("AWS region not properly configured")
        if not setup_s3_bucket():
            raise Exception("Failed to setup S3 bucket")
        
        predictor = ChargePredictor()
        
        # Define current_time early, as it's used for next_day_target_date
        current_time = datetime.now().replace(minute=0, second=0, microsecond=0)

        # Generate all required predictions
        print("\nGenerating Prophet predictions for current day (full 24h)...") 
        prophet_results_current_day_full = predictor.predict_for_target_date() 
        
        # Calculate next day's target date for full 24h predictions
        next_day_target_date = (current_time + timedelta(days=1)).replace(hour=0, minute=0, second=0)
        print(f"\nGenerating Prophet predictions for next day (full 24h): {next_day_target_date.date()}")
        next_day_prophet_enhanced_full = predictor.predict_next_day_enhanced(next_day_target_date)
        
        print("\nGenerating extended 48-hour rolling predictions (from current hour)...")
        extended_rolling_predictions_df = predictor.get_extended_rolling_predictions()
        
        print("\nGetting ledger information...")
        ledger_info = predictor.get_ledger_information()
        
        # Fetch historical data for previous year comparisons
        prev_year_date = current_time - pd.DateOffset(years=1)
        next_day_prev_year_date = next_day_target_date - pd.DateOffset(years=1) 
        prev_year_records = predictor.get_previous_year_data(prev_year_date)
        next_day_prev_year_records = predictor.get_previous_year_data(next_day_prev_year_date)
        
        # Get actual data for the current day up to now
        current_day_records = predictor.get_current_day_data(current_time=datetime.combine(predictor.target_date.date(), datetime.now().time()))

        # Get Prophet predictions without same-day actuals influence (for pure model view)
        no_same_day_current = predictor.predict_without_same_day_influence(predictor.target_date)

        # Calculate historical trend summaries
        historical_trends_data = predictor.calculate_historical_trends(days_prior=45, num_weeks_for_avg=6, short_term_ma_occurrences=3)

        # Get historical hourly shapes for LLM feed
        historical_hourly_shapes_data = predictor._get_historical_hourly_shapes(days_to_analyze=21)


        # Compile all data into the final JSON structure for VIZ.json
        json_data = {
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "current_day": {
                "date": predictor.target_date.strftime("%Y-%m-%d"),
                "network_prediction": predictor.network_prediction,
                "sarima_predictions": prophet_results_current_day_full.to_dict(orient='records') if prophet_results_current_day_full is not None else [],
                "predictions_no_same_day": no_same_day_current.to_dict(orient='records') if no_same_day_current is not None else [],
                "previous_year_data": prev_year_records,
                "current_day_data": current_day_records
            },
            "next_day": {
                "date": next_day_target_date.strftime("%Y-%m-%d"),
                "sarima_predictions": next_day_prophet_enhanced_full.to_dict(orient='records') if next_day_prophet_enhanced_full is not None else [],
                "previous_year_data": next_day_prev_year_records
            },
            "extended_predictions": {
                "predictions": extended_rolling_predictions_df.to_dict(orient='records') if extended_rolling_predictions_df is not None else []
            },
            "Ledger_Information": ledger_info if ledger_info else {},
            "historical_context": historical_trends_data,
            "historical_hourly_shapes": historical_hourly_shapes_data # Added historical hourly shapes here
        }
        
        # Add new, top-level keys for model performance insights and comparison
        json_data["prophet_performance_metrics"] = {
            "current_day_final_prophet_total": prophet_results_current_day_full['Predicted_Workable'].iloc[-1] if prophet_results_current_day_full is not None and not prophet_results_current_day_full.empty else 0,
            "next_day_final_prophet_total": next_day_prophet_enhanced_full['Predicted_Workable'].iloc[-1] if next_day_prophet_enhanced_full is not None and not next_day_prophet_enhanced_full.empty else 0,
            "network_prediction_target": predictor.network_prediction,
            "next_day_expected_increase_from_prophet": (next_day_prophet_enhanced_full['Predicted_Workable'].iloc[-1] - next_day_prophet_enhanced_full['Predicted_Workable'].iloc[0]) if next_day_prophet_enhanced_full is not None and not next_day_prophet_enhanced_full.empty else 0
        }

        # Save and upload the generated VIZ.json
        current_dir = os.path.dirname(os.path.abspath(__file__))
        local_file = os.path.join(current_dir, 'VIZ.json')
        
        print(f"\nWriting to JSON file at: {local_file}")
        try:
            with open(local_file, 'w') as f:
                json.dump(json_data, f, indent=4)
            print("Successfully wrote to VIZ.json")
            
            if S3_BUCKET_NAME: 
                s3_file_name = f"predictions/{datetime.now().strftime('%Y-%m-%d_%H')}/VIZ.json"
                if upload_to_s3(local_file, S3_BUCKET_NAME, s3_file_name):
                    print(f"Successfully uploaded {local_file} to S3 bucket {S3_BUCKET_NAME}")
                else:
                    print(f"Failed to upload {local_file} to S3 bucket {S3_BUCKET_NAME}")
            else:
                print("No valid S3 bucket name available for upload (S3_BUCKET_NAME not set).")
                
        except Exception as e:
            print(f"Error writing/uploading JSON file: {str(e)}")
            traceback.print_exc()
        
    except Exception as e:
        print(f"\nError in main: {str(e)}")
        traceback.print_exc()


if __name__ == "__main__":
    main()


# The following commented-out block is for scheduling the script.
# Uncomment and configure if you wish to run this script automatically at specified times.
'''
def signal_handler(sig, frame):
    """Handles graceful shutdown on Ctrl+C."""
    print(f"\nShutdown initiated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Gracefully shutting down... (This might take a moment)")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler) # Set up signal handler for Ctrl+C
    
    start_time = datetime.now()
    print(f"\nScheduler started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("Setting up daily runs at 08:30, 12:15, 20:30, and 23:15")
    print("\nPress Ctrl+C to exit gracefully")
    
    # Calculate and display time until next run
    now = datetime.now()
    scheduled_times = ["08:30", "12:15", "20:30", "23:15"]
    next_run = None
    
    for time_str in scheduled_times:
        hours, minutes = map(int, time_str.split(':'))
        potential_next = now.replace(hour=hours, minute=minutes, second=0, microsecond=0)
        
        if potential_next <= now:
            potential_next += timedelta(days=1)
            
        if next_run is None or potential_next < next_run:
            next_run = potential_next
    
    wait_time = (next_run - now).total_seconds() / 60
    
    print(f"Next run scheduled for: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Time until next run: {wait_time:.1f} minutes")
    
    # Schedule the tasks
    schedule.every().day.at("08:30").do(run_scheduled_task)
    schedule.every().day.at("12:15").do(run_scheduled_task)
    schedule.every().day.at("20:30").do(run_scheduled_task)
    schedule.every().day.at("23:15").do(run_scheduled_task)
    
    run_count = 0
    
    # Run forever until Ctrl+C
    try:
        while True:
            schedule.run_pending()
            
            # Update run count and display status every 10 minutes
            if datetime.now().minute % 10 == 0 and datetime.now().second == 0:
                current_time = datetime.now()
                uptime = current_time - start_time
                next_job = schedule.next_run()
                time_to_next = (next_job - current_time).total_seconds() / 60 if next_job else 0
                
                print(f"\nStatus Update ({current_time.strftime('%Y-%m-%d %H:%M:%S')}):")
                print(f"Uptime: {uptime}")
                print(f"Runs completed: {run_count}")
                print(f"Next run in: {time_to_next:.1f} minutes")
                print("Press Ctrl+C to exit")
                
            time.sleep(1) # Sleep for 1 second to prevent high CPU usage
            
    except KeyboardInterrupt:
        print(f"\nShutdown initiated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total runtime: {datetime.now() - start_time}")
        print(f"Total runs completed: {run_count}")
        print("Shutting down gracefully...")
        sys.exit(0)
    except Exception as e:
        print(f"\nError in scheduler: {str(e)}")
        traceback.print_exc()
        print("\nPress Ctrl+C to exit")
'''
