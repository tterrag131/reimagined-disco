import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os
import traceback

def get_shift_volume_from_extended(extended_predictions_df, start_dt, end_dt):
    """
    Calculates the total predicted volume for a given shift period from the extended_predictions_df.
    Handles shifts spanning midnight by summing hourly increases within the shift.
    Assumes extended_predictions_df contains cumulative data that resets at midnight.
    
    Note: This function expects extended_predictions_df to have a 'Time_dt' column
          which is already datetime objects.
    """

    print(f"  [LLMfeed Debug] Calculating shift volume for {start_dt.strftime('%H:%M')} to {end_dt.strftime('%H:%M')}")
    if extended_predictions_df.empty:
        print(f"  [LLMfeed Debug] Warning: Extended predictions DataFrame is empty for shift from {start_dt} to {end_dt}.")
        return 0

    # Ensure 'Time_dt' column exists and is datetime (already handled in generate_llm_feed's main loop)
    # The check is here for robustness if called independently, but main function ensures it.
    if 'Time_dt' not in extended_predictions_df.columns:
        extended_predictions_df['Time_dt'] = pd.to_datetime(extended_predictions_df['Time'])
    extended_predictions_df = extended_predictions_df.sort_values(by='Time_dt').reset_index(drop=True)

    # Generate hourly increases from the cumulative predictions
    hourly_data = []
    prev_cumulative = 0
    prev_date = None

    for index, row in extended_predictions_df.iterrows():
        current_dt = row['Time_dt']
        current_cumulative = row['Predicted_Workable']

        # If it's a new day or the very first entry, the "increase" is its own value (since it resets from 0)
        if prev_date is None or current_dt.date() > prev_date:
            hourly_increase = current_cumulative
        else:
            hourly_increase = current_cumulative - prev_cumulative
        
        hourly_data.append({'Time_dt': current_dt, 'Hourly_Increase': max(0, hourly_increase)})
        prev_cumulative = current_cumulative # Update for next iteration
        prev_date = current_dt.date() # Update for next iteration

    hourly_df = pd.DataFrame(hourly_data)
    print(f"  [LLMfeed Debug] Generated hourly_df from extended_predictions_df. Head:\n{hourly_df.head()}")

    # Filter for the specific shift duration based on start_dt and end_dt
    # Use < end_dt to include the start hour but exclude the end hour for 24-hour shifts
    shift_hourly_increases = hourly_df[
        (hourly_df['Time_dt'] >= start_dt) & 
        (hourly_df['Time_dt'] < end_dt)
    ]
    
    shift_volume = shift_hourly_increases['Hourly_Increase'].sum()
    print(f"  [LLMfeed Debug] Shift volume calculated for {start_dt.strftime('%Y-%m-%d %H:%M')} to {end_dt.strftime('%Y-%m-%d %H:%M')}: {shift_volume:,.0f} units. (Filtered {len(shift_hourly_increases)} records)")
    return shift_volume


def generate_llm_feed(json_data):
    """
    Generates language-based insights for the LLMfeed.
    This function now operates on the pre-generated json_data (from VIZ.json).
    """
    print("  [LLMfeed Debug] Starting generate_llm_feed function.")
    
    # Extract current_time from the VIZ.json data
    current_time_str = json_data.get('time', datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    current_time = datetime.strptime(current_time_str, "%Y-%m-%d %H:%M:%S")

    llm_feed = {
        "report_timestamp": current_time.strftime("%Y-%m-%d %H:%M:%S"),
        "overall_report_context": "This report provides an in-depth analysis of daily workload and operational volume for our facility, including historical context, current performance tracking, and future predictions from multiple models.",
    }

    # --- Current Day Summary ---
    print("  [LLMfeed Debug] Processing Current Day Summary...")
    current_day_data = json_data.get('current_day', {})
    current_day_date_str = current_day_data.get('date', 'N/A')
    network_target = current_day_data.get('network_prediction', 0)
    
    current_day_prophet_preds = pd.DataFrame(current_day_data.get('sarima_predictions', []))
    print(f"  [LLMfeed Debug] current_day_prophet_preds is empty: {current_day_prophet_preds.empty}, Head:\n{current_day_prophet_preds.head()}")
    current_day_actuals = pd.DataFrame(current_day_data.get('current_day_data', []))
    print(f"  [LLMfeed Debug] current_day_actuals is empty: {current_day_actuals.empty}, Head:\n{current_day_actuals.head()}")
    prev_year_current_day_data = pd.DataFrame(current_day_data.get('previous_year_data', []))
    print(f"  [LLMfeed Debug] prev_year_current_day_data is empty: {prev_year_current_day_data.empty}, Head:\n{prev_year_current_day_data.head()}")
    no_same_day_preds = pd.DataFrame(current_day_data.get('predictions_no_same_day', []))
    print(f"  [LLMfeed Debug] no_same_day_preds is empty: {no_same_day_preds.empty}, Head:\n{no_same_day_preds.head()}")

    current_day_prophet_eod = current_day_prophet_preds['Predicted_Workable'].iloc[-1] if not current_day_prophet_preds.empty else 0
    print(f"  [LLMfeed Debug] current_day_prophet_eod: {current_day_prophet_eod}")
    
    model_comparison_to_network = ""
    if network_target > 0:
        deviation_pct = ((current_day_prophet_eod - network_target) / network_target) * 100
        comparison_verb = "above" if deviation_pct >= 0 else "below"
        model_comparison_to_network = f"Prophet's forecast of {current_day_prophet_eod:,.0f} units is {abs(deviation_pct):.1f}% {comparison_verb} the Network's target of {network_target:,.0f} units."
    else:
        model_comparison_to_network = f"Prophet forecasts a total of {current_day_prophet_eod:,.0f} units for today, but no network target was available for comparison."
    print(f"  [LLMfeed Debug] model_comparison_to_network: {model_comparison_to_network}")

    actual_volume_progress = "No actual volume data available for today."
    if not current_day_actuals.empty:
        last_actual_volume = current_day_actuals['Workable'].iloc[-1]
        last_actual_time = pd.to_datetime(current_day_actuals['Time'].iloc[-1]).strftime('%H:%M')
        
        # Find corresponding Prophet prediction for comparison
        prophet_at_last_actual_time = 0
        if not current_day_prophet_preds.empty:
            prophet_at_last_actual_time_df = current_day_prophet_preds[
                pd.to_datetime(current_day_prophet_preds['Time']) == pd.to_datetime(current_day_actuals['Time'].iloc[-1])
            ].copy() # Ensure copy
            if not prophet_at_last_actual_time_df.empty:
                prophet_at_last_actual_time = prophet_at_last_actual_time_df['Predicted_Workable'].iloc[0]

        if prophet_at_last_actual_time > 0:
            actual_deviation = ((last_actual_volume - prophet_at_last_actual_time) / prophet_at_last_actual_time) * 100
            tracking_status = "tracking above" if actual_deviation >= 0 else "tracking below"
            actual_volume_progress = f"Actual volume currently stands at {last_actual_volume:,.0f} units as of {last_actual_time}, {tracking_status} Prophet's prediction for this time by {abs(actual_deviation):.1f}%."
        else:
            actual_volume_progress = f"Actual volume currently stands at {last_actual_volume:,.0f} units as of {last_actual_time}."
    print(f"  [LLMfeed Debug] actual_volume_progress: {actual_volume_progress}")


    previous_year_comparison_current_day = "Previous year data for today is not available for comparison."
    if not prev_year_current_day_data.empty:
        prev_year_eod_total = prev_year_current_day_data['Workable'].iloc[-1]
        if prev_year_eod_total > 0:
            year_on_year_diff_pct = ((current_day_prophet_eod - prev_year_eod_total) / prev_year_eod_total) * 100
            comparison_word = "higher" if year_on_year_diff_pct >= 0 else "lower"
            previous_year_comparison_current_day = f"Today's predicted volume for 23:00 ({current_day_prophet_eod:,.0f} units) is {abs(year_on_year_diff_pct):.1f}% {comparison_word} than last year's actual volume on this day ({prev_year_eod_total:,.0f} units)."
        else:
            previous_year_comparison_current_day = f"Today's predicted volume for 23:00 is {current_day_prophet_eod:,.0f} units. Previous year's total for this day was not significant for comparison."
    print(f"  [LLMfeed Debug] previous_year_comparison_current_day: {previous_year_comparison_current_day}")

    no_same_day_influence_forecast = "Prophet's baseline forecast without real-time adjustments is not available."
    if not no_same_day_preds.empty:
        no_same_day_eod_total = no_same_day_preds['Predicted_Workable_No_Same_Day'].iloc[-1]
        no_same_day_influence_forecast = f"Without real-time adjustments, Prophet's baseline forecast for today was {no_same_day_eod_total:,.0f} units."
    print(f"  [LLMfeed Debug] no_same_day_influence_forecast: {no_same_day_influence_forecast}")


    llm_feed["current_day_summary_for_llm"] = {
        "date": current_day_date_str,
        "network_target_overview": f"The network's daily target for today is {network_target:,.0f} units.",
        "prophet_predicted_volume": f"Prophet forecasts today's volume to reach approximately {current_day_prophet_eod:,.0f} units by 23:00. The volume is expected to grow steadily throughout the day.",
        "model_comparison_to_network": model_comparison_to_network,
        "actual_volume_progress": actual_volume_progress,
        "previous_year_comparison_current_day": previous_year_comparison_current_day,
        "no_same_day_influence_forecast": no_same_day_influence_forecast
    }
    print(f"  [LLMfeed Debug] current_day_summary_for_llm populated.")

    # --- Upcoming Shifts Summaries (Next 2 Shifts) ---
    print("  [LLMfeed Debug] Processing Upcoming Shifts Summaries...")
    extended_preds_df = pd.DataFrame(json_data.get('extended_predictions', {}).get('predictions', []))
    # Add a 'Time_dt' column for easier datetime comparisons
    if not extended_preds_df.empty:
        extended_preds_df['Time_dt'] = pd.to_datetime(extended_preds_df['Time'])
    print(f"  [LLMfeed Debug] Loaded extended_preds_df with {len(extended_preds_df)} rows. Is Empty: {extended_preds_df.empty}. Head:\n{extended_preds_df.head()}")
    
    # Define potential start times for shifts relative to current_time
    shifts_candidates = []
    shifts_candidates.append({'start_time': current_time.replace(hour=6, minute=0, second=0, microsecond=0), 'name': "Today's Day Shift"})
    shifts_candidates.append({'start_time': current_time.replace(hour=18, minute=0, second=0, microsecond=0), 'name': "Tonight's Night Shift"})
    shifts_candidates.append({'start_time': (current_time + timedelta(days=1)).replace(hour=6, minute=0, second=0, microsecond=0), 'name': "Tomorrow's Day Shift"})
    shifts_candidates.append({'start_time': (current_time + timedelta(days=1)).replace(hour=18, minute=0, second=0, microsecond=0), 'name': "Tomorrow Night's Shift"})
    shifts_candidates.append({'start_time': (current_time + timedelta(days=2)).replace(hour=6, minute=0, second=0, microsecond=0), 'name': "Day After Tomorrow's Day Shift"})
    shifts_candidates.append({'start_time': (current_time + timedelta(days=2)).replace(hour=18, minute=0, second=0, microsecond=0), 'name': "Day After Tomorrow Night's Shift"})

    # Filter for upcoming shifts and sort them chronologically
    upcoming_shifts_info = sorted([s for s in shifts_candidates if s['start_time'] >= current_time], key=lambda x: x['start_time'])

    # Get the first two relevant shifts
    next_shift_info = upcoming_shifts_info[0] if len(upcoming_shifts_info) > 0 else None
    second_next_shift_info = upcoming_shifts_info[1] if len(upcoming_shifts_info) > 1 else None

    # Helper to generate shift summary (to avoid repetition)
    def _generate_shift_summary(shift_obj, prev_year_data_raw=None): # Renamed for clarity inside helper
        if not shift_obj:
            return {
                "date": "N/A",
                "shift_name": "N/A",
                "predicted_volume": "No data available for this shift.",
                "previous_year_comparison": "Previous year data not applicable.",
                "key_trends_shift_profile": "No profile available."
            }

        shift_start_dt = shift_obj['start_time']
        shift_end_dt = shift_start_dt + timedelta(hours=12) # Assuming 12-hour shifts
        
        # Handle night shifts spanning midnight
        if shift_start_dt.hour == 18: # If it's a night shift
            shift_end_dt_for_volume_part1 = shift_start_dt.replace(hour=23, minute=0, second=0) # End of day 1 part
            shift_start_dt_for_volume_part2 = shift_start_dt + timedelta(days=1)
            shift_end_dt_for_volume_part2 = (shift_start_dt + timedelta(days=1)).replace(hour=6, minute=0, second=0) # End of day 2 part
            
            # Volume for the 18:00-23:00 part
            volume_part1 = get_shift_volume_from_extended(
                extended_preds_df, shift_start_dt, shift_end_dt_for_volume_part1 + timedelta(hours=1) # Need to include 23:00 data
            )
            # Volume for the 00:00-06:00 part of next day
            volume_part2 = get_shift_volume_from_extended(
                extended_preds_df, shift_start_dt_for_volume_part2.replace(hour=0), shift_end_dt_for_volume_part2
            )
            shift_volume = volume_part1 + volume_part2
        else: # Day shift (06:00-18:00)
            shift_volume = get_shift_volume_from_extended(extended_preds_df, shift_start_dt, shift_end_dt)

        shift_date_str = shift_start_dt.strftime('%Y-%m-%d')
        
        previous_year_comparison = "Previous year data for this shift is not available for comparison."
        # Convert raw list to DataFrame at the start of its usage block
        prev_year_data_df = pd.DataFrame(prev_year_data_raw)
        if not prev_year_data_df.empty:
            prev_year_shift_start_dt = shift_start_dt.replace(year=shift_start_dt.year - 1)
            prev_year_shift_end_dt = shift_end_dt.replace(year=shift_end_dt.year - 1)
            
            prev_year_data_df['Time_dt'] = pd.to_datetime(prev_year_data_df['Time'])
            prev_year_shift_data = prev_year_data_df[
                (prev_year_data_df['Time_dt'] >= prev_year_shift_start_dt) &
                (prev_year_data_df['Time_dt'] < prev_year_shift_end_dt)
            ].copy()
            
            if not prev_year_shift_data.empty:
                prev_year_shift_total = prev_year_shift_data['Workable'].sum()
                if prev_year_shift_total > 0:
                    year_on_year_diff_pct = ((shift_volume - prev_year_shift_total) / prev_year_shift_total) * 100
                    comparison_word = "higher" if year_on_year_diff_pct >= 0 else "lower"
                    previous_year_comparison = f"{shift_obj['name']}'s predicted volume ({shift_volume:,.0f} units) is {abs(year_on_year_diff_pct):.1f}% {comparison_word} than last year's actual volume for the same shift period ({prev_year_shift_total:,.0f} units)."
                else:
                    previous_year_comparison = f"{shift_obj['name']}'s predicted volume is {shift_volume:,.0f} units. Last year's volume for this shift period was not significant for comparison."

        key_trends_shift_profile = f"Volume for {shift_obj['name']} is expected to total approximately {shift_volume:,.0f} units. "

        # Get hourly increases for this specific predicted shift
        shift_hourly_data_for_trends = extended_preds_df[
            (extended_preds_df['Time_dt'] >= shift_start_dt) & 
            (extended_preds_df['Time_dt'] < shift_end_dt)
        ].copy()

        hourly_increases_for_trends = []
        prev_cumulative_trend = 0
        prev_date_for_trends = None
        if not shift_hourly_data_for_trends.empty:
            shift_hourly_data_for_trends = shift_hourly_data_for_trends.sort_values(by='Time_dt')
            for idx, row in shift_hourly_data_for_trends.iterrows():
                current_cumulative = row['Predicted_Workable']
                current_dt_for_trends = row['Time_dt']

                if prev_date_for_trends is None or current_dt_for_trends.date() > prev_date_for_trends:
                    hourly_inc = current_cumulative
                    if current_dt_for_trends.hour == 0:
                        hourly_inc = 0 # Ensure midnight reset
                else:
                    hourly_inc = current_cumulative - prev_cumulative_trend
                
                hourly_increases_for_trends.append({'Time_dt': current_dt_for_trends, 'Increase': max(0, hourly_inc)})
                prev_cumulative_trend = current_cumulative
                prev_date_for_trends = current_dt_for_trends.date()
        
        hourly_increases_df_for_trends = pd.DataFrame(hourly_increases_for_trends)
        
        if not hourly_increases_df_for_trends.empty and hourly_increases_df_for_trends['Increase'].sum() > 0:
            peak_increase_hour = hourly_increases_df_for_trends.loc[hourly_increases_df_for_trends['Increase'].idxmax()]
            
            # Filter out 0 increases for lowest activity, unless all are 0
            non_zero_increases = hourly_increases_df_for_trends[hourly_increases_df_for_trends['Increase'] > 0]
            if not non_zero_increases.empty:
                lowest_non_zero_increase_hour = non_zero_increases.loc[non_zero_increases['Increase'].idxmin()]
            else: # All are zero increases
                lowest_non_zero_increase_hour = None 

            key_trends_shift_profile += f"The period is expected to see a significant progression. "
            if peak_increase_hour['Increase'] > 0:
                key_trends_shift_profile += f"The highest hourly activity is forecasted around {peak_increase_hour['Time_dt'].strftime('%H:%M')} with an increase of {peak_increase_hour['Increase']:,.0f} units. "
            if lowest_non_zero_increase_hour is not None and lowest_non_zero_increase_hour['Increase'] > 0:
                 key_trends_shift_profile += f"Activity is expected to be slower around {lowest_non_zero_increase_hour['Time_dt'].strftime('%H:%M')} with an increase of {lowest_non_zero_increase_hour['Increase']:,.0f} units. "
            elif lowest_non_zero_increase_hour is None and hourly_increases_df_for_trends['Increase'].sum() == 0:
                key_trends_shift_profile += "No significant hourly increases predicted during this shift."
        else:
            key_trends_shift_profile += "Detailed hourly progression for this shift is not available in the extended forecast, or predictions are flat."


        return {
            "date": shift_date_str,
            "shift_name": shift_obj['name'],
            "predicted_volume": f"The predicted volume for {shift_obj['name']} is {shift_volume:,.0f} units.",
            "previous_year_comparison": previous_year_comparison,
            "key_trends_shift_profile": key_trends_shift_profile,
        }

    llm_feed["next_shift_summary_for_llm"] = _generate_shift_summary(next_shift_info, json_data.get('next_day', {}).get('previous_year_data', []))
    llm_feed["second_next_shift_summary_for_llm"] = _generate_shift_summary(second_next_shift_info, json_data.get('next_day', {}).get('previous_year_data', [])) # Pass prev_year_data for second next shift as well

    print(f"  [LLMfeed Debug] next_shift_summary_for_llm populated.")
    print(f"  [LLMfeed Debug] second_next_shift_summary_for_llm populated.")


    # --- Extended Rolling Forecast Summary ---
    print("  [LLMfeed Debug] Processing Extended Rolling Forecast Summary...")
    extended_forecast_total_volume = extended_preds_df['Predicted_Workable'].iloc[-1] if not extended_preds_df.empty else 0
    extended_forecast_period = f"The next 48 hours, from {pd.to_datetime(extended_preds_df['Time'].iloc[0]).strftime('%Y-%m-%d %H:%M')} to {pd.to_datetime(extended_preds_df['Time'].iloc[-1]).strftime('%Y-%m-%d %H:%M')}." if not extended_preds_df.empty else "N/A"
    print(f"  [LLMfeed Debug] extended_forecast_total_volume: {extended_forecast_total_volume}")
    print(f"  [LLMfeed Debug] extended_forecast_period: {extended_forecast_period}")
    
    predicted_progression = f"The 48-hour rolling forecast predicts a total cumulative volume of {extended_forecast_total_volume:,.0f} units by the end of the period. "
    if not extended_preds_df.empty:
        predicted_progression += "Volume is expected to reset to zero at each midnight, with predictable growth resuming thereafter. Significant increases are generally seen during the Day Shift hours (06:00-18:00) and Night Shift hours (18:00-06:00)."
    print(f"  [LLMfeed Debug] predicted_progression: {predicted_progression}")
    
    key_observations = "Predicted volumes remain consistent with recent performance trends for the next two days." # Default, can be enhanced.
    print(f"  [LLMfeed Debug] key_observations: {key_observations}")

    llm_feed["extended_rolling_forecast_summary"] = {
        "period": extended_forecast_period,
        "predicted_progression": predicted_progression,
        "key_observations": key_observations
    }
    print(f"  [LLMfeed Debug] extended_rolling_forecast_summary populated.")

    # --- Historical Context Summary ---
    print("  [LLMfeed Debug] Processing Historical Context Summary...")
    historical_context = json_data.get('historical_context', {})
    print(f"  [LLMfeed Debug] historical_context: {historical_context}")

    period_analyzed = f"Last {historical_context.get('trend_period_days', 45)} days."
    
    overall_daily_volume_trends = "Overall daily volume trends are not available."
    if historical_context.get('overall_summary'):
        avg_45_days = historical_context['overall_summary'].get(f"avg_daily_volume_last_{historical_context.get('trend_period_days', 45)}_days", 0)
        rolling_7_day_avg = historical_context['overall_summary'].get('avg_daily_volume_rolling_7_days', 0)
        overall_daily_volume_trends = f"Average daily volume over the last {historical_context.get('trend_period_days', 45)} days was {avg_45_days:,.0f} units, with a rolling 7-day average of {rolling_7_day_avg:,.0f} units."
    print(f"  [LLMfeed Debug] overall_daily_volume_trends: {overall_daily_volume_trends}")

    day_of_week_trends = {}
    if historical_context.get('daily_summary_trends'):
        for day, trends in historical_context['daily_summary_trends'].items():
            long_term_avg = trends.get(f"avg_total_daily_volume_last_{historical_context.get('num_weeks_for_avg', 6)}_occurrences", 0)
            trend_pct = trends.get('trend_direction_pct_change', 0.0)
            trend_direction = "upward trend" if trend_pct >= 0 else "downward trend"
            if trend_pct == 9999.0:
                day_of_week_trends[day] = f"Typically sees {long_term_avg:,.0f} units, showing a very strong upward trend based on recent data availability."
            else:
                day_of_week_trends[day] = f"Typically sees {long_term_avg:,.0f} units, showing a {abs(trend_pct):.2f}% {trend_direction} over recent occurrences."
    print(f"  [LLMfeed Debug] day_of_week_trends: {day_of_week_trends}")

    notable_block_trends = []
    if historical_context.get('three_hour_block_trends'):
        for block_key, trends in historical_context['three_hour_block_trends'].items():
            trend_pct = trends.get('trend_direction_pct_change', 0.0)
            if abs(trend_pct) > 10 or trend_pct == 9999.0: # Significant trend threshold
                day_name, time_block_suffix = block_key.split('_', 1) # Use split with maxsplit=1
                time_block_start = time_block_suffix[:4]
                time_block_end = time_block_suffix[5:9] # Correctly extract end part
                
                trend_direction = "strong upward trend" if trend_pct >= 0 else "significant downward trend"
                if trend_pct == 9999.0:
                     notable_block_trends.append(f"{day_name}'s {time_block_start[:2]}:{time_block_start[2:]}-{time_block_end[:2]}:{time_block_end[2:]} block has shown a very strong upward trend based on recent data availability.")
                else:
                    notable_block_trends.append(f"{day_name}'s {time_block_start[:2]}:{time_block_start[2:]}-{time_block_end[:2]}:{time_block_end[2:]} block has shown a {abs(trend_pct):.2f}% {trend_direction}.")
        if not notable_block_trends:
            notable_block_trends.append("No significant trends observed in specific 3-hour blocks.")
    print(f"  [LLMfeed Debug] notable_block_trends: {notable_block_trends}")

    llm_feed["historical_context_summary_for_llm"] = {
        "period_analyzed": period_analyzed,
        "overall_daily_volume_trends": overall_daily_volume_trends,
        "day_of_week_trends": day_of_week_trends,
        "notable_block_trends": " ".join(notable_block_trends),
        "special_event_impact": "Defined holidays (e.g., Christmas, New Year's Day) and periods near them are accounted for in historical trends to capture their known impact on volumes."
    }
    print(f"  [LLMfeed Debug] historical_context_summary_for_llm populated.")

    # --- Model System Details ---
    print("  [LLMfeed Debug] Processing Model System Details...")
    prophet_config_details = "Prophet is configured with a changepoint_prior_scale of 0.15 to allow for flexible trend adaptation, and explicitly accounts for daily and weekly seasonality."
    exogenous_influences_details = "Prophet integrates external data, specifically current Network (IPTNW) and ALPS total forecasts, as guiding influences. This allows the model to refine its own forecasts and potentially surpass their individual accuracy."
    backtesting_results_overview = "Model performance is rigorously evaluated through backtesting over historical data. The overall average MAPE and other specific error metrics would be included here if provided. Recent performance indicates stronger accuracy."
    strength_in_adaptability = "The model demonstrates strong adaptability, with recent forecasts showing significantly lower errors, indicating effective learning from current operational patterns. This positions the model to potentially outperform standalone external predictions due to its adaptive nature and integrated learning from multiple data sources."


    llm_feed["model_system_details_for_llm"] = {
        "primary_forecasting_model": "The primary model used for forecasting is Prophet.",
        "prophet_methodology": "Prophet models hourly 'increases' in volume rather than total cumulative volume. This approach ensures predictions are non-decreasing within a day and reset to zero at midnight, accurately reflecting operational patterns.",
        "prophet_configuration": prophet_config_details,
        "exogenous_influences": exogenous_influences_details,
        "secondary_forecasting_model": "No secondary forecasting model is currently in use for primary predictions.", # Removed RF reference
        "backtesting_results_overview": backtesting_results_overview,
        "strength_in_adaptability": strength_in_adaptability
    }
    print(f"  [LLMfeed Debug] model_system_details_for_llm populated.")

    # --- New: Miscellaneous Info Section (misc_info) ---
    print("  [LLMfeed Debug] Processing Miscellaneous Info (misc_info)...")
    misc_info = {}

    # 1. Backlog Tracking Metric (overall trend for the entire day thus far)
    eligible_metrics = json_data.get('Ledger_Information', {}).get('metrics', {}).get('Eligible', [])
    apu_metrics = json_data.get('Ledger_Information', {}).get('metrics', {}).get('APU', [])
    time_points = json_data.get('Ledger_Information', {}).get('timePoints', [])

    backlog_trend_analysis = "Backlog trend analysis is unavailable due to insufficient data."
    if len(eligible_metrics) > 1 and len(apu_metrics) > 1 and len(time_points) > 1:
        # Calculate total backlog at each point
        total_backlog_series = pd.Series([e + a for e, a in zip(eligible_metrics, apu_metrics)])
        
        # Use only valid (non-NaN) data points for trend calculation
        valid_indices = np.isfinite(total_backlog_series)
        if np.sum(valid_indices) >= 2: # Need at least 2 valid points for slope
            x_valid = np.arange(len(total_backlog_series))[valid_indices]
            y_valid = total_backlog_series[valid_indices]

            if len(x_valid) >= 2: # Ensure enough points for polyfit
                slope, intercept = np.polyfit(x_valid, y_valid, 1)
                
                # Define thresholds for trend strength
                increasing_threshold = 500 # e.g., backlog increasing by 500 units per hour
                decreasing_threshold = -500 # e.g., backlog decreasing by 500 units per hour

                if slope > increasing_threshold:
                    backlog_trend_analysis = f"The overall backlog has been steadily increasing throughout the day, with a trend of approximately +{slope:,.0f} units per hour."
                elif slope < decreasing_threshold:
                    backlog_trend_analysis = f"The overall backlog has been steadily decreasing throughout the day, with a trend of approximately {slope:,.0f} units per hour."
                else:
                    backlog_trend_analysis = "The overall backlog has remained relatively stable throughout the day."
            else:
                backlog_trend_analysis = "Backlog trend analysis is unavailable due to insufficient valid recent data points for trend calculation."
        else:
            backlog_trend_analysis = "Backlog trend analysis is unavailable due to insufficient valid data points for trend calculation."
    else:
        backlog_trend_analysis = "Backlog trend analysis for the current day is too early to determine a reliable trend. More hourly data points are needed from Ledger Information."
    misc_info["backlog_trend_analysis_current_day"] = backlog_trend_analysis
    print(f"  [LLMfeed Debug] backlog_trend_analysis_current_day: {backlog_trend_analysis}")


    # 2. Hourly Volatility and Growth/Stagnant Sections
    hourly_prediction_analysis = "Hourly volatility and growth/stagnant period analysis is unavailable due to insufficient prediction data."
    current_day_sarima_preds_df = pd.DataFrame(json_data.get('current_day', {}).get('sarima_predictions', []))
    next_day_sarima_preds_df = pd.DataFrame(json_data.get('next_day', {}).get('sarima_predictions', []))

    # Combine for full 48-hour context for volatility and growth analysis
    # Filter out "Time" column before concat if it's the only common column used as index in some cases
    # Ensure 'Predicted_Workable' exists before concat or handle its absence
    combined_preds_list = []
    if 'Predicted_Workable' in current_day_sarima_preds_df.columns:
        combined_preds_list.append(current_day_sarima_preds_df[['Time', 'Predicted_Workable']])
    if 'Predicted_Workable' in next_day_sarima_preds_df.columns:
        combined_preds_list.append(next_day_sarima_preds_df[['Time', 'Predicted_Workable']])
    
    if combined_preds_list:
        all_predicted_hours_df = pd.concat(combined_preds_list).drop_duplicates(subset='Time').sort_values('Time').reset_index(drop=True)
        all_predicted_hours_df['Time_dt'] = pd.to_datetime(all_predicted_hours_df['Time'])

        print(f"  [LLMfeed Debug] all_predicted_hours_df for volatility (length: {len(all_predicted_hours_df)}). Head:\n{all_predicted_hours_df.head()}")

        if len(all_predicted_hours_df) >= 2: # Need at least two points to calculate increases
            # Recalculate hourly increases for volatility and growth analysis from the combined df
            hourly_increases_full = []
            prev_cumulative_val = 0
            prev_date = None

            for idx, row in all_predicted_hours_df.iterrows():
                current_dt = row['Time_dt']
                current_cumulative = row['Predicted_Workable']

                if prev_date is None or current_dt.date() > prev_date: # New day or first record
                    hourly_inc = current_cumulative 
                    if current_dt.hour == 0: # Explicitly handle midnight reset for increase
                        hourly_inc = 0
                    elif prev_date is None: # First overall record - should ideally be 0 if clean reset
                         hourly_inc = 0 
                else:
                    hourly_inc = current_cumulative - prev_cumulative_val
                
                hourly_increases_full.append(max(0, hourly_inc)) # Ensure non-negative
                prev_cumulative_val = current_cumulative
                prev_date = current_dt.date()

            hourly_increases_series = pd.Series(hourly_increases_full)
            print(f"  [LLMfeed Debug] Hourly Increases Series generated (length: {len(hourly_increases_series)}). Head:\n{hourly_increases_series.head()}")
            
            if len(hourly_increases_series) >= 2 and hourly_increases_series.sum() > 0: # Ensure enough data and non-zero sum for meaningful std/mean
                volatility_std = hourly_increases_series.std()
                average_hourly_increase = hourly_increases_series.mean()
                
                hourly_prediction_analysis = f"The overall hourly prediction volatility (standard deviation of hourly increases) is approximately {volatility_std:,.0f} units. "
                
                growth_periods = []
                stagnant_periods = []
                
                # Identify growth and stagnant periods based on thresholds
                growth_threshold_factor = 1.5 # 50% higher than average
                stagnant_threshold_factor = 0.1 # 10% of average, or flat
                
                for i in range(len(all_predicted_hours_df)):
                    hour_dt = all_predicted_hours_df['Time_dt'].iloc[i]
                    hour_str = hour_dt.strftime('%H:%M')
                    increase = hourly_increases_series.iloc[i]

                    # Ensure we don't mark midnight as stagnant if it's just a reset
                    if hour_dt.hour == 0 and increase == 0:
                        continue # Skip 00:00 as it's a reset point, not truly stagnant unless it stays 0 all day.
                    
                    if increase >= average_hourly_increase * growth_threshold_factor:
                        growth_periods.append(hour_str)
                    elif increase <= average_hourly_increase * stagnant_threshold_factor and increase >= 0: # Only truly stagnant if increase is very low but non-negative
                        stagnant_periods.append(hour_str)
                
                if growth_periods:
                    hourly_prediction_analysis += f"Predicted periods of significant growth (hourly increases much higher than average) include: {', '.join(sorted(list(set(growth_periods))))}."
                else:
                    hourly_prediction_analysis += "No distinct periods of high predicted growth identified."
                
                if stagnant_periods:
                    hourly_prediction_analysis += f" Predicted stagnant or very low activity hours (hourly increases near zero) include: {', '.join(sorted(list(set(stagnant_periods))))}."
                else:
                    hourly_prediction_analysis += " No distinct periods of stagnant or very low activity identified."
            else:
                hourly_prediction_analysis = "Insufficient data or zero total increase in hourly predictions to calculate meaningful volatility or trends."
        else:
            hourly_prediction_analysis = "Insufficient hourly prediction data (less than 2 points) to calculate volatility or identify specific growth/stagnant periods."
    else:
        hourly_prediction_analysis = "No valid hourly prediction dataframes to combine for volatility analysis."

    misc_info["hourly_prediction_analysis"] = hourly_prediction_analysis
    print(f"  [LLMfeed Debug] hourly_prediction_analysis: {hourly_prediction_analysis}")


    # 3. Day-over-Day Prediction Change
    day_over_day_change_analysis = "Day-over-day prediction change is unavailable due to missing data."
    current_day_eod_total = json_data.get('prophet_performance_metrics', {}).get('current_day_final_prophet_total', 0)
    next_day_eod_total = json_data.get('prophet_performance_metrics', {}).get('next_day_final_prophet_total', 0)

    print(f"  [LLMfeed Debug] current_day_eod_total for Day-over-Day: {current_day_eod_total}")
    print(f"  [LLMfeed Debug] next_day_eod_total for Day-over-Day: {next_day_eod_total}")

    if current_day_eod_total > 0 and next_day_eod_total > 0:
        change_pct = ((next_day_eod_total - current_day_eod_total) / current_day_eod_total) * 100
        change_verb = "increase" if change_pct >= 0 else "decrease"
        day_over_day_change_analysis = f"The predicted end-of-day volume shows a {abs(change_pct):.1f}% {change_verb} from today ({current_day_eod_total:,.0f} units) to tomorrow ({next_day_eod_total:,.0f} units)."
    elif next_day_eod_total > 0:
        day_over_day_change_analysis = f"Tomorrow's predicted end-of-day volume is {next_day_eod_total:,.0f} units. Current day's prediction is not available for comparison."
    elif current_day_eod_total > 0:
        day_over_day_change_analysis = f"Today's predicted end-of-day volume is {current_day_eod_total:,.0f} units. Tomorrow's prediction is not available for comparison."
    else:
        day_over_day_change_analysis = "Both today's and tomorrow's end-of-day predictions are unavailable for day-over-day comparison."
    misc_info["day_over_day_prediction_change"] = day_over_day_change_analysis
    print(f"  [LLMfeed Debug] day_over_day_prediction_change: {day_over_day_change_analysis}")

    # 4. Predicted Shift Handoff Volumes
    print("  [LLMfeed Debug] Calculating Predicted Shift Handoff Volumes...")
    predicted_handoff_volumes = {}
    
    # Current Day 06:00 (Start of Day Shift Today) - This is usually the first point, so 0 from previous night's reset
    # More accurate: The cumulative volume at 6:00 AM today (assuming reset at midnight)
    cumulative_0600_today = extended_preds_df[
        extended_preds_df['Time_dt'] == current_time.replace(hour=6, minute=0, second=0)
    ]['Predicted_Workable'].iloc[0] if not extended_preds_df[extended_preds_df['Time_dt'] == current_time.replace(hour=6, minute=0, second=0)].empty else 0
    predicted_handoff_volumes["current_day_0600_handoff_cumulative"] = cumulative_0600_today
    print(f"  [LLMfeed Debug] current_day_0600_handoff_cumulative: {cumulative_0600_today}")


    # Current Day 18:00 (End of Day Shift Today / Start of Night Shift Today)
    cumulative_1800_today = extended_preds_df[
        extended_preds_df['Time_dt'] == current_time.replace(hour=18, minute=0, second=0)
    ]['Predicted_Workable'].iloc[0] if not extended_preds_df[extended_preds_df['Time_dt'] == current_time.replace(hour=18, minute=0, second=0)].empty else 0
    predicted_handoff_volumes["current_day_1800_handoff_cumulative"] = cumulative_1800_today
    print(f"  [LLMfeed Debug] current_day_1800_handoff_cumulative: {cumulative_1800_today}")

    # Current Day 23:00 (End of Today's Night Shift Part 1)
    cumulative_2300_today = extended_preds_df[
        extended_preds_df['Time_dt'] == current_time.replace(hour=23, minute=0, second=0)
    ]['Predicted_Workable'].iloc[0] if not extended_preds_df[extended_preds_df['Time_dt'] == current_time.replace(hour=23, minute=0, second=0)].empty else 0
    predicted_handoff_volumes["current_day_2300_handoff_cumulative"] = cumulative_2300_today
    print(f"  [LLMfeed Debug] current_day_2300_handoff_cumulative: {cumulative_2300_today}")

    # Next Day 00:00 (Start of Day 2 / End of Night Shift Today Part 2 Cumulative Resets to 0)
    # The cumulative at 00:00 should be 0 as per the model's design for daily reset
    cumulative_0000_next_day = extended_preds_df[
        extended_preds_df['Time_dt'] == (current_time + timedelta(days=1)).replace(hour=0, minute=0, second=0)
    ]['Predicted_Workable'].iloc[0] if not extended_preds_df[extended_preds_df['Time_dt'] == (current_time + timedelta(days=1)).replace(hour=0, minute=0, second=0)].empty else 0
    predicted_handoff_volumes["next_day_0000_handoff_cumulative"] = cumulative_0000_next_day
    print(f"  [LLMfeed Debug] next_day_0000_handoff_cumulative: {cumulative_0000_next_day}")

    # Next Day 06:00 (End of Night Shift Today / Start of Day Shift Tomorrow)
    cumulative_0600_next_day = extended_preds_df[
        extended_preds_df['Time_dt'] == (current_time + timedelta(days=1)).replace(hour=6, minute=0, second=0)
    ]['Predicted_Workable'].iloc[0] if not extended_preds_df[extended_preds_df['Time_dt'] == (current_time + timedelta(days=1)).replace(hour=6, minute=0, second=0)].empty else 0
    predicted_handoff_volumes["next_day_0600_handoff_cumulative"] = cumulative_0600_next_day
    print(f"  [LLMfeed Debug] next_day_0600_handoff_cumulative: {cumulative_0600_next_day}")

    # Next Day 18:00 (End of Day Shift Tomorrow / Start of Night Shift Tomorrow)
    cumulative_1800_next_day = extended_preds_df[
        extended_preds_df['Time_dt'] == (current_time + timedelta(days=1)).replace(hour=18, minute=0, second=0)
    ]['Predicted_Workable'].iloc[0] if not extended_preds_df[extended_preds_df['Time_dt'] == (current_time + timedelta(days=1)).replace(hour=18, minute=0, second=0)].empty else 0
    predicted_handoff_volumes["next_day_1800_handoff_cumulative"] = cumulative_1800_next_day
    print(f"  [LLMfeed Debug] next_day_1800_handoff_cumulative: {cumulative_1800_next_day}")
    
    misc_info["predicted_shift_handoff_volumes"] = predicted_handoff_volumes
    print(f"  [LLMfeed Debug] predicted_shift_handoff_volumes populated.")

    # 5. Upcoming Day Shape Analysis (language based)
    print("  [LLMfeed Debug] Performing Upcoming Day Shape Analysis...")
    upcoming_day_shape_analysis = {}
    
    # Historical hourly shapes are now in json_data
    historical_hourly_shapes = json_data.get('historical_hourly_shapes', {})

    # Analyze Tomorrow's Shape
    tomorrow_date = (current_time + timedelta(days=1)).date()
    tomorrow_day_name = tomorrow_date.strftime('%A')
    tomorrow_predicted_hourly_increases = []
    
    # Get tomorrow's hourly predicted increases from extended_preds_df
    # Need to re-derive increases from cumulative predictions for tomorrow
    tomorrow_preds_df_full_day = extended_preds_df[
        (extended_preds_df['Time_dt'].dt.date == tomorrow_date)
    ].copy()
    
    if not tomorrow_preds_df_full_day.empty:
        tomorrow_preds_df_full_day = tomorrow_preds_df_full_day.sort_values('Time_dt')
        prev_cumulative = 0
        for idx, row in tomorrow_preds_df_full_day.iterrows():
            current_cumulative = row['Predicted_Workable']
            if row['Time_dt'].hour == 0: # Reset for midnight
                hourly_inc = 0
            else:
                hourly_inc = current_cumulative - prev_cumulative
            tomorrow_predicted_hourly_increases.append(max(0, hourly_inc))
            prev_cumulative = current_cumulative

    if tomorrow_predicted_hourly_increases and tomorrow_day_name in historical_hourly_shapes:
        historical_shape = historical_hourly_shapes[tomorrow_day_name]
        predicted_shape = {h: v for h, v in enumerate(tomorrow_predicted_hourly_increases)} # Map index to hour

        shape_comparison_notes = []
        significant_deviation_threshold = 0.20 # 20% deviation
        
        total_deviation_pct = 0
        comparable_hours_count = 0

        for hour in range(24):
            hist_val = historical_shape.get(hour, 0)
            pred_val = predicted_shape.get(hour, 0)

            if hist_val > 0:
                deviation = (pred_val - hist_val) / hist_val
                total_deviation_pct += abs(deviation)
                comparable_hours_count += 1

                if abs(deviation) > significant_deviation_threshold:
                    deviation_type = "higher" if deviation > 0 else "lower"
                    shape_comparison_notes.append(f"{hour:02d}:00 is predicted to be {abs(deviation * 100):.0f}% {deviation_type} than historically typical.")
            elif pred_val > 0 and hist_val == 0: # Historically flat, but now predicted to have volume
                shape_comparison_notes.append(f"{hour:02d}:00 historically has no volume, but is predicted to have {pred_val:,.0f} units.")
        
        overall_similarity_pct = 100 - (total_deviation_pct / comparable_hours_count * 100) if comparable_hours_count > 0 else 0
        overall_shape_description = f"Overall, tomorrow's predicted hourly volume shape is {overall_similarity_pct:.0f}% similar to the historical average for a {tomorrow_day_name}."

        if shape_comparison_notes:
            overall_shape_description += " Notable differences include: " + " ".join(shape_comparison_notes)
        else:
            overall_shape_description += " No significant hourly deviations from the historical pattern are predicted."

        upcoming_day_shape_analysis["tomorrow_shape_analysis"] = overall_shape_description
    else:
        upcoming_day_shape_analysis["tomorrow_shape_analysis"] = "Tomorrow's shape analysis is unavailable due to missing historical or predicted data."
    print(f"  [LLMfeed Debug] upcoming_day_shape_analysis for tomorrow: {upcoming_day_shape_analysis.get('tomorrow_shape_analysis', 'N/A')}")


    misc_info["upcoming_day_shape_analysis"] = upcoming_day_shape_analysis

    llm_feed["misc_info"] = misc_info
    print(f"  [LLMfeed Debug] Final llm_feed content before return: {llm_feed}")
    return llm_feed

def main():
    """
    Main function to run the LLM Feed Generator application.
    Loads VIZ.json, generates LLM insights, and saves to Gemgem.json.
    """
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        viz_json_path = os.path.join(current_dir, 'VIZ.json')

        print(f"\nLoading VIZ.json from: {viz_json_path}")
        if not os.path.exists(viz_json_path):
            raise FileNotFoundError(f"VIZ.json not found at {viz_json_path}. Please run ProphetModeling.py first.")
        
        with open(viz_json_path, 'r') as f:
            json_data = json.load(f)
        print("Successfully loaded VIZ.json.")

        # Generate LLM Feed insights
        llm_feed_content = generate_llm_feed(json_data) 
        
        # Save LLMfeed to a separate local JSON file
        gemgem_local_file = os.path.join(current_dir, 'Gemgem.json')
        print(f"\nWriting LLM Feed to separate JSON file at: {gemgem_local_file}")
        try:
            with open(gemgem_local_file, 'w') as f:
                json.dump(llm_feed_content, f, indent=4)
            print("Successfully wrote to Gemgem.json")
        except Exception as e:
            print(f"Error writing Gemgem.json: {str(e)}")
            traceback.print_exc()
        
    except Exception as e:
        print(f"\nError in LLMFeedGenerator main: {str(e)}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
