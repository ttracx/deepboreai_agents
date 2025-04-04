import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def process_data(raw_data):
    """
    Process the raw drilling data to prepare it for ML models.
    
    Args:
        raw_data (dict): Raw drilling data from WITSML source
        
    Returns:
        dict: Processed data ready for ML model input
    """
    if raw_data is None:
        logger.warning("No raw data provided for processing")
        return None
    
    try:
        # Create a copy of the data to avoid modifying the original
        processed_data = raw_data.copy()
        
        # Calculate derived features
        # Mechanical Specific Energy (MSE) = (WOB*4*π*RPM*Torque)/(ROP*bit diameter^2)
        # Simplified formula if MSE not provided:
        if 'MSE' not in processed_data:
            # Assuming 8.5" bit diameter as a default
            bit_diameter = 8.5
            wob = processed_data.get('WOB', 0)
            rpm = processed_data.get('RPM', 0)
            torque = processed_data.get('Torque', 0)
            rop = max(0.1, processed_data.get('ROP', 0.1))  # Avoid division by zero
            
            processed_data['MSE'] = (4 * np.pi * wob * rpm * torque) / (rop * bit_diameter**2)
        
        # D-exponent calculation for formation pressure indication
        # d_exp = log(ROP / (RPM * WOB)) / log(bit diameter)
        try:
            bit_diameter = 8.5  # Assumed bit diameter
            wob = max(0.1, processed_data.get('WOB', 0.1))  # Avoid log(0)
            rpm = max(0.1, processed_data.get('RPM', 0.1))
            rop = max(0.1, processed_data.get('ROP', 0.1))
            
            processed_data['d_exp'] = np.log10(rop / (rpm * wob)) / np.log10(bit_diameter)
        except Exception as e:
            logger.warning(f"Could not calculate d-exponent: {str(e)}")
            processed_data['d_exp'] = 0
        
        # Calculate torque-drag indicators
        # Simplified model: hookload vs. theoretical weight (TVD * string weight)
        # Pickup drag = measured hookload / theoretical weight
        try:
            # Simplified calculation with assumptions
            if 'Hook_Load' in processed_data and 'depth' in processed_data:
                # Assume 30 lbs/ft average drillstring weight
                avg_string_weight = 30  # lbs/ft
                tvd = processed_data['depth'] * 0.95  # Simplified TVD calculation
                theoretical_weight = tvd * avg_string_weight / 1000  # in klbs
                
                processed_data['Drag_Factor'] = processed_data['Hook_Load'] / max(1, theoretical_weight)
            else:
                processed_data['Drag_Factor'] = 1.0
        except Exception as e:
            logger.warning(f"Could not calculate drag factor: {str(e)}")
            processed_data['Drag_Factor'] = 1.0
        
        # Calculate differential pressure
        # Simplified: hydrostatic pressure - pore pressure
        try:
            # Assume pore pressure gradient of 0.465 psi/ft (normal pressure)
            pore_pressure_gradient = 0.465
            ecd = processed_data.get('ECD', 12.5)
            depth = processed_data.get('depth', 10000)
            
            hydrostatic_pressure = ecd * 0.052 * depth
            pore_pressure = pore_pressure_gradient * depth
            processed_data['differential_pressure'] = hydrostatic_pressure - pore_pressure
        except Exception as e:
            logger.warning(f"Could not calculate differential pressure: {str(e)}")
            processed_data['differential_pressure'] = 500  # Default value
        
        # Calculate hole cleaning indicators
        try:
            # Simplified hole cleaning efficiency calculation
            # Higher ratio of ECD to mud weight indicates worse hole cleaning
            mud_weight = ecd - 0.2  # Approximation of mud weight from ECD
            processed_data['hole_cleaning_index'] = ecd / max(mud_weight, 8.0)
        except Exception as e:
            logger.warning(f"Could not calculate hole cleaning index: {str(e)}")
            processed_data['hole_cleaning_index'] = 1.0
        
        # Process time series data if available
        if 'time_series' in processed_data:
            # Calculate rolling average and standard deviation for key parameters
            time_series = processed_data['time_series']
            
            # Convert to pandas DataFrame for easier manipulation
            time_series_df = pd.DataFrame(time_series)
            
            # Calculate rolling statistics (window of 10 points)
            for param in ['WOB', 'ROP', 'RPM', 'Torque', 'SPP', 'Flow_Rate']:
                if param in time_series_df.columns:
                    # Calculate 10-point rolling average
                    rolling_avg = time_series_df[param].rolling(window=10, min_periods=1).mean()
                    time_series_df[f'{param}_rolling_avg'] = rolling_avg
                    
                    # Calculate 10-point rolling standard deviation
                    rolling_std = time_series_df[param].rolling(window=10, min_periods=1).std()
                    time_series_df[f'{param}_rolling_std'] = rolling_std
                    
                    # Calculate rate of change (derivative)
                    time_series_df[f'{param}_rate'] = time_series_df[param].diff()
            
            # Add rolling statistics to the processed data
            processed_data['time_series'] = time_series_df.to_dict('list')
            
            # Extract latest statistics for model input
            latest_idx = -1  # Last data point
            for param in ['WOB', 'ROP', 'RPM', 'Torque', 'SPP', 'Flow_Rate']:
                if f'{param}_rolling_avg' in time_series_df.columns:
                    processed_data[f'{param}_avg'] = time_series_df[f'{param}_rolling_avg'].iloc[latest_idx]
                    processed_data[f'{param}_std'] = time_series_df[f'{param}_rolling_std'].iloc[latest_idx]
                    processed_data[f'{param}_rate'] = time_series_df[f'{param}_rate'].iloc[latest_idx]
        
        logger.info("Data processing completed successfully")
        return processed_data
    
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        return raw_data  # Return the original data if processing fails
