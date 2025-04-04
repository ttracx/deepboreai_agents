"""
Data processor for the drilling prediction application.

This module provides functions to process raw drilling data from WITSML sources
and prepare it for use in ML models and visualization.
"""

import logging
from datetime import datetime

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
    try:
        # If raw_data is None, return empty data
        if not raw_data:
            logger.error("No data to process")
            return {}
        
        # Log incoming data for debugging
        logger.debug(f"Processing data with {len(raw_data)} parameters")
        
        # Make a copy to avoid modifying the original
        processed_data = raw_data.copy()
        
        # Ensure all required fields are present with default values if needed
        required_fields = [
            'depth', 'WOB', 'ROP', 'RPM', 'Torque', 'SPP', 'Flow_Rate',
            'ECD', 'hook_load', 'MSE', 'drag_factor', 'differential_pressure',
            'hole_cleaning_index'
        ]
        
        for field in required_fields:
            if field not in processed_data:
                processed_data[field] = 0
                logger.warning(f"Missing field '{field}' in data, using default value 0")
        
        # Ensure all change fields are present
        change_fields = ['WOB_change', 'ROP_change', 'RPM_change', 'Torque_change', 'SPP_change', 'Flow_Rate_change']
        for field in change_fields:
            if field not in processed_data:
                base_field = field.split('_')[0]
                processed_data[field] = 0
                logger.warning(f"Missing field '{field}' in data, using default value 0")
        
        # Ensure statistical fields are present
        stat_fields = [
            'wob_avg', 'wob_std', 'wob_rate',
            'rop_avg', 'rop_std', 'rop_rate',
            'rpm_avg', 'rpm_std', 'rpm_rate',
            'torque_avg', 'torque_std', 'torque_rate',
            'spp_avg', 'spp_std', 'spp_rate',
            'flow_rate_avg', 'flow_rate_std', 'flow_rate_rate'
        ]
        
        for field in stat_fields:
            if field not in processed_data:
                processed_data[field] = 0
                logger.debug(f"Missing statistical field '{field}' in data, using default value 0")
        
        # Calculate additional derived features if needed
        
        # Mechanical Specific Energy (MSE) if not already present
        if processed_data['MSE'] == 0 and processed_data['WOB'] > 0 and processed_data['RPM'] > 0 and processed_data['ROP'] > 0:
            bit_diameter = 8.5  # Assumed bit diameter in inches
            wob = processed_data['WOB']  # klbs
            rpm = processed_data['RPM']
            torque = processed_data['Torque']  # kft-lbs
            rop = processed_data['ROP']  # ft/hr
            
            # Convert to consistent units and calculate MSE
            processed_data['MSE'] = 4 * wob * 1000 / (3.14159 * (bit_diameter ** 2)) + \
                                   (480 * rpm * torque) / (3.14159 * (bit_diameter ** 2) * rop)
            
            logger.debug("Calculated MSE from basic parameters")
        
        # Calculate hole cleaning index if not present
        if processed_data['hole_cleaning_index'] == 0:
            # Simplified model based on flow rate, RPM, and ROP
            flow_rate = processed_data['Flow_Rate']
            rpm = processed_data['RPM']
            rop = processed_data['ROP']
            
            # Higher flow rate and RPM improve hole cleaning, higher ROP reduces it
            if flow_rate > 0 and rpm > 0:
                processed_data['hole_cleaning_index'] = min(1.0, max(0.1, 
                    (0.5 + 0.3 * (flow_rate / 800) + 0.2 * (rpm / 150) - 0.1 * (rop / 50))
                ))
                logger.debug("Calculated hole cleaning index")
        
        # Calculate differential pressure if not present
        if processed_data['differential_pressure'] == 0:
            # Simplified model using ECD and depth
            ecd = processed_data['ECD']
            depth = processed_data['depth']
            
            if ecd > 0 and depth > 0:
                # Simple hydrostatic pressure calculation
                mud_weight = ecd  # ppg
                hydrostatic_pressure = 0.052 * mud_weight * depth  # psi
                
                # Assume a pore pressure gradient of 0.45 psi/ft (typical)
                pore_pressure = 0.45 * depth
                
                # Differential pressure is the difference
                processed_data['differential_pressure'] = max(0, hydrostatic_pressure - pore_pressure)
                logger.debug("Calculated differential pressure")
        
        # Calculate drag factor if not present
        if processed_data['drag_factor'] == 0:
            # Simplified model based on hook load and depth
            hook_load = processed_data['hook_load']
            depth = processed_data['depth']
            
            if hook_load > 0 and depth > 0:
                # Very simplified model - in reality this would be more complex
                drill_string_weight = depth * 0.02  # Assume 20 lbs per foot of drill string
                theoretical_hook_load = drill_string_weight
                
                if theoretical_hook_load > 0:
                    processed_data['drag_factor'] = min(1.0, max(0.1, hook_load / theoretical_hook_load))
                    logger.debug("Calculated drag factor")
        
        # Add a timestamp if not present
        if 'timestamp' not in processed_data:
            processed_data['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        logger.info("Data processing completed successfully")
        return processed_data
    
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        return raw_data  # Return the original data if processing fails